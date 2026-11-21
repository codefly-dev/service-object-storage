package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

const (
	deleteBatch = 500
	erasePasses = 64
)

// Maintenance is the prefix-wide capability of one base: enumerate, audit,
// export and erase, across partitions. It is built from the gateway client at
// a composition root and from nothing else. No Binding yields it, a Store has
// no method that returns it, and it resolves no Binding: what it may touch is
// decided by who was handed it.
//
// It is confined to its base like a Store. It is not confined by the store:
// it holds the same client, and a second gateway placement on its own
// credential is what would separate the two there.
type Maintenance struct {
	client storagev0.ObjectStorageClient
	codec  objectkey.Codec
}

// NewMaintenance builds the maintenance capability of a base.
func NewMaintenance(client storagev0.ObjectStorageClient, base string) (*Maintenance, error) {
	if isNil(client) {
		return nil, errors.New("object storage client is required")
	}
	codec, err := objectkey.New(base)
	if err != nil {
		return nil, err
	}
	return &Maintenance{client: client, codec: codec}, nil
}

// Entry is one key under the base: an object the convention wrote, or a key
// it did not.
type Entry struct {
	// Key is the logical address. It is zero when Problem is set.
	Key  objectkey.Key
	Info Info
	// Physical is the key as the gateway holds it, for a report.
	Physical string
	// Problem is why the key does not read back, and nil for an object.
	Problem error
}

func (m *Maintenance) walk(ctx context.Context, prefix string, visit func(Entry) error) error {
	token := ""
	for {
		listed, err := m.client.List(ctx, &storagev0.ListRequest{Prefix: prefix, PageToken: token})
		if err != nil {
			return translate(err)
		}
		for _, object := range listed.GetObjects() {
			entry := Entry{Physical: object.GetKey()}
			if key, err := m.codec.Parse(object.GetKey()); err != nil {
				entry.Problem = err
				entry.Info.Size = object.GetSize()
			} else {
				entry.Key, entry.Info = key, infoOf(key, object)
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
		if token = listed.GetNextPageToken(); token == "" {
			return nil
		}
	}
}

// Walk visits every key under the base.
func (m *Maintenance) Walk(ctx context.Context, visit func(Entry) error) error {
	return m.walk(ctx, m.codec.BasePrefix(), visit)
}

// children lists the next segment under a prefix.
func (m *Maintenance) children(ctx context.Context, prefix string) ([]string, error) {
	var found []string
	token := ""
	for {
		listed, err := m.client.List(ctx, &storagev0.ListRequest{Prefix: prefix, Delimiter: "/", PageToken: token})
		if err != nil {
			return nil, translate(err)
		}
		found = append(found, listed.GetCommonPrefixes()...)
		if token = listed.GetNextPageToken(); token == "" {
			sort.Strings(found)
			return found, nil
		}
	}
}

// Partitions lists the partitions that hold anything.
func (m *Maintenance) Partitions(ctx context.Context) ([]string, error) {
	prefixes, err := m.children(ctx, m.codec.BasePrefix())
	if err != nil {
		return nil, err
	}
	partitions := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		partition, _, _, _, err := m.codec.ParsePrefix(prefix)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partition)
	}
	return partitions, nil
}

// Scopes lists the scopes of a partition that hold anything.
func (m *Maintenance) Scopes(ctx context.Context, partition string) ([]string, error) {
	prefix, err := m.codec.PartitionPrefix(partition)
	if err != nil {
		return nil, err
	}
	prefixes, err := m.children(ctx, prefix)
	if err != nil {
		return nil, err
	}
	scopes := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		_, scope, _, _, err := m.codec.ParsePrefix(prefix)
		if err != nil {
			return nil, err
		}
		scopes = append(scopes, scope)
	}
	return scopes, nil
}

// Principals lists the principals that hold anything in a scope.
func (m *Maintenance) Principals(ctx context.Context, partition, scope string) ([]string, error) {
	prefix, err := m.codec.ScopePrefix(partition, scope)
	if err != nil {
		return nil, err
	}
	prefixes, err := m.children(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var principals []string
	for _, prefix := range prefixes {
		_, _, slot, _, err := m.codec.ParsePrefix(prefix)
		if err != nil {
			return nil, err
		}
		if reference := slot.Reference(); reference != "" {
			principals = append(principals, reference)
		}
	}
	return principals, nil
}

// Erasure is what an erase removed and what it could not.
type Erasure struct {
	Objects int
	Bytes   int64
	// Failed are the keys the store refused to delete, with its reason.
	Failed map[string]string
	// VersionsMayRemain is set when the store keeps versions: a delete there
	// leaves the bytes behind a marker, and this gateway cannot list or purge
	// them. Erasure is then complete only once the bucket's own lifecycle
	// rule expires noncurrent versions.
	VersionsMayRemain bool
}

func (erasure *Erasure) add(other Erasure) {
	erasure.Objects += other.Objects
	erasure.Bytes += other.Bytes
	for key, reason := range other.Failed {
		if erasure.Failed == nil {
			erasure.Failed = map[string]string{}
		}
		erasure.Failed[key] = reason
	}
	erasure.VersionsMayRemain = erasure.VersionsMayRemain || other.VersionsMayRemain
}

// erase deletes everything under a prefix, convention-written or not: what
// sits under a principal's prefix is that principal's whatever it is called.
func (m *Maintenance) erase(ctx context.Context, prefix string, keep func(Entry) bool) (Erasure, error) {
	var erasure Erasure
	capabilities, err := m.client.Capabilities(ctx, &storagev0.CapabilitiesRequest{})
	if err != nil {
		return erasure, translate(err)
	}
	erasure.VersionsMayRemain = capabilities.GetVersions()
	var batch []string
	sizes := map[string]int64{}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		result, err := m.client.DeleteMany(ctx, &storagev0.DeleteManyRequest{Keys: batch})
		if err != nil {
			return translate(err)
		}
		refused := map[string]string{}
		for _, entry := range result.GetEntries() {
			if entry.GetError() != "" {
				refused[entry.GetKey()] = entry.GetError()
			}
		}
		for _, key := range batch {
			if reason, failed := refused[key]; failed {
				if erasure.Failed == nil {
					erasure.Failed = map[string]string{}
				}
				erasure.Failed[key] = reason
				continue
			}
			erasure.Objects++
			erasure.Bytes += sizes[key]
		}
		batch, sizes = batch[:0], map[string]int64{}
		return nil
	}
	// Collect first, delete after: a listing is not deleted from under itself.
	// Then again, until a pass finds nothing: a store may hide the keys under
	// a key that is itself an object, and show them once it is gone. The
	// convention never writes such a pair; what else sits under the prefix
	// may be one.
	for pass := 0; pass < erasePasses; pass++ {
		var doomed []Entry
		if err := m.walk(ctx, prefix, func(entry Entry) error {
			if _, failed := erasure.Failed[entry.Physical]; !failed && (keep == nil || !keep(entry)) {
				doomed = append(doomed, entry)
			}
			return nil
		}); err != nil {
			return erasure, err
		}
		if len(doomed) == 0 {
			return erasure, nil
		}
		for _, entry := range doomed {
			batch = append(batch, entry.Physical)
			sizes[entry.Physical] = entry.Info.Size
			if len(batch) == deleteBatch {
				if err := flush(); err != nil {
					return erasure, err
				}
			}
		}
		if err := flush(); err != nil {
			return erasure, err
		}
	}
	return erasure, fmt.Errorf("object store: the prefix still held objects after %d passes", erasePasses)
}

// ErasePrincipal removes every object of one principal, in every scope of a
// partition: what is under the principal's own prefixes, uploads in flight
// included. It does not touch the shared slot or the content slot, which hold
// nothing of a principal's by name.
func (m *Maintenance) ErasePrincipal(ctx context.Context, partition, principal string) (Erasure, error) {
	var total Erasure
	scopes, err := m.Scopes(ctx, partition)
	if err != nil {
		return total, err
	}
	for _, scope := range scopes {
		prefix, err := m.codec.SlotPrefix(partition, scope, objectkey.Principal(principal))
		if err != nil {
			return total, err
		}
		erased, err := m.erase(ctx, prefix, nil)
		total.add(erased)
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// EraseScope removes everything of one scope: every principal's objects, the
// shared ones and the content.
func (m *Maintenance) EraseScope(ctx context.Context, partition, scope string) (Erasure, error) {
	prefix, err := m.codec.ScopePrefix(partition, scope)
	if err != nil {
		return Erasure{}, err
	}
	return m.erase(ctx, prefix, nil)
}

// ErasePartition removes everything of one partition.
func (m *Maintenance) ErasePartition(ctx context.Context, partition string) (Erasure, error) {
	prefix, err := m.codec.PartitionPrefix(partition)
	if err != nil {
		return Erasure{}, err
	}
	return m.erase(ctx, prefix, nil)
}

// ExportPrincipal visits every object of one principal in a partition, with a
// function that opens it. Uploads in flight are not objects and are skipped.
func (m *Maintenance) ExportPrincipal(ctx context.Context, partition, principal string, visit func(Info, func(context.Context) (io.ReadCloser, error)) error) error {
	scopes, err := m.Scopes(ctx, partition)
	if err != nil {
		return err
	}
	for _, scope := range scopes {
		prefix, err := m.codec.SlotPrefix(partition, scope, objectkey.Principal(principal))
		if err != nil {
			return err
		}
		if err := m.walk(ctx, prefix, func(entry Entry) error {
			if entry.Problem != nil || entry.Key.Staging {
				return nil
			}
			return visit(entry.Info, func(ctx context.Context) (io.ReadCloser, error) {
				body, _, err := open(ctx, m.client, entry.Physical, entry.Key)
				return body, err
			})
		}); err != nil {
			return err
		}
	}
	return nil
}

// SweepStaging removes uploads in flight older than an age: what a writer
// that died between its upload and its copy left behind.
func (m *Maintenance) SweepStaging(ctx context.Context, olderThan time.Duration) (Erasure, error) {
	cutoff := time.Now().Add(-olderThan)
	return m.erase(ctx, m.codec.BasePrefix(), func(entry Entry) bool {
		return entry.Problem != nil || !entry.Key.Staging || entry.Info.Modified.IsZero() || entry.Info.Modified.After(cutoff)
	})
}

// SweepContent removes the content of a scope that nothing references any
// more. Only the application knows what references a digest, so it answers:
// referenced reports whether to keep one. Content younger than the grace is
// kept unasked, because a writer stores the bytes before it records the
// reference.
func (m *Maintenance) SweepContent(ctx context.Context, partition, scope string, grace time.Duration, referenced func(digest string) (bool, error)) (Erasure, error) {
	prefix, err := m.codec.SlotPrefix(partition, scope, objectkey.Content())
	if err != nil {
		return Erasure{}, err
	}
	cutoff := time.Now().Add(-grace)
	var asked error
	erasure, err := m.erase(ctx, prefix, func(entry Entry) bool {
		if asked != nil || entry.Problem != nil || entry.Info.Modified.IsZero() || entry.Info.Modified.After(cutoff) {
			return true
		}
		keep, err := referenced(entry.Key.Name)
		if err != nil {
			asked = err
			return true
		}
		return keep
	})
	if asked != nil {
		return Erasure{}, asked
	}
	return erasure, err
}

// Known is what an audit holds a base's layout to. A nil function accepts
// every value.
type Known struct {
	Partition func(partition string) bool
	Scope     func(partition, scope string) bool
	Principal func(partition, scope, principal string) bool
}

// Finding is one key an audit refuses.
type Finding struct {
	Physical string
	Reason   string
}

func (finding Finding) String() string { return finding.Physical + ": " + finding.Reason }

// Audit checks that every key under the base was written by the convention
// and sits under a partition, a scope and a principal the application knows.
// It returns what does not; an empty result is the gate.
func (m *Maintenance) Audit(ctx context.Context, known Known) ([]Finding, error) {
	var findings []Finding
	err := m.Walk(ctx, func(entry Entry) error {
		key := entry.Key
		switch {
		case entry.Problem != nil:
			findings = append(findings, Finding{entry.Physical, entry.Problem.Error()})
		case known.Partition != nil && !known.Partition(key.Partition):
			findings = append(findings, Finding{entry.Physical, fmt.Sprintf("unknown partition %q", key.Partition)})
		case known.Scope != nil && !known.Scope(key.Partition, key.Scope):
			findings = append(findings, Finding{entry.Physical, fmt.Sprintf("unknown scope %q in partition %q", key.Scope, key.Partition)})
		case known.Principal != nil && key.Slot.Reference() != "" && !known.Principal(key.Partition, key.Scope, key.Slot.Reference()):
			findings = append(findings, Finding{entry.Physical, fmt.Sprintf("unknown principal %q in scope %q", key.Slot.Reference(), key.Scope)})
		}
		return nil
	})
	return findings, err
}

// Strays lists what the gateway holds outside every declared base: top-level
// objects, and top-level prefixes that are no base's. It is how a layout that
// predates the convention is found, and how two stores are shown not to
// share a prefix.
func Strays(ctx context.Context, client storagev0.ObjectStorageClient, bases ...string) ([]Finding, error) {
	if isNil(client) {
		return nil, errors.New("object storage client is required")
	}
	declared := map[string]bool{}
	for _, base := range bases {
		codec, err := objectkey.New(base)
		if err != nil {
			return nil, err
		}
		declared[codec.BasePrefix()] = true
	}
	var findings []Finding
	token := ""
	for {
		listed, err := client.List(ctx, &storagev0.ListRequest{Delimiter: "/", PageToken: token})
		if err != nil {
			return nil, translate(err)
		}
		for _, object := range listed.GetObjects() {
			findings = append(findings, Finding{object.GetKey(), "an object outside every base"})
		}
		for _, prefix := range listed.GetCommonPrefixes() {
			if !declared[prefix] {
				findings = append(findings, Finding{prefix, "a prefix that is no declared base"})
			}
		}
		if token = listed.GetNextPageToken(); token == "" {
			sort.Slice(findings, func(i, j int) bool { return findings[i].Physical < findings[j].Physical })
			return findings, nil
		}
	}
}
