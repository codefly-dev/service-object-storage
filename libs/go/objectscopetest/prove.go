// Package objectscopetest proves, against a real gateway, that a confined
// object store holds the confinement model for every Binding: it seeds random
// objects across partitions, scopes and principals through the gateway client
// itself, then acts and asserts through the store under proof, and compares
// everything the store returns, lists, changes or refuses with a model that
// knows nothing about keys.
//
// A module calls Prove from its own suite, on the gateway of a disposable run,
// with the base and the store constructor it uses in production. RequireLayout
// is the other half, for a store that already holds objects: every key under
// the base was written by the convention and sits where the module knows.
package objectscopetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	objectstorage "github.com/codefly-dev/service-object-storage/libs/go"
	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

// Confined is the store under proof. *objectstorage.Store is one.
type Confined interface {
	Put(ctx context.Context, ref objectstorage.Ref, body io.Reader, options objectstorage.PutOptions) (objectstorage.Info, error)
	PutContent(ctx context.Context, scope string, body io.Reader, options objectstorage.PutOptions) (objectstorage.Info, error)
	Get(ctx context.Context, ref objectstorage.Ref) (io.ReadCloser, objectstorage.Info, error)
	Head(ctx context.Context, ref objectstorage.Ref) (objectstorage.Info, error)
	Delete(ctx context.Context, ref objectstorage.Ref) error
	Copy(ctx context.Context, from, to objectstorage.Ref, options objectstorage.CopyOptions) (objectstorage.Info, error)
	List(ctx context.Context, in objectstorage.Ref, options objectstorage.ListOptions) (objectstorage.Page, error)
	ListScope(ctx context.Context, scope string, options objectstorage.ListOptions) (objectstorage.Page, error)
	PresignGet(ctx context.Context, ref objectstorage.Ref, lifetime time.Duration) (objectstorage.Presigned, error)
	PresignPut(ctx context.Context, ref objectstorage.Ref, lifetime time.Duration) (objectstorage.Presigned, error)
}

// Proof names the gateway, the base and the store of one proof.
type Proof struct {
	// Client is the gateway client. The proof seeds and inspects with it,
	// outside any binding; it never asserts confinement through it.
	Client storagev0.ObjectStorageClient
	// Base is the store's base. The proof writes only under partitions it
	// names itself, and erases them when it ends.
	Base string
	// Open builds the store under proof on a binder. Nil means
	// objectstorage.NewStore on Client and Base.
	Open func(objectstorage.Binder) (Confined, error)
	// Fetch, when set, follows a presigned GET and returns the body: the
	// proof then also checks what a presigned URL actually serves.
	Fetch func(url string) ([]byte, error)
	// Objects and Rounds size the proof; zero means 160 and 300. Seed zero
	// means a fresh one, which is logged.
	Objects, Rounds int
	Seed            uint64
}

// Reporter is the part of testing.TB the proof uses.
type Reporter interface {
	Helper()
	Logf(format string, arguments ...any)
	Fatalf(format string, arguments ...any)
	Cleanup(func())
}

type bindingKey struct{}

// The universe a proof draws from. The identifiers are chosen to be each
// other's near misses: one is a prefix of another, two differ by case, and
// some hold the bytes the key convention gives a meaning to.
var (
	scopeIDs     = []string{"inst-1", "inst-10", "inst-1/human.amy", "_shared", "Inst-1"}
	principalIDs = []string{"human:amy", "human:amy2", "human:AMY", "human:a/b", "agent:inst-1", "service:_shared", "human:zoë"}
	names        = []string{"a", "b", "a/b", "a/b/c", "b.txt", "A", "_shared", "_staging/x", "x y", "é", "a_2fb", "human.amy/a", "..."}
)

type prover struct {
	t          Reporter
	ctx        context.Context
	proof      Proof
	random     *rand.Rand
	codec      objectkey.Codec
	maintain   *objectstorage.Maintenance
	store      Confined
	partitions []string
	model      map[objectkey.Key]string

	reads, accepted, refused int
}

// Prove fails t on the first Binding for which the store returns, lists or
// changes an object the model says it may not, or withholds one it must
// return.
func Prove(t Reporter, proof Proof) {
	t.Helper()
	if proof.Client == nil || proof.Base == "" {
		t.Fatalf("objectscopetest: the gateway client and the base are required")
	}
	if proof.Objects == 0 {
		proof.Objects = 160
	}
	if proof.Rounds == 0 {
		proof.Rounds = 300
	}
	if proof.Seed == 0 {
		proof.Seed = rand.Uint64() | 1
	}
	p := &prover{t: t, proof: proof, model: map[objectkey.Key]string{}}
	p.random = rand.New(rand.NewPCG(proof.Seed, proof.Seed>>1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	p.ctx = ctx

	codec, err := objectkey.New(proof.Base)
	if err != nil {
		t.Fatalf("objectscopetest: %v", err)
	}
	p.codec = codec
	if p.maintain, err = objectstorage.NewMaintenance(proof.Client, proof.Base); err != nil {
		t.Fatalf("objectscopetest: %v", err)
	}
	// The proof's partitions are its own: two runs on one base do not meet.
	run := fmt.Sprintf("%08x", uint32(proof.Seed))
	p.partitions = []string{"proof-" + run + "-a", "proof-" + run + "-a/b", "proof-" + run + "-ab"}
	t.Cleanup(func() {
		for _, partition := range p.partitions {
			_, _ = p.maintain.ErasePartition(context.Background(), partition)
		}
	})

	binder := objectstorage.BinderFunc(func(ctx context.Context) (objectstorage.Binding, error) {
		binding, ok := ctx.Value(bindingKey{}).(objectstorage.Binding)
		if !ok {
			return objectstorage.Binding{}, errors.New("this call carries no binding")
		}
		return binding, nil
	})
	if proof.Open == nil {
		p.store, err = objectstorage.NewStore(proof.Client, proof.Base, binder)
	} else {
		p.store, err = proof.Open(binder)
	}
	if err != nil {
		t.Fatalf("objectscopetest: open the store under proof: %v", err)
	}

	p.seed()
	p.compare("seeding")
	p.unbound()
	for round := range proof.Rounds {
		p.round(round)
	}
	p.unbound()
	p.compare("the last round")
	t.Logf("objectscopetest: base %s held over %d bindings: %d reads and listings, %d writes accepted, %d calls refused, %d objects at the end (seed %d)",
		proof.Base, proof.Rounds, p.reads, p.accepted, p.refused, len(p.model), proof.Seed)
}

func (p *prover) pick(from []string) string { return from[p.random.IntN(len(from))] }

func digestOf(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func (p *prover) randomKey() (objectkey.Key, string) {
	key := objectkey.Key{Partition: p.pick(p.partitions), Scope: p.pick(scopeIDs), Name: p.pick(names)}
	body := fmt.Sprintf("seed-%d", p.random.Uint32())
	switch p.random.IntN(8) {
	case 0, 1:
		key.Slot = objectkey.Shared()
	case 2:
		key.Slot, key.Name = objectkey.Content(), digestOf(body)
	default:
		key.Slot = objectkey.Principal(p.pick(principalIDs))
	}
	return key, body
}

func (p *prover) randomBinding() objectstorage.Binding {
	binding := objectstorage.Binding{
		Partition: p.pick(p.partitions), Principal: p.pick(principalIDs),
		Mode: objectstorage.Strict, Access: objectstorage.ReadWrite, Actor: "proof",
	}
	if p.random.IntN(3) == 0 {
		binding.Mode = objectstorage.Privileged
	}
	if p.random.IntN(4) == 0 {
		binding.Access = objectstorage.Read
	}
	binding.Shared = objectstorage.Access(p.random.IntN(3))
	order := p.random.Perm(len(scopeIDs))
	for _, index := range order[:1+p.random.IntN(3)] {
		binding.Reach = append(binding.Reach, scopeIDs[index])
	}
	return binding
}

// The model. It is written from the design, not from the library: these four
// functions are everything the proof believes.

func reaches(binding objectstorage.Binding, key objectkey.Key) bool {
	return key.Partition == binding.Partition && slices.Contains(binding.Reach, key.Scope)
}

func mayRead(binding objectstorage.Binding, key objectkey.Key) bool {
	return reaches(binding, key) && (binding.Mode == objectstorage.Privileged ||
		key.Slot.IsShared() || key.Slot.IsContent() || key.Slot.Reference() == binding.Principal)
}

func mayWriteShared(binding objectstorage.Binding) bool {
	return binding.Shared == objectstorage.ReadWrite || binding.Shared == 0 && binding.Mode == objectstorage.Privileged
}

func mayWrite(binding objectstorage.Binding, key objectkey.Key) bool {
	switch {
	case !reaches(binding, key) || binding.Access != objectstorage.ReadWrite || key.Slot.IsContent():
		return false
	case key.Slot.IsShared():
		return mayWriteShared(binding)
	default:
		return binding.Mode == objectstorage.Privileged || key.Slot.Reference() == binding.Principal
	}
}

// raw writes one object with the gateway client, outside any store.
func (p *prover) raw(key objectkey.Key, body string) {
	p.t.Helper()
	physical, err := p.codec.Key(key)
	if err != nil {
		p.t.Fatalf("objectscopetest: seed %+v: %v", key, err)
	}
	stream, err := p.proof.Client.Put(p.ctx)
	if err == nil {
		err = stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: &storagev0.PutHeader{Key: physical, TotalSize: int64(len(body))}}})
	}
	if err == nil {
		err = stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: []byte(body)}})
	}
	if err == nil {
		_, err = stream.CloseAndRecv()
	}
	if err != nil {
		p.t.Fatalf("objectscopetest: seed %s: %v", physical, err)
	}
}

func (p *prover) seed() {
	p.t.Helper()
	for range p.proof.Objects {
		key, body := p.randomKey()
		p.raw(key, body)
		p.model[key] = body
	}
}

// ref addresses a key the way a caller of the binding would: by scope, slot
// and name. The partition is not something a reference can say.
func (p *prover) ref(binding objectstorage.Binding, key objectkey.Key) objectstorage.Ref {
	ref := objectstorage.Ref{Scope: key.Scope, Principal: key.Slot.Reference(), Shared: key.Slot.IsShared(), Content: key.Slot.IsContent(), Name: key.Name}
	// Half the time, the short forms a handler writes.
	if p.random.IntN(2) == 0 {
		if ref.Principal == binding.Principal {
			ref.Principal = ""
		}
		if len(binding.Reach) == 1 && binding.Reach[0] == ref.Scope {
			ref.Scope = ""
		}
	}
	return ref
}

// target picks the object a call is aimed at, as seen from the binding's
// partition. Aiming at random would mostly miss by the scope alone; so half
// the time it is the nearest miss there is, an object the binding reaches and
// may not read or may not write.
func (p *prover) target(binding objectstorage.Binding) objectkey.Key {
	keys := p.keys()
	if len(keys) == 0 {
		key, _ := p.randomKey()
		key.Partition = binding.Partition
		return key
	}
	if p.random.IntN(2) == 0 {
		start := p.random.IntN(len(keys))
		for offset := range keys {
			candidate := keys[(start+offset)%len(keys)]
			if reaches(binding, candidate) && (!mayRead(binding, candidate) || !mayWrite(binding, candidate)) {
				return candidate
			}
		}
	}
	candidate := keys[p.random.IntN(len(keys))]
	// Another partition's object, addressed from here, is the object of the
	// same scope, slot and name in this partition, which may not exist.
	candidate.Partition = binding.Partition
	return candidate
}

func (p *prover) keys() []objectkey.Key {
	keys := make([]objectkey.Key, 0, len(p.model))
	for key := range p.model {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
	return keys
}

func refused(err error) bool {
	return errors.Is(err, objectstorage.ErrUnauthorized) || errors.Is(err, objectstorage.ErrUnauthenticated)
}

func (p *prover) listing(describe string, list func(objectstorage.ListOptions) (objectstorage.Page, error)) ([]string, error) {
	var seen []string
	options := objectstorage.ListOptions{Limit: 1 + p.random.IntN(40)}
	for {
		page, err := list(options)
		if err != nil {
			return nil, err
		}
		for _, object := range page.Objects {
			seen = append(seen, fmt.Sprintf("%s (%d bytes)", fullRef(object.Ref), object.Size))
		}
		if options.Token = page.Next; options.Token == "" {
			p.reads++
			return seen, nil
		}
	}
}

func fullRef(ref objectstorage.Ref) string {
	return fmt.Sprintf("%q %q shared=%v content=%v %q", ref.Scope, ref.Principal, ref.Shared, ref.Content, ref.Name)
}

func (p *prover) expected(binding objectstorage.Binding, within func(objectkey.Key) bool) []string {
	var want []string
	for key, body := range p.model {
		if mayRead(binding, key) && within(key) {
			want = append(want, fmt.Sprintf("%s (%d bytes)", fullRef(objectstorage.Ref{
				Scope: key.Scope, Principal: key.Slot.Reference(), Shared: key.Slot.IsShared(), Content: key.Slot.IsContent(), Name: key.Name,
			}), len(body)))
		}
	}
	return want
}

func (p *prover) same(what string, got, want []string) {
	p.t.Helper()
	sort.Strings(got)
	sort.Strings(want)
	if slices.Equal(got, want) {
		return
	}
	var extra, missing []string
	for _, entry := range got {
		if !slices.Contains(want, entry) {
			extra = append(extra, entry)
		}
	}
	for _, entry := range want {
		if !slices.Contains(got, entry) {
			missing = append(missing, entry)
		}
	}
	p.t.Fatalf("objectscopetest: %s: the store and the model disagree (seed %d)\n returned and not allowed: %v\n allowed and not returned: %v",
		what, p.proof.Seed, extra, missing)
}

// lists checks every listing the binding can ask for in one scope.
func (p *prover) lists(ctx context.Context, describe string, binding objectstorage.Binding) {
	p.t.Helper()
	scope := p.pick(scopeIDs)
	inReach := slices.Contains(binding.Reach, scope)
	slots := []objectkey.Slot{objectkey.Principal(binding.Principal), objectkey.Shared(), objectkey.Content(), objectkey.Principal(p.pick(principalIDs))}
	for _, slot := range slots {
		in := objectstorage.Ref{Scope: scope, Principal: slot.Reference(), Shared: slot.IsShared(), Content: slot.IsContent()}
		what := fmt.Sprintf("%s: list %s", describe, fullRef(in))
		seen, err := p.listing(what, func(options objectstorage.ListOptions) (objectstorage.Page, error) {
			return p.store.List(ctx, in, options)
		})
		allowed := inReach && (binding.Mode == objectstorage.Privileged || slot.IsShared() || slot.Reference() == binding.Principal)
		switch {
		case !allowed && err == nil:
			p.t.Fatalf("objectscopetest: %s: listed a slot the binding may not list, and got %v (seed %d)", what, seen, p.proof.Seed)
		case !allowed && !refused(err):
			p.t.Fatalf("objectscopetest: %s: %v, want a refusal", what, err)
		case !allowed:
			p.refused++
		case err != nil:
			p.t.Fatalf("objectscopetest: %s: %v", what, err)
		default:
			p.same(what, seen, p.expected(binding, func(key objectkey.Key) bool { return key.Scope == scope && key.Slot == slot }))
		}
	}
	// A directory of the binding's own slot: whole segments, and no sibling.
	if inReach {
		in := objectstorage.Own("a").In(scope)
		what := fmt.Sprintf("%s: list %s", describe, fullRef(in))
		seen, err := p.listing(what, func(options objectstorage.ListOptions) (objectstorage.Page, error) {
			return p.store.List(ctx, in, options)
		})
		if err != nil {
			p.t.Fatalf("objectscopetest: %s: %v", what, err)
		}
		p.same(what, seen, p.expected(binding, func(key objectkey.Key) bool {
			return key.Scope == scope && key.Slot == objectkey.Principal(binding.Principal) && strings.HasPrefix(key.Name, "a/")
		}))
	}
	what := fmt.Sprintf("%s: list the whole scope %q", describe, scope)
	seen, err := p.listing(what, func(options objectstorage.ListOptions) (objectstorage.Page, error) {
		return p.store.ListScope(ctx, scope, options)
	})
	switch allowed := inReach && binding.Mode == objectstorage.Privileged; {
	case !allowed && err == nil:
		p.t.Fatalf("objectscopetest: %s: listed a scope the binding may not list, and got %v (seed %d)", what, seen, p.proof.Seed)
	case !allowed && !refused(err):
		p.t.Fatalf("objectscopetest: %s: %v, want a refusal", what, err)
	case !allowed:
		p.refused++
	case err != nil:
		p.t.Fatalf("objectscopetest: %s: %v", what, err)
	default:
		p.same(what, seen, p.expected(binding, func(key objectkey.Key) bool { return key.Scope == scope }))
	}
}

// reads checks Get, Head and PresignGet on one object.
func (p *prover) readsOne(ctx context.Context, describe string, binding objectstorage.Binding) {
	p.t.Helper()
	key := p.target(binding)
	ref := p.ref(binding, key)
	want, exists := p.model[key]
	allowed := mayRead(binding, key)
	what := fmt.Sprintf("%s: read %s", describe, fullRef(ref))

	var got []byte
	body, _, err := p.store.Get(ctx, ref)
	if err == nil {
		got, err = io.ReadAll(body)
		_ = body.Close()
	}
	p.verdict(what+" (Get)", err, allowed, exists)
	if allowed && exists && string(got) != want {
		p.t.Fatalf("objectscopetest: %s: Get returned %q, the object holds %q (seed %d)", what, got, want, p.proof.Seed)
	}
	info, err := p.store.Head(ctx, ref)
	p.verdict(what+" (Head)", err, allowed, exists)
	if allowed && exists && info.Size != int64(len(want)) {
		p.t.Fatalf("objectscopetest: %s: Head reports %d bytes, the object holds %d (seed %d)", what, info.Size, len(want), p.proof.Seed)
	}
	signed, err := p.store.PresignGet(ctx, ref, time.Minute)
	// Signing does not look at the store: a URL for an object that is not
	// there is still a URL, and it serves nothing.
	p.verdict(what+" (PresignGet)", err, allowed, true)
	if allowed && exists && p.proof.Fetch != nil {
		served, err := p.proof.Fetch(signed.URL)
		if err != nil || string(served) != want {
			p.t.Fatalf("objectscopetest: %s: the presigned URL served %q, %v; the object holds %q (seed %d)", what, served, err, want, p.proof.Seed)
		}
	}
	p.reads++
}

// verdict holds one call to the model: refused when it must be, served when
// it must be, and not found only when there is nothing there.
func (p *prover) verdict(what string, err error, allowed, exists bool) {
	p.t.Helper()
	switch {
	case !allowed && err == nil:
		p.t.Fatalf("objectscopetest: %s: succeeded on an object the binding may not touch (seed %d)", what, p.proof.Seed)
	case !allowed && !refused(err):
		p.t.Fatalf("objectscopetest: %s: %v, want a refusal (seed %d)", what, err, p.proof.Seed)
	case !allowed:
		p.refused++
	case exists && err != nil:
		p.t.Fatalf("objectscopetest: %s: %v, on an object the binding may reach (seed %d)", what, err, p.proof.Seed)
	case !exists && err == nil:
		p.t.Fatalf("objectscopetest: %s: succeeded on an object that does not exist (seed %d)", what, p.proof.Seed)
	case !exists && !errors.Is(err, objectstorage.ErrNotFound):
		p.t.Fatalf("objectscopetest: %s: %v, want not found (seed %d)", what, err, p.proof.Seed)
	}
}

func (p *prover) round(round int) {
	p.t.Helper()
	binding := p.randomBinding()
	ctx := context.WithValue(p.ctx, bindingKey{}, binding)
	describe := fmt.Sprintf("round %d, %s %s as %s in %q reaching %q (shared: %s)",
		round, binding.Mode, binding.Access, binding.Principal, binding.Partition, binding.Reach, binding.Shared)

	p.lists(ctx, describe, binding)
	p.readsOne(ctx, describe, binding)

	body := fmt.Sprintf("round-%d-%d", round, p.random.Uint32())
	switch p.random.IntN(6) {
	case 0, 1: // write one object: the caller's own, or one step away from it
		key := objectkey.Key{Partition: binding.Partition, Scope: p.pick(binding.Reach), Slot: objectkey.Principal(binding.Principal), Name: p.pick(names)}
		switch p.random.IntN(6) {
		case 1:
			key.Slot = objectkey.Principal(p.pick(principalIDs))
		case 2:
			key.Slot = objectkey.Shared()
		case 3:
			key.Scope = p.pick(scopeIDs)
		case 4:
			key.Slot, key.Name = objectkey.Content(), digestOf(body)
		case 5:
			key = p.target(binding)
		}
		ref := p.ref(binding, key)
		_, err := p.store.Put(ctx, ref, strings.NewReader(body), objectstorage.PutOptions{})
		p.wrote(fmt.Sprintf("%s: put %s", describe, fullRef(ref)), err, mayWrite(binding, key))
		if mayWrite(binding, key) {
			p.model[key] = body
		}
	case 2: // delete one object, whoever's it is
		key := p.target(binding)
		ref := p.ref(binding, key)
		err := p.store.Delete(ctx, ref)
		p.wrote(fmt.Sprintf("%s: delete %s", describe, fullRef(ref)), err, mayWrite(binding, key))
		if mayWrite(binding, key) {
			delete(p.model, key)
		}
	case 3: // copy: a read of the source and a write of the destination
		source := p.target(binding)
		destination := objectkey.Key{Partition: binding.Partition, Scope: p.pick(binding.Reach), Slot: objectkey.Principal(binding.Principal), Name: p.pick(names)}
		if p.random.IntN(4) == 0 {
			destination = p.target(binding)
		}
		if destination == source {
			break // a store refuses to copy an object onto itself
		}
		from, to := p.ref(binding, source), p.ref(binding, destination)
		what := fmt.Sprintf("%s: copy %s to %s", describe, fullRef(from), fullRef(to))
		_, err := p.store.Copy(ctx, from, to, objectstorage.CopyOptions{})
		content, exists := p.model[source]
		switch allowed := mayRead(binding, source) && mayWrite(binding, destination); {
		case allowed && !exists:
			if !errors.Is(err, objectstorage.ErrNotFound) {
				p.t.Fatalf("objectscopetest: %s: %v, want not found (seed %d)", what, err, p.proof.Seed)
			}
		default:
			p.wrote(what, err, allowed)
			if allowed {
				p.model[destination] = content
			}
		}
	case 4: // content: written by anyone who may write, under its digest
		scope := p.pick(scopeIDs)
		info, err := p.store.PutContent(ctx, scope, strings.NewReader(body), objectstorage.PutOptions{})
		allowed := slices.Contains(binding.Reach, scope) && binding.Access == objectstorage.ReadWrite
		what := fmt.Sprintf("%s: put content in %q", describe, scope)
		p.wrote(what, err, allowed)
		if allowed {
			if info.Ref.Name != digestOf(body) || !info.Ref.Content || info.Ref.Scope != scope {
				p.t.Fatalf("objectscopetest: %s: stored as %s, want the digest %s", what, fullRef(info.Ref), digestOf(body))
			}
			p.model[objectkey.Key{Partition: binding.Partition, Scope: scope, Slot: objectkey.Content(), Name: digestOf(body)}] = body
		}
	case 5: // a presigned PUT is a write that happens later
		key := p.target(binding)
		ref := p.ref(binding, key)
		_, err := p.store.PresignPut(ctx, ref, time.Minute)
		p.wrote(fmt.Sprintf("%s: presign a put of %s", describe, fullRef(ref)), err, mayWrite(binding, key))
	}
	// What the store now holds is what the model says, whatever each call
	// reported about itself.
	if round%20 == 19 {
		p.compare(describe)
	}
}

func (p *prover) wrote(what string, err error, allowed bool) {
	p.t.Helper()
	switch {
	case allowed && err != nil:
		p.t.Fatalf("objectscopetest: %s: %v, on a write the binding may make (seed %d)", what, err, p.proof.Seed)
	case allowed:
		p.accepted++
	case err == nil:
		p.t.Fatalf("objectscopetest: %s: succeeded on a write the binding may not make (seed %d)", what, p.proof.Seed)
	case !refused(err):
		p.t.Fatalf("objectscopetest: %s: %v, want a refusal (seed %d)", what, err, p.proof.Seed)
	default:
		p.refused++
	}
}

// unbound: without a Binding, or with one that reaches nothing, every call is
// refused and nothing changes.
func (p *prover) unbound() {
	p.t.Helper()
	nowhere := objectstorage.Binding{Partition: p.partitions[0], Principal: principalIDs[0], Mode: objectstorage.Privileged, Access: objectstorage.ReadWrite}
	undecided := objectstorage.Binding{Partition: p.partitions[0], Principal: principalIDs[0], Reach: []string{scopeIDs[0]}}
	for describe, ctx := range map[string]context.Context{
		"no binding":                p.ctx,
		"a binding with no reach":   context.WithValue(p.ctx, bindingKey{}, nowhere),
		"a binding nobody decided":  context.WithValue(p.ctx, bindingKey{}, undecided),
		"a binding with no content": context.WithValue(p.ctx, bindingKey{}, objectstorage.Binding{}),
	} {
		ref := objectstorage.Shared("a").In(scopeIDs[0])
		calls := map[string]error{}
		_, calls["put"] = p.store.Put(ctx, ref, strings.NewReader("x"), objectstorage.PutOptions{})
		_, calls["put content"] = p.store.PutContent(ctx, scopeIDs[0], strings.NewReader("x"), objectstorage.PutOptions{})
		_, _, calls["get"] = p.store.Get(ctx, ref)
		_, calls["head"] = p.store.Head(ctx, ref)
		calls["delete"] = p.store.Delete(ctx, ref)
		_, calls["copy"] = p.store.Copy(ctx, ref, ref, objectstorage.CopyOptions{})
		_, calls["list"] = p.store.List(ctx, objectstorage.Shared("").In(scopeIDs[0]), objectstorage.ListOptions{})
		_, calls["list scope"] = p.store.ListScope(ctx, scopeIDs[0], objectstorage.ListOptions{})
		_, calls["presign get"] = p.store.PresignGet(ctx, ref, time.Minute)
		_, calls["presign put"] = p.store.PresignPut(ctx, ref, time.Minute)
		for call, err := range calls {
			if !refused(err) {
				p.t.Fatalf("objectscopetest: %s: %s: %v, want a refusal", describe, call, err)
			}
			p.refused++
		}
	}
	p.compare("the unbound calls")
}

// compare reads the proof's partitions with the gateway client and holds them
// to the model, key by key and byte by byte.
func (p *prover) compare(after string) {
	p.t.Helper()
	held := map[objectkey.Key]string{}
	for _, partition := range p.partitions {
		prefix, err := p.codec.PartitionPrefix(partition)
		if err != nil {
			p.t.Fatalf("objectscopetest: %v", err)
		}
		token := ""
		for {
			listed, err := p.proof.Client.List(p.ctx, &storagev0.ListRequest{Prefix: prefix, PageToken: token})
			if err != nil {
				p.t.Fatalf("objectscopetest: inspect the store: %v", err)
			}
			for _, object := range listed.GetObjects() {
				key, err := p.codec.Parse(object.GetKey())
				if err != nil {
					p.t.Fatalf("objectscopetest: after %s the store holds %q, which the convention did not write: %v (seed %d)", after, object.GetKey(), err, p.proof.Seed)
				}
				if key.Staging {
					p.t.Fatalf("objectscopetest: after %s an upload in flight was left behind at %q (seed %d)", after, object.GetKey(), p.proof.Seed)
				}
				held[key] = p.fetch(object.GetKey())
			}
			if token = listed.GetNextPageToken(); token == "" {
				break
			}
		}
	}
	var wrong []string
	for key, body := range held {
		if want, found := p.model[key]; !found {
			wrong = append(wrong, fmt.Sprintf("holds %+v, which the model does not", key))
		} else if want != body {
			wrong = append(wrong, fmt.Sprintf("%+v holds %q, want %q", key, body, want))
		}
	}
	for key := range p.model {
		if _, found := held[key]; !found {
			wrong = append(wrong, fmt.Sprintf("lost %+v", key))
		}
	}
	if len(wrong) > 0 {
		sort.Strings(wrong)
		p.t.Fatalf("objectscopetest: after %s the store is not what the model says (seed %d):\n %s", after, p.proof.Seed, strings.Join(wrong, "\n "))
	}
}

func (p *prover) fetch(physical string) string {
	p.t.Helper()
	stream, err := p.proof.Client.Get(p.ctx, &storagev0.GetRequest{Key: physical})
	if err != nil {
		p.t.Fatalf("objectscopetest: inspect %s: %v", physical, err)
	}
	var body bytes.Buffer
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return body.String()
		}
		if err != nil {
			p.t.Fatalf("objectscopetest: inspect %s: %v", physical, err)
		}
		body.Write(frame.GetData())
	}
}

// LayoutReporter is the part of testing.TB RequireLayout uses.
type LayoutReporter interface {
	Helper()
	Fatalf(format string, arguments ...any)
}

// RequireLayout is the layout gate of a store that holds objects: it fails t
// unless every key under the base was written by the key convention and sits
// under a partition, a scope and a principal the module knows, and unless the
// gateway holds nothing outside the declared bases. Pass every base the
// module uses on this gateway.
func RequireLayout(t LayoutReporter, ctx context.Context, client storagev0.ObjectStorageClient, known objectstorage.Known, bases ...string) {
	t.Helper()
	if len(bases) == 0 {
		t.Fatalf("objectscopetest: at least one base is required")
	}
	var findings []objectstorage.Finding
	for _, base := range bases {
		maintain, err := objectstorage.NewMaintenance(client, base)
		if err != nil {
			t.Fatalf("objectscopetest: %v", err)
		}
		found, err := maintain.Audit(ctx, known)
		if err != nil {
			t.Fatalf("objectscopetest: audit base %s: %v", base, err)
		}
		findings = append(findings, found...)
	}
	strays, err := objectstorage.Strays(ctx, client, bases...)
	if err != nil {
		t.Fatalf("objectscopetest: look for strays: %v", err)
	}
	findings = append(findings, strays...)
	if len(findings) == 0 {
		return
	}
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		lines = append(lines, finding.String())
	}
	t.Fatalf("objectscopetest: %d keys are not where the convention puts them:\n %s", len(findings), strings.Join(lines, "\n "))
}
