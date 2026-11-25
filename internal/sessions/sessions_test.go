package sessions_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/sessions"
)

func scopeOf(partition, scope string, mode sessions.Mode) sessions.Scope {
	return sessions.Scope{Base: "files", Partition: partition, Scope: scope, Mode: mode}
}

// A scope is a key prefix, and the mapping is the key convention's: every
// provider's grant is rendered from this one string.
func TestAScopeMapsToTheKeyConventionsPrefix(t *testing.T) {
	prefix, err := scopeOf("tenant-a", "installation-a", sessions.Read).Prefix()
	require.NoError(t, err)
	require.Equal(t, "files/tenant-a/installation-a/", prefix)

	// An identifier that would spell another scope's prefix is escaped, so
	// no partition and scope pair is the prefix of a different pair.
	hostile, err := scopeOf("tenant-a/installation-a", "x", sessions.Read).Prefix()
	require.NoError(t, err)
	require.Equal(t, "files/tenant-a_2finstallation-a/x/", hostile)
	wild, err := scopeOf("*", "a?b", sessions.Read).Prefix()
	require.NoError(t, err)
	require.NotContains(t, wild, "*")
	require.NotContains(t, wild, "?")

	for name, scope := range map[string]sessions.Scope{
		"no mode":         {Base: "files", Partition: "t", Scope: "i"},
		"an unknown mode": {Base: "files", Partition: "t", Scope: "i", Mode: 9},
		"no base":         {Partition: "t", Scope: "i", Mode: sessions.Read},
		"a base path":     {Base: "files/other", Partition: "t", Scope: "i", Mode: sessions.Read},
		"no partition":    {Base: "files", Scope: "i", Mode: sessions.Read},
		"no scope":        {Base: "files", Partition: "t", Mode: sessions.Read},
		"a dot scope":     {Base: "files", Partition: "t", Scope: "..", Mode: sessions.Read},
	} {
		_, err := scope.Prefix()
		require.ErrorIs(t, err, sessions.ErrInvalidScope, name)
	}

	scope := scopeOf("tenant-a", "installation-a", sessions.Read)
	require.True(t, scope.Contains("files/tenant-a/installation-a/_sha256/"+strings.Repeat("a", 64)))
	require.False(t, scope.Contains("files/tenant-a/installation-b/_sha256/"+strings.Repeat("a", 64)))
	require.False(t, scope.Contains("files/tenant-a/installation-a/"))
	require.False(t, scope.Contains("tenant-a/blobs/"+strings.Repeat("a", 64)))
}

var (
	longID     = strings.Repeat("a", 240)
	longPrefix = strings.Repeat("p", 600) + "/"
)

type statement struct {
	Effect    string
	Action    []string
	Resource  []string
	Condition map[string]map[string][]string
}

func s3Policy(t *testing.T, mode sessions.Mode) []statement {
	t.Helper()
	rendered, err := sessions.S3SessionPolicy("bucket", "gateway/", scopeOf("tenant-a", "installation-a", mode))
	require.NoError(t, err)
	var document struct{ Statement []statement }
	require.NoError(t, json.Unmarshal(rendered, &document))
	return document.Statement
}

// The three modes are three disjoint grants over one prefix: read cannot
// write, ingest can neither read, list nor delete.
func TestS3SessionPolicyGrantsEachModeItsVerbsAndNoOthers(t *testing.T) {
	rendered, err := sessions.S3SessionPolicy("bucket", "gateway/", scopeOf("tenant-a", "installation-a", sessions.Read))
	require.NoError(t, err)
	require.JSONEq(t, `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/gateway/files/tenant-a/installation-a/*"]},
		{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::bucket"],
		 "Condition":{"StringLike":{"s3:prefix":["gateway/files/tenant-a/installation-a/*"]}}}]}`, string(rendered))

	verbs := func(mode sessions.Mode) []string {
		var all []string
		for _, statement := range s3Policy(t, mode) {
			require.Equal(t, "Allow", statement.Effect)
			for _, resource := range statement.Resource {
				require.True(t, resource == "arn:aws:s3:::bucket" || resource == "arn:aws:s3:::bucket/gateway/files/tenant-a/installation-a/*", resource)
			}
			if statement.Resource[0] == "arn:aws:s3:::bucket" {
				require.Equal(t, []string{"gateway/files/tenant-a/installation-a/*"}, statement.Condition["StringLike"]["s3:prefix"],
					"a bucket-level verb is always held to the scope's prefix")
			}
			all = append(all, statement.Action...)
		}
		return all
	}
	require.ElementsMatch(t, []string{"s3:GetObject", "s3:ListBucket"}, verbs(sessions.Read))
	require.ElementsMatch(t, []string{"s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"}, verbs(sessions.Ingest))
	require.ElementsMatch(t, []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListBucket"}, verbs(sessions.Maintenance))

	for name, arguments := range map[string][2]string{
		"no bucket": {"", ""}, "a wildcard bucket": {"buck*t", ""}, "a wildcard prefix": {"bucket", "a*/"},
		"a variable prefix": {"bucket", "${aws:username}/"}, "a prefix without its separator": {"bucket", "gateway"},
		"a quote": {"bucket", "it's/"},
	} {
		_, err := sessions.S3SessionPolicy(arguments[0], arguments[1], scopeOf("t", "i", sessions.Read))
		require.ErrorIs(t, err, sessions.ErrInvalidScope, name)
	}
}

// A scope whose identifiers make the policy too long for the store is
// refused. Nothing shorter and wider is rendered in its place.
func TestAnOverlargeSessionPolicyIsRefusedNeverBroadened(t *testing.T) {
	// The key convention caps an identifier at 240 bytes, so a policy is
	// overlarge only when two identifiers near that cap meet a long gateway
	// prefix. That is the case refused here.
	for _, mode := range []sessions.Mode{sessions.Read, sessions.Maintenance} {
		rendered, err := sessions.S3SessionPolicy("bucket", longPrefix, sessions.Scope{Base: "files", Partition: longID, Scope: longID, Mode: mode})
		require.ErrorIs(t, err, sessions.ErrPolicyTooLarge, mode.String())
		require.Nil(t, rendered)
		boundary, err := sessions.GCSAccessBoundary("bucket", longPrefix, sessions.Scope{Base: "files", Partition: longID, Scope: longID, Mode: mode})
		require.ErrorIs(t, err, sessions.ErrPolicyTooLarge, mode.String())
		require.Nil(t, boundary)
	}
	// The same identifiers under a short prefix fit, and are rendered whole.
	rendered, err := sessions.S3SessionPolicy("bucket", "", sessions.Scope{Base: "files", Partition: longID, Scope: longID, Mode: sessions.Read})
	require.NoError(t, err)
	require.Contains(t, string(rendered), "files/"+longID+"/"+longID+"/*")
	// The largest scope of ordinary identifiers fits with room to spare.
	uuid := "018f6f1e-7c1a-7b3c-9d2e-0123456789ab"
	rendered, err = sessions.S3SessionPolicy("a-bucket-name-of-ordinary-length", "a-gateway-prefix/",
		sessions.Scope{Base: "documents-content", Partition: uuid, Scope: uuid, Mode: sessions.Maintenance})
	require.NoError(t, err)
	require.Less(t, len(rendered), sessions.MaxS3SessionPolicyBytes/2)
}

// GCS and Azure: the document a scope maps to, rendered and never used. This
// is not evidence that either provider enforces it.
func TestGCSAndAzureGrantsAreRenderedForTheScopesPrefix(t *testing.T) {
	type boundary struct {
		AccessBoundary struct {
			AccessBoundaryRules []struct {
				AvailableResource     string
				AvailablePermissions  []string
				AvailabilityCondition struct{ Expression string }
			}
		}
	}
	roles := map[sessions.Mode]string{sessions.Read: "objectViewer", sessions.Ingest: "objectCreator", sessions.Maintenance: "objectAdmin"}
	permissions := map[sessions.Mode]string{sessions.Read: "rl", sessions.Ingest: "cw", sessions.Maintenance: "rcwdl"}
	for mode, role := range roles {
		rendered, err := sessions.GCSAccessBoundary("bucket", "gateway/", scopeOf("tenant-a", "installation-a", mode))
		require.NoError(t, err)
		var got boundary
		require.NoError(t, json.Unmarshal(rendered, &got))
		require.Len(t, got.AccessBoundary.AccessBoundaryRules, 1)
		rule := got.AccessBoundary.AccessBoundaryRules[0]
		require.Equal(t, "//storage.googleapis.com/projects/_/buckets/bucket", rule.AvailableResource)
		require.Equal(t, []string{"inRole:roles/storage." + role}, rule.AvailablePermissions)
		name := "resource.name.startsWith('projects/_/buckets/bucket/objects/gateway/files/tenant-a/installation-a/')"
		list := "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('gateway/files/tenant-a/installation-a/')"
		if mode == sessions.Ingest {
			require.Equal(t, name, rule.AvailabilityCondition.Expression, "an ingest boundary lists nothing")
		} else {
			require.Equal(t, name+" || "+list, rule.AvailabilityCondition.Expression)
		}

		sas, err := sessions.AzureUserDelegationSAS("container", "gateway/", scopeOf("tenant-a", "installation-a", mode))
		require.NoError(t, err)
		require.Equal(t, sessions.AzureDirectorySAS{
			SignedResource: "d", Container: "container", Directory: "gateway/files/tenant-a/installation-a", Depth: 4,
			Permissions: permissions[mode], RequiresHierarchicalNamespace: true,
		}, sas)
	}
	_, err := sessions.GCSAccessBoundary("bucket", "it's/", scopeOf("t", "i", sessions.Read))
	require.ErrorIs(t, err, sessions.ErrInvalidScope, "a quote would end the condition's string")
}

// Only MinIO issues. The others say what they would grant and refuse to
// issue, and a Manager refuses to be built on one.
func TestOnlyAQualifiedIssuerIssues(t *testing.T) {
	scope := scopeOf("tenant-a", "installation-a", sessions.Read)
	minio, err := sessions.IssuerFor(backend.Config{Kind: "minio", Bucket: "bucket"})
	require.NoError(t, err)
	require.Equal(t, sessions.Qualified, minio.Qualification())

	for _, kind := range []string{"s3", "gcs", "azure"} {
		issuer, err := sessions.IssuerFor(backend.Config{Kind: kind, Bucket: "bucket", Prefix: "gateway"})
		require.NoError(t, err, kind)
		require.Equal(t, kind, issuer.Kind())
		require.Equal(t, sessions.Unqualified, issuer.Qualification(), kind)
		grant, err := issuer.Grant(scope)
		require.NoError(t, err, kind)
		require.Contains(t, string(grant), "gateway/files/tenant-a/installation-a", kind)
		_, err = issuer.Issue(context.Background(), scope, sessions.DefaultTTL)
		require.ErrorIs(t, err, sessions.ErrUnqualified, kind)
		_, err = sessions.NewManager(sessions.Options{Issuer: issuer, Authorizer: allowAll{}})
		require.ErrorIs(t, err, sessions.ErrUnqualified, kind)
	}
	_, err = sessions.IssuerFor(backend.Config{Kind: "mem", Bucket: "bucket"})
	require.ErrorIs(t, err, sessions.ErrUnqualified)
}

func TestGrantsAdmitOnlyWhatWasGranted(t *testing.T) {
	grants, err := sessions.ParseGrants(`[
		{"base":"files","modes":["read","ingest"]},
		{"base":"pinned","modes":["read"],"partitions":["tenant-a"]}]`)
	require.NoError(t, err)
	ctx := context.Background()
	admit := func(base, partition string, mode sessions.Mode) error {
		_, err := grants.Authorize(ctx, sessions.Scope{Base: base, Partition: partition, Scope: "i", Mode: mode})
		return err
	}
	require.NoError(t, admit("files", "any-tenant", sessions.Read))
	require.NoError(t, admit("files", "any-tenant", sessions.Ingest))
	require.ErrorIs(t, admit("files", "any-tenant", sessions.Maintenance), sessions.ErrNotGranted, "a mode that was not granted")
	require.ErrorIs(t, admit("other", "any-tenant", sessions.Read), sessions.ErrNotGranted, "a base that was not granted")
	require.NoError(t, admit("pinned", "tenant-a", sessions.Read))
	require.ErrorIs(t, admit("pinned", "tenant-b", sessions.Read), sessions.ErrNotGranted, "a partition outside the grant")
	require.ErrorIs(t, admit("pinned", "tenant-a", sessions.Ingest), sessions.ErrNotGranted)

	none, err := sessions.ParseGrants("  ")
	require.NoError(t, err)
	_, err = none.Authorize(ctx, scopeOf("t", "i", sessions.Read))
	require.ErrorIs(t, err, sessions.ErrNotGranted, "no grant admits nothing")

	for name, raw := range map[string]string{
		"not JSON":           `files=read`,
		"an unknown field":   `[{"base":"files","modes":["read"],"scopes":["*"]}]`,
		"an unknown mode":    `[{"base":"files","modes":["write"]}]`,
		"no mode":            `[{"base":"files","modes":[]}]`,
		"a base twice":       `[{"base":"files","modes":["read"]},{"base":"files","modes":["ingest"]}]`,
		"a base path":        `[{"base":"files/x","modes":["read"]}]`,
		"a wildcard base":    `[{"base":"*","modes":["read"]}]`,
		"an empty partition": `[{"base":"files","modes":["read"],"partitions":[""]}]`,
		"trailing content":   `[{"base":"files","modes":["read"]}] []`,
	} {
		_, err := sessions.ParseGrants(raw)
		require.Error(t, err, name)
	}
}

type allowAll struct{}

func (allowAll) Authorize(context.Context, sessions.Scope) (string, error) { return "caller", nil }

// callerFrom reads the caller from the context, as an authorizer of a
// verified per-request identity would.
type callerKey struct{}

type callerFrom struct{}

func (callerFrom) Authorize(ctx context.Context, _ sessions.Scope) (string, error) {
	caller, _ := ctx.Value(callerKey{}).(string)
	if caller == "" {
		return "", sessions.ErrNotGranted
	}
	return caller, nil
}

func as(caller string) context.Context {
	return context.WithValue(context.Background(), callerKey{}, caller)
}

// fakeIssuer issues numbered credentials and records for what.
type fakeIssuer struct {
	mu     sync.Mutex
	prefix string
	now    func() time.Time
	issued []sessions.Scope
	fail   error
	slow   chan struct{}
}

func (f *fakeIssuer) Kind() string                          { return "fake" }
func (f *fakeIssuer) Qualification() sessions.Qualification { return sessions.Qualified }
func (f *fakeIssuer) Grant(scope sessions.Scope) ([]byte, error) {
	return sessions.S3SessionPolicy("bucket", f.prefix, scope)
}

func (f *fakeIssuer) Issue(_ context.Context, scope sessions.Scope, ttl time.Duration) (sessions.Credential, error) {
	if f.slow != nil {
		<-f.slow
	}
	if _, err := f.Grant(scope); err != nil {
		return sessions.Credential{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return sessions.Credential{}, f.fail
	}
	f.issued = append(f.issued, scope)
	return sessions.Credential{AccessKey: fmt.Sprintf("session-%d", len(f.issued)), SecretKey: "secret", SessionToken: "token", Expires: f.now().Add(ttl)}, nil
}

func (f *fakeIssuer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.issued)
}

// held is a backend that only says which credential opened it.
type held struct {
	backend.Backend
	accessKey string
	closed    atomic.Bool
}

func (h *held) Close() error { h.closed.Store(true); return nil }

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(by time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(by)
}

func manager(t *testing.T, size int, authorizer sessions.Authorizer) (*sessions.Manager, *fakeIssuer, *clock) {
	t.Helper()
	wall := &clock{now: time.Unix(1_800_000_000, 0)}
	issuer := &fakeIssuer{now: wall.Now}
	m, err := sessions.NewManager(sessions.Options{
		Backend: backend.Config{Kind: "fake", Bucket: "bucket", AccessKey: "root", SecretKey: "root-secret"},
		Issuer:  issuer, Authorizer: authorizer, CacheSize: size, Now: wall.Now,
		Open: func(_ context.Context, cfg backend.Config) (backend.Backend, error) {
			if cfg.SessionToken == "" || cfg.AccessKey == "root" {
				return nil, errors.New("a session backend was opened with the gateway's own credential")
			}
			return &held{accessKey: cfg.AccessKey}, nil
		},
	})
	require.NoError(t, err)
	return m, issuer, wall
}

func keyOf(t *testing.T, session *sessions.Session) string {
	t.Helper()
	return session.Backend.(*held).accessKey
}

func TestASessionIsReusedUntilShortlyBeforeItExpiresThenReissued(t *testing.T) {
	m, issuer, wall := manager(t, 8, allowAll{})
	ctx := context.Background()
	scope := scopeOf("tenant-a", "installation-a", sessions.Read)

	first, err := m.Session(ctx, scope)
	require.NoError(t, err)
	require.Equal(t, wall.Now().Add(sessions.DefaultTTL), first.Expires)
	first.Release()
	again, err := m.Session(ctx, scope)
	require.NoError(t, err)
	require.Equal(t, keyOf(t, first), keyOf(t, again), "the session is reused")
	again.Release()
	require.Equal(t, 1, issuer.count())

	// One minute before the store stops honouring it, no new call starts on it.
	wall.Advance(sessions.DefaultTTL - time.Minute - time.Second)
	still, err := m.Session(ctx, scope)
	require.NoError(t, err)
	require.Equal(t, keyOf(t, first), keyOf(t, still))
	wall.Advance(time.Second)
	fresh, err := m.Session(ctx, scope)
	require.NoError(t, err)
	require.NotEqual(t, keyOf(t, first), keyOf(t, fresh), "a session past its margin is re-issued, not reused")
	require.Equal(t, 2, issuer.count())
	require.Equal(t, 1, m.Held())

	// The replaced session closes once the call still on it ends.
	old := still.Backend.(*held)
	require.False(t, old.closed.Load(), "a session in use is not closed under its call")
	still.Release()
	still.Release()
	require.True(t, old.closed.Load())
	fresh.Release()
}

// The cache is keyed by the caller and every field of the scope as whole
// values: no caller is handed a session issued for another caller, another
// partition, another scope or another mode, however their identifiers spell.
func TestTheCacheNeverHandsOneCallerAnothersSession(t *testing.T) {
	m, issuer, _ := manager(t, 64, callerFrom{})
	type request struct {
		caller string
		scope  sessions.Scope
	}
	requests := []request{
		{"alpha", scopeOf("tenant-a", "installation-a", sessions.Read)},
		{"beta", scopeOf("tenant-a", "installation-a", sessions.Read)},
		{"alpha", scopeOf("tenant-a", "installation-b", sessions.Read)},
		{"alpha", scopeOf("tenant-b", "installation-a", sessions.Read)},
		{"alpha", scopeOf("tenant-a", "installation-a", sessions.Ingest)},
		{"alpha", scopeOf("tenant-a", "installation-a", sessions.Maintenance)},
		{"alpha", sessions.Scope{Base: "other", Partition: "tenant-a", Scope: "installation-a", Mode: sessions.Read}},
		// Pairs a joined string key would confuse.
		{"alpha", scopeOf("a/b", "c", sessions.Read)},
		{"alpha", scopeOf("a", "b/c", sessions.Read)},
		{"alpha/files/a", scopeOf("b", "c", sessions.Read)},
		{"alpha", scopeOf("a\x00b", "c", sessions.Read)},
	}
	keys := map[string]request{}
	for round := 0; round < 3; round++ {
		for _, request := range requests {
			session, err := m.Session(as(request.caller), request.scope)
			if strings.ContainsRune(request.scope.Partition, 0) {
				require.ErrorIs(t, err, sessions.ErrInvalidScope)
				continue
			}
			require.NoError(t, err)
			key := keyOf(t, session)
			session.Release()
			if owner, seen := keys[key]; seen {
				require.Equal(t, owner, request, "session %s was handed to two different requests", key)
			}
			keys[key] = request
		}
	}
	require.Len(t, keys, 10, "one session per caller, scope and mode")
	require.Equal(t, 10, issuer.count(), "and each was issued once")
	for index, scope := range issuer.issued {
		require.Equal(t, keys[fmt.Sprintf("session-%d", index+1)].scope, scope, "a session is used for the scope it was issued for")
	}

	_, err := m.Session(context.Background(), scopeOf("tenant-a", "installation-a", sessions.Read))
	require.ErrorIs(t, err, sessions.ErrNotGranted, "a caller nobody identified gets nothing from the cache")
}

func TestTheCacheIsBounded(t *testing.T) {
	m, issuer, wall := manager(t, 3, allowAll{})
	ctx := context.Background()
	var first *held
	for index := 0; index < 10; index++ {
		session, err := m.Session(ctx, scopeOf("tenant-a", fmt.Sprintf("installation-%d", index), sessions.Read))
		require.NoError(t, err)
		if index == 0 {
			first = session.Backend.(*held)
		}
		session.Release()
		require.LessOrEqual(t, m.Held(), 3)
		wall.Advance(time.Second)
	}
	require.Equal(t, 10, issuer.count())
	require.True(t, first.closed.Load(), "an evicted session is closed")
	// An evicted scope is simply issued again.
	session, err := m.Session(ctx, scopeOf("tenant-a", "installation-0", sessions.Read))
	require.NoError(t, err)
	session.Release()
	require.Equal(t, 11, issuer.count())

	_, err = sessions.NewManager(sessions.Options{Issuer: issuer, Authorizer: allowAll{}, CacheSize: sessions.MaxCacheSize + 1})
	require.Error(t, err)
	for _, ttl := range []time.Duration{time.Minute, sessions.MinTTL - time.Second, sessions.MaxTTL + time.Second} {
		_, err = sessions.NewManager(sessions.Options{Issuer: issuer, Authorizer: allowAll{}, TTL: ttl})
		require.Error(t, err, ttl.String())
	}
}

// An issuer that does not answer fails the call. There is nothing to fall
// back to, the failure is not remembered, and the next call asks again.
func TestAnIssuerFailureFailsClosedAndIsNotCached(t *testing.T) {
	m, issuer, _ := manager(t, 8, allowAll{})
	ctx := context.Background()
	scope := scopeOf("tenant-a", "installation-a", sessions.Read)
	issuer.fail = errors.New("connection refused")
	for range 2 {
		session, err := m.Session(ctx, scope)
		require.ErrorIs(t, err, sessions.ErrIssuerUnavailable)
		require.Nil(t, session)
		require.Zero(t, m.Held())
	}
	issuer.fail = nil
	session, err := m.Session(ctx, scope)
	require.NoError(t, err)
	session.Release()

	issuer.prefix = longPrefix
	_, err = m.Session(ctx, sessions.Scope{Base: "files", Partition: longID, Scope: longID, Mode: sessions.Read})
	require.ErrorIs(t, err, sessions.ErrPolicyTooLarge)
	require.NotErrorIs(t, err, sessions.ErrIssuerUnavailable)
	require.Equal(t, 1, m.Held())
}

func TestConcurrentCallsOnOneScopeShareOneIssuance(t *testing.T) {
	m, issuer, _ := manager(t, 8, allowAll{})
	issuer.slow = make(chan struct{})
	scope := scopeOf("tenant-a", "installation-a", sessions.Read)
	var wait sync.WaitGroup
	keys := make([]string, 16)
	for index := range keys {
		wait.Add(1)
		go func() {
			defer wait.Done()
			session, err := m.Session(context.Background(), scope)
			if err != nil {
				t.Error(err)
				return
			}
			keys[index] = session.Backend.(*held).accessKey
			session.Release()
		}()
	}
	close(issuer.slow)
	wait.Wait()
	require.Equal(t, 1, issuer.count())
	for _, key := range keys {
		require.Equal(t, "session-1", key)
	}

	// A caller that gives up stops waiting; the issuance is not cancelled
	// under the callers still waiting on it.
	issuer.slow = make(chan struct{})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		_, err := m.Session(cancelled, scopeOf("tenant-a", "installation-b", sessions.Read))
		done <- err
	}()
	close(issuer.slow)
	err := <-done
	if err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}
}

// An issuer that answers with a session already at its end is a refusal, not
// a loop and not a session handed out to expire under its call.
func TestASessionIssuedAtItsEndIsRefused(t *testing.T) {
	m, issuer, wall := manager(t, 8, allowAll{})
	issuer.now = func() time.Time { return wall.Now().Add(-sessions.DefaultTTL + 30*time.Second) }
	session, err := m.Session(context.Background(), scopeOf("tenant-a", "installation-a", sessions.Read))
	require.ErrorIs(t, err, sessions.ErrIssuerUnavailable)
	require.Nil(t, session)
	require.Zero(t, m.Held())
}
