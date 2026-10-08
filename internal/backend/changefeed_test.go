package backend_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"

	// The capability tests open real backends, which register themselves.
	_ "github.com/codefly-dev/service-object-storage/internal/backend/azure"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/gcs"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/s3"
)

// feedingBackend is a store that streams changes: it records the prefix it was
// asked for and replays whatever the test scripted, including keys outside that
// prefix, so the decorator's own filtering is what is under test rather than
// the store's.
type feedingBackend struct {
	backend.Backend
	asked  chan string
	script []backend.ChangeEvent
}

func (f *feedingBackend) Changes(ctx context.Context, prefix string) <-chan backend.ChangeEvent {
	f.asked <- prefix
	out := make(chan backend.ChangeEvent)
	go func() {
		defer close(out)
		for _, ev := range f.script {
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
		<-ctx.Done()
	}()
	return out
}

func newFeedingBackend(t *testing.T, script ...backend.ChangeEvent) *feedingBackend {
	t.Helper()
	inner, err := mem.New(context.Background(), backend.Config{Bucket: "shared"})
	require.NoError(t, err)
	return &feedingBackend{Backend: inner, asked: make(chan string, 1), script: script}
}

func changed(key string, op backend.ChangeOp, etag string) backend.ChangeEvent {
	return backend.ChangeEvent{Change: backend.Change{Key: key, Op: op, ETag: etag}}
}

func receiveChange(t *testing.T, in <-chan backend.ChangeEvent) backend.ChangeEvent {
	t.Helper()
	select {
	case ev, ok := <-in:
		require.True(t, ok, "the feed closed before delivering anything")
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("nothing delivered")
		return backend.ChangeEvent{}
	}
}

// TestAsChangeFeederRejectsABackendWithoutOne is what keeps Watch honest: a
// store that cannot stream its own writes must not look like one that can, or
// the gateway would report a cross-replica capability it cannot deliver.
func TestAsChangeFeederRejectsABackendWithoutOne(t *testing.T) {
	store, err := mem.New(context.Background(), backend.Config{Bucket: "shared"})
	require.NoError(t, err)

	_, ok := backend.AsChangeFeeder(store)
	require.False(t, ok)

	_, ok = backend.AsChangeFeeder(backend.WithPrefix(store, "tenant-a/"))
	require.False(t, ok, "a prefix cannot add a feed the store does not have")
}

// TestPrefixedFeedConfinesAndStripsKeys: a subscriber under a prefix works in
// relative keys everywhere else in the API, so the feed must agree — and must
// not hand it a key from outside the prefix even if the store reports one.
func TestPrefixedFeedConfinesAndStripsKeys(t *testing.T) {
	store := newFeedingBackend(t,
		changed("tenant-b/theirs.txt", backend.ChangePut, ""),
		changed("tenant-a/docs/mine.txt", backend.ChangePut, "etag-1"),
	)

	feeder, ok := backend.AsChangeFeeder(backend.WithPrefix(store, "tenant-a/"))
	require.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := feeder.Changes(ctx, "docs/")

	require.Equal(t, "tenant-a/docs/", <-store.asked, "the store filters on the joined prefix")

	ev := receiveChange(t, in)
	require.NoError(t, ev.Err)
	require.Equal(t, "docs/mine.txt", ev.Change.Key, "the other tenant's key must not reach this subscriber")
	require.Equal(t, "etag-1", ev.Change.ETag)
}

// TestPrefixedFeedForwardsErrors keeps the caller's reconnect decision intact:
// it re-subscribes on what the store reports, so an error swallowed here would
// look like a healthy, silent stream.
func TestPrefixedFeedForwardsErrors(t *testing.T) {
	store := newFeedingBackend(t, backend.ChangeEvent{Err: errors.New("store refused the listener")})

	feeder, ok := backend.AsChangeFeeder(backend.WithPrefix(store, "tenant-a/"))
	require.True(t, ok)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ev := receiveChange(t, feeder.Changes(ctx, ""))
	require.ErrorContains(t, ev.Err, "store refused the listener")
}

// TestMinIOReportsCrossReplicaOnlyWhenConfigured: the capability describes the
// backend AND its configuration. A MinIO that could stream notifications but
// was not asked to still serves a per-replica Watch, and saying otherwise would
// have a consumer skip the reconciliation it still needs.
func TestMinIOReportsCrossReplicaOnlyWhenConfigured(t *testing.T) {
	cfg := backend.Config{Kind: "minio", Endpoint: "127.0.0.1:9000", Bucket: "documents"}

	off, err := backend.Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = off.Close() })
	require.False(t, off.Capabilities().WatchCrossReplica)

	cfg.ChangeFeed = true
	on, err := backend.Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = on.Close() })
	require.True(t, on.Capabilities().WatchCrossReplica)

	_, ok := backend.AsChangeFeeder(on)
	require.True(t, ok, "minio streams the store's own notifications")
}

// TestCloudBackendsHaveNoChangeFeed records the boundary this PR stops at: S3,
// GCS and Azure carry their writes on delivery targets nothing here consumes,
// so they report a per-replica Watch rather than a capability they cannot back.
func TestCloudBackendsHaveNoChangeFeed(t *testing.T) {
	for kind, cfg := range map[string]backend.Config{
		"s3":    {Kind: "s3", Bucket: "documents", Region: "us-east-1"},
		"gcs":   {Kind: "gcs", Bucket: "documents"},
		"azure": {Kind: "azure", Bucket: "documents", AzureAccount: "acct"},
	} {
		t.Run(kind, func(t *testing.T) {
			store, err := backend.Open(context.Background(), cfg)
			if err != nil {
				t.Skipf("%s backend needs credentials this environment has not got: %v", kind, err)
			}
			t.Cleanup(func() { _ = store.Close() })
			_, ok := backend.AsChangeFeeder(store)
			require.False(t, ok)
			require.False(t, store.Capabilities().WatchCrossReplica)
		})
	}
}
