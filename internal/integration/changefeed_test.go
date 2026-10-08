//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/feed"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

// newFeedStack builds the gateway with the store change feed running, and
// returns it alongside a raw MinIO client. The raw client is the point of this
// suite: it writes to the bucket with no gateway involved, which is exactly
// what a per-replica Watch can never report.
func newFeedStack(t *testing.T) (storagev0.ObjectStorageClient, *miniogo.Client, string) {
	t.Helper()
	endpoint := mustEnv(t, "MINIO_ENDPOINT")
	ak := mustEnv(t, "MINIO_ACCESS_KEY")
	sk := mustEnv(t, "MINIO_SECRET_KEY")
	bucket := mustEnv(t, "MINIO_BUCKET")
	ensureBucket(t, endpoint, ak, sk, bucket)

	be, err := backend.Open(context.Background(), backend.Config{
		Kind:         "minio",
		Endpoint:     endpoint,
		Region:       "us-east-1",
		AccessKey:    ak,
		SecretKey:    sk,
		Bucket:       bucket,
		UsePathStyle: true,
		ChangeFeed:   true,
	})
	require.NoError(t, err)

	feeder, ok := backend.AsChangeFeeder(be)
	require.True(t, ok, "the minio backend must stream the store's own notifications")

	hub := events.NewHub()
	feedCtx, stopFeed := context.WithCancel(context.Background())
	go feed.Run(feedCtx, feeder, hub)

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(s, server.New(be, hub, probetest.Monitor(t, be)))
	go func() { _ = s.Serve(lis) }()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	direct, err := miniogo.New(endpoint, &miniogo.Options{
		Creds:  miniocreds.NewStaticV4(ak, sk, ""),
		Secure: false,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
		s.Stop()
		stopFeed()
		hub.Close()
		_ = be.Close()
	})
	return storagev0.NewObjectStorageClient(conn), direct, bucket
}

func putDirect(t *testing.T, cl *miniogo.Client, bucket, key string, body []byte) {
	t.Helper()
	_, err := cl.PutObject(context.Background(), bucket, key, bytes.NewReader(body), int64(len(body)),
		miniogo.PutObjectOptions{})
	require.NoError(t, err)
}

// awaitEvent reads the stream until it sees key, skipping anything else — the
// bucket is shared with the other tests in this suite, so an unrelated event is
// normal and is not a failure.
func awaitEvent(t *testing.T, stream storagev0.ObjectStorage_WatchClient, key string) *storagev0.WriteEvent {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ev, err := stream.Recv()
		require.NoError(t, err)
		if ev.GetKey() == key {
			return ev
		}
	}
	t.Fatalf("no event for %q arrived within the deadline", key)
	return nil
}

// openFeedWatch starts a Watch and proves the store's listener is actually
// established before the test writes the object it asserts on. The listener is
// a long poll the gateway opens asynchronously, and MinIO reports nothing that
// happened before it was accepted: without this the test would race the feed
// and fail intermittently for a reason that has nothing to do with the
// behaviour under test.
func openFeedWatch(t *testing.T, c storagev0.ObjectStorageClient, direct *miniogo.Client, bucket string) storagev0.ObjectStorage_WatchClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := c.Watch(ctx, &storagev0.WatchRequest{})
	require.NoError(t, err)
	_, err = stream.Header()
	require.NoError(t, err)

	warm := make(chan *storagev0.WriteEvent, 1)
	go func() {
		for {
			ev, rerr := stream.Recv()
			if rerr != nil {
				return
			}
			if strings.HasPrefix(ev.GetKey(), "changefeed/warmup/") {
				select {
				case warm <- ev:
				default:
				}
				return
			}
		}
	}()

	deadline := time.Now().Add(60 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		putDirect(t, direct, bucket, fmt.Sprintf("changefeed/warmup/%d", attempt), []byte("warmup"))
		select {
		case <-warm:
			return stream
		case <-time.After(time.Second):
		}
	}
	t.Fatal("the store's notification listener never delivered a warmup event")
	return nil
}

// TestChangeFeedCarriesAWriteMadeDirectlyToMinIO is the acceptance criterion:
// a write that never touched the gateway reaches a Watch subscriber, which is
// what lets a cache over this gateway invalidate on an out-of-band write
// (a presigned PUT, another tool) instead of serving stale bytes.
func TestChangeFeedCarriesAWriteMadeDirectlyToMinIO(t *testing.T) {
	c, direct, bucket := newFeedStack(t)

	caps, err := c.Capabilities(context.Background(), &storagev0.CapabilitiesRequest{})
	require.NoError(t, err)
	require.True(t, caps.GetWatchCrossReplica(), "a consumer reads this to know Watch covers the bucket")

	stream := openFeedWatch(t, c, direct, bucket)

	key := fmt.Sprintf("changefeed/direct-%d.txt", time.Now().UnixNano())
	putDirect(t, direct, bucket, key, []byte("written straight to the store"))

	ev := awaitEvent(t, stream, key)
	require.Equal(t, storagev0.WriteOp_WRITE_OP_PUT, ev.GetOp())
	require.NotEmpty(t, ev.GetEtag(), "the store reports the etag of the object it wrote")
	require.NotZero(t, ev.GetTimeUnixMs())
}

// TestChangeFeedCarriesADirectDelete: an invalidating consumer needs removals
// as much as writes, and a delete made outside the gateway is the case the
// in-process hub could never see.
func TestChangeFeedCarriesADirectDelete(t *testing.T) {
	c, direct, bucket := newFeedStack(t)
	stream := openFeedWatch(t, c, direct, bucket)

	key := fmt.Sprintf("changefeed/direct-delete-%d.txt", time.Now().UnixNano())
	putDirect(t, direct, bucket, key, []byte("about to go"))
	require.Equal(t, storagev0.WriteOp_WRITE_OP_PUT, awaitEvent(t, stream, key).GetOp())

	require.NoError(t, direct.RemoveObject(context.Background(), bucket, key, miniogo.RemoveObjectOptions{}))
	require.Equal(t, storagev0.WriteOp_WRITE_OP_DELETE, awaitEvent(t, stream, key).GetOp())
}

// TestChangeFeedDeliversAGatewayWriteTwice pins the documented trade-off: the
// feed is an additional publisher, so a write this replica serves arrives once
// when it is served and once when the store reports it. Watch is at-least-once
// by contract, and suppressing the local publication would cost a consumer
// every event whenever the store's stream was down.
func TestChangeFeedDeliversAGatewayWriteTwice(t *testing.T) {
	c, direct, bucket := newFeedStack(t)
	stream := openFeedWatch(t, c, direct, bucket)

	key := fmt.Sprintf("changefeed/through-the-gateway-%d.txt", time.Now().UnixNano())
	_, err := put(t, c, key, []byte("served by this replica"), nil)
	require.NoError(t, err)

	for i := range 2 {
		ev := awaitEvent(t, stream, key)
		require.Equal(t, storagev0.WriteOp_WRITE_OP_PUT, ev.GetOp(), "delivery %d", i+1)
	}
}

// TestChangeFeedReachesEveryReplica is the cross-replica claim itself. Each
// gateway opens its own listener on the store, so a write reaches every one of
// them — unlike the in-process hub, where a write reaches only the replica that
// served it and a consumer's coverage depended on which replica it dialled.
func TestChangeFeedReachesEveryReplica(t *testing.T) {
	first, direct, bucket := newFeedStack(t)
	second, _, _ := newFeedStack(t)

	firstWatch := openFeedWatch(t, first, direct, bucket)
	secondWatch := openFeedWatch(t, second, direct, bucket)

	key := fmt.Sprintf("changefeed/every-replica-%d.txt", time.Now().UnixNano())
	putDirect(t, direct, bucket, key, []byte("one write, two replicas"))

	require.Equal(t, storagev0.WriteOp_WRITE_OP_PUT, awaitEvent(t, firstWatch, key).GetOp())
	require.Equal(t, storagev0.WriteOp_WRITE_OP_PUT, awaitEvent(t, secondWatch, key).GetOp())
}
