package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/config"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

func openMem(t *testing.T) backend.Backend {
	t.Helper()
	be, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	return be
}

func TestOpenStore_RejectsUnreachableRedis(t *testing.T) {
	cfg := config.Config{}
	cfg.Backend.Kind = "mem"
	cfg.Cache.Enabled = true
	cfg.Cache.RedisAddr = "127.0.0.1:1" // refuses immediately

	store, rdb, err := openStore(context.Background(), cfg)
	require.Error(t, err)
	require.Nil(t, store)
	require.Nil(t, rdb)
}

func TestOpenStore_ReachableRedis(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	cfg := config.Config{}
	cfg.Backend.Kind = "mem"
	cfg.Cache.Enabled = true
	cfg.Cache.RedisAddr = mr.Addr()

	store, rdb, err := openStore(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, store)
	require.NotNil(t, rdb)
	t.Cleanup(func() { _ = store.Close(); _ = rdb.Close() })
}

func TestOpenStore_CacheDisabled(t *testing.T) {
	cfg := config.Config{}
	cfg.Backend.Kind = "mem"
	cfg.Cache.Enabled = false

	store, rdb, err := openStore(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, store)
	require.Nil(t, rdb)
	t.Cleanup(func() { _ = store.Close() })
}

// TestGracefulStop_EscalatesOnStuckRPC proves shutdown cannot hang: with an RPC
// held open, GracefulStop alone would block forever, so gracefulStop must fall
// back to Stop within the grace window.
func TestGracefulStop_EscalatesOnStuckRPC(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	be := openMem(t)
	storagev0.RegisterObjectStorageServer(s, server.New(be, events.NewHub(be.Name(), be.Identity(), nil), probetest.Monitor(t, be)))
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := storagev0.NewObjectStorageClient(conn)

	// Open a Put and send only the header — the server handler now blocks in
	// Recv waiting for more frames, keeping the RPC in flight.
	st, err := client.Put(context.Background())
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{
		Kind: &storagev0.PutRequest_Header{Header: &storagev0.PutHeader{Key: "k", TotalSize: 1024}},
	}))

	grace := 300 * time.Millisecond
	start := time.Now()
	gracefulStop(s, grace)
	elapsed := time.Since(start)

	require.GreaterOrEqual(t, elapsed, grace, "should wait the grace window before forcing")
	require.Less(t, elapsed, 5*time.Second, "must not hang on the in-flight RPC")
}

// TestServeHealthAnswersKubernetesProbes drives the health surface the
// deployment manifest points at: the ObjectStorage service carries readiness
// and the overall service carries liveness, over a real gRPC health client.
func TestServeHealthAnswersKubernetesProbes(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	be := openMem(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveHealth(ctx, s, be, config.HealthConfig{Interval: 50 * time.Millisecond, Timeout: time.Second})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := healthv1.NewHealthClient(conn)

	check := func(service string) healthv1.HealthCheckResponse_ServingStatus {
		res, cerr := client.Check(context.Background(), &healthv1.HealthCheckRequest{Service: service})
		if cerr != nil {
			require.Equal(t, codes.NotFound, status.Code(cerr))
			return healthv1.HealthCheckResponse_UNKNOWN
		}
		return res.GetStatus()
	}

	require.Equal(t, healthv1.HealthCheckResponse_SERVING, check(""), "liveness serves while the process serves")
	require.Eventually(t, func() bool {
		return check(storagev0.ObjectStorage_ServiceDesc.ServiceName) == healthv1.HealthCheckResponse_SERVING
	}, 5*time.Second, 20*time.Millisecond, "readiness must follow the backend probe")
}
