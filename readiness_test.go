package main

import (
	"context"
	"net"
	"testing"
	"time"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

// serveGateway runs a real gRPC gateway over be on a real socket and returns its
// address.
func serveGateway(t *testing.T, be backend.Backend) string {
	t.Helper()
	hub := events.NewHub(be.Name(), be.Identity(), nil)
	t.Cleanup(hub.Close)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(srv, server.New(be, hub, probetest.Monitor(t, be)))
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	return ln.Addr().String()
}

// runtimeAt returns a loaded Runtime whose gRPC endpoint maps to address over
// the given access kinds.
func runtimeAt(t *testing.T, address string, access ...*basev0.NetworkAccess) *Runtime {
	t.Helper()
	rt := NewRuntime()
	require.NoError(t, rt.HeadlessLoad(context.Background(), &basev0.ServiceIdentity{
		Workspace: "readiness", Module: "module", Name: "storage", Version: "0.0.0",
	}))
	rt.Runtime.WithContext(resources.NewRuntimeContextNative())
	rt.GrpcEndpoint = &basev0.Endpoint{Name: "grpc", Module: "module", Service: "storage", Api: "grpc"}

	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	p, err := net.LookupPort("tcp", port)
	require.NoError(t, err)

	instances := make([]*basev0.NetworkInstance, 0, len(access))
	for _, a := range access {
		inst := resources.NewNetworkInstance(host, uint16(p))
		inst.Access = a
		instances = append(instances, inst)
	}
	rt.NetworkMappings = []*basev0.NetworkMapping{{Endpoint: rt.GrpcEndpoint, Instances: instances}}
	return rt
}

// TestWaitForReadyRejectsUnreachableBackend is the acceptance contract for the
// readiness defect: a real gateway over the real MinIO client pointed at an
// endpoint that is not there answers Capabilities from its static feature
// table, and readiness must not follow it.
func TestWaitForReadyRejectsUnreachableBackend(t *testing.T) {
	ctx := context.Background()
	address := serveGateway(t, probetest.UnreachableMinIO(t, "readiness"))

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	client := storagev0.NewObjectStorageClient(conn)

	caps, err := client.Capabilities(ctx, &storagev0.CapabilitiesRequest{})
	require.NoError(t, err, "Capabilities is static introspection and still answers")
	require.Equal(t, "minio", caps.GetBackend())

	ready, err := client.Ready(ctx, &storagev0.ReadyRequest{})
	require.NoError(t, err)
	require.False(t, ready.GetReady())
	require.Equal(t, "Unavailable", ready.GetCode())
	require.NotEmpty(t, ready.GetDetail())
	require.NotZero(t, ready.GetCheckedAtUnixMs())

	rt := runtimeAt(t, address, resources.NewNativeNetworkAccess())
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = rt.WaitForReady(waitCtx)
	require.Error(t, err, "an unreachable backend must not be reported ready")
	require.Contains(t, err.Error(), "Unavailable", "the backend cause must reach the caller")
}

// TestWaitForReadySucceedsOnReachableBackend is the other half of the contract:
// a gateway whose store answers is reported ready.
func TestWaitForReadySucceedsOnReachableBackend(t *testing.T) {
	be, err := mem.New(context.Background(), backend.Config{Bucket: "readiness"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	rt := runtimeAt(t, serveGateway(t, be), resources.NewNativeNetworkAccess())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, rt.WaitForReady(ctx))
}

// TestWaitForReadyUsesNativeNetworkView pins the address family the agent
// probes with. The agent is a host process even when the service it started
// runs in a container, so it must take the native instance; dialing the
// container-only name resolves to nothing here.
func TestWaitForReadyUsesNativeNetworkView(t *testing.T) {
	be, err := mem.New(context.Background(), backend.Config{Bucket: "readiness"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	address := serveGateway(t, be)
	rt := runtimeAt(t, address, resources.NewNativeNetworkAccess())
	// A container instance the agent cannot resolve, alongside the native one.
	container := resources.NewNetworkInstance("host.docker.internal", 1)
	container.Access = resources.NewContainerNetworkAccess()
	rt.NetworkMappings[0].Instances = append([]*basev0.NetworkInstance{container}, rt.NetworkMappings[0].Instances...)
	rt.Runtime.WithContext(resources.NewRuntimeContextContainer())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, rt.WaitForReady(ctx))
}

// TestWaitForReadyReturnsOnCancellation proves the retry loop is cancellable:
// before, the loop slept through cancellation and ran its full retry count.
func TestWaitForReadyReturnsOnCancellation(t *testing.T) {
	rt := runtimeAt(t, serveGateway(t, probetest.UnreachableMinIO(t, "readiness")), resources.NewNativeNetworkAccess())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := rt.WaitForReady(ctx)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, readinessBudget, "cancellation must cut the wait short of its budget")
}
