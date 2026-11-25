package main

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// requireUnbindable asserts a loopback port is bound by someone: a probe for a
// free port can never be handed it.
func requireUnbindable(t *testing.T, port uint16, why string) {
	t.Helper()
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))))
	if err == nil {
		_ = listener.Close()
	}
	require.Error(t, err, why)
}

// portObservingEnvironment is a container environment that looks, at the moment
// it is started, at which host ports the agent still holds.
type portObservingEnvironment struct {
	recordingEnvironment
	observe func()
}

func (e *portObservingEnvironment) Init(ctx context.Context) error {
	e.observe()
	return e.recordingEnvironment.Init(ctx)
}

// TestGatewayPortIsHeldWhileMinIOStarts pins the port race a composed run hit:
// "failed to bind host port 0.0.0.0:57062/tcp: address already in use". The
// gateway's port was free from Init's first line to the gateway's start, and
// MinIO's port was a probe closed before use, so the kernel could hand either
// to someone else — the probe included. Each port is bound by the agent from
// the moment it is known, and let go only at the start of the container that
// publishes it: MinIO starting does not free the gateway's.
func TestGatewayPortIsHeldWhileMinIOStarts(t *testing.T) {
	rt := newNetworkRuntime(t, newFakeNetworks())
	t.Cleanup(rt.ports.releaseAll)
	ctx := context.Background()
	calls := &[]string{}

	gateway, err := rt.ports.claimFree()
	require.NoError(t, err)
	minio, err := rt.minioPort(ctx)
	require.NoError(t, err)
	require.NotEqual(t, gateway, minio)
	requireUnbindable(t, gateway, "the gateway's port is free while MinIO's is being chosen")
	requireUnbindable(t, minio, "MinIO's port was probed and closed: the kernel can hand it out again")

	minioEnv := &portObservingEnvironment{recordingEnvironment: recordingEnvironment{name: "minio", calls: calls}}
	minioEnv.observe = func() {
		require.False(t, rt.ports.holds(minio), "Docker cannot bind a port the agent still holds")
		requireUnbindable(t, gateway, "the gateway's port is free for the whole MinIO start")
	}
	require.NoError(t, rt.startContainer(ctx, "minio", minioEnv, minio))

	gatewayEnv := &portObservingEnvironment{recordingEnvironment: recordingEnvironment{name: "gateway", calls: calls}}
	gatewayEnv.observe = func() {
		require.False(t, rt.ports.holds(gateway), "Docker cannot bind a port the agent still holds")
	}
	require.NoError(t, rt.startContainer(ctx, "gateway", gatewayEnv, gateway))
	require.Equal(t, []string{"start minio", "start gateway"}, *calls)
}

// TestClaimedPortsAreNeverOfferedAgain: a port the agent picks for itself is
// held from the moment the kernel names it, so asking again cannot return it or
// any other port the agent holds.
func TestClaimedPortsAreNeverOfferedAgain(t *testing.T) {
	var ports hostPorts
	t.Cleanup(ports.releaseAll)
	seen := map[uint16]bool{}
	for range 64 {
		port, err := ports.claimFree()
		require.NoError(t, err)
		require.False(t, seen[port], "port %d was offered twice", port)
		seen[port] = true
		require.True(t, ports.holds(port))
	}
	ports.releaseAll()
	for port := range seen {
		require.False(t, ports.holds(port))
	}
}

// TestClaimLeavesAPortSomeoneElseHolds: a port that is already bound is the
// service's own kept container or a stranger Docker will name; the agent
// neither fails on it nor closes what it does not own.
func TestClaimLeavesAPortSomeoneElseHolds(t *testing.T) {
	var ports hostPorts
	t.Cleanup(ports.releaseAll)
	first, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	port := uint16(first.Addr().(*net.TCPAddr).Port)

	ports.claim(port)
	ports.handOver(port)
	requireUnbindable(t, port, "handing over closed a listener the agent does not own")
}
