//go:build e2e

package main

import (
	"context"
	"os"
	"testing"

	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/stretchr/testify/require"
)

// TestInitHoldsTheGatewayPortUntilTheGatewayStarts is the port race on the real
// Init, with real containers: the run assigns the gateway its host port, and the
// gateway is the last thing Init starts. At the moment MinIO's container starts
// the gateway's port must be bound by the agent — so MinIO's own port, and any
// other process asking the kernel for one, cannot be given it — and at the
// moment the gateway's container starts the agent must have let go of it. When
// Init returns the agent holds nothing.
func TestInitHoldsTheGatewayPortUntilTheGatewayStarts(t *testing.T) {
	if os.Getenv("SOS_GATEWAY_IMAGE") == "" {
		t.Skip("SOS_GATEWAY_IMAGE not set; skipping agent-runtime end-to-end test")
	}
	ctx := context.Background()
	rt, mappings, runtimeContext := loadedRuntime(t, ctx)
	defer func() { _, _ = rt.Destroy(context.Background(), &runtimev0.DestroyRequest{}) }()

	assigned, err := resources.FindNetworkInstanceInNetworkMappings(ctx, mappings, rt.GrpcEndpoint, resources.NewNativeNetworkAccess())
	require.NoError(t, err)
	gateway := uint16(assigned.GetPort())

	var started []string
	rt.starting = func(container string) {
		started = append(started, container)
		switch container {
		case "minio":
			require.True(t, rt.ports.holds(gateway), "the agent does not hold the gateway's port while MinIO starts")
			requireUnbindable(t, gateway, "the gateway's port is free for the whole MinIO start")
			require.NotEqual(t, gateway, rt.minioHostPort, "MinIO was given the gateway's port")
			require.False(t, rt.ports.holds(rt.minioHostPort), "Docker cannot bind a port the agent still holds")
		case "gateway":
			require.False(t, rt.ports.holds(gateway), "Docker cannot bind a port the agent still holds")
		}
	}

	init, err := rt.Init(ctx, &runtimev0.InitRequest{RuntimeContext: runtimeContext, ProposedNetworkMappings: mappings})
	require.NoError(t, err)
	require.Equal(t, runtimev0.InitStatus_READY, init.GetStatus().GetState(), init.GetStatus().GetMessage())
	require.Equal(t, []string{"minio", "gateway"}, started)
	require.False(t, rt.ports.holds(gateway))
	require.False(t, rt.ports.holds(rt.minioHostPort))
	require.Empty(t, rt.ports.held, "Init returned still holding a port")
}
