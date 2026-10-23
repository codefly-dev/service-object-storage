package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/resources"
)

// instance builds a mapping instance the way core does, where Host already
// carries the port — the shape a live run hands the Runtime.
func instance(access *basev0.NetworkAccess, hostname string, port uint16) *basev0.NetworkInstance {
	inst := resources.NewNetworkInstance(hostname, port)
	inst.Access = access
	return inst
}

// Only a dependency's container view is admitted — the gateway runs in a
// container — and a service with no dependency admits nothing.
func TestComposedEndpointsAreTheDependenciesContainerView(t *testing.T) {
	ctx := context.Background()
	mappings := []*basev0.NetworkMapping{{Instances: []*basev0.NetworkInstance{
		instance(resources.NewNativeNetworkAccess(), "localhost", 56826),
		instance(resources.NewContainerNetworkAccess(), "host.docker.internal", 56826),
	}}, {Instances: []*basev0.NetworkInstance{
		instance(resources.NewNativeNetworkAccess(), "localhost", 5432),
	}}}
	require.Equal(t, []string{"host.docker.internal:56826"}, composedEndpoints(ctx, mappings))
	require.Empty(t, composedEndpoints(ctx, nil))
}
