package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/resources"
)

// Only a dependency's container view is admitted — the gateway runs in a
// container — and a service with no dependency admits nothing.
func TestComposedEndpointsAreTheDependenciesContainerView(t *testing.T) {
	ctx := context.Background()
	mappings := []*basev0.NetworkMapping{{Instances: []*basev0.NetworkInstance{
		{Access: resources.NewNativeNetworkAccess(), Host: "localhost", Port: 56826},
		{Access: resources.NewContainerNetworkAccess(), Host: "host.docker.internal", Port: 56826},
	}}, {Instances: []*basev0.NetworkInstance{
		{Access: resources.NewNativeNetworkAccess(), Host: "localhost", Port: 5432},
	}}}
	require.Equal(t, []string{"host.docker.internal:56826"}, composedEndpoints(ctx, mappings))
	require.Empty(t, composedEndpoints(ctx, nil))
}
