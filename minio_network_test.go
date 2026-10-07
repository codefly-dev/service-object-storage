package main

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/network"
	"github.com/stretchr/testify/require"

	dockerrun "github.com/codefly-dev/core/runners/dockerrun"
)

// fakeNetworks is a Docker daemon's network table: name → attached container
// IDs and their aliases. Failures are injected per call.
type fakeNetworks struct {
	networks map[string]map[string][]string
	created  []network.CreateOptions
	calls    []string
	closed   int

	// createdBehind makes NetworkCreate fail after another run created it.
	createdBehind bool
	connectErr    error
	removeErr     error
}

func newFakeNetworks() *fakeNetworks {
	return &fakeNetworks{networks: map[string]map[string][]string{}}
}

func (f *fakeNetworks) NetworkInspect(_ context.Context, name string, _ network.InspectOptions) (network.Inspect, error) {
	f.calls = append(f.calls, "inspect")
	members, ok := f.networks[name]
	if !ok {
		return network.Inspect{}, errdefs.ErrNotFound
	}
	containers := map[string]network.EndpointResource{}
	for id := range members {
		containers[id] = network.EndpointResource{}
	}
	return network.Inspect{Name: name, Containers: containers}, nil
}

func (f *fakeNetworks) NetworkCreate(_ context.Context, name string, options network.CreateOptions) (network.CreateResponse, error) {
	f.calls = append(f.calls, "create")
	if f.createdBehind {
		f.networks[name] = map[string][]string{}
		return network.CreateResponse{}, errdefs.ErrConflict
	}
	f.created = append(f.created, options)
	f.networks[name] = map[string][]string{}
	return network.CreateResponse{ID: name}, nil
}

func (f *fakeNetworks) NetworkConnect(_ context.Context, name, containerID string, config *network.EndpointSettings) error {
	f.calls = append(f.calls, "connect")
	if f.connectErr != nil {
		return f.connectErr
	}
	members, ok := f.networks[name]
	if !ok {
		return errdefs.ErrNotFound
	}
	if _, attached := members[containerID]; attached {
		return errdefs.ErrPermissionDenied // what the daemon says for a duplicate endpoint
	}
	members[containerID] = slices.Clone(config.Aliases)
	return nil
}

func (f *fakeNetworks) NetworkRemove(_ context.Context, name string) error {
	f.calls = append(f.calls, "remove")
	if f.removeErr != nil {
		return f.removeErr
	}
	members, ok := f.networks[name]
	if !ok {
		return errdefs.ErrNotFound
	}
	if len(members) > 0 {
		return errdefs.ErrPermissionDenied
	}
	delete(f.networks, name)
	return nil
}

func (f *fakeNetworks) Close() error {
	f.closed++
	return nil
}

func newNetworkRuntime(t *testing.T, fake *fakeNetworks) *Runtime {
	t.Helper()
	rt := newProjectionRuntime(t, t.TempDir(), "")
	rt.conf.backend = "minio"
	rt.networkClient = func() (dockerNetworks, error) { return fake, nil }
	return rt
}

// TestMinIONetworkIsSharedByAlias pins the topology: one bridge network per
// service, named like its containers, with MinIO under its alias and the
// gateway beside it, and every client the steps open is closed.
func TestMinIONetworkIsSharedByAlias(t *testing.T) {
	fake := newFakeNetworks()
	rt := newNetworkRuntime(t, fake)
	ctx := context.Background()

	require.NoError(t, rt.joinMinIONetwork(ctx, "minio-id", minioNetworkAlias))
	require.NoError(t, rt.joinMinIONetwork(ctx, "gateway-id"))

	name := rt.minioNetworkName()
	require.Equal(t, dockerrun.ContainerName(rt.UniqueWithWorkspace()+"-network"), name)
	require.Equal(t, name, rt.minioNetwork)
	require.Len(t, fake.created, 1, "the second join reuses the network")
	require.Equal(t, "bridge", fake.created[0].Driver)
	require.Equal(t, "true", fake.created[0].Labels[dockerrun.LabelCodeflyOwner])
	require.Equal(t, []string{minioNetworkAlias}, fake.networks[name]["minio-id"])
	require.Contains(t, fake.networks[name], "gateway-id")
	require.Equal(t, 2, fake.closed)
}

// TestMinIONetworkJoinIsIdempotent covers a container core reused rather than
// recreated — already attached — and a network a concurrent run created
// between this run's inspect and create.
func TestMinIONetworkJoinIsIdempotent(t *testing.T) {
	ctx := context.Background()

	fake := newFakeNetworks()
	rt := newNetworkRuntime(t, fake)
	require.NoError(t, rt.joinMinIONetwork(ctx, "minio-id", minioNetworkAlias))
	require.NoError(t, rt.joinMinIONetwork(ctx, "minio-id", minioNetworkAlias))

	raced := newFakeNetworks()
	raced.createdBehind = true
	rt = newNetworkRuntime(t, raced)
	require.NoError(t, rt.joinMinIONetwork(ctx, "minio-id", minioNetworkAlias))
	require.Contains(t, raced.networks[rt.minioNetworkName()], "minio-id")
}

// TestMinIONetworkJoinFailureIsReported keeps a refused attach visible: a
// gateway left off the network could never reach its backend.
func TestMinIONetworkJoinFailureIsReported(t *testing.T) {
	fake := newFakeNetworks()
	fake.connectErr = errors.New("daemon refused")
	rt := newNetworkRuntime(t, fake)
	err := rt.joinMinIONetwork(context.Background(), "gateway-id")
	require.ErrorContains(t, err, "daemon refused")
	require.ErrorContains(t, err, rt.minioNetworkName())
}

// TestMinIONetworkRemoval: the network goes with the containers, survives for a
// successor still attached to it, and a real failure is reported.
func TestMinIONetworkRemoval(t *testing.T) {
	ctx := context.Background()

	t.Run("nothing joined opens no client", func(t *testing.T) {
		rt := newNetworkRuntime(t, nil)
		rt.networkClient = func() (dockerNetworks, error) { return nil, errors.New("must not be called") }
		require.NoError(t, rt.removeMinIONetwork(ctx))
	})
	t.Run("removed once empty", func(t *testing.T) {
		fake := newFakeNetworks()
		rt := newNetworkRuntime(t, fake)
		require.NoError(t, rt.joinMinIONetwork(ctx, "minio-id", minioNetworkAlias))
		delete(fake.networks[rt.minioNetwork], "minio-id") // core removed the container
		name := rt.minioNetwork
		require.NoError(t, rt.removeMinIONetwork(ctx))
		require.NotContains(t, fake.networks, name)
		require.Empty(t, rt.minioNetwork)
	})
	t.Run("already gone", func(t *testing.T) {
		fake := newFakeNetworks()
		rt := newNetworkRuntime(t, fake)
		rt.minioNetwork = rt.minioNetworkName()
		require.NoError(t, rt.removeMinIONetwork(ctx))
	})
	t.Run("kept for a successor", func(t *testing.T) {
		fake := newFakeNetworks()
		rt := newNetworkRuntime(t, fake)
		require.NoError(t, rt.joinMinIONetwork(ctx, "successor-minio", minioNetworkAlias))
		name := rt.minioNetwork
		require.NoError(t, rt.removeMinIONetwork(ctx))
		require.Contains(t, fake.networks[name], "successor-minio")
		require.Empty(t, rt.minioNetwork, "the stale run no longer owns it")
	})
	t.Run("failure on an empty network is reported", func(t *testing.T) {
		fake := newFakeNetworks()
		rt := newNetworkRuntime(t, fake)
		require.NoError(t, rt.joinMinIONetwork(ctx, "minio-id", minioNetworkAlias))
		delete(fake.networks[rt.minioNetwork], "minio-id")
		fake.removeErr = errors.New("daemon unreachable")
		require.ErrorContains(t, rt.removeMinIONetwork(ctx), "daemon unreachable")
		require.NotEmpty(t, rt.minioNetwork, "kept so a later teardown retries")
	})
}

// TestTeardownRemovesNetworkAfterContainers: the network is the last Docker
// object teardown releases, and it is released even when a container shutdown
// failed and left an endpoint on it — in which case it stays.
func TestTeardownRemovesNetworkAfterContainers(t *testing.T) {
	ctx := context.Background()
	fake := newFakeNetworks()
	rt := newNetworkRuntime(t, fake)
	require.NoError(t, rt.joinMinIONetwork(ctx, "minio-id", minioNetworkAlias))
	name := rt.minioNetwork

	minio := &stubEnvironment{err: errors.New("daemon unreachable")}
	rt.minioEnv = minio
	require.Error(t, rt.teardown(ctx))
	require.Contains(t, fake.networks, name, "a container still on it keeps the network")

	minio.err = nil
	rt.minioEnv = minio
	rt.minioNetwork = name
	delete(fake.networks[name], "minio-id")
	require.NoError(t, rt.teardown(ctx))
	require.NotContains(t, fake.networks, name)
	require.Empty(t, rt.minioNetwork)
}
