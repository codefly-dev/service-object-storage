package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/network"

	dockerrun "github.com/codefly-dev/core/runners/dockerrun"
)

// minioNetworkAlias is the name the gateway dials MinIO by on the service's own
// network. The network is private to one service in one workspace, so a fixed
// alias cannot collide with another service's MinIO.
const minioNetworkAlias = "minio"

// dockerNetworks is the part of the Docker API the agent uses to give the
// gateway and MinIO a network of their own. Core's runner has no network
// primitive, so the agent drives these calls itself, through the same client
// core constructs (DOCKER_HOST, the active Docker context).
type dockerNetworks interface {
	NetworkInspect(ctx context.Context, networkID string, options network.InspectOptions) (network.Inspect, error)
	NetworkCreate(ctx context.Context, name string, options network.CreateOptions) (network.CreateResponse, error)
	NetworkConnect(ctx context.Context, networkID, containerID string, config *network.EndpointSettings) error
	NetworkRemove(ctx context.Context, networkID string) error
	Close() error
}

func newDockerNetworks() (dockerNetworks, error) {
	return dockerrun.NewClient()
}

// minioNetworkName names the service's network after its containers, so it is
// scoped to the workspace exactly as they are.
func (s *Runtime) minioNetworkName() string {
	return dockerrun.ContainerName(s.UniqueWithWorkspace() + "-network")
}

// joinMinIONetwork attaches a running container to the service's network,
// creating the network first if it does not exist yet.
//
// The gateway reaches MinIO here by alias on MinIO's container port. Reaching
// it through a published host port and host.docker.internal instead depends on
// how the daemon routes container-to-host traffic: a rootless daemon run with
// --disable-host-loopback refuses it, and a host firewall can too. A
// user-defined bridge network is resolved and routed by the container engine
// itself, on rootful and rootless Docker, Podman and Docker Desktop alike.
//
// Both steps are idempotent: a network another run already created is reused,
// and a container already attached (one core reused rather than recreated)
// stays attached.
func (s *Runtime) joinMinIONetwork(ctx context.Context, containerID string, aliases ...string) error {
	cli, err := s.networks()
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	name := s.minioNetworkName()
	if err = ensureNetwork(ctx, cli, name); err != nil {
		return err
	}
	s.minioNetwork = name
	connectErr := cli.NetworkConnect(ctx, name, containerID, &network.EndpointSettings{Aliases: aliases})
	if connectErr == nil {
		return nil
	}
	inspect, err := cli.NetworkInspect(ctx, name, network.InspectOptions{})
	if err == nil {
		if _, attached := inspect.Containers[containerID]; attached {
			return nil
		}
	}
	return fmt.Errorf("attach container %s to network %s: %w", containerID, name, connectErr)
}

// ensureNetwork creates the named bridge network unless it already exists. A
// create that fails because a concurrent run created it first is not an error.
func ensureNetwork(ctx context.Context, cli dockerNetworks, name string) error {
	_, err := cli.NetworkInspect(ctx, name, network.InspectOptions{})
	if err == nil {
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return fmt.Errorf("inspect network %s: %w", name, err)
	}
	_, createErr := cli.NetworkCreate(ctx, name, network.CreateOptions{
		Driver: "bridge",
		Labels: map[string]string{
			dockerrun.LabelCodeflyOwner: "true",
			dockerrun.LabelCodeflyName:  name,
		},
	})
	if createErr == nil {
		return nil
	}
	if _, err = cli.NetworkInspect(ctx, name, network.InspectOptions{}); err == nil {
		return nil
	}
	return fmt.Errorf("create network %s: %w", name, createErr)
}

// removeMinIONetwork removes the service's network once no container is left on
// it. A network that still has endpoints belongs to a successor run of the same
// service — the stale-teardown path — and is left in place for it; a missing
// one is already gone. The network holds no data, so removing it never touches
// what MinIO retains.
func (s *Runtime) removeMinIONetwork(ctx context.Context) error {
	if s.minioNetwork == "" {
		return nil
	}
	cli, err := s.networks()
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	removeErr := cli.NetworkRemove(ctx, s.minioNetwork)
	if removeErr != nil && !errdefs.IsNotFound(removeErr) {
		inspect, err := cli.NetworkInspect(ctx, s.minioNetwork, network.InspectOptions{})
		switch {
		case errdefs.IsNotFound(err):
		case err == nil && len(inspect.Containers) > 0:
		default:
			return fmt.Errorf("remove network %s: %w", s.minioNetwork, errors.Join(removeErr, err))
		}
	}
	s.minioNetwork = ""
	return nil
}

// networks opens the Docker API the network steps use; tests substitute it.
func (s *Runtime) networks() (dockerNetworks, error) {
	if s.networkClient != nil {
		return s.networkClient()
	}
	return newDockerNetworks()
}
