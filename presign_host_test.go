package main

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/shared"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/config"
)

// TestLocalMinIOSignsForTheConfiguredHost is the agent-to-gateway relation
// behind the broken workbook viewer: the gateway dials the agent's MinIO as
// host.docker.internal, a name no host process resolves, and presigned URLs
// used to name it (or, where an operator substituted it, the LAN address the
// next DHCP lease invalidated). The environment the Runtime hands the gateway
// is fed through the gateway's own configuration and backend here, so the
// assertion is on the URL a caller actually receives.
func TestLocalMinIOSignsForTheConfiguredHost(t *testing.T) {
	rt := NewRuntime()
	require.NoError(t, rt.LoadConfiguration(context.Background(), nil))
	rt.conf.presignHost = "localhost"
	rt.minioHostPort = 63947
	rt.minioPassword = "per-run-synthetic"
	rt.gatewayToken = "per-run-token"
	rt.pointGatewayAtLocalMinIO()

	env := map[string]string{}
	for _, e := range rt.gatewayEnvironment() {
		env[e.Key] = fmt.Sprint(e.Value)
	}
	require.Equal(t, "http://host.docker.internal:63947", env["SOS_ENDPOINT"], "the gateway keeps dialling MinIO over the container bridge")
	require.Equal(t, "public", env["SOS_PRESIGN_ORIGIN"])
	require.Equal(t, "http://localhost:63947", env["SOS_PUBLIC_ENDPOINT"])

	for k, v := range env {
		t.Setenv(k, v)
	}
	cfg, err := config.FromEnv()
	require.NoError(t, err)
	be, err := backend.Open(context.Background(), cfg.Backend)
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	res, err := be.Presign(context.Background(), "workbooks/book.xlsx", backend.PresignGet, time.Minute)
	require.NoError(t, err)
	u, err := url.Parse(res.URL)
	require.NoError(t, err)
	require.Equal(t, "localhost:63947", u.Host, "a caller on this machine must be able to fetch the URL: %s", res.URL)
}

func TestLocalPublicEndpoint(t *testing.T) {
	require.Equal(t, "http://localhost:9000", localPublicEndpoint("localhost", 9000))
	require.Equal(t, "http://127.0.0.1:9000", localPublicEndpoint("127.0.0.1", 9000))
	require.Equal(t, "http://[::1]:9000", localPublicEndpoint("::1", 9000))
}

func TestValidatePresignHost(t *testing.T) {
	for _, ok := range []string{"localhost", "127.0.0.1", "::1", "dev-box.internal"} {
		require.NoError(t, validatePresignHost(ok), ok)
	}
	require.ErrorContains(t, validatePresignHost(""), "presign-host is required")
	for _, bad := range []string{"http://localhost", "localhost:9000", "localhost/x", "user@localhost"} {
		require.ErrorContains(t, validatePresignHost(bad), "bare host name", bad)
	}
}

// TestCheckPresignConfiguration: the agent's own MinIO needs presign-host and
// refuses an endpoint-shaped setting it could never honour (the port is
// allocated per run); a store the agent does not run takes the gateway's
// settings as configured.
func TestCheckPresignConfiguration(t *testing.T) {
	local := func(mut func(*resolved)) *Runtime {
		rt := NewRuntime()
		require.NoError(t, rt.LoadConfiguration(context.Background(), nil))
		mut(&rt.conf)
		return rt
	}
	require.ErrorContains(t, local(func(*resolved) {}).checkPresignConfiguration(), "presign-host is required")
	require.NoError(t, local(func(r *resolved) { r.presignHost = "localhost" }).checkPresignConfiguration())
	require.ErrorContains(t, local(func(r *resolved) {
		r.presignHost = "localhost"
		r.publicEndpoint = "http://localhost:9000"
	}).checkPresignConfiguration(), "cannot be configured for the agent-managed local MinIO")
	require.ErrorContains(t, local(func(r *resolved) {
		r.presignHost = "localhost"
		r.presignOrigin = "endpoint"
	}).checkPresignConfiguration(), "cannot be configured for the agent-managed local MinIO")
	require.NoError(t, local(func(r *resolved) {
		r.backend = "s3"
		r.presignOrigin = "public"
		r.publicEndpoint = "http://localhost:9000"
	}).checkPresignConfiguration())
}

// TestLoadConfigurationReadsPresignSettings covers both delivery channels: the
// spec's presign-host and its SOS_PRESIGN_HOST override, and the gateway's
// SOS_PRESIGN_ORIGIN / SOS_PUBLIC_ENDPOINT passed through configuration.
func TestLoadConfigurationReadsPresignSettings(t *testing.T) {
	svc := NewService()
	svc.PresignHost = "localhost"
	require.NoError(t, svc.LoadConfiguration(context.Background(), nil))
	require.Equal(t, "localhost", svc.conf.presignHost)

	conf := &basev0.Configuration{Infos: []*basev0.ConfigurationInformation{{
		Name: "object-storage",
		ConfigurationValues: []*basev0.ConfigurationValue{
			{Key: "SOS_PRESIGN_HOST", Value: "dev-box.internal\n"},
			{Key: "SOS_PRESIGN_ORIGIN", Value: "public"},
			{Key: "SOS_PUBLIC_ENDPOINT", Value: "https://objects.example.test\n"},
		},
	}}}
	require.NoError(t, svc.LoadConfiguration(context.Background(), conf))
	require.Equal(t, "dev-box.internal", svc.conf.presignHost)
	require.Equal(t, "public", svc.conf.presignOrigin)
	require.Equal(t, "https://objects.example.test", svc.conf.publicEndpoint)
}

// TestInitRefusesLocalRunWithoutPresignHost: the refusal happens before any
// container exists. Docker is pointed at a socket that does not exist, so a
// Runtime that skipped the check would fail on Docker instead, naming the
// wrong cause.
func TestInitRefusesLocalRunWithoutPresignHost(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix://"+filepath.Join(t.TempDir(), "no-docker.sock"))
	t.Setenv(resources.CodeflyHomeEnv, t.TempDir())
	rt := runtimeAt(t, "127.0.0.1:9464", resources.NewNativeNetworkAccess())

	resp, err := rt.Init(context.Background(), &runtimev0.InitRequest{
		RuntimeContext:          resources.NewRuntimeContextNative(),
		ProposedNetworkMappings: rt.NetworkMappings,
	})
	require.NoError(t, err)
	require.Equal(t, runtimev0.InitStatus_ERROR, resp.GetStatus().GetState())
	require.Contains(t, resp.GetStatus().GetMessage(), "presign-host is required")
	require.Nil(t, rt.minioEnv, "no MinIO may be started for a run that cannot name its presign host")
	require.Nil(t, rt.gatewayEnv)
}

// TestDeployRendersConfiguredPresignOrigin: a deployment against a store the
// gateway signs for (minio or an S3-compatible endpoint) needs
// SOS_PRESIGN_ORIGIN, and the manifest carries the configured choice. Nothing
// is rendered when nothing was configured: the gateway, not the render, owns
// the refusal, because the environment binding lands after the render.
func TestDeployRendersConfiguredPresignOrigin(t *testing.T) {
	ctx := context.Background()
	builder := newDeployBuilder(t, ctx)
	destination := t.TempDir()
	resp, err := builder.Deploy(ctx, deployRequest(destination, map[string]string{
		"SOS_BACKEND":         "minio",
		"SOS_ENDPOINT":        "http://minio.storage.svc:9000",
		"SOS_PRESIGN_ORIGIN":  "public",
		"SOS_PUBLIC_ENDPOINT": "https://objects.example.test",
	}))
	require.NoError(t, err)
	require.Equal(t, builderv0.DeploymentStatus_SUCCESS, resp.GetState().GetState(), resp.GetState().GetMessage())
	env := containerEnv(t, destination)
	require.Equal(t, "http://minio.storage.svc:9000", env["SOS_ENDPOINT"])
	require.Equal(t, "public", env["SOS_PRESIGN_ORIGIN"])
	require.Equal(t, "https://objects.example.test", env["SOS_PUBLIC_ENDPOINT"])

	builder = newDeployBuilder(t, ctx)
	destination = t.TempDir()
	resp, err = builder.Deploy(ctx, deployRequest(destination, map[string]string{"SOS_BACKEND": "gcs"}))
	require.NoError(t, err)
	require.Equal(t, builderv0.DeploymentStatus_SUCCESS, resp.GetState().GetState(), resp.GetState().GetMessage())
	env = containerEnv(t, destination)
	require.NotContains(t, env, "SOS_PRESIGN_ORIGIN")
	require.NotContains(t, env, "SOS_PUBLIC_ENDPOINT")
}

// TestCreatedServiceStatesItsPresignHost: a service scaffolded by the Builder
// carries presign-host in its own spec, where the caller sees and can change
// it, so the Runtime that loads the spec has the answer its refusal asks for.
func TestCreatedServiceStatesItsPresignHost(t *testing.T) {
	ctx := context.Background()
	t.Setenv(resources.CodeflyHomeEnv, t.TempDir())
	dir := t.TempDir()
	service := resources.Service{Name: "storage", Version: "0.0.0"}
	require.NoError(t, service.SaveAtDir(ctx, filepath.Join(dir, "mod", service.Name)))
	identity := &basev0.ServiceIdentity{
		Name: service.Name, Version: service.Version, Module: "mod", Workspace: "ws",
		WorkspacePath: dir, RelativeToWorkspace: "mod/" + service.Name,
	}

	builder := NewBuilder()
	_, err := builder.Load(ctx, &builderv0.LoadRequest{DisableCatch: true, Identity: identity, CreationMode: &builderv0.CreationMode{Communicate: false}})
	require.NoError(t, err)
	_, err = builder.Create(ctx, &builderv0.CreateRequest{})
	require.NoError(t, err)

	rt := NewRuntime()
	_, err = rt.Load(ctx, &runtimev0.LoadRequest{Identity: identity, Environment: shared.Must(resources.LocalEnvironment().Proto()), DisableCatch: true})
	require.NoError(t, err)
	require.Equal(t, "localhost", rt.PresignHost)
}
