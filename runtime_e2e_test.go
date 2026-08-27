//go:build e2e

// This file drives the agent Runtime through its real lifecycle — Load → Init
// → Start — standing up the gateway and a local MinIO container exactly as
// `codefly run` does, then performs a real Put/Get over the advertised gRPC
// endpoint. It replaces the manual grpcurl check the runtime path was validated
// with. Run with:
//
//	SOS_GATEWAY_IMAGE=<locally-built gateway image> go test -tags e2e .
//
// The gateway image is a prerequisite because the release pipeline that
// publishes ghcr.io/codefly-dev/service-object-storage is a separate workstream;
// the test points the Runtime at a locally-built image through the same
// SOS_GATEWAY_IMAGE override the Runtime already honors.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/codefly-dev/core/network"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/shared"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
)

// TestRuntimeEndToEndPutGet scaffolds a service through the Builder, drives the
// Runtime lifecycle against the real containers, and round-trips bytes through
// the started gateway — the honest end-to-end path a consumer takes.
func TestRuntimeEndToEndPutGet(t *testing.T) {
	if os.Getenv("SOS_GATEWAY_IMAGE") == "" {
		t.Skip("SOS_GATEWAY_IMAGE not set; skipping agent-runtime end-to-end test")
	}
	ctx := context.Background()

	workspace := &resources.Workspace{Name: "test"}
	tmpDir := t.TempDir()
	serviceName := fmt.Sprintf("svc-%v", time.Now().UnixMilli())
	service := resources.Service{Name: serviceName, Version: "test-me"}
	require.NoError(t, service.SaveAtDir(ctx, path.Join(tmpDir, "mod", service.Name)))

	identity := &basev0.ServiceIdentity{
		Name:                service.Name,
		Module:              "mod",
		Workspace:           workspace.Name,
		WorkspacePath:       tmpDir,
		RelativeToWorkspace: fmt.Sprintf("mod/%s", service.Name),
	}

	builder := NewBuilder()
	_, err := builder.Load(ctx, &builderv0.LoadRequest{
		DisableCatch: true,
		Identity:     identity,
		CreationMode: &builderv0.CreationMode{Communicate: false},
	})
	require.NoError(t, err)
	_, err = builder.Create(ctx, &builderv0.CreateRequest{})
	require.NoError(t, err)

	rt := NewRuntime()

	networkManager, err := network.NewRuntimeManager(ctx, nil)
	require.NoError(t, err)
	networkManager.WithTemporaryPorts()

	env := resources.LocalEnvironment()

	_, err = rt.Load(ctx, &runtimev0.LoadRequest{
		Identity:     identity,
		Environment:  shared.Must(env.Proto()),
		DisableCatch: true,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(rt.Endpoints))

	// Native context: the test process runs on the host, so both the Runtime's
	// readiness probe and this test reach the gateway at localhost:<hostPort>.
	// The gateway container still resolves MinIO through the host.docker.internal
	// bridge address dockerrun injects.
	runtimeContext := resources.NewRuntimeContextNative()
	networkMappings, err := networkManager.GenerateNetworkMappings(ctx, env, workspace, rt.Identity, rt.Endpoints, runtimeContext)
	require.NoError(t, err)

	init, err := rt.Init(ctx, &runtimev0.InitRequest{
		RuntimeContext:          runtimeContext,
		ProposedNetworkMappings: networkMappings,
	})
	require.NoError(t, err)
	require.NotNil(t, init)
	defer func() { _, _ = rt.Destroy(context.Background(), &runtimev0.DestroyRequest{}) }()

	_, err = rt.Start(ctx, &runtimev0.StartRequest{})
	require.NoError(t, err)

	// A consumer running in its own container reaches these through
	// host.docker.internal, which resolves to the bridge gateway on Linux — a
	// 127.0.0.1-bound port is unreachable there. Both must publish on all
	// interfaces or the container topology this agent advertises is broken.
	gatewayID, err := rt.gatewayEnv.ContainerID()
	require.NoError(t, err)
	requirePublishedOnAllInterfaces(t, gatewayID, gatewayContainerPort)
	minioID, err := rt.minioEnv.ContainerID()
	require.NoError(t, err)
	requirePublishedOnAllInterfaces(t, minioID, minioContainerPort)

	conf, err := resources.ExtractConfiguration(init.RuntimeConfigurations, resources.NewRuntimeContextNative())
	require.NoError(t, err)
	endpoint, err := resources.GetConfigurationValue(ctx, conf, "object-storage", "endpoint")
	require.NoError(t, err)
	require.NotEmpty(t, endpoint)

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := storagev0.NewObjectStorageClient(conn)

	key := "e2e/hello.txt"
	payload := []byte("real bytes through the agent runtime")

	st, err := client.Put(ctx)
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: &storagev0.PutHeader{
		Key:         key,
		TotalSize:   int64(len(payload)),
		ContentType: "text/plain",
	}}}))
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: payload}}))
	pr, err := st.CloseAndRecv()
	require.NoError(t, err)
	require.NotEmpty(t, pr.GetEtag())

	gs, err := client.Get(ctx, &storagev0.GetRequest{Key: key})
	require.NoError(t, err)
	var body []byte
	var hdr *storagev0.GetHeader
	for {
		msg, rerr := gs.Recv()
		if rerr == io.EOF {
			break
		}
		require.NoError(t, rerr)
		if h := msg.GetHeader(); h != nil {
			hdr = h
		}
		body = append(body, msg.GetData()...)
	}
	require.Equal(t, payload, body)
	require.Equal(t, "text/plain", hdr.GetInfo().GetContentType())
	require.Equal(t, int64(len(payload)), hdr.GetInfo().GetSize())
}

// requirePublishedOnAllInterfaces asserts the container's port is published on
// 0.0.0.0, not 127.0.0.1 — the difference between reachable and refused from a
// consumer container on a Linux bridge network.
func requirePublishedOnAllInterfaces(t *testing.T, containerID string, containerPort int) {
	t.Helper()
	out, err := exec.Command("docker", "inspect", "-f",
		fmt.Sprintf(`{{(index (index .NetworkSettings.Ports "%d/tcp") 0).HostIp}}`, containerPort),
		containerID).Output()
	require.NoError(t, err)
	require.Equal(t, "0.0.0.0", strings.TrimSpace(string(out)),
		"container port %d must publish on all interfaces", containerPort)
}
