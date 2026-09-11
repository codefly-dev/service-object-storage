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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/codefly-dev/core/network"
	"github.com/codefly-dev/core/resources"
	dockerrun "github.com/codefly-dev/core/runners/dockerrun"
	"github.com/codefly-dev/core/shared"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/auth"
)

// TestRuntimeEndToEndPutGet scaffolds a service through the Builder, drives the
// Runtime lifecycle against the real containers, and round-trips bytes through
// the started gateway — the honest end-to-end path a consumer takes.
func TestRuntimeEndToEndPutGet(t *testing.T) {
	if os.Getenv("SOS_GATEWAY_IMAGE") == "" {
		t.Skip("SOS_GATEWAY_IMAGE not set; skipping agent-runtime end-to-end test")
	}
	ctx := context.Background()

	rt, networkMappings, runtimeContext := loadedRuntime(t, ctx)

	// Register teardown before Init: Init starts the MinIO and gateway containers
	// one after the other, so a failure between them (MinIO up, gateway not) must
	// still clean up what did start. Destroy nil-checks each environment, so it is
	// safe even if Init failed before starting anything. The flag keeps this
	// safety net from running a second Shutdown after the explicit teardown the
	// test asserts on below.
	destroyed := false
	defer func() {
		if !destroyed {
			_, _ = rt.Destroy(context.Background(), &runtimev0.DestroyRequest{})
		}
	}()

	init, err := rt.Init(ctx, &runtimev0.InitRequest{
		RuntimeContext:          runtimeContext,
		ProposedNetworkMappings: networkMappings,
	})
	require.NoError(t, err)
	require.NotNil(t, init)
	require.Equal(t, runtimev0.InitStatus_READY, init.GetStatus().GetState(), init.GetStatus().GetMessage())

	_, err = rt.Start(ctx, &runtimev0.StartRequest{})
	require.NoError(t, err)

	// A consumer running in its own container reaches these through
	// host.docker.internal, which resolves to the bridge gateway on Linux — a
	// 127.0.0.1-bound port is unreachable there. Both must publish on all
	// interfaces or the container topology this agent advertises is broken; the
	// token checked below is what keeps that binding from being an open door.
	gatewayID, err := rt.gatewayEnv.ContainerID()
	require.NoError(t, err)
	requirePublishedOnAllInterfaces(t, gatewayID, gatewayContainerPort)
	minioID, err := rt.minioEnv.ContainerID()
	require.NoError(t, err)
	requireMinIOFilesystemOwner(t, rt, minioID)
	requirePublishedOnAllInterfaces(t, minioID, minioContainerPort)

	conf, err := resources.ExtractConfiguration(init.RuntimeConfigurations, resources.NewRuntimeContextNative())
	require.NoError(t, err)
	endpoint, err := resources.GetConfigurationValue(ctx, conf, "object-storage", "endpoint")
	require.NoError(t, err)
	require.NotEmpty(t, endpoint)
	connection, err := resources.GetConfigurationValue(ctx, conf, "object-storage", "connection")
	require.NoError(t, err)

	token, err := resources.GetConfigurationValue(ctx, conf, "object-storage", "token")
	require.NoError(t, err)
	require.NotEmpty(t, token, "the runtime must hand consumers a gateway credential")
	require.NotContains(t, endpoint, token, "endpoint must not carry the credential")
	require.NotContains(t, connection, token, "connection string must not carry the credential")
	requireTokenIsSecret(t, conf, token)

	// Every RPC an unauthenticated peer on the published port could try — unary,
	// client-streaming and server-streaming — must be refused before it reaches
	// the backend.
	requireUnauthenticatedPeerIsRefused(t, endpoint)

	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		auth.DialOption(token))
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

	// Teardown owns exactly what it started. The agent creates no Docker network
	// of its own, so removing both containers removes everything it holds —
	// including the only place the session token lives.
	_, err = rt.Destroy(context.Background(), &runtimev0.DestroyRequest{})
	require.NoError(t, err)
	destroyed = true
	requireContainerRemoved(t, gatewayID)
	requireContainerRemoved(t, minioID)
}

// requireContainerRemoved asserts the container no longer exists on the daemon.
func requireContainerRemoved(t *testing.T, containerID string) {
	t.Helper()
	err := exec.Command("docker", "inspect", containerID).Run()
	require.Error(t, err, "container %s survived teardown", containerID)
}

// requireTokenIsSecret asserts the gateway credential is carried as a
// secret-marked configuration value, so nothing downstream logs or renders it.
func requireTokenIsSecret(t *testing.T, conf *basev0.Configuration, token string) {
	t.Helper()
	for _, info := range conf.GetInfos() {
		for _, value := range info.GetConfigurationValues() {
			if value.GetKey() == "token" {
				require.Equal(t, token, value.GetValue())
				require.True(t, value.GetSecret(), "gateway token must be marked secret")
				return
			}
		}
	}
	t.Fatal("no token value in the runtime configuration")
}

// requireUnauthenticatedPeerIsRefused dials the published gateway port the way
// any other host on the network could — no credential at all, then a plausible
// wrong one — and asserts every read, write, delete, presign and subscribe is
// rejected. This is the acceptance case for the all-interface binding asserted
// above: reachable, but useless without the session token.
func requireUnauthenticatedPeerIsRefused(t *testing.T, endpoint string) {
	t.Helper()
	peers := map[string][]grpc.DialOption{
		"no credentials": nil,
		"wrong token":    {auth.DialOption("0000000000000000000000000000000000000000000000000000000000000000")},
	}
	for peerName, extra := range peers {
		conn, err := grpc.NewClient(endpoint,
			append(extra, grpc.WithTransportCredentials(insecure.NewCredentials()))...)
		require.NoError(t, err)
		client := storagev0.NewObjectStorageClient(conn)

		for rpcName, call := range map[string]func(context.Context, storagev0.ObjectStorageClient) error{
			"Capabilities": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
				_, err := c.Capabilities(ctx, &storagev0.CapabilitiesRequest{})
				return err
			},
			"Stat": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
				_, err := c.Stat(ctx, &storagev0.StatRequest{Key: "e2e/hello.txt"})
				return err
			},
			"Delete": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
				_, err := c.Delete(ctx, &storagev0.DeleteRequest{Key: "e2e/hello.txt"})
				return err
			},
			"Presign": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
				_, err := c.Presign(ctx, &storagev0.PresignRequest{Key: "e2e/hello.txt", ExpirySeconds: 60})
				return err
			},
			"Put": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
				st, sErr := c.Put(ctx)
				if sErr != nil {
					return sErr
				}
				if sErr = st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{
					Header: &storagev0.PutHeader{Key: "e2e/intruder.txt", TotalSize: 4},
				}}); sErr != nil && sErr != io.EOF {
					return sErr
				}
				_, sErr = st.CloseAndRecv()
				return sErr
			},
			"Get": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
				st, sErr := c.Get(ctx, &storagev0.GetRequest{Key: "e2e/hello.txt"})
				if sErr != nil {
					return sErr
				}
				_, sErr = st.Recv()
				return sErr
			},
			"Watch": func(ctx context.Context, c storagev0.ObjectStorageClient) error {
				st, sErr := c.Watch(ctx, &storagev0.WatchRequest{})
				if sErr != nil {
					return sErr
				}
				_, sErr = st.Recv()
				return sErr
			},
		} {
			t.Run(peerName+"/"+rpcName, func(t *testing.T) {
				cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				err := call(cctx, client)
				require.Error(t, err)
				require.Equal(t, codes.Unauthenticated, status.Code(err), "got %v", err)
			})
		}
		_ = conn.Close()
	}
}

// loadedRuntime scaffolds a service through the Builder and returns a Runtime
// loaded against it, with the network mappings and runtime context Init needs.
//
// The native runtime context puts the test process on the host, so both the
// Runtime's readiness probe and the test reach the gateway at localhost:<hostPort>;
// the gateway container still resolves MinIO through the host.docker.internal
// bridge address dockerrun injects.
func loadedRuntime(t *testing.T, ctx context.Context) (*Runtime, []*basev0.NetworkMapping, *basev0.RuntimeContext) {
	t.Helper()

	// Keep all persistent test data outside the developer's real Codefly home.
	codeflyHome, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	t.Setenv(resources.CodeflyHomeEnv, codeflyHome)
	t.Setenv("SOS_LOCAL_MINIO_INITIALIZE", "true")
	workspace := &resources.Workspace{Name: "test"}
	tmpDir := t.TempDir()
	serviceName := fmt.Sprintf("svc-%v", time.Now().UnixMilli())
	service := resources.Service{Name: serviceName, Version: "test-me"}
	require.NoError(t, service.SaveAtDir(ctx, path.Join(tmpDir, "mod", service.Name)))

	identity := &basev0.ServiceIdentity{
		Name:                service.Name,
		Version:             service.Version,
		Module:              "mod",
		Workspace:           workspace.Name,
		WorkspacePath:       tmpDir,
		RelativeToWorkspace: fmt.Sprintf("mod/%s", service.Name),
	}

	builder := NewBuilder()
	_, err = builder.Load(ctx, &builderv0.LoadRequest{
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

	runtimeContext := resources.NewRuntimeContextNative()
	networkMappings, err := networkManager.GenerateNetworkMappings(ctx, env, workspace, rt.Identity, rt.Endpoints, runtimeContext)
	require.NoError(t, err)

	return rt, networkMappings, runtimeContext
}

// requirePublishedOnAllInterfaces asserts the container's port is published on
// 0.0.0.0, not 127.0.0.1 — the difference between reachable and refused from a
// consumer container on a Linux bridge network.
//
// Docker can publish a single port under more than one host binding (e.g. an
// IPv6 "::" entry beside the IPv4 "0.0.0.0" one), and their order is not
// guaranteed, so it ranges over every binding rather than trusting index 0: the
// IPv4 wildcard must be present and no binding may be loopback-only.
func requirePublishedOnAllInterfaces(t *testing.T, containerID string, containerPort int) {
	t.Helper()
	out, err := exec.Command("docker", "inspect", "-f",
		fmt.Sprintf(`{{range (index .NetworkSettings.Ports "%d/tcp")}}{{.HostIp}} {{end}}`, containerPort),
		containerID).Output()
	require.NoError(t, err)
	hostIPs := strings.Fields(string(out))
	require.NotEmpty(t, hostIPs, "container port %d has no published host binding", containerPort)
	require.NotContains(t, hostIPs, "127.0.0.1",
		"container port %d must not be published loopback-only", containerPort)
	require.Contains(t, hostIPs, "0.0.0.0",
		"container port %d must publish on all interfaces", containerPort)
}

// probeImage is a shell-carrying image used to read what the gateway container
// sees; the gateway image itself is distroless and has no shell.
const probeImage = "busybox:1.36"

// TestGCSCredentialProjection drives Init against the gcs backend with a
// disposable service-account key on the host and proves the gateway reads that
// key from the declared container path, read-only, while the host path never
// reaches the container.
func TestGCSCredentialProjection(t *testing.T) {
	if os.Getenv("SOS_GATEWAY_IMAGE") == "" {
		t.Skip("SOS_GATEWAY_IMAGE not set; skipping gcs credential projection test")
	}
	ctx := context.Background()

	hostDir := t.TempDir()
	keyFile := path.Join(hostDir, "gcs-key.json")
	key := disposableServiceAccountKey(t)
	require.NoError(t, os.WriteFile(keyFile, key, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(key))

	rt, networkMappings, runtimeContext := loadedRuntime(t, ctx)
	defer func() { _, _ = rt.Destroy(context.Background(), &runtimev0.DestroyRequest{}) }()

	response, err := rt.Init(ctx, &runtimev0.InitRequest{
		RuntimeContext:          runtimeContext,
		ProposedNetworkMappings: networkMappings,
		Configuration: &basev0.Configuration{Infos: []*basev0.ConfigurationInformation{{
			Name: "object-storage",
			ConfigurationValues: []*basev0.ConfigurationValue{
				{Key: "SOS_BACKEND", Value: "gcs"},
				{Key: "SOS_GCS_CREDENTIALS_FILE", Value: keyFile},
			},
		}}},
	})
	require.NoError(t, err)
	require.Equal(t, runtimev0.InitStatus_READY, response.GetStatus().GetState(), response.GetStatus().GetMessage())
	require.Nil(t, rt.minioEnv, "a gcs run must not stand up the local MinIO fixture")

	projected := rt.gcsCredentialsHostFile
	require.NotEmpty(t, projected)

	gatewayID, err := rt.gatewayEnv.ContainerID()
	require.NoError(t, err)

	mounts := dockerInspect(t, gatewayID, `{{range .Mounts}}{{.Source}}:{{.Destination}} {{end}}`)
	require.Equal(t, []string{projected + ":" + gcsCredentialsContainerPath}, strings.Fields(mounts),
		"the gateway must mount the projected key and nothing else")
	require.NotContains(t, mounts, hostDir, "the operator's key directory must not be mounted")

	env := dockerInspect(t, gatewayID, `{{range .Config.Env}}{{.}}{{"\n"}}{{end}}`)
	require.Contains(t, strings.Fields(env), "SOS_GCS_CREDENTIALS_FILE="+gcsCredentialsContainerPath)
	require.NotContains(t, env, keyFile, "the gateway must never be handed the host path")

	// The read-only guarantee rests on the gateway not owning the file and not
	// being root, so the container must actually run as the declared user
	// whatever image SOS_GATEWAY_IMAGE names.
	require.Equal(t, gatewayContainerUser, strings.TrimSpace(dockerInspect(t, gatewayID, `{{.Config.User}}`)))

	// The probe runs as that same user, so what it can read and write is what the
	// gateway can. It reports a digest rather than the key, so no credential bytes
	// reach the test log. Only stdout is read: docker writes image-pull progress
	// to stderr, and a daemon without the probe image cached would otherwise mix
	// that chatter into the probe's answer.
	probe := exec.Command("docker", "run", "--rm", "--user", gatewayContainerUser,
		"--mount", fmt.Sprintf("type=bind,source=%s,target=%s", projected, gcsCredentialsContainerPath),
		probeImage, "sh", "-c", fmt.Sprintf(
			`sha256sum %[1]s | cut -d' ' -f1; if (echo tampered >> %[1]s) 2>/dev/null; then echo WRITABLE; else echo READONLY; fi`,
			gcsCredentialsContainerPath))
	var probeErr bytes.Buffer
	probe.Stderr = &probeErr
	out, err := probe.Output()
	require.NoError(t, err, probeErr.String())
	require.Equal(t, []string{digest, "READONLY"}, strings.Fields(string(out)))

	onHost, err := os.ReadFile(keyFile)
	require.NoError(t, err)
	require.Equal(t, key, onHost, "the operator's key must be untouched")

	_, err = rt.Destroy(ctx, &runtimev0.DestroyRequest{})
	require.NoError(t, err)
	_, err = os.Stat(projected)
	require.True(t, os.IsNotExist(err), "Destroy must remove the projected key from the host")
}

func dockerInspect(t *testing.T, containerID, format string) string {
	t.Helper()
	out, err := exec.Command("docker", "inspect", "-f", format, containerID).Output()
	require.NoError(t, err)
	return string(out)
}

// disposableServiceAccountKey builds a structurally valid GCS service-account
// key around a freshly generated private key. It authenticates against nothing —
// it exists so the gateway's GCS client opens and the projection can be observed
// end to end without a real cloud credential.
func disposableServiceAccountKey(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	blob, err := json.Marshal(map[string]string{
		"type":         "service_account",
		"project_id":   "codefly-projection-test",
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email": "projection-test@codefly-projection-test.iam.gserviceaccount.com",
		"token_uri":    "https://oauth2.googleapis.com/token",
	})
	require.NoError(t, err)
	return blob
}

// TestGatewayStartFailureIsOwnedByRuntime covers the ownership rule with a real
// failed start: Docker refuses a container whose published port another
// container already holds. dockerrun removes the container it could not start,
// but the environment around it still holds an open Docker client, and a
// container that was already present and merely failed to restart is not
// removed at all. Both are released only by Shutdown, so the Runtime has to own
// the environment before it is started rather than after.
func TestGatewayStartFailureIsOwnedByRuntime(t *testing.T) {
	if os.Getenv("SOS_GATEWAY_IMAGE") == "" {
		t.Skip("SOS_GATEWAY_IMAGE not set; skipping gateway rollback test")
	}
	ctx := context.Background()

	port := holdPortInDocker(t)

	rt, _, _ := loadedRuntime(t, ctx)
	require.NoError(t, rt.LoadConfiguration(ctx, nil))
	// startGateway publishes the resolved bearer, so the container under test is
	// the one Init would have built: the held port is the only reason it fails.
	token, err := rt.resolveGatewayToken()
	require.NoError(t, err)
	rt.gatewayToken = token

	containerName := dockerrun.ContainerName(rt.UniqueWithWorkspace() + "-gateway")

	require.Error(t, rt.startGateway(ctx, port))
	require.NotNil(t, rt.gatewayEnv,
		"the Runtime must own the environment it created, or nothing can release it")

	require.NoError(t, rt.teardown(ctx))
	require.Nil(t, rt.gatewayEnv)
	require.Empty(t, containersNamed(t, containerName))
	require.NoDirExists(t, path.Join(resources.CodeflyHomeDir(), "runtime-credentials"))
}

// holdPortInDocker publishes a free host port from a throwaway container and
// returns it, so the next container asking for it is refused by the daemon.
func holdPortInDocker(t *testing.T) uint16 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())

	name := fmt.Sprintf("sos-port-holder-%d", port)
	out, err := exec.Command("docker", "run", "-d", "--name", name,
		"-p", fmt.Sprintf("%d:9464", port), probeImage, "sleep", "300").CombinedOutput()
	require.NoError(t, err, string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	return port
}

func containersNamed(t *testing.T, name string) string {
	t.Helper()
	out, err := exec.Command("docker", "ps", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.Names}}").Output()
	require.NoError(t, err)
	return strings.TrimSpace(string(out))
}
