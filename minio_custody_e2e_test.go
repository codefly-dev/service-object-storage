//go:build e2e

package main

import (
	"bytes"
	"context"
	"fmt"
	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/codefly-dev/core/runners/dockerrun"
	"github.com/codefly-dev/core/shared"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/auth"
)

func TestMinIOPersistentCustody(t *testing.T) {
	require.NotEmpty(t, os.Getenv("SOS_GATEWAY_IMAGE"), "build and set SOS_GATEWAY_IMAGE to run the custody proof")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	rt, mappings, runtimeContext := loadedRuntime(t, ctx)
	// Cleanup is restricted to environments created by this test. Data is in its
	// temporary CODEFLY_HOME; no shared stack or retained volume is selected.
	defer func() {
		_, err := rt.Destroy(context.Background(), &runtimev0.DestroyRequest{})
		require.NoError(t, err)
	}()
	init := func() {
		response, err := rt.Init(ctx, &runtimev0.InitRequest{RuntimeContext: runtimeContext, ProposedNetworkMappings: mappings})
		require.NoError(t, err)
		require.NotEqual(t, runtimev0.InitStatus_ERROR, response.GetStatus().GetState(), response.GetStatus().GetMessage())
		_, err = rt.Start(ctx, &runtimev0.StartRequest{})
		require.NoError(t, err)
	}
	connect := func(token string) storagev0.ObjectStorageClient {
		endpoint, err := rt.gatewayEnv.ContainerID()
		require.NoError(t, err)
		out, err := exec.Command("docker", "inspect", "-f", `{{(index (index .NetworkSettings.Ports "9464/tcp") 0).HostPort}}`, endpoint).Output()
		require.NoError(t, err)
		conn, err := grpc.NewClient("localhost:"+strings.TrimSpace(string(out)), grpc.WithTransportCredentials(insecure.NewCredentials()), auth.DialOption(token))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return storagev0.NewObjectStorageClient(conn)
	}
	client := func() storagev0.ObjectStorageClient { return connect(rt.gatewayToken) }
	init()
	firstID, err := rt.minioEnv.ContainerID()
	require.NoError(t, err)
	firstPassword, firstToken := rt.minioPassword, rt.gatewayToken
	keys := []string{"sources/handbook/original.txt", "sources/handbook/nested/bytes.bin"}
	payloads := [][]byte{[]byte("persistent source bytes\n"), bytes.Repeat([]byte{0, 1, 255, 42}, 32768)}
	originals := make([]*storagev0.GetHeader, len(keys))
	cl := client()
	for i, key := range keys {
		put, putErr := cl.Put(ctx)
		require.NoError(t, putErr)
		require.NoError(t, put.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: &storagev0.PutHeader{Key: key, TotalSize: int64(len(payloads[i])), ContentType: "application/octet-stream", UserMetadata: map[string]string{"source": "disposable", "document-version": "unchanged"}}}}))
		require.NoError(t, put.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: payloads[i]}}))
		_, putErr = put.CloseAndRecv()
		require.NoError(t, putErr)
		originals[i], err = cl.Stat(ctx, &storagev0.StatRequest{Key: key})
		require.NoError(t, err)
	}
	verify := func() {
		c := client()
		listed, listErr := c.List(ctx, &storagev0.ListRequest{Prefix: "sources/"})
		require.NoError(t, listErr)
		var actualKeys []string
		for _, obj := range listed.GetObjects() {
			actualKeys = append(actualKeys, obj.Key)
		}
		require.ElementsMatch(t, keys, actualKeys)
		for i, key := range keys {
			info, statErr := c.Stat(ctx, &storagev0.StatRequest{Key: key})
			require.NoError(t, statErr)
			require.True(t, proto.Equal(originals[i], info), "metadata changed for %s", key)
			get, getErr := c.Get(ctx, &storagev0.GetRequest{Key: key})
			require.NoError(t, getErr)
			var body []byte
			for {
				msg, recvErr := get.Recv()
				if recvErr == io.EOF {
					break
				}
				require.NoError(t, recvErr)
				body = append(body, msg.GetData()...)
			}
			require.Equal(t, payloads[i], body)
		}
	}
	_, err = rt.Stop(ctx, &runtimev0.StopRequest{})
	require.NoError(t, err)
	_, err = rt.Start(ctx, &runtimev0.StartRequest{})
	require.NoError(t, err)
	verify()
	// Also exercise a real Docker Stop/Start on only this test's MinIO.
	require.NoError(t, exec.Command("docker", "stop", firstID).Run())
	require.NoError(t, exec.Command("docker", "start", firstID).Run())
	// Connection publication reloads resolved configuration; restore the local
	// backend credentials for this direct readiness check.
	rt.conf.accessKey, rt.conf.secretKey = localMinioUser, rt.minioPassword
	require.NoError(t, rt.ensureBucket(ctx))
	verify()
	// A new agent instance rotates credentials and the allocated MinIO port.
	// Leave the existing containers in place so core takes its actual
	// configuration-fingerprint replacement path (not merely create-after-rm).
	previous := rt
	// Register before Load so even an assertion failure cleans up this test.
	defer func() { require.NoError(t, previous.teardown(context.Background())) }()
	rt = NewRuntime()
	_, err = rt.Load(ctx, &runtimev0.LoadRequest{Identity: shared.Must(previous.Identity.Proto()), Environment: previous.Environment, DisableCatch: true})
	require.NoError(t, err)
	t.Setenv("SOS_LOCAL_MINIO_INITIALIZE", "false")
	init()
	secondID, err := rt.minioEnv.ContainerID()
	require.NoError(t, err)
	require.NotEqual(t, firstID, secondID)
	require.True(t, firstPassword != rt.minioPassword, "MinIO credentials must rotate")
	require.True(t, firstToken != rt.gatewayToken, "gateway credentials must rotate")
	requireContainerRemoved(t, firstID)
	_, err = connect(firstToken).Stat(ctx, &storagev0.StatRequest{Key: keys[0]})
	require.Equal(t, codes.Unauthenticated, status.Code(err), "the previous gateway token must be revoked")
	verify()
	owner := dockerrun.ContainerName(rt.UniqueWithWorkspace() + "-minio")
	root := minioCustodyDir(owner)
	marker := filepath.Join(root, "data", ".codefly-custody.json")
	originalMarker, err := os.ReadFile(marker)
	require.NoError(t, err)
	for _, fault := range []string{"missing marker", "mismatched marker", "missing record"} {
		t.Run(fault, func(t *testing.T) {
			record := filepath.Join(root, "custody.json")
			switch fault {
			case "missing marker":
				require.NoError(t, os.Remove(marker))
			case "mismatched marker":
				require.NoError(t, os.WriteFile(marker, []byte(`{}`), 0o600))
			case "missing record":
				require.NoError(t, os.Rename(record, record+".retained"))
			}
			// A rejected Init on the same runtime must not roll back its live containers.
			response, prepErr := rt.Init(ctx, &runtimev0.InitRequest{RuntimeContext: runtimeContext, ProposedNetworkMappings: mappings})
			require.NoError(t, prepErr)
			require.Equal(t, runtimev0.InitStatus_ERROR, response.GetStatus().GetState())
			require.Contains(t, response.GetStatus().GetMessage(), "MinIO custody")
			require.NoError(t, exec.Command("docker", "inspect", secondID).Run())
			if fault == "missing record" {
				require.NoError(t, os.Rename(record+".retained", record))
			} else {
				require.NoError(t, os.WriteFile(marker, originalMarker, 0o600))
			}
			verify()
		})
	}
	t.Log("authenticated keys, exact bytes, full metadata survive Stop/Start and configuration replacement; custody failures preserve the container")
}

func TestMinIOLegacyVolumeRefusesReplacement(t *testing.T) {
	ctx := context.Background()
	rt, _, _ := loadedRuntime(t, ctx)
	owner := dockerrun.ContainerName(rt.UniqueWithWorkspace() + "-minio")
	// Reproduce the original anonymous /data volume, allocated only for this
	// test. Capture its exact name before registering narrow cleanup.
	out, err := exec.Command("docker", "create", "--name", owner, "--mount", "type=volume,target=/data", probeImage, "sh", "-c", "echo retained > /data/object").Output()
	require.NoError(t, err)
	id := strings.TrimSpace(string(out))
	volume := ""
	t.Cleanup(func() {
		require.NoError(t, exec.Command("docker", "rm", "-f", id).Run())
		if volume != "" {
			require.NoError(t, exec.Command("docker", "volume", "rm", volume).Run())
		}
	})
	out, err = exec.Command("docker", "inspect", "-f", `{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}`, id).Output()
	require.NoError(t, err)
	volume = strings.TrimSpace(string(out))
	require.NotEmpty(t, volume)
	require.NoError(t, exec.Command("docker", "start", "-a", id).Run())
	rt.conf.bucket = "documents"
	err = rt.startLocalMinIO(ctx)
	require.ErrorContains(t, err, "refusing replacement")
	require.NoError(t, exec.Command("docker", "inspect", id).Run())
	out, err = exec.Command("docker", "run", "--rm", "--mount", "type=volume,source="+volume+",target=/data,readonly", probeImage, "cat", "/data/object").Output()
	require.NoError(t, err)
	require.Equal(t, "retained\n", string(out))
	_, err = os.Stat(minioCustodyDir(owner))
	require.True(t, os.IsNotExist(err), "no empty replacement store should be provisioned")
}

// Losing an established bucket must not turn the next run into bootstrap.
func TestMinIOMissingBucketFailsClosed(t *testing.T) {
	ctx := context.Background()
	rt, _, _ := loadedRuntime(t, ctx)
	defer func() { require.NoError(t, rt.teardown(context.Background())) }()
	require.NoError(t, rt.LoadConfiguration(ctx, nil))
	require.NoError(t, rt.startLocalMinIO(ctx))
	cl, err := miniogo.New(fmt.Sprintf("localhost:%d", rt.minioHostPort), &miniogo.Options{Creds: miniocreds.NewStaticV4(localMinioUser, rt.minioPassword, "")})
	require.NoError(t, err)
	require.NoError(t, cl.RemoveBucket(ctx, rt.conf.bucket))
	previous := rt.minioEnv
	defer func() { require.NoError(t, previous.Shutdown(context.Background())) }()
	t.Setenv("SOS_LOCAL_MINIO_INITIALIZE", "false")
	err = rt.startLocalMinIO(ctx)
	require.ErrorContains(t, err, "refusing to create an empty replacement")
	cl, err = miniogo.New(fmt.Sprintf("localhost:%d", rt.minioHostPort), &miniogo.Options{Creds: miniocreds.NewStaticV4(localMinioUser, rt.minioPassword, "")})
	require.NoError(t, err)
	exists, err := cl.BucketExists(ctx, rt.conf.bucket)
	require.NoError(t, err)
	require.False(t, exists)
}
