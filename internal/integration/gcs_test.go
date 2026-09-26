//go:build integration

// The GCS half of the integration suite runs the gateway's gcs backend against
// fake-gcs-server, the emulator the Go storage SDK speaks to natively through
// STORAGE_EMULATOR_HOST:
//
//	docker run -d --name fake-gcs -p 4443:4443 \
//	  fsouza/fake-gcs-server@sha256:d47b4cf8b87006cab8fbbecfa5f06a2a3c5722e464abddc0d107729663d40ec4 \
//	  -scheme http -public-host 127.0.0.1:4443
//	STORAGE_EMULATOR_HOST=127.0.0.1:4443 GCS_BUCKET=sos-test \
//	  go test -tags integration -count=1 -v -run GCS ./internal/integration/...
//
// What it proves: the backend opens with NO key file (the configuration a
// keyless deployment runs with), serves the gRPC surface, and confines a
// configured SOS_PREFIX. What it cannot prove: the emulator accepts any
// caller, so Application Default Credentials / Workload Identity and the bucket
// IAM grant are not exercised here.
package integration

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"

	_ "github.com/codefly-dev/service-object-storage/internal/backend/gcs"
)

// gcsEmulator returns a raw SDK client on the emulator and the bucket to use,
// creating the bucket if it is missing. It skips when no emulator is named.
func gcsEmulator(t *testing.T) (*gcs.Client, string) {
	t.Helper()
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("STORAGE_EMULATOR_HOST not set; skipping GCS integration test")
	}
	bucket := os.Getenv("GCS_BUCKET")
	if bucket == "" {
		t.Skip("GCS_BUCKET not set; skipping GCS integration test")
	}
	ctx := context.Background()
	client, err := gcs.NewClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Bucket(bucket).Attrs(ctx); errors.Is(err, gcs.ErrBucketNotExist) {
		require.NoError(t, client.Bucket(bucket).Create(ctx, "sos-test", nil))
	} else {
		require.NoError(t, err)
	}
	return client, bucket
}

func newGCSStack(t *testing.T, bucket, prefix string) storagev0.ObjectStorageClient {
	t.Helper()
	// No GCSCredentialsFile: the keyless path a Workload Identity deployment
	// takes (the emulator stands in for the credential exchange).
	be, err := backend.Open(context.Background(), backend.Config{Kind: "gcs", Bucket: bucket, Prefix: prefix})
	require.NoError(t, err)
	hub := events.NewHub()

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(s, server.New(be, hub, probetest.Monitor(t, be)))
	go func() { _ = s.Serve(lis) }()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); s.Stop(); hub.Close(); _ = be.Close() })
	return storagev0.NewObjectStorageClient(conn)
}

func TestGCSFullStack(t *testing.T) {
	_, bucket := gcsEmulator(t)
	c := newGCSStack(t, bucket, "")
	ctx := context.Background()
	key := "integration/gcs-hello.txt"
	payload := []byte("fake gcs, real client")

	pr, err := put(t, c, key, payload, &storagev0.PutHeader{ContentType: "text/plain"})
	require.NoError(t, err)
	require.NotEmpty(t, pr.GetEtag())

	body, hdr := get(t, c, key)
	require.Equal(t, payload, body)
	require.Equal(t, "text/plain", hdr.GetInfo().GetContentType())
	require.Equal(t, int64(len(payload)), hdr.GetInfo().GetSize())

	// Create-if-absent is atomic on GCS (generation-match 0); the contract
	// reports the conflict as AlreadyExists on every backend.
	_, err = put(t, c, key, []byte("again"), &storagev0.PutHeader{IfNoneMatch: "*"})
	require.Equal(t, codes.AlreadyExists, status.Code(err), "got %v", err)

	_, err = c.Copy(ctx, &storagev0.CopyRequest{SourceKey: key, DestKey: "integration/gcs-copy.txt"})
	require.NoError(t, err)
	lr, err := c.List(ctx, &storagev0.ListRequest{Prefix: "integration/gcs-"})
	require.NoError(t, err)
	require.Len(t, lr.GetObjects(), 2)

	_, err = c.DeleteMany(ctx, &storagev0.DeleteManyRequest{Keys: []string{key, "integration/gcs-copy.txt"}})
	require.NoError(t, err)
	st, err := c.Get(ctx, &storagev0.GetRequest{Key: key})
	require.NoError(t, err)
	_, rerr := st.Recv()
	require.Equal(t, codes.NotFound, status.Code(rerr))
}

// TestGCSPrefixConfinesKeys drives a prefixed gateway and inspects the bucket
// with the raw SDK: objects land under the prefix, and the gateway reports and
// lists keys relative to it.
func TestGCSPrefixConfinesKeys(t *testing.T) {
	raw, bucket := gcsEmulator(t)
	ctx := context.Background()
	prefix := "consumer-" + time.Now().Format("150405.000000")
	c := newGCSStack(t, bucket, prefix)

	_, err := put(t, c, "docs/a.txt", []byte("a"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Bucket(bucket).Object(prefix + "/docs/a.txt").Delete(context.Background()) })

	attrs, err := raw.Bucket(bucket).Object(prefix + "/docs/a.txt").Attrs(ctx)
	require.NoError(t, err, "object must be stored under the prefix")
	require.Equal(t, int64(1), attrs.Size)
	_, err = raw.Bucket(bucket).Object("docs/a.txt").Attrs(ctx)
	require.ErrorIs(t, err, gcs.ErrObjectNotExist, "nothing may land outside the prefix")

	lr, err := c.List(ctx, &storagev0.ListRequest{})
	require.NoError(t, err)
	require.Len(t, lr.GetObjects(), 1)
	require.Equal(t, "docs/a.txt", lr.GetObjects()[0].GetKey())

	// A gateway on another prefix of the same bucket sees none of it.
	other := newGCSStack(t, bucket, prefix+"-other")
	lr, err = other.List(ctx, &storagev0.ListRequest{})
	require.NoError(t, err)
	require.Empty(t, lr.GetObjects())
}

// TestGCSProbe checks the readiness probe against the emulator for both
// strategies, and that a missing bucket is reported as NotFound.
func TestGCSProbe(t *testing.T) {
	_, bucket := gcsEmulator(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, strategy := range []backend.ProbeStrategy{backend.ProbeList, backend.ProbeStat} {
		t.Run(string(strategy), func(t *testing.T) {
			be, err := backend.Open(ctx, backend.Config{Kind: "gcs", Bucket: bucket, Prefix: "p",
				ProbeStrategy: strategy, ProbeKey: ".codefly-readiness"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = be.Close() })
			require.NoError(t, be.Probe(ctx))
		})
	}
	t.Run("missing bucket", func(t *testing.T) {
		be, err := backend.Open(ctx, backend.Config{Kind: "gcs", Bucket: bucket + "-absent"})
		require.NoError(t, err)
		t.Cleanup(func() { _ = be.Close() })
		require.Error(t, be.Probe(ctx))
	})
}
