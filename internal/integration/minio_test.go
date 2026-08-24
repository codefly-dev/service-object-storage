//go:build integration

// Package integration exercises the full stack — gRPC server → cache → real
// backend — against a live MinIO, the universal local/test backend. Run with:
//
//	go test -tags integration ./internal/integration/...
//
// It requires a running MinIO reachable via env:
//
//	MINIO_ENDPOINT (host:port), MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
//
// The test never mocks — this is the "test on MinIO, ship on S3" guarantee in
// practice: the same gRPC surface a client uses in production, over real infra.
package integration

import (
	"context"
	"io"
	"net"
	"os"
	"testing"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/cache"
	"github.com/codefly-dev/service-object-storage/internal/server"

	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
)

func mustEnv(t *testing.T, key string) string {
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s not set; skipping MinIO integration test", key)
	}
	return v
}

func ensureBucket(t *testing.T, endpoint, ak, sk, bucket string) {
	t.Helper()
	cl, err := miniogo.New(endpoint, &miniogo.Options{
		Creds:  miniocreds.NewStaticV4(ak, sk, ""),
		Secure: false,
	})
	require.NoError(t, err)
	exists, err := cl.BucketExists(context.Background(), bucket)
	require.NoError(t, err)
	if !exists {
		require.NoError(t, cl.MakeBucket(context.Background(), bucket, miniogo.MakeBucketOptions{}))
	}
}

func newStack(t *testing.T) storagev0.ObjectStorageClient {
	t.Helper()
	endpoint := mustEnv(t, "MINIO_ENDPOINT")
	ak := mustEnv(t, "MINIO_ACCESS_KEY")
	sk := mustEnv(t, "MINIO_SECRET_KEY")
	bucket := mustEnv(t, "MINIO_BUCKET")
	ensureBucket(t, endpoint, ak, sk, bucket)

	be, err := backend.Open(context.Background(), backend.Config{
		Kind:         "minio",
		Endpoint:     endpoint,
		Region:       "us-east-1",
		AccessKey:    ak,
		SecretKey:    sk,
		Bucket:       bucket,
		UsePathStyle: true,
	})
	require.NoError(t, err)
	cached := cache.New(be, nil, cache.Options{})

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(s, server.New(cached))
	go func() { _ = s.Serve(lis) }()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); s.Stop(); _ = cached.Close() })
	return storagev0.NewObjectStorageClient(conn)
}

func put(t *testing.T, c storagev0.ObjectStorageClient, key string, data []byte, h *storagev0.PutHeader) (*storagev0.PutResult, error) {
	if h == nil {
		h = &storagev0.PutHeader{}
	}
	h.Key = key
	h.TotalSize = int64(len(data))
	st, err := c.Put(context.Background())
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: h}}))
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: data}}))
	return st.CloseAndRecv()
}

func get(t *testing.T, c storagev0.ObjectStorageClient, key string) ([]byte, *storagev0.GetHeader) {
	st, err := c.Get(context.Background(), &storagev0.GetRequest{Key: key})
	require.NoError(t, err)
	var body []byte
	var hdr *storagev0.GetHeader
	for {
		msg, rerr := st.Recv()
		if rerr == io.EOF {
			break
		}
		require.NoError(t, rerr)
		if h := msg.GetHeader(); h != nil {
			hdr = h
		}
		body = append(body, msg.GetData()...)
	}
	return body, hdr
}

func TestMinIOFullStack(t *testing.T) {
	c := newStack(t)
	key := "integration/hello.txt"
	payload := []byte("real minio, real bytes")

	pr, err := put(t, c, key, payload, &storagev0.PutHeader{ContentType: "text/plain"})
	require.NoError(t, err)
	require.NotEmpty(t, pr.GetEtag())

	body, hdr := get(t, c, key)
	require.Equal(t, payload, body)
	require.Equal(t, "text/plain", hdr.GetInfo().GetContentType())
	require.Equal(t, int64(len(payload)), hdr.GetInfo().GetSize())

	// Cached second read must match.
	body2, _ := get(t, c, key)
	require.Equal(t, payload, body2)

	// Presign returns a usable URL.
	ps, err := c.Presign(context.Background(), &storagev0.PresignRequest{
		Key: key, Method: storagev0.PresignMethod_PRESIGN_METHOD_GET, ExpirySeconds: 600,
	})
	require.NoError(t, err)
	require.Contains(t, ps.GetUrl(), key)

	// Copy then list under the prefix.
	_, err = c.Copy(context.Background(), &storagev0.CopyRequest{SourceKey: key, DestKey: "integration/copy.txt"})
	require.NoError(t, err)
	lr, err := c.List(context.Background(), &storagev0.ListRequest{Prefix: "integration/"})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(lr.GetObjects()), 2)

	// Cleanup.
	_, err = c.DeleteMany(context.Background(), &storagev0.DeleteManyRequest{
		Keys: []string{key, "integration/copy.txt"},
	})
	require.NoError(t, err)
}

// TestMinIOConditionalDelete asserts the compare-and-delete precondition is
// honored against MinIO — a wrong If-Match must refuse and preserve the object,
// not silently delete it unconditionally.
func TestMinIOConditionalDelete(t *testing.T) {
	c := newStack(t)
	key := "integration/conditional-delete.txt"

	pr, err := put(t, c, key, []byte("v1"), nil)
	require.NoError(t, err)
	etag := pr.GetEtag()
	require.NotEmpty(t, etag)

	// A stale precondition must fail and leave the object intact.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: key, IfMatch: `"00000000000000000000000000000000"`})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, hdr := get(t, c, key)
	require.Equal(t, etag, hdr.GetInfo().GetEtag(), "object must survive a failed conditional delete")

	// The matching precondition deletes.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: key, IfMatch: etag})
	require.NoError(t, err)

	st, err := c.Get(context.Background(), &storagev0.GetRequest{Key: key})
	require.NoError(t, err)
	_, rerr := st.Recv()
	require.Equal(t, codes.NotFound, status.Code(rerr), "object must be gone after a matching conditional delete")
}
