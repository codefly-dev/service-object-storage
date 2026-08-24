package server_test

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

func newTestClient(t *testing.T) storagev0.ObjectStorageClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	be, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)

	s := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(s, server.New(be))
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); s.Stop() })
	return storagev0.NewObjectStorageClient(conn)
}

func putObject(t *testing.T, c storagev0.ObjectStorageClient, key string, data []byte, header *storagev0.PutHeader) (*storagev0.PutResult, error) {
	t.Helper()
	if header == nil {
		header = &storagev0.PutHeader{}
	}
	header.Key = key
	header.TotalSize = int64(len(data))
	st, err := c.Put(context.Background())
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: header}}))
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: data}}))
	return st.CloseAndRecv()
}

func getObject(t *testing.T, c storagev0.ObjectStorageClient, req *storagev0.GetRequest) (*storagev0.GetHeader, []byte, error) {
	t.Helper()
	st, err := c.Get(context.Background(), req)
	require.NoError(t, err)
	var hdr *storagev0.GetHeader
	var body []byte
	for {
		msg, rerr := st.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return hdr, body, rerr
		}
		if h := msg.GetHeader(); h != nil {
			hdr = h
		}
		body = append(body, msg.GetData()...)
	}
	return hdr, body, nil
}

func TestPutGetRoundTrip(t *testing.T) {
	c := newTestClient(t)
	payload := []byte("hello generic storage")

	pr, err := putObject(t, c, "docs/a.txt", payload, &storagev0.PutHeader{ContentType: "text/plain"})
	require.NoError(t, err)
	require.NotEmpty(t, pr.GetEtag())

	hdr, body, err := getObject(t, c, &storagev0.GetRequest{Key: "docs/a.txt"})
	require.NoError(t, err)
	require.False(t, hdr.GetNotModified())
	require.Equal(t, pr.GetEtag(), hdr.GetInfo().GetEtag())
	require.Equal(t, "text/plain", hdr.GetInfo().GetContentType())
	require.Equal(t, payload, body)
}

func TestStatConditionalNotModified(t *testing.T) {
	c := newTestClient(t)
	pr, err := putObject(t, c, "k", []byte("v"), nil)
	require.NoError(t, err)

	hdr, err := c.Stat(context.Background(), &storagev0.StatRequest{Key: "k", IfNoneMatch: pr.GetEtag()})
	require.NoError(t, err)
	require.True(t, hdr.GetNotModified())

	_, err = c.Stat(context.Background(), &storagev0.StatRequest{Key: "k", IfMatch: "\"other\""})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestGetRange(t *testing.T) {
	c := newTestClient(t)
	_, err := putObject(t, c, "r", []byte("0123456789"), nil)
	require.NoError(t, err)

	_, body, err := getObject(t, c, &storagev0.GetRequest{Key: "r", Range: &storagev0.ByteRange{Offset: 2, Length: 4}})
	require.NoError(t, err)
	require.Equal(t, []byte("2345"), body)
}

func TestConditionalCreateIfAbsent(t *testing.T) {
	c := newTestClient(t)
	_, err := putObject(t, c, "once", []byte("a"), &storagev0.PutHeader{IfNoneMatch: "*"})
	require.NoError(t, err)

	_, err = putObject(t, c, "once", []byte("b"), &storagev0.PutHeader{IfNoneMatch: "*"})
	require.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestNotFound(t *testing.T) {
	c := newTestClient(t)
	_, err := c.Stat(context.Background(), &storagev0.StatRequest{Key: "missing"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestListWithDelimiter(t *testing.T) {
	c := newTestClient(t)
	for _, k := range []string{"a/1", "a/2", "b/1", "top"} {
		_, err := putObject(t, c, k, []byte("x"), nil)
		require.NoError(t, err)
	}
	res, err := c.List(context.Background(), &storagev0.ListRequest{Delimiter: "/"})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a/", "b/"}, res.GetCommonPrefixes())
	require.Len(t, res.GetObjects(), 1)
	require.Equal(t, "top", res.GetObjects()[0].GetKey())
}

func TestCopyAndDelete(t *testing.T) {
	c := newTestClient(t)
	_, err := putObject(t, c, "src", []byte("data"), nil)
	require.NoError(t, err)

	_, err = c.Copy(context.Background(), &storagev0.CopyRequest{SourceKey: "src", DestKey: "dst"})
	require.NoError(t, err)

	_, body, err := getObject(t, c, &storagev0.GetRequest{Key: "dst"})
	require.NoError(t, err)
	require.Equal(t, []byte("data"), body)

	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: "dst"})
	require.NoError(t, err)
	_, err = c.Stat(context.Background(), &storagev0.StatRequest{Key: "dst"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestConditionalDelete(t *testing.T) {
	c := newTestClient(t)
	pr, err := putObject(t, c, "cad", []byte("v1"), nil)
	require.NoError(t, err)
	etag := pr.GetEtag()

	// Compare-and-delete against a missing object cannot match its precondition.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: "ghost", IfMatch: etag})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	// A stale ETag refuses and preserves the object.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: "cad", IfMatch: `"stale"`})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = c.Stat(context.Background(), &storagev0.StatRequest{Key: "cad"})
	require.NoError(t, err)

	// An unconditional delete of a missing object stays idempotent.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: "ghost"})
	require.NoError(t, err)

	// The matching ETag deletes.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: "cad", IfMatch: etag})
	require.NoError(t, err)
	_, err = c.Stat(context.Background(), &storagev0.StatRequest{Key: "cad"})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestPresignAndCapabilities(t *testing.T) {
	c := newTestClient(t)
	caps, err := c.Capabilities(context.Background(), &storagev0.CapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, "mem", caps.GetBackend())
	require.True(t, caps.GetPresign())

	pr, err := c.Presign(context.Background(), &storagev0.PresignRequest{
		Key: "k", Method: storagev0.PresignMethod_PRESIGN_METHOD_GET, ExpirySeconds: 3600,
	})
	require.NoError(t, err)
	require.Contains(t, pr.GetUrl(), "mem://local/k")
}

func TestNativeUnsupported(t *testing.T) {
	c := newTestClient(t)
	_, err := c.Native(context.Background(), &storagev0.NativeRequest{Verb: "set-acl"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
