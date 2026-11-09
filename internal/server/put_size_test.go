package server_test

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// sizeRecordingBackend records the Size each Put hands the store. A real
// backend (minio) reads exactly that many bytes when it is not -1.
type sizeRecordingBackend struct {
	backend.Backend
	mu    sync.Mutex
	sizes []int64
}

func (b *sizeRecordingBackend) Put(ctx context.Context, key string, r io.Reader, opts backend.PutOptions) (*backend.PutResult, error) {
	b.mu.Lock()
	b.sizes = append(b.sizes, opts.Size)
	b.mu.Unlock()
	return b.Backend.Put(ctx, key, r, opts)
}

// putChunks streams header then each chunk, as a client does.
func putChunks(t *testing.T, c storagev0.ObjectStorageClient, header *storagev0.PutHeader, chunks ...[]byte) (*storagev0.PutResult, error) {
	t.Helper()
	st, err := c.Put(context.Background())
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: header}}))
	for _, chunk := range chunks {
		if err := st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: chunk}}); err != nil {
			break // the server refused mid-stream; CloseAndRecv carries why
		}
	}
	return st.CloseAndRecv()
}

// TestPutWithoutTotalSizeStoresTheBody is the regression: proto3 sends an
// unset total_size as 0, and the gateway handed that to the store as an exact
// length, so the object was stored empty and the body discarded, without an
// error.
func TestPutWithoutTotalSizeStoresTheBody(t *testing.T) {
	be := &sizeRecordingBackend{Backend: openMemBackend(t)}
	c := newTestClientFor(t, be)
	payload := bytes.Repeat([]byte("recording "), 10_000)

	_, err := putChunks(t, c, &storagev0.PutHeader{Key: "calls/a.wav", ContentType: "audio/wav"},
		payload[:40_000], payload[40_000:])
	require.NoError(t, err)

	_, body, err := getObject(t, c, &storagev0.GetRequest{Key: "calls/a.wav"})
	require.NoError(t, err)
	require.Equal(t, payload, body)
	require.Equal(t, []int64{-1}, be.sizes, "the store streams; it is never handed a length to stop at")
}

func TestPutWithUnknownTotalSizeStoresTheBody(t *testing.T) {
	c := newTestClientFor(t, openMemBackend(t))
	_, err := putChunks(t, c, &storagev0.PutHeader{Key: "k", TotalSize: -1}, []byte("streamed"))
	require.NoError(t, err)
	_, body, err := getObject(t, c, &storagev0.GetRequest{Key: "k"})
	require.NoError(t, err)
	require.Equal(t, []byte("streamed"), body)
}

func TestPutWithDeclaredTotalSizeStoresAnExactBody(t *testing.T) {
	be := &sizeRecordingBackend{Backend: openMemBackend(t)}
	c := newTestClientFor(t, be)
	_, err := putChunks(t, c, &storagev0.PutHeader{Key: "k", TotalSize: 9}, []byte("hello "), []byte("you"))
	require.NoError(t, err)
	_, body, err := getObject(t, c, &storagev0.GetRequest{Key: "k"})
	require.NoError(t, err)
	require.Equal(t, []byte("hello you"), body)
	require.Equal(t, []int64{-1}, be.sizes)
}

// A body longer than its declared size was stored truncated to the declared
// length; one shorter failed only by accident of the backend. Both are now
// refused before anything is stored.
func TestPutRefusesABodyThatDisagreesWithItsDeclaredSize(t *testing.T) {
	for name, tc := range map[string]struct {
		declared int64
		chunks   [][]byte
	}{
		"longer":  {declared: 4, chunks: [][]byte{[]byte("hel"), []byte("lo world")}},
		"shorter": {declared: 100, chunks: [][]byte{[]byte("hello")}},
	} {
		t.Run(name, func(t *testing.T) {
			c := newTestClientFor(t, openMemBackend(t))
			_, err := putChunks(t, c, &storagev0.PutHeader{Key: "k", TotalSize: tc.declared}, tc.chunks...)
			info, _ := requireReason(t, err, codes.Aborted, serr.ReasonBodySizeMismatch)
			require.Equal(t, strconv.FormatInt(tc.declared, 10), info.GetMetadata()["declared_bytes"])

			_, err = c.Stat(context.Background(), &storagev0.StatRequest{Key: "k"})
			require.Equal(t, codes.NotFound, status.Code(err), "a refused Put stores nothing")
		})
	}
}
