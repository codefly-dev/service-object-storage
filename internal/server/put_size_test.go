package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

// exactSizeBackend stores over the mem backend the way the MinIO/S3 backend
// treats PutOptions.Size: a non-negative size is an exact length, read and no
// further, so whatever comes after it is never seen. It records each size it
// was handed.
type exactSizeBackend struct {
	backend.Backend
	mu    sync.Mutex
	sizes []int64
}

func (b *exactSizeBackend) Put(ctx context.Context, key string, r io.Reader, opts backend.PutOptions) (*backend.PutResult, error) {
	b.mu.Lock()
	b.sizes = append(b.sizes, opts.Size)
	b.mu.Unlock()
	if opts.Size >= 0 {
		buf := make([]byte, opts.Size)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		r = bytes.NewReader(buf)
	}
	return b.Backend.Put(ctx, key, r, opts)
}

func (b *exactSizeBackend) handed() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int64(nil), b.sizes...)
}

func newExactSizeClient(t *testing.T) (storagev0.ObjectStorageClient, *exactSizeBackend) {
	t.Helper()
	be := &exactSizeBackend{Backend: openMemBackend(t)}
	return newTestClientFor(t, be), be
}

// putChunks streams header then each chunk, as a client does.
func putChunks(t *testing.T, c storagev0.ObjectStorageClient, header *storagev0.PutHeader, chunks ...[]byte) (*storagev0.PutResult, error) {
	t.Helper()
	st, err := c.Put(context.Background())
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: header}}))
	for _, chunk := range chunks {
		if err := st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: chunk}}); err != nil {
			break // the server ended the stream; CloseAndRecv carries why
		}
	}
	return st.CloseAndRecv()
}

func requireStored(t *testing.T, c storagev0.ObjectStorageClient, key string, want []byte) {
	t.Helper()
	_, body, err := getObject(t, c, &storagev0.GetRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, len(want), len(body))
	require.True(t, bytes.Equal(want, body), "stored body differs from the one sent")
}

func requireAbsent(t *testing.T, c storagev0.ObjectStorageClient, key string) {
	t.Helper()
	_, err := c.Stat(context.Background(), &storagev0.StatRequest{Key: key})
	require.Equal(t, codes.NotFound, status.Code(err), "a refused Put stores nothing")
}

// TestPutWithoutTotalSizeStoresTheBody is the regression: proto3 sends an
// unset total_size as 0, which reached the store as an exact length, so the
// object was stored empty and the body discarded, without an error.
func TestPutWithoutTotalSizeStoresTheBody(t *testing.T) {
	c, be := newExactSizeClient(t)
	payload := bytes.Repeat([]byte("recording "), 10_000)

	_, err := putChunks(t, c, &storagev0.PutHeader{Key: "calls/a.wav", ContentType: "audio/wav"},
		payload[:40_000], payload[40_000:])
	require.NoError(t, err)
	requireStored(t, c, "calls/a.wav", payload)
	require.Equal(t, []int64{int64(len(payload))}, be.handed(),
		"a body that fits the probe is written with its exact size: one request, conditionals atomic")
}

func TestPutWithoutTotalSizeStoresAnEmptyBody(t *testing.T) {
	c, be := newExactSizeClient(t)
	_, err := putChunks(t, c, &storagev0.PutHeader{Key: "empty"})
	require.NoError(t, err)
	requireStored(t, c, "empty", []byte{})
	require.Equal(t, []int64{0}, be.handed())
}

func TestPutWithUnknownTotalSizeStoresTheBody(t *testing.T) {
	c, _ := newExactSizeClient(t)
	_, err := putChunks(t, c, &storagev0.PutHeader{Key: "k", TotalSize: -1}, []byte("stream"), []byte("ed"))
	require.NoError(t, err)
	requireStored(t, c, "k", []byte("streamed"))
}

// A body longer than the size probe streams with an unknown size.
func TestPutLongerThanTheProbeStreams(t *testing.T) {
	c, be := newExactSizeClient(t)
	chunk := bytes.Repeat([]byte{0xA5}, 1<<20)
	var chunks [][]byte
	var payload []byte
	for i := 0; i < server.PutSizeProbeBytes/len(chunk)+2; i++ {
		c := bytes.Clone(chunk)
		c[0] = byte(i)
		chunks = append(chunks, c)
		payload = append(payload, c...)
	}
	_, err := putChunks(t, c, &storagev0.PutHeader{Key: "big"}, chunks...)
	require.NoError(t, err)
	requireStored(t, c, "big", payload)
	require.Equal(t, []int64{-1}, be.handed())
}

func TestPutWithDeclaredTotalSizeStoresAnExactBody(t *testing.T) {
	c, be := newExactSizeClient(t)
	_, err := putChunks(t, c, &storagev0.PutHeader{Key: "k", TotalSize: 9}, []byte("hello "), []byte("you"))
	require.NoError(t, err)
	requireStored(t, c, "k", []byte("hello you"))
	require.Equal(t, []int64{9}, be.handed(), "a declared size reaches the store")
}

// A body longer than its declared size was stored truncated to the declared
// length by a store that reads exactly that many bytes; one shorter failed
// only by accident of the store. Both are refused, with nothing stored.
func TestPutRefusesABodyThatDisagreesWithItsDeclaredSize(t *testing.T) {
	for name, tc := range map[string]struct {
		declared int64
		chunks   [][]byte
	}{
		// The first chunk alone fills the declared length exactly: a store
		// that reads 5 bytes and stops would commit "hello" if it got it.
		"longer, overflowing on a later chunk": {declared: 5, chunks: [][]byte{[]byte("hello"), []byte(" world")}},
		"longer, overflowing within a chunk":   {declared: 4, chunks: [][]byte{[]byte("hel"), []byte("lo world")}},
		"shorter":                              {declared: 100, chunks: [][]byte{[]byte("hello")}},
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := newExactSizeClient(t)
			_, err := putChunks(t, c, &storagev0.PutHeader{Key: "k", TotalSize: tc.declared}, tc.chunks...)
			info, _ := requireReason(t, err, codes.Aborted, serr.ReasonBodySizeMismatch)
			require.Equal(t, strconv.FormatInt(tc.declared, 10), info.GetMetadata()["declared_bytes"])
			requireAbsent(t, c, "k")
		})
	}
}

// failingPutBackend refuses every Put without reading the body.
type failingPutBackend struct{ backend.Backend }

func (failingPutBackend) Put(context.Context, string, io.Reader, backend.PutOptions) (*backend.PutResult, error) {
	return nil, serr.New(serr.PreconditionFailed, "put", "if-match etag mismatch")
}

// A store that refuses a Put before reading answers at once, rather than
// waiting for a client that is still streaming to send its next message.
func TestPutReturnsABackendRefusalWithoutWaitingOnTheClient(t *testing.T) {
	c := newTestClientFor(t, failingPutBackend{Backend: openMemBackend(t)})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := c.Put(ctx)
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: &storagev0.PutHeader{Key: "k", TotalSize: 1 << 20}}}))
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: []byte("first chunk")}}))

	// Keep the stream open, as a slow client would, and read the answer.
	start := time.Now()
	var res storagev0.PutResult
	err = st.RecvMsg(&res)
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
}

// A client that cancels mid-stream ends the Put with nothing stored.
func TestPutCancelledByTheClientStoresNothing(t *testing.T) {
	c, _ := newExactSizeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	st, err := c.Put(ctx)
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: &storagev0.PutHeader{Key: "k", TotalSize: 100}}}))
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: []byte("partial")}}))
	cancel()
	_, err = st.CloseAndRecv()
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled, "%v", err)

	require.Eventually(t, func() bool {
		_, serr := c.Stat(context.Background(), &storagev0.StatRequest{Key: "k"})
		return status.Code(serr) == codes.NotFound
	}, 2*time.Second, 50*time.Millisecond)
}
