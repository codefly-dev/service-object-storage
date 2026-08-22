package cache_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/cache"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// counting wraps a backend and counts Stat/Get calls so tests can assert
// read-through behavior.
type counting struct {
	backend.Backend
	statCalls atomic.Int64
	getCalls  atomic.Int64
}

func (c *counting) Stat(ctx context.Context, key, versionID string) (*backend.ObjectInfo, error) {
	c.statCalls.Add(1)
	return c.Backend.Stat(ctx, key, versionID)
}

func (c *counting) Get(ctx context.Context, key string, opts backend.GetOptions) (*backend.GetResult, error) {
	c.getCalls.Add(1)
	return c.Backend.Get(ctx, key, opts)
}

func newCounting(t *testing.T) *counting {
	t.Helper()
	be, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	return &counting{Backend: be}
}

func newRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func putRaw(t *testing.T, be backend.Backend, key string, data []byte) {
	t.Helper()
	_, err := be.Put(context.Background(), key, bytes.NewReader(data), backend.PutOptions{Size: int64(len(data))})
	require.NoError(t, err)
}

func readAll(t *testing.T, res *backend.GetResult) []byte {
	t.Helper()
	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())
	return b
}

func TestStatReadThrough(t *testing.T) {
	cnt := newCounting(t)
	c := cache.New(cnt, newRedis(t), cache.Options{})
	putRaw(t, cnt.Backend, "k", []byte("v"))

	for i := 0; i < 3; i++ {
		info, err := c.Stat(context.Background(), "k", "")
		require.NoError(t, err)
		require.Equal(t, int64(1), info.Size)
	}
	require.Equal(t, int64(1), cnt.statCalls.Load(), "stat should be served from cache after the first call")
}

func TestNegativeCaching(t *testing.T) {
	cnt := newCounting(t)
	c := cache.New(cnt, newRedis(t), cache.Options{})

	for i := 0; i < 3; i++ {
		_, err := c.Stat(context.Background(), "missing", "")
		require.True(t, serr.Is(err, serr.NotFound))
	}
	require.Equal(t, int64(1), cnt.statCalls.Load(), "404 should be negatively cached")
}

func TestBytesCachedAndImmutable(t *testing.T) {
	cnt := newCounting(t)
	c := cache.New(cnt, newRedis(t), cache.Options{})
	putRaw(t, cnt.Backend, "obj", []byte("first"))

	for i := 0; i < 3; i++ {
		res, err := c.Get(context.Background(), "obj", backend.GetOptions{})
		require.NoError(t, err)
		require.Equal(t, []byte("first"), readAll(t, res))
	}
	require.Equal(t, int64(1), cnt.getCalls.Load(), "small object bytes should be cached")

	// Overwrite through the cache -> invalidation; the next read reflects it.
	_, err := c.Put(context.Background(), "obj", strings.NewReader("second"), backend.PutOptions{Size: 6})
	require.NoError(t, err)

	res, err := c.Get(context.Background(), "obj", backend.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []byte("second"), readAll(t, res))
}

func TestInvalidationClearsStaleMeta(t *testing.T) {
	cnt := newCounting(t)
	c := cache.New(cnt, newRedis(t), cache.Options{})
	putRaw(t, cnt.Backend, "k", []byte("v1"))

	first, err := c.Stat(context.Background(), "k", "")
	require.NoError(t, err)

	_, err = c.Put(context.Background(), "k", strings.NewReader("v2-longer"), backend.PutOptions{Size: 9})
	require.NoError(t, err)

	after, err := c.Stat(context.Background(), "k", "")
	require.NoError(t, err)
	require.NotEqual(t, first.ETag, after.ETag, "stat must reflect the new object after a write")
	require.Equal(t, int64(9), after.Size)
}

func TestL1OnlyNoRedis(t *testing.T) {
	cnt := newCounting(t)
	c := cache.New(cnt, nil, cache.Options{}) // L1-only
	putRaw(t, cnt.Backend, "k", []byte("v"))

	_, err := c.Stat(context.Background(), "k", "")
	require.NoError(t, err)
	_, err = c.Stat(context.Background(), "k", "")
	require.NoError(t, err)
	require.Equal(t, int64(1), cnt.statCalls.Load())
}

func TestLargeObjectNotByteCached(t *testing.T) {
	cnt := newCounting(t)
	c := cache.New(cnt, nil, cache.Options{MaxCachedObjectBytes: 4})
	putRaw(t, cnt.Backend, "big", []byte("0123456789")) // 10 bytes > threshold

	for i := 0; i < 2; i++ {
		res, err := c.Get(context.Background(), "big", backend.GetOptions{})
		require.NoError(t, err)
		require.Len(t, readAll(t, res), 10)
	}
	require.Equal(t, int64(2), cnt.getCalls.Load(), "large objects stream through uncached")
}
