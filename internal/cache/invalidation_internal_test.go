package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
)

// TestHandleInvalidationBucketScoped verifies the cross-replica eviction filter
// keys on both backend and bucket: a write in another bucket that shares a key
// name must not evict this replica's entry.
func TestHandleInvalidationBucketScoped(t *testing.T) {
	be, err := mem.New(context.Background(), backend.Config{Bucket: "bucket-b"})
	require.NoError(t, err)
	c := New(be, nil, Options{}) // L1-only; we drive handleInvalidation directly

	mk := metaKey(c.opts.Namespace, c.name, c.bucket, "k", "")
	c.metaL1.Add(mk, metaEntry{Info: &backend.ObjectInfo{Key: "k"}, Exp: c.now().Add(time.Hour)})

	// A same backend name but different bucket must be ignored.
	c.handleInvalidation("mem\x00bucket-a\x00k")
	_, ok := c.metaL1.Get(mk)
	require.True(t, ok, "a different-bucket invalidation must not evict")

	// A different backend name must be ignored.
	c.handleInvalidation("s3\x00bucket-b\x00k")
	_, ok = c.metaL1.Get(mk)
	require.True(t, ok, "a different-backend invalidation must not evict")

	// The matching backend and bucket evicts.
	c.handleInvalidation("mem\x00bucket-b\x00k")
	_, ok = c.metaL1.Get(mk)
	require.False(t, ok, "a matching invalidation must evict")
}
