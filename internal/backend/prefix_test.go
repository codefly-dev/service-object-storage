package backend_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

func TestNormalizePrefix(t *testing.T) {
	for raw, want := range map[string]string{
		"":             "",
		"  ":           "",
		"/":            "",
		"documents":    "documents/",
		"/documents/":  "documents/",
		" tenant/a/ ":  "tenant/a/",
		"documents///": "documents/",
	} {
		got, err := backend.NormalizePrefix(raw)
		require.NoError(t, err, raw)
		require.Equal(t, want, got, raw)
	}
	for _, raw := range []string{"a//b", "a/./b", "../b", "a/.."} {
		_, err := backend.NormalizePrefix(raw)
		require.Error(t, err, raw)
		require.Contains(t, err.Error(), "SOS_PREFIX")
	}
}

// TestOpenAppliesNoWrapperWithoutPrefix keeps the unprefixed path byte-for-byte
// what it was: the backend Open returns is the implementation itself.
func TestOpenAppliesNoWrapperWithoutPrefix(t *testing.T) {
	be, err := backend.Open(context.Background(), backend.Config{Kind: "mem", Bucket: "b"})
	require.NoError(t, err)
	require.IsType(t, &mem.Backend{}, be)
	require.Equal(t, "b", be.Identity())
}

func TestOpenRejectsUnusablePrefix(t *testing.T) {
	_, err := backend.Open(context.Background(), backend.Config{Kind: "mem", Bucket: "b", Prefix: "a/../b"})
	require.Error(t, err)
}

// TestPrefixConfinesKeys drives every key-bearing verb through two prefixes
// over ONE store: each sees only its own keys, relative to its prefix, and the
// store holds them under the full key.
func TestPrefixConfinesKeys(t *testing.T) {
	ctx := context.Background()
	store, err := mem.New(ctx, backend.Config{Bucket: "shared"})
	require.NoError(t, err)
	a := backend.WithPrefix(store, "tenant-a/")
	b := backend.WithPrefix(store, "tenant-b/")

	_, err = a.Put(ctx, "docs/one.txt", strings.NewReader("a1"), backend.PutOptions{})
	require.NoError(t, err)
	_, err = b.Put(ctx, "docs/one.txt", strings.NewReader("b1"), backend.PutOptions{})
	require.NoError(t, err)

	// Stored under the full key.
	raw, err := store.Stat(ctx, "tenant-a/docs/one.txt", "")
	require.NoError(t, err)
	require.Equal(t, "tenant-a/docs/one.txt", raw.Key)

	// Read back relative, and isolated.
	got, err := a.Get(ctx, "docs/one.txt", backend.GetOptions{})
	require.NoError(t, err)
	body, err := io.ReadAll(got.Body)
	require.NoError(t, err)
	require.NoError(t, got.Body.Close())
	require.Equal(t, "a1", string(body))
	require.Equal(t, "docs/one.txt", got.Info.Key)

	info, err := b.Stat(ctx, "docs/one.txt", "")
	require.NoError(t, err)
	require.Equal(t, "docs/one.txt", info.Key)

	// List is relative and confined, common prefixes included.
	list, err := a.List(ctx, backend.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Objects, 1)
	require.Equal(t, "docs/one.txt", list.Objects[0].Key)
	list, err = a.List(ctx, backend.ListOptions{Delimiter: "/"})
	require.NoError(t, err)
	require.Equal(t, []string{"docs/"}, list.CommonPrefixes)

	// Copy stays inside the prefix on both ends.
	_, err = a.Copy(ctx, "docs/one.txt", "docs/two.txt", backend.CopyOptions{})
	require.NoError(t, err)
	_, err = store.Stat(ctx, "tenant-a/docs/two.txt", "")
	require.NoError(t, err)
	_, err = b.Stat(ctx, "docs/two.txt", "")
	require.True(t, serr.Is(err, serr.NotFound), "tenant-b must not see tenant-a's copy: %v", err)

	// DeleteMany reports relative keys and removes only its own.
	entries, err := a.DeleteMany(ctx, []string{"docs/one.txt", "docs/two.txt"})
	require.NoError(t, err)
	require.Equal(t, "docs/one.txt", entries[0].Key)
	require.Equal(t, "docs/two.txt", entries[1].Key)
	_, err = b.Stat(ctx, "docs/one.txt", "")
	require.NoError(t, err, "deleting under tenant-a must not touch tenant-b")

	require.NoError(t, b.Delete(ctx, "docs/one.txt", backend.DeleteOptions{}))
	_, err = store.Stat(ctx, "tenant-b/docs/one.txt", "")
	require.True(t, serr.Is(err, serr.NotFound), "%v", err)
}

// TestPrefixSeparatesIdentity: two prefixes on one bucket address different
// objects under the same relative key, so they must not share an identity.
func TestPrefixSeparatesIdentity(t *testing.T) {
	store, err := mem.New(context.Background(), backend.Config{Bucket: "shared"})
	require.NoError(t, err)
	a := backend.WithPrefix(store, "tenant-a/")
	b := backend.WithPrefix(store, "tenant-b/")
	require.NotEqual(t, a.Identity(), b.Identity())
	require.NotEqual(t, store.Identity(), a.Identity())
	require.Equal(t, store.Name(), a.Name())
}

func TestPrefixRefusesNative(t *testing.T) {
	store, err := mem.New(context.Background(), backend.Config{Bucket: "shared"})
	require.NoError(t, err)
	_, err = backend.WithPrefix(store, "p/").Native(context.Background(), "anything", nil)
	require.True(t, serr.Is(err, serr.Unsupported), "%v", err)
}
