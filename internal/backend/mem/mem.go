// Package mem is a real, deterministic in-memory object-storage backend. It is
// not a mock of a cloud — it is a working store used for fast local runs and as
// the fixture the cache/server tests exercise real behavior against. It honors
// conditional writes, ranges, conditional reads, prefix listing, and copy.
package mem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

func init() { backend.Register("mem", New) }

type object struct {
	data []byte
	info backend.ObjectInfo
}

// Backend is an in-memory object store.
type Backend struct {
	mu     sync.RWMutex
	objs   map[string]object
	bucket string
	now    func() time.Time
}

// New constructs an empty in-memory backend.
func New(_ context.Context, cfg backend.Config) (backend.Backend, error) {
	return &Backend{objs: map[string]object{}, bucket: cfg.Bucket, now: time.Now}, nil
}

func (b *Backend) Name() string { return "mem" }

func (b *Backend) Identity() string { return b.bucket }

func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		Backend:             "mem",
		ConditionalPut:      true,
		ConditionalCopy:     true,
		AtomicRename:        false,
		Versions:            false,
		Tags:                false,
		Presign:             true,
		PresignMaxExpiry:    7 * 24 * time.Hour,
		PresignAmbientCreds: true,
		BatchDeleteMax:      1000,
	}
}

// Probe always succeeds, under either strategy: the store lives in this
// process, so it is reachable exactly as long as the caller holding it is, and
// there is no endpoint, bucket or credential that could answer differently.
func (b *Backend) Probe(_ context.Context) error { return nil }

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "\"" + hex.EncodeToString(sum[:]) + "\""
}

func (b *Backend) Stat(_ context.Context, key, _ string) (*backend.ObjectInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	o, ok := b.objs[key]
	if !ok {
		return nil, serr.New(serr.NotFound, "stat", key)
	}
	info := o.info
	return &info, nil
}

func (b *Backend) Get(_ context.Context, key string, opts backend.GetOptions) (*backend.GetResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	o, ok := b.objs[key]
	if !ok {
		return nil, serr.New(serr.NotFound, "get", key)
	}
	if opts.IfNoneMatch != "" && opts.IfNoneMatch == o.info.ETag {
		return &backend.GetResult{Info: o.info, NotModified: true}, nil
	}
	if !opts.IfModifiedSince.IsZero() && !o.info.LastModified.After(opts.IfModifiedSince) {
		return &backend.GetResult{Info: o.info, NotModified: true}, nil
	}
	if opts.IfMatch != "" && opts.IfMatch != o.info.ETag {
		return nil, serr.New(serr.PreconditionFailed, "get", "if-match mismatch")
	}
	data := o.data
	if r := opts.Range; r != nil {
		start := r.Offset
		if start < 0 || start > int64(len(data)) {
			return nil, serr.New(serr.InvalidArgument, "get", "range out of bounds")
		}
		end := int64(len(data))
		if r.Length > 0 && start+r.Length < end {
			end = start + r.Length
		}
		data = data[start:end]
	}
	info := o.info
	return &backend.GetResult{Info: info, Body: io.NopCloser(bytes.NewReader(append([]byte(nil), data...)))}, nil
}

func (b *Backend) Put(_ context.Context, key string, r io.Reader, opts backend.PutOptions) (*backend.PutResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, "put", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	existing, exists := b.objs[key]
	if opts.IfNoneMatch == "*" && exists {
		return nil, serr.New(serr.AlreadyExists, "put", key)
	}
	if opts.IfMatch != "" {
		if !exists || existing.info.ETag != opts.IfMatch {
			return nil, serr.New(serr.PreconditionFailed, "put", "if-match mismatch")
		}
	}
	etag := etagOf(data)
	info := backend.ObjectInfo{
		Key:             key,
		ETag:            etag,
		Size:            int64(len(data)),
		LastModified:    b.now(),
		ContentType:     opts.ContentType,
		ContentEncoding: opts.ContentEncoding,
		CacheControl:    opts.CacheControl,
		UserMetadata:    opts.UserMetadata,
	}
	b.objs[key] = object{data: data, info: info}
	return &backend.PutResult{ETag: etag}, nil
}

func (b *Backend) Delete(_ context.Context, key string, opts backend.DeleteOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, ok := b.objs[key]
	if !ok {
		if opts.IfMatch != "" {
			// A compare-and-delete can't match a missing object; fail the
			// precondition rather than report a delete that never happened.
			return serr.New(serr.PreconditionFailed, "delete", "if-match on missing object")
		}
		return nil // unconditional delete is idempotent
	}
	if opts.IfMatch != "" && o.info.ETag != opts.IfMatch {
		return serr.New(serr.PreconditionFailed, "delete", "if-match mismatch")
	}
	delete(b.objs, key)
	return nil
}

func (b *Backend) DeleteMany(ctx context.Context, keys []string) ([]backend.DeleteEntry, error) {
	out := make([]backend.DeleteEntry, 0, len(keys))
	for _, k := range keys {
		err := b.Delete(ctx, k, backend.DeleteOptions{})
		e := backend.DeleteEntry{Key: k}
		if err != nil {
			e.Error = err.Error()
		}
		out = append(out, e)
	}
	return out, nil
}

func (b *Backend) List(_ context.Context, opts backend.ListOptions) (*backend.ListResult, error) {
	b.mu.RLock()
	keys := make([]string, 0, len(b.objs))
	for k := range b.objs {
		if strings.HasPrefix(k, opts.Prefix) {
			keys = append(keys, k)
		}
	}
	infos := map[string]backend.ObjectInfo{}
	for _, k := range keys {
		infos[k] = b.objs[k].info
	}
	b.mu.RUnlock()
	sort.Strings(keys)

	limit := int(opts.Limit)
	if limit <= 0 {
		limit = 1000
	}
	start := 0
	if opts.PageToken != "" {
		if raw, err := base64.RawURLEncoding.DecodeString(opts.PageToken); err == nil {
			after := string(raw)
			start = sort.SearchStrings(keys, after)
			for start < len(keys) && keys[start] <= after {
				start++
			}
		}
	}

	res := &backend.ListResult{}
	seenPrefix := map[string]bool{}
	count := 0
	i := start
	for ; i < len(keys) && count < limit; i++ {
		k := keys[i]
		if opts.Delimiter != "" {
			rest := k[len(opts.Prefix):]
			if idx := strings.Index(rest, opts.Delimiter); idx >= 0 {
				cp := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				if !seenPrefix[cp] {
					seenPrefix[cp] = true
					res.CommonPrefixes = append(res.CommonPrefixes, cp)
					count++
				}
				continue
			}
		}
		info := infos[k]
		res.Objects = append(res.Objects, info)
		count++
	}
	if i < len(keys) {
		res.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(keys[i-1]))
	}
	return res, nil
}

func (b *Backend) Copy(_ context.Context, srcKey, dstKey string, opts backend.CopyOptions) (*backend.PutResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	src, ok := b.objs[srcKey]
	if !ok {
		return nil, serr.New(serr.NotFound, "copy", srcKey)
	}
	if opts.IfNoneMatch == "*" {
		if _, exists := b.objs[dstKey]; exists {
			return nil, serr.New(serr.AlreadyExists, "copy", dstKey)
		}
	}
	info := src.info
	info.Key = dstKey
	info.LastModified = b.now()
	b.objs[dstKey] = object{data: append([]byte(nil), src.data...), info: info}
	return &backend.PutResult{ETag: info.ETag}, nil
}

func (b *Backend) Presign(_ context.Context, key string, method backend.PresignMethod, expiry time.Duration) (*backend.PresignResult, error) {
	if expiry <= 0 || expiry > 7*24*time.Hour {
		expiry = 7 * 24 * time.Hour
	}
	verb := "GET"
	if method == backend.PresignPut {
		verb = "PUT"
	}
	return &backend.PresignResult{
		Method:    method,
		URL:       fmt.Sprintf("mem://local/%s?verb=%s", key, verb),
		ExpiresAt: b.now().Add(expiry),
	}, nil
}

func (b *Backend) Native(_ context.Context, verb string, _ map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "native", verb)
}

func (b *Backend) Close() error { return nil }
