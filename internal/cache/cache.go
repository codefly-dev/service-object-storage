// Package cache is the gateway's read-through caching layer. It decorates a
// backend.Backend, so the server treats a cached backend exactly like a raw one.
//
// Design (from the object-storage cache research):
//   - Two tiers: an in-process L1 (expirable LRU) over a shared Redis L2.
//   - Immutable-block keying: object bytes are keyed by their validator (ETag),
//     so they never need invalidation; only the key→validator metadata pointer
//     is invalidated on write.
//   - Read-through with singleflight (stampede protection), short negative
//     caching, and write-around + invalidate on every mutating op.
//   - Byte caching only below a size threshold; larger objects stream through
//     (and in the server, take the presigned-URL path).
//   - Version-pinned reads are immutable and cached without revalidation.
//
// Consistency: strong read-after-write when writes go through this layer (a
// mutating op invalidates synchronously and publishes a cross-replica eviction);
// bounded staleness for out-of-band writes. List is not cached in v0.
package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// Options tunes the cache. Zero values fall back to sane defaults.
type Options struct {
	// MaxCachedObjectBytes caps which objects are byte-cached; larger objects
	// stream through uncached. Default 1 MiB.
	MaxCachedObjectBytes int64
	// MetaTTL bounds cached metadata freshness. Default 30s.
	MetaTTL time.Duration
	// BytesTTL bounds cached object bytes. Default 5m.
	BytesTTL time.Duration
	// NegativeTTL bounds cached 404s. Default 3s.
	NegativeTTL time.Duration
	// L1MetaEntries / L1BytesEntries size the in-process caches. Defaults 4096 / 512.
	L1MetaEntries  int
	L1BytesEntries int
	// Namespace prefixes all keys. Default "sos/v1".
	Namespace string
}

func (o *Options) withDefaults() {
	if o.MaxCachedObjectBytes == 0 {
		o.MaxCachedObjectBytes = 1 << 20
	}
	if o.MetaTTL == 0 {
		o.MetaTTL = 30 * time.Second
	}
	if o.BytesTTL == 0 {
		o.BytesTTL = 5 * time.Minute
	}
	if o.NegativeTTL == 0 {
		o.NegativeTTL = 3 * time.Second
	}
	if o.L1MetaEntries == 0 {
		o.L1MetaEntries = 4096
	}
	if o.L1BytesEntries == 0 {
		o.L1BytesEntries = 512
	}
	if o.Namespace == "" {
		o.Namespace = "sos/v1"
	}
}

const invalidationChannel = "sos:invalidate"

// metaEntry is a cached ObjectInfo (Info nil = a cached negative/404). Exp lets
// negatives expire faster than positives inside one LRU TTL.
type metaEntry struct {
	Info *backend.ObjectInfo `json:"info,omitempty"`
	Exp  time.Time           `json:"exp"`
}

// Caching decorates a backend.Backend with the two-tier cache. It implements
// backend.Backend, so it is a drop-in for the server.
type Caching struct {
	be     backend.Backend
	rdb    redis.UniversalClient // may be nil (L1-only)
	name   string
	bucket string

	metaL1  *lru.LRU[string, metaEntry]
	bytesL1 *lru.LRU[string, []byte]
	sf      singleflight.Group
	opts    Options
	stop    chan struct{}
	nowFn   func() time.Time
}

// New wraps be with the cache. rdb may be nil for an L1-only cache (used by
// local single-replica runs and tests without a shared tier).
func New(be backend.Backend, rdb redis.UniversalClient, opts Options) *Caching {
	opts.withDefaults()
	c := &Caching{
		be:      be,
		rdb:     rdb,
		name:    be.Name(),
		bucket:  be.Bucket(),
		metaL1:  lru.NewLRU[string, metaEntry](opts.L1MetaEntries, nil, opts.MetaTTL),
		bytesL1: lru.NewLRU[string, []byte](opts.L1BytesEntries, nil, opts.BytesTTL),
		opts:    opts,
		stop:    make(chan struct{}),
		nowFn:   time.Now,
	}
	if rdb != nil {
		go c.subscribeInvalidations()
	}
	return c
}

func (c *Caching) now() time.Time { return c.nowFn() }

// --- pass-through metadata ---

func (c *Caching) Name() string                       { return c.be.Name() }
func (c *Caching) Bucket() string                     { return c.be.Bucket() }
func (c *Caching) Capabilities() backend.Capabilities { return c.be.Capabilities() }

func (c *Caching) List(ctx context.Context, opts backend.ListOptions) (*backend.ListResult, error) {
	// v0: lists are not cached (snapshot semantics make them the most dangerous
	// to cache); pass through for correctness.
	return c.be.List(ctx, opts)
}

func (c *Caching) Presign(ctx context.Context, key string, m backend.PresignMethod, exp time.Duration) (*backend.PresignResult, error) {
	return c.be.Presign(ctx, key, m, exp)
}

func (c *Caching) Native(ctx context.Context, verb string, params map[string]string) (map[string]string, error) {
	return c.be.Native(ctx, verb, params)
}

func (c *Caching) Close() error {
	close(c.stop)
	return c.be.Close()
}

// --- reads ---

// Stat is read-through with negative caching and singleflight.
func (c *Caching) Stat(ctx context.Context, key, versionID string) (*backend.ObjectInfo, error) {
	info, err := c.statCached(ctx, key, versionID)
	if err != nil {
		return nil, err
	}
	cp := *info
	return &cp, nil
}

func (c *Caching) statCached(ctx context.Context, key, versionID string) (*backend.ObjectInfo, error) {
	mk := metaKey(c.opts.Namespace, c.name, c.bucket, key, versionID)

	if e, ok := c.metaL1.Get(mk); ok && c.now().Before(e.Exp) {
		return infoOrNotFound(e)
	}
	if c.rdb != nil {
		if raw, rerr := c.rdb.Get(ctx, mk).Bytes(); rerr == nil {
			var e metaEntry
			if json.Unmarshal(raw, &e) == nil {
				c.metaL1.Add(mk, e)
				return infoOrNotFound(e)
			}
		}
	}

	v, err, _ := c.sf.Do("stat:"+mk, func() (any, error) {
		info, serrErr := c.be.Stat(ctx, key, versionID)
		if serrErr != nil {
			if serr.Is(serrErr, serr.NotFound) {
				c.storeMeta(ctx, mk, metaEntry{Exp: c.now().Add(c.opts.NegativeTTL)})
			}
			return nil, serrErr
		}
		e := metaEntry{Info: info, Exp: c.now().Add(c.opts.MetaTTL)}
		c.storeMeta(ctx, mk, e)
		return info, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*backend.ObjectInfo), nil
}

func infoOrNotFound(e metaEntry) (*backend.ObjectInfo, error) {
	if e.Info == nil {
		return nil, serr.New(serr.NotFound, "stat", "cached negative")
	}
	return e.Info, nil
}

func (c *Caching) storeMeta(ctx context.Context, mk string, e metaEntry) {
	c.metaL1.Add(mk, e)
	if c.rdb == nil {
		return
	}
	ttl := c.opts.MetaTTL
	if e.Info == nil {
		ttl = c.opts.NegativeTTL
	}
	if raw, err := json.Marshal(e); err == nil {
		_ = c.rdb.Set(ctx, mk, raw, ttl).Err()
	}
}

// Get serves whole small objects from the byte cache (keyed by validator) and
// streams everything else through. Range/conditional reads bypass the cache.
func (c *Caching) Get(ctx context.Context, key string, opts backend.GetOptions) (*backend.GetResult, error) {
	cacheable := opts.Range == nil && opts.IfNoneMatch == "" && opts.IfMatch == "" && opts.IfModifiedSince.IsZero()
	if !cacheable {
		return c.be.Get(ctx, key, opts)
	}

	info, err := c.statCached(ctx, key, opts.VersionID)
	if err != nil {
		return nil, err
	}
	if info.Size < 0 || info.Size > c.opts.MaxCachedObjectBytes || info.ETag == "" {
		return c.be.Get(ctx, key, opts) // too big / unkeyable → stream through
	}

	b, err := c.bytesCached(ctx, key, opts.VersionID, info.ETag)
	if err != nil {
		return nil, err
	}
	cp := *info
	return &backend.GetResult{Info: cp, Body: io.NopCloser(bytes.NewReader(b))}, nil
}

func (c *Caching) bytesCached(ctx context.Context, key, versionID, etag string) ([]byte, error) {
	bk := bytesKey(c.opts.Namespace, c.name, c.bucket, etag)

	if b, ok := c.bytesL1.Get(bk); ok {
		return b, nil
	}
	if c.rdb != nil {
		if b, rerr := c.rdb.Get(ctx, bk).Bytes(); rerr == nil {
			c.bytesL1.Add(bk, b)
			return b, nil
		}
	}

	v, err, _ := c.sf.Do("bytes:"+bk, func() (any, error) {
		res, gerr := c.be.Get(ctx, key, backend.GetOptions{VersionID: versionID})
		if gerr != nil {
			return nil, gerr
		}
		defer res.Body.Close()
		b, rerr := io.ReadAll(res.Body)
		if rerr != nil {
			return nil, serr.Wrap(serr.Internal, "get", rerr)
		}
		c.bytesL1.Add(bk, b)
		if c.rdb != nil {
			_ = c.rdb.Set(ctx, bk, b, c.opts.BytesTTL).Err()
		}
		return b, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// --- writes: delegate, then invalidate ---

func (c *Caching) Put(ctx context.Context, key string, r io.Reader, opts backend.PutOptions) (*backend.PutResult, error) {
	res, err := c.be.Put(ctx, key, r, opts)
	if err != nil {
		return nil, err
	}
	c.invalidate(ctx, key)
	return res, nil
}

func (c *Caching) Delete(ctx context.Context, key string, opts backend.DeleteOptions) error {
	if err := c.be.Delete(ctx, key, opts); err != nil {
		return err
	}
	c.invalidate(ctx, key)
	return nil
}

func (c *Caching) DeleteMany(ctx context.Context, keys []string) ([]backend.DeleteEntry, error) {
	entries, err := c.be.DeleteMany(ctx, keys)
	for _, e := range entries {
		if e.Error == "" {
			c.invalidate(ctx, e.Key)
		}
	}
	return entries, err
}

func (c *Caching) Copy(ctx context.Context, srcKey, dstKey string, opts backend.CopyOptions) (*backend.PutResult, error) {
	res, err := c.be.Copy(ctx, srcKey, dstKey, opts)
	if err != nil {
		return nil, err
	}
	c.invalidate(ctx, dstKey)
	return res, nil
}

// invalidate evicts the live-metadata pointer locally and, via Redis pub/sub,
// across replicas. Bytes are keyed by validator (immutable) so they need no
// eviction — a new write yields a new validator, and the old entry ages out.
func (c *Caching) invalidate(ctx context.Context, key string) {
	c.evictLocal(key)
	if c.rdb != nil {
		mk := metaKey(c.opts.Namespace, c.name, c.bucket, key, "")
		_ = c.rdb.Del(ctx, mk).Err()
		_ = c.rdb.Publish(ctx, invalidationChannel, c.name+"\x00"+c.bucket+"\x00"+key).Err()
	}
}

func (c *Caching) evictLocal(key string) {
	c.metaL1.Remove(metaKey(c.opts.Namespace, c.name, c.bucket, key, ""))
}

func (c *Caching) subscribeInvalidations() {
	ctx := context.Background()
	sub := c.rdb.Subscribe(ctx, invalidationChannel)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-c.stop:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.handleInvalidation(msg.Payload)
		}
	}
}

// handleInvalidation evicts the local entry named by a cross-replica message,
// but only when both the backend and the bucket match: a write in another
// bucket that happens to share this key name must not evict our entry.
func (c *Caching) handleInvalidation(payload string) {
	name, rest, found := splitNull(payload)
	if !found || name != c.name {
		return
	}
	bucket, key, found := splitNull(rest)
	if found && bucket == c.bucket {
		c.evictLocal(key)
	}
}

func splitNull(s string) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '\x00' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}
