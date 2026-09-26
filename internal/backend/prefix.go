package backend

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// NormalizePrefix canonicalizes a configured key prefix: surrounding
// whitespace and slashes are dropped and exactly one trailing "/" is added, so
// "tenant-a", "/tenant-a/" and "tenant-a/" all name the same key space. Empty
// stays empty (the whole bucket). A prefix with an empty, "." or ".." segment
// is refused: object stores treat those bytes literally, so the key space they
// name is not the one an operator reading the configuration would expect.
func NormalizePrefix(raw string) (string, error) {
	trimmed := strings.Trim(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return "", nil
	}
	for _, segment := range strings.Split(trimmed, "/") {
		switch segment {
		case "", ".", "..":
			return "", fmt.Errorf("SOS_PREFIX %q is not a usable key prefix: segments must be non-empty and not %q or %q", raw, ".", "..")
		}
	}
	return trimmed + "/", nil
}

// prefixed confines a backend to one key prefix. Every key a caller passes is
// joined onto the prefix before it reaches the store, and every key the store
// reports is stripped of it on the way back, so callers work in keys relative
// to the prefix and can neither see nor address anything outside it.
type prefixed struct {
	inner  Backend
	prefix string
}

func (p *prefixed) key(k string) string { return p.prefix + k }

func (p *prefixed) strip(k string) string { return strings.TrimPrefix(k, p.prefix) }

func (p *prefixed) info(i *ObjectInfo) {
	if i != nil {
		i.Key = p.strip(i.Key)
	}
}

func (p *prefixed) Name() string { return p.inner.Name() }

// Identity folds the prefix in: two gateways on one bucket with different
// prefixes address different objects under the same relative key, so they are
// different locations.
func (p *prefixed) Identity() string { return p.inner.Identity() + "#prefix=" + p.prefix }

func (p *prefixed) Capabilities() Capabilities { return p.inner.Capabilities() }

func (p *prefixed) Probe(ctx context.Context) error { return p.inner.Probe(ctx) }

func (p *prefixed) Stat(ctx context.Context, key, versionID string) (*ObjectInfo, error) {
	info, err := p.inner.Stat(ctx, p.key(key), versionID)
	p.info(info)
	return info, err
}

func (p *prefixed) Get(ctx context.Context, key string, opts GetOptions) (*GetResult, error) {
	res, err := p.inner.Get(ctx, p.key(key), opts)
	if res != nil {
		p.info(&res.Info)
	}
	return res, err
}

func (p *prefixed) Put(ctx context.Context, key string, r io.Reader, opts PutOptions) (*PutResult, error) {
	return p.inner.Put(ctx, p.key(key), r, opts)
}

func (p *prefixed) Delete(ctx context.Context, key string, opts DeleteOptions) error {
	return p.inner.Delete(ctx, p.key(key), opts)
}

func (p *prefixed) DeleteMany(ctx context.Context, keys []string) ([]DeleteEntry, error) {
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = p.key(k)
	}
	entries, err := p.inner.DeleteMany(ctx, full)
	for i := range entries {
		entries[i].Key = p.strip(entries[i].Key)
	}
	return entries, err
}

func (p *prefixed) List(ctx context.Context, opts ListOptions) (*ListResult, error) {
	opts.Prefix = p.key(opts.Prefix)
	res, err := p.inner.List(ctx, opts)
	if res != nil {
		for i := range res.Objects {
			p.info(&res.Objects[i])
		}
		for i, cp := range res.CommonPrefixes {
			res.CommonPrefixes[i] = p.strip(cp)
		}
	}
	return res, err
}

func (p *prefixed) Copy(ctx context.Context, srcKey, dstKey string, opts CopyOptions) (*PutResult, error) {
	return p.inner.Copy(ctx, p.key(srcKey), p.key(dstKey), opts)
}

func (p *prefixed) Presign(ctx context.Context, key string, method PresignMethod, expiry time.Duration) (*PresignResult, error) {
	return p.inner.Presign(ctx, p.key(key), method, expiry)
}

// Native is refused under a prefix. Its parameters are opaque here, so a verb
// that addressed keys would reach outside the prefix; failing closed means a
// future native verb has to be taught the prefix before it is offered here.
func (p *prefixed) Native(_ context.Context, verb string, _ map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "native", verb+" is not available on a prefixed store")
}

func (p *prefixed) Close() error { return p.inner.Close() }
