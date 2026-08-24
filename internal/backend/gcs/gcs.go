// Package gcs implements backend.Backend against Google Cloud Storage.
//
// GCS uses generation numbers as its version identifier and generation-match
// preconditions for conditional writes. Two contract points are emulated rather
// than served natively:
//
//   - Conditional reads (If-None-Match / If-Modified-Since / If-Match on ETag):
//     the GCS reader exposes no ETag precondition, so Get first HEADs the object
//     and evaluates the condition client-side before opening the range reader.
//   - Presign requires signing credentials (a service-account private key or an
//     IAM signBlob path). A client opened from ambient/ADC credentials without a
//     usable key cannot sign, and Presign returns serr.Unsupported in that case.
package gcs

import (
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

func init() {
	backend.Register("gcs", New)
}

// maxPresignExpiry is the hard ceiling GCS enforces on V4 signed URLs.
const maxPresignExpiry = 7 * 24 * time.Hour

// defaultListPageSize is used when ListOptions.Limit is not positive.
const defaultListPageSize = 1000

// batchDeleteMax reflects the per-call chunk we advertise; GCS has no server
// batch delete, so DeleteMany issues one Delete per key.
const batchDeleteMax = 100

// Backend is a GCS-backed implementation of backend.Backend.
type Backend struct {
	client *storage.Client
	bucket *storage.BucketHandle
	name   string
	cfg    backend.Config
}

// New opens a GCS backend bound to cfg.Bucket.
func New(ctx context.Context, cfg backend.Config) (backend.Backend, error) {
	var opts []option.ClientOption
	if cfg.GCSCredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.GCSCredentialsFile))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, mapErr("new", err)
	}
	return &Backend{
		client: client,
		bucket: client.Bucket(cfg.Bucket),
		name:   "gcs",
		cfg:    cfg,
	}, nil
}

// Name reports the backend kind.
func (b *Backend) Name() string { return "gcs" }

// Identity reports a globally unique identifier for this bucket. GCS bucket
// names are globally unique, so the bucket alone suffices.
func (b *Backend) Identity() string { return b.cfg.Bucket }

// Capabilities reports the GCS feature set.
func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		Backend:             "gcs",
		ConditionalPut:      true,
		ConditionalCopy:     true,
		AtomicRename:        false,
		Versions:            true,
		Tags:                false,
		Presign:             true,
		PresignMaxExpiry:    maxPresignExpiry,
		PresignAmbientCreds: false,
		BatchDeleteMax:      batchDeleteMax,
		NativeVerbs:         nil,
	}
}

// obj returns an object handle, pinned to a generation when versionID is set.
func (b *Backend) obj(op, key, versionID string) (*storage.ObjectHandle, error) {
	o := b.bucket.Object(key)
	if versionID != "" {
		gen, err := strconv.ParseInt(versionID, 10, 64)
		if err != nil {
			return nil, serr.Wrap(serr.InvalidArgument, op, err)
		}
		o = o.Generation(gen)
	}
	return o, nil
}

// infoFromAttrs maps GCS object attributes into the neutral ObjectInfo.
func infoFromAttrs(key string, a *storage.ObjectAttrs) backend.ObjectInfo {
	return backend.ObjectInfo{
		Key:             key,
		ETag:            a.Etag,
		Size:            a.Size,
		LastModified:    a.Updated,
		VersionID:       strconv.FormatInt(a.Generation, 10),
		Generation:      a.Generation,
		MetaGeneration:  a.Metageneration,
		ContentType:     a.ContentType,
		ContentEncoding: a.ContentEncoding,
		CacheControl:    a.CacheControl,
		UserMetadata:    a.Metadata,
	}
}

// Stat returns object metadata via a HEAD-equivalent Attrs call.
func (b *Backend) Stat(ctx context.Context, key, versionID string) (*backend.ObjectInfo, error) {
	const op = "stat"
	o, err := b.obj(op, key, versionID)
	if err != nil {
		return nil, err
	}
	a, err := o.Attrs(ctx)
	if err != nil {
		return nil, mapErr(op, err)
	}
	info := infoFromAttrs(key, a)
	return &info, nil
}

// Get reads an object, emulating conditional-read semantics client-side.
func (b *Backend) Get(ctx context.Context, key string, opts backend.GetOptions) (*backend.GetResult, error) {
	const op = "get"
	o, err := b.obj(op, key, opts.VersionID)
	if err != nil {
		return nil, err
	}

	a, err := o.Attrs(ctx)
	if err != nil {
		return nil, mapErr(op, err)
	}
	info := infoFromAttrs(key, a)

	// Emulated conditional read: GCS readers carry no ETag precondition.
	if opts.IfNoneMatch != "" && a.Etag == opts.IfNoneMatch {
		return &backend.GetResult{Info: info, NotModified: true}, nil
	}
	if !opts.IfModifiedSince.IsZero() && !a.Updated.After(opts.IfModifiedSince) {
		return &backend.GetResult{Info: info, NotModified: true}, nil
	}
	if opts.IfMatch != "" && a.Etag != opts.IfMatch {
		return nil, serr.New(serr.PreconditionFailed, op, "if-match etag mismatch")
	}

	offset, length := int64(0), int64(-1)
	if opts.Range != nil {
		offset = opts.Range.Offset
		if opts.Range.Length <= 0 {
			length = -1
		} else {
			length = opts.Range.Length
		}
	}
	rc, err := o.NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, mapErr(op, err)
	}
	return &backend.GetResult{Info: info, Body: rc}, nil
}

// Put writes an object, honoring create-if-absent and compare-and-swap intent.
func (b *Backend) Put(ctx context.Context, key string, r io.Reader, opts backend.PutOptions) (*backend.PutResult, error) {
	const op = "put"
	o := b.bucket.Object(key)

	createIfAbsent := opts.IfNoneMatch == "*"
	switch {
	case createIfAbsent:
		o = o.If(storage.Conditions{DoesNotExist: true})
	case opts.IfMatch != "":
		a, err := b.bucket.Object(key).Attrs(ctx)
		if err != nil {
			return nil, mapErr(op, err)
		}
		if a.Etag != opts.IfMatch {
			return nil, serr.New(serr.PreconditionFailed, op, "if-match etag mismatch")
		}
		o = o.If(storage.Conditions{GenerationMatch: a.Generation})
	}

	w := o.NewWriter(ctx)
	w.ContentType = opts.ContentType
	w.ContentEncoding = opts.ContentEncoding
	w.CacheControl = opts.CacheControl
	w.Metadata = opts.UserMetadata

	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return nil, mapErr(op, err)
	}
	if err := w.Close(); err != nil {
		if createIfAbsent && serr.CodeOf(mapErr(op, err)) == serr.PreconditionFailed {
			return nil, serr.Wrap(serr.AlreadyExists, op, err)
		}
		return nil, mapErr(op, err)
	}

	a := w.Attrs()
	return &backend.PutResult{
		ETag:       a.Etag,
		VersionID:  strconv.FormatInt(a.Generation, 10),
		Generation: a.Generation,
	}, nil
}

// Delete removes an object (or a version); missing objects are treated as
// already-deleted so unconditional deletes are idempotent.
func (b *Backend) Delete(ctx context.Context, key string, opts backend.DeleteOptions) error {
	const op = "delete"
	o, err := b.obj(op, key, opts.VersionID)
	if err != nil {
		return err
	}
	if opts.IfMatch != "" {
		a, err := b.bucket.Object(key).Attrs(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				return nil
			}
			return mapErr(op, err)
		}
		if a.Etag != opts.IfMatch {
			return serr.New(serr.PreconditionFailed, op, "if-match etag mismatch")
		}
		o = o.If(storage.Conditions{GenerationMatch: a.Generation})
	}
	if err := o.Delete(ctx); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil
		}
		return mapErr(op, err)
	}
	return nil
}

// DeleteMany deletes keys one at a time (GCS has no server-side batch delete)
// and reports a per-key outcome; a missing object counts as success.
func (b *Backend) DeleteMany(ctx context.Context, keys []string) ([]backend.DeleteEntry, error) {
	out := make([]backend.DeleteEntry, 0, len(keys))
	for _, key := range keys {
		entry := backend.DeleteEntry{Key: key}
		if err := b.bucket.Object(key).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
			entry.Error = err.Error()
		}
		out = append(out, entry)
	}
	return out, nil
}

// List enumerates objects by prefix, folding sub-prefixes into CommonPrefixes.
func (b *Backend) List(ctx context.Context, opts backend.ListOptions) (*backend.ListResult, error) {
	const op = "list"
	it := b.bucket.Objects(ctx, &storage.Query{
		Prefix:    opts.Prefix,
		Delimiter: opts.Delimiter,
	})

	pageSize := defaultListPageSize
	if opts.Limit > 0 {
		pageSize = int(opts.Limit)
	}
	pager := iterator.NewPager(it, pageSize, opts.PageToken)

	var items []*storage.ObjectAttrs
	nextTok, err := pager.NextPage(&items)
	if err != nil {
		return nil, mapErr(op, err)
	}

	res := &backend.ListResult{NextPageToken: nextTok}
	for _, a := range items {
		if a.Prefix != "" {
			res.CommonPrefixes = append(res.CommonPrefixes, a.Prefix)
			continue
		}
		res.Objects = append(res.Objects, infoFromAttrs(a.Name, a))
	}
	return res, nil
}

// Copy performs a server-side copy, optionally gated on the destination not
// already existing.
func (b *Backend) Copy(ctx context.Context, srcKey, dstKey string, opts backend.CopyOptions) (*backend.PutResult, error) {
	const op = "copy"
	src := b.bucket.Object(srcKey)
	dst := b.bucket.Object(dstKey)
	if opts.IfNoneMatch == "*" {
		dst = dst.If(storage.Conditions{DoesNotExist: true})
	}
	a, err := dst.CopierFrom(src).Run(ctx)
	if err != nil {
		return nil, mapErr(op, err)
	}
	return &backend.PutResult{
		ETag:       a.Etag,
		VersionID:  strconv.FormatInt(a.Generation, 10),
		Generation: a.Generation,
	}, nil
}

// Presign returns a V4 signed URL; it requires signing credentials and returns
// serr.Unsupported when the client cannot sign.
func (b *Backend) Presign(ctx context.Context, key string, method backend.PresignMethod, expiry time.Duration) (*backend.PresignResult, error) {
	const op = "presign"

	httpMethod := "GET"
	if method == backend.PresignPut {
		httpMethod = "PUT"
	}

	expiry = clampExpiry(expiry, b.cfg.PresignMaxExpiry)
	expiresAt := time.Now().Add(expiry)

	url, err := b.bucket.SignedURL(key, &storage.SignedURLOptions{
		Method:  httpMethod,
		Expires: expiresAt,
		Scheme:  storage.SigningSchemeV4,
	})
	if err != nil {
		return nil, serr.New(serr.Unsupported, op,
			"no signing credentials (needs SA key or IAM signBlob)")
	}
	return &backend.PresignResult{
		Method:    method,
		URL:       url,
		ExpiresAt: expiresAt,
	}, nil
}

// clampExpiry bounds a requested lifetime to the GCS ceiling and the configured
// maximum (when set).
func clampExpiry(expiry, cfgMax time.Duration) time.Duration {
	if expiry <= 0 || expiry > maxPresignExpiry {
		expiry = maxPresignExpiry
	}
	if cfgMax > 0 && expiry > cfgMax {
		expiry = cfgMax
	}
	return expiry
}

// Native has no GCS-specific verbs.
func (b *Backend) Native(ctx context.Context, verb string, params map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "native", verb)
}

// Close releases the underlying client.
func (b *Backend) Close() error {
	return b.client.Close()
}

// mapErr normalizes GCS/googleapi errors into serr codes.
func mapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) {
		return serr.Wrap(serr.NotFound, op, err)
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		switch gerr.Code {
		case 304:
			return serr.Wrap(serr.NotModified, op, err)
		case 403:
			return serr.Wrap(serr.PermissionDenied, op, err)
		case 404:
			return serr.Wrap(serr.NotFound, op, err)
		case 409:
			return serr.Wrap(serr.AlreadyExists, op, err)
		case 412:
			return serr.Wrap(serr.PreconditionFailed, op, err)
		case 429, 503:
			return serr.Wrap(serr.Throttled, op, err)
		}
	}
	return serr.Wrap(serr.Internal, op, err)
}
