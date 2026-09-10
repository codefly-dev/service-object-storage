// Package minio implements backend.Backend against a MinIO / S3-compatible
// server using github.com/minio/minio-go/v7.
//
// Conditional writes: minio-go exposes the MinIO optimistic-locking extension
// on PutObjectOptions via SetMatchETag / SetMatchETagExcept, which set the
// If-Match / If-None-Match request headers. We use those directly, so
// conditional PUT (create-if-absent via IfNoneMatch=="*" and compare-and-swap
// via IfMatch) is honored ATOMICALLY by the server — Capabilities.ConditionalPut
// is therefore true. Note this depends on the server actually enforcing those
// headers; genuine AWS S3 historically ignored them, but MinIO enforces them.
//
// Conditional COPY (copy-if-absent on the DESTINATION) has no atomic primitive:
// minio-go's CopySrcOptions.NoMatchETag conditions on the SOURCE, not the
// destination. We emulate CopyOptions.IfNoneMatch=="*" with a best-effort,
// NON-ATOMIC pre-Stat of the destination, and report ConditionalCopy=false to
// stay honest about the missing atomicity.
package minio

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// presignHardMax is the S3/MinIO protocol ceiling on presigned-URL lifetime.
const presignHardMax = 7 * 24 * time.Hour

func init() { backend.Register("minio", New) }

// Backend is the MinIO implementation of backend.Backend.
type Backend struct {
	client     *minio.Client
	bucket     string
	endpoint   string
	presignMax time.Duration
	probe      backend.ProbeStrategy
	probeKey   string
}

// New opens a MinIO backend against cfg.Bucket.
func New(_ context.Context, cfg backend.Config) (backend.Backend, error) {
	const op = "minio.New"

	endpoint := cfg.Endpoint
	secure := false
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		endpoint = strings.TrimPrefix(endpoint, "https://")
		secure = true
	case strings.HasPrefix(endpoint, "http://"):
		endpoint = strings.TrimPrefix(endpoint, "http://")
		secure = false
	}
	endpoint = strings.TrimSuffix(endpoint, "/")

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: secure,
		Region: region,
	})
	if err != nil {
		return nil, serr.Wrap(serr.InvalidArgument, op, err)
	}

	return &Backend{
		client:     client,
		bucket:     cfg.Bucket,
		endpoint:   endpoint,
		presignMax: cfg.PresignMaxExpiry,
		probe:      cfg.Strategy(),
		probeKey:   cfg.ProbeKey,
	}, nil
}

// Name reports the backend kind.
func (b *Backend) Name() string { return "minio" }

// Identity reports a globally unique identifier for this bucket. MinIO bucket
// names are unique only within a cluster, so the endpoint is included.
func (b *Backend) Identity() string { return b.endpoint + "/" + b.bucket }

// Capabilities reports what this backend honors.
func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		Backend:             "minio",
		ConditionalPut:      true,  // atomic via If-Match / If-None-Match extension
		ConditionalCopy:     false, // only best-effort emulated (non-atomic)
		AtomicRename:        false,
		Versions:            true,
		Tags:                false,
		Presign:             true,
		PresignMaxExpiry:    presignHardMax,
		PresignAmbientCreds: true,
		BatchDeleteMax:      1000,
		NativeVerbs:         nil,
	}
}

// Probe verifies access to the bucket without mutating it.
func (b *Backend) Probe(ctx context.Context) error {
	const op = "minio.Probe"

	switch b.probe {
	case backend.ProbeStat:
		_, err := b.client.StatObject(ctx, b.bucket, b.probeKey, minio.StatObjectOptions{})
		if err == nil {
			return nil
		}
		// A HEAD carries no error body, so this strategy can only attest that
		// the endpoint answered and authenticated the request — nothing finer.
		// Both a 404 and a 403 establish exactly that, and S3-compatible stores
		// choose between them by grant, not by fact: without list permission
		// they answer 403 for a key that merely does not exist. Treating 403 as
		// failure would make the probe permanently red under the very
		// least-privilege grant this strategy exists to serve, so both pass.
		if code := serr.CodeOf(mapErr(op, err)); code == serr.NotFound || code == serr.PermissionDenied {
			return nil
		}
		return probeErr(op, err)

	default:
		lctx, cancel := context.WithCancel(ctx)
		defer cancel()
		// One key is enough to prove the bucket answers, so take at most one
		// item: a closed channel means an empty bucket, which is still access.
		if obj, ok := <-b.client.ListObjects(lctx, b.bucket, minio.ListObjectsOptions{MaxKeys: 1}); ok && obj.Err != nil {
			return probeErr(op, obj.Err)
		}
		if err := ctx.Err(); err != nil {
			return serr.Wrap(serr.Unavailable, op, err)
		}
		return nil
	}
}

// probeErr normalizes a probe failure, separating an endpoint that never
// answered from a refusal the service actually returned.
func probeErr(op string, err error) error {
	if serr.Unreachable(err) {
		return serr.Wrap(serr.Unavailable, op, err)
	}
	return mapErr(op, err)
}

// mapErr normalizes a minio error into a serr.Error.
func mapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	resp := minio.ToErrorResponse(err)
	switch {
	case resp.StatusCode == 404, resp.Code == "NoSuchKey", resp.Code == "NoSuchBucket":
		return serr.Wrap(serr.NotFound, op, err)
	case resp.StatusCode == 304:
		return serr.Wrap(serr.NotModified, op, err)
	case resp.StatusCode == 412, resp.Code == "PreconditionFailed":
		return serr.Wrap(serr.PreconditionFailed, op, err)
	case resp.StatusCode == 409:
		return serr.Wrap(serr.AlreadyExists, op, err)
	case resp.StatusCode == 403, resp.Code == "AccessDenied":
		return serr.Wrap(serr.PermissionDenied, op, err)
	case resp.StatusCode == 429, resp.StatusCode == 503, resp.Code == "SlowDown":
		return serr.Wrap(serr.Throttled, op, err)
	default:
		return serr.Wrap(serr.Internal, op, err)
	}
}

// toObjectInfo maps a minio.ObjectInfo into a backend.ObjectInfo.
func toObjectInfo(key string, mi minio.ObjectInfo) backend.ObjectInfo {
	oi := backend.ObjectInfo{
		Key:             key,
		ETag:            mi.ETag,
		Size:            mi.Size,
		LastModified:    mi.LastModified,
		VersionID:       mi.VersionID,
		ContentType:     mi.ContentType,
		ContentEncoding: mi.ContentEncoding,
	}
	if oi.Key == "" {
		oi.Key = mi.Key
	}
	if mi.Metadata != nil {
		oi.CacheControl = mi.Metadata.Get("Cache-Control")
		if oi.ContentEncoding == "" {
			oi.ContentEncoding = mi.Metadata.Get("Content-Encoding")
		}
	}
	oi.UserMetadata = userMetadata(mi)
	return oi
}

// userMetadata extracts user metadata, preferring the stripped UserMetadata map
// and falling back to x-amz-meta-* / x-minio-meta-* headers in Metadata.
func userMetadata(mi minio.ObjectInfo) map[string]string {
	if len(mi.UserMetadata) > 0 {
		out := make(map[string]string, len(mi.UserMetadata))
		for k, v := range mi.UserMetadata {
			out[k] = v
		}
		return out
	}
	if mi.Metadata == nil {
		return nil
	}
	var out map[string]string
	for k, vs := range mi.Metadata {
		lower := strings.ToLower(k)
		for _, prefix := range []string{"x-amz-meta-", "x-minio-meta-"} {
			if strings.HasPrefix(lower, prefix) && len(vs) > 0 {
				if out == nil {
					out = map[string]string{}
				}
				out[strings.TrimPrefix(lower, prefix)] = vs[0]
			}
		}
	}
	return out
}

// Stat returns object metadata.
func (b *Backend) Stat(ctx context.Context, key, versionID string) (*backend.ObjectInfo, error) {
	const op = "minio.Stat"
	mi, err := b.client.StatObject(ctx, b.bucket, key, minio.StatObjectOptions{VersionID: versionID})
	if err != nil {
		return nil, mapErr(op, err)
	}
	oi := toObjectInfo(key, mi)
	return &oi, nil
}

// Get reads an object, honoring range and conditional headers.
func (b *Backend) Get(ctx context.Context, key string, opts backend.GetOptions) (*backend.GetResult, error) {
	const op = "minio.Get"

	mopts := minio.GetObjectOptions{VersionID: opts.VersionID}

	if r := opts.Range; r != nil {
		if r.Length > 0 {
			// Half-open [Offset, Offset+Length) -> inclusive byte range.
			if err := mopts.SetRange(r.Offset, r.Offset+r.Length-1); err != nil {
				return nil, serr.Wrap(serr.InvalidArgument, op, err)
			}
		} else {
			// To end: SetRange(start,0) would only mean "to end" for start==0,
			// so send the open-ended Range header explicitly.
			mopts.Set("Range", fmt.Sprintf("bytes=%d-", r.Offset))
		}
	}
	if opts.IfNoneMatch != "" {
		if err := mopts.SetMatchETagExcept(opts.IfNoneMatch); err != nil {
			return nil, serr.Wrap(serr.InvalidArgument, op, err)
		}
	}
	if opts.IfMatch != "" {
		if err := mopts.SetMatchETag(opts.IfMatch); err != nil {
			return nil, serr.Wrap(serr.InvalidArgument, op, err)
		}
	}
	if !opts.IfModifiedSince.IsZero() {
		if err := mopts.SetModified(opts.IfModifiedSince); err != nil {
			return nil, serr.Wrap(serr.InvalidArgument, op, err)
		}
	}

	obj, err := b.client.GetObject(ctx, b.bucket, key, mopts)
	if err != nil {
		return nil, mapErr(op, err)
	}

	// GetObject is lazy; Stat() forces the request so we can surface errors and
	// read the object metadata before streaming the body.
	mi, statErr := obj.Stat()
	if statErr != nil {
		_ = obj.Close()
		if resp := minio.ToErrorResponse(statErr); resp.StatusCode == 304 {
			return &backend.GetResult{
				Info:        backend.ObjectInfo{Key: key, ETag: opts.IfNoneMatch},
				NotModified: true,
			}, nil
		}
		return nil, mapErr(op, statErr)
	}

	return &backend.GetResult{
		Info: toObjectInfo(key, mi),
		Body: obj,
	}, nil
}

// Put writes an object from r.
func (b *Backend) Put(ctx context.Context, key string, r io.Reader, opts backend.PutOptions) (*backend.PutResult, error) {
	const op = "minio.Put"

	mopts := minio.PutObjectOptions{
		ContentType:     opts.ContentType,
		ContentEncoding: opts.ContentEncoding,
		CacheControl:    opts.CacheControl,
		UserMetadata:    opts.UserMetadata,
	}
	// Conditional writes via the MinIO optimistic-locking extension (atomic).
	if opts.IfNoneMatch != "" {
		mopts.SetMatchETagExcept(opts.IfNoneMatch) // "*" => create-if-absent
	}
	if opts.IfMatch != "" {
		mopts.SetMatchETag(opts.IfMatch) // compare-and-swap
	}

	size := opts.Size
	if size < 0 {
		// Unknown length: -1 lets minio-go stream and auto-multipart.
		size = -1
	}

	info, err := b.client.PutObject(ctx, b.bucket, key, r, size, mopts)
	if err != nil {
		return nil, mapErr(op, err)
	}
	return &backend.PutResult{ETag: info.ETag, VersionID: info.VersionID}, nil
}

// Delete removes an object (or a specific version); idempotent when
// unconditional.
//
// Conditional delete (IfMatch, compare-and-delete) has no atomic primitive in
// minio-go: RemoveObjectOptions carries no ETag precondition. We emulate it with
// a best-effort, NON-ATOMIC pre-Stat of the target — matching the conditional-
// copy emulation above — so the precondition is honored in the common case
// rather than silently dropped. A concurrent overwrite between the Stat and the
// remove is a TOCTOU window this cannot close.
func (b *Backend) Delete(ctx context.Context, key string, opts backend.DeleteOptions) error {
	const op = "minio.Delete"

	if opts.IfMatch != "" {
		mi, err := b.client.StatObject(ctx, b.bucket, key, minio.StatObjectOptions{VersionID: opts.VersionID})
		if err != nil {
			// A compare-and-delete can't match a missing object; fail the
			// precondition rather than surface it as a plain NotFound, so the
			// outcome matches the other backends' conditional-delete semantics.
			if serr.Is(mapErr(op, err), serr.NotFound) {
				return serr.New(serr.PreconditionFailed, op, "if-match on missing object")
			}
			return mapErr(op, err)
		}
		if mi.ETag != opts.IfMatch {
			return serr.New(serr.PreconditionFailed, op, "if-match mismatch")
		}
	}

	err := b.client.RemoveObject(ctx, b.bucket, key, minio.RemoveObjectOptions{VersionID: opts.VersionID})
	if err != nil {
		return mapErr(op, err)
	}
	return nil
}

// DeleteMany removes many keys, returning a per-key outcome.
func (b *Backend) DeleteMany(_ context.Context, keys []string) ([]backend.DeleteEntry, error) {
	// Feed keys into the channel minio consumes.
	objectsCh := make(chan minio.ObjectInfo, len(keys))
	for _, k := range keys {
		objectsCh <- minio.ObjectInfo{Key: k}
	}
	close(objectsCh)

	// Default every key to success; RemoveObjects only reports failures.
	entries := make([]backend.DeleteEntry, len(keys))
	idx := make(map[string]int, len(keys))
	for i, k := range keys {
		entries[i] = backend.DeleteEntry{Key: k}
		idx[k] = i
	}

	for rerr := range b.client.RemoveObjects(context.Background(), b.bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if rerr.Err == nil {
			continue
		}
		if i, ok := idx[rerr.ObjectName]; ok {
			entries[i].Error = rerr.Err.Error()
		}
	}
	return entries, nil
}

// List enumerates objects by prefix with an opaque page token.
func (b *Backend) List(ctx context.Context, opts backend.ListOptions) (*backend.ListResult, error) {
	const op = "minio.List"

	startAfter, err := decodeToken(opts.PageToken)
	if err != nil {
		return nil, serr.Wrap(serr.InvalidArgument, op, err)
	}

	mopts := minio.ListObjectsOptions{
		Prefix:     opts.Prefix,
		Recursive:  opts.Delimiter == "", // no delimiter => recursive (flat) listing
		StartAfter: startAfter,
	}
	limit := int(opts.Limit)
	if limit > 0 {
		mopts.MaxKeys = limit
	}

	// Cancel the listing goroutine when we stop reading early.
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()

	res := &backend.ListResult{}
	var lastKey string
	count := 0
	for obj := range b.client.ListObjects(lctx, b.bucket, mopts) {
		if obj.Err != nil {
			return nil, mapErr(op, obj.Err)
		}
		if opts.Delimiter != "" && strings.HasSuffix(obj.Key, "/") {
			res.CommonPrefixes = append(res.CommonPrefixes, obj.Key)
		} else {
			res.Objects = append(res.Objects, toObjectInfo(obj.Key, obj))
		}
		lastKey = obj.Key
		count++
		if limit > 0 && count >= limit {
			break
		}
	}

	// A full page implies there may be more; hand back the resume cursor.
	if limit > 0 && count >= limit && lastKey != "" {
		res.NextPageToken = encodeToken(lastKey)
	}
	return res, nil
}

// Copy performs a server-side copy.
func (b *Backend) Copy(ctx context.Context, srcKey, dstKey string, opts backend.CopyOptions) (*backend.PutResult, error) {
	const op = "minio.Copy"

	// Best-effort, non-atomic copy-if-absent emulation (see package doc).
	if opts.IfNoneMatch == "*" {
		if _, err := b.client.StatObject(ctx, b.bucket, dstKey, minio.StatObjectOptions{}); err == nil {
			return nil, serr.New(serr.AlreadyExists, op, "destination already exists")
		} else if resp := minio.ToErrorResponse(err); resp.StatusCode != 404 && resp.Code != "NoSuchKey" {
			return nil, mapErr(op, err)
		}
	}

	info, err := b.client.CopyObject(ctx,
		minio.CopyDestOptions{Bucket: b.bucket, Object: dstKey},
		minio.CopySrcOptions{Bucket: b.bucket, Object: srcKey},
	)
	if err != nil {
		return nil, mapErr(op, err)
	}
	return &backend.PutResult{ETag: info.ETag, VersionID: info.VersionID}, nil
}

// Presign returns a time-limited URL. This is computed locally (no server call).
func (b *Backend) Presign(ctx context.Context, key string, method backend.PresignMethod, expiry time.Duration) (*backend.PresignResult, error) {
	const op = "minio.Presign"

	if expiry <= 0 || expiry > presignHardMax {
		expiry = presignHardMax
	}
	if b.presignMax > 0 && expiry > b.presignMax {
		expiry = b.presignMax
	}

	var u *url.URL
	var err error
	switch method {
	case backend.PresignGet:
		u, err = b.client.PresignedGetObject(ctx, b.bucket, key, expiry, url.Values{})
	case backend.PresignPut:
		u, err = b.client.PresignedPutObject(ctx, b.bucket, key, expiry)
	default:
		return nil, serr.New(serr.InvalidArgument, op, fmt.Sprintf("unknown presign method %d", method))
	}
	if err != nil {
		return nil, mapErr(op, err)
	}

	return &backend.PresignResult{
		Method:    method,
		URL:       u.String(),
		ExpiresAt: time.Now().Add(expiry),
	}, nil
}

// Native is the escape hatch for backend-specific verbs; none are offered.
func (b *Backend) Native(_ context.Context, verb string, _ map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "native", verb)
}

// Close releases resources. The minio client holds only an HTTP transport with
// no explicit close, so this is a no-op.
func (b *Backend) Close() error { return nil }

// encodeToken makes an opaque page token from a resume key.
func encodeToken(key string) string {
	return base64.URLEncoding.EncodeToString([]byte(key))
}

// decodeToken recovers the resume key from an opaque page token.
func decodeToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
