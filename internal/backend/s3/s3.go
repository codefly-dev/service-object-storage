// Package s3 implements backend.Backend against Amazon S3 (and S3-API-compatible
// services reached via cfg.Endpoint) using github.com/aws/aws-sdk-go-v2.
//
// Conditional writes: modern S3 honors If-None-Match: * (create-if-absent) and
// If-Match: <etag> (compare-and-swap) on PutObject, so ConditionalPut is true. A
// failed If-None-Match: * returns HTTP 412, which we normalize to AlreadyExists
// (the create-if-absent contract); a failed If-Match returns 412 too, which we
// surface as PreconditionFailed elsewhere.
//
// Conditional COPY on the DESTINATION has no atomic primitive on CopyObject
// (its IfNoneMatch conditions the SOURCE read, not the destination write), so we
// do not emulate it and report ConditionalCopy=false to stay honest.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// presignHardMax is the S3 protocol ceiling on presigned-URL lifetime.
const presignHardMax = 7 * 24 * time.Hour

// batchDeleteMax is the S3 DeleteObjects per-request key limit.
const batchDeleteMax = 1000

func init() { backend.Register("s3", New) }

// Backend is the Amazon S3 implementation of backend.Backend.
type Backend struct {
	client     *s3.Client
	uploader   *manager.Uploader
	presign    *s3.PresignClient
	creds      aws.CredentialsProvider
	bucket     string
	presignMax time.Duration
}

// New opens an S3 backend against cfg.Bucket. Credentials come from the static
// AccessKey/SecretKey when set, otherwise from the default AWS credential chain.
func New(ctx context.Context, cfg backend.Config) (backend.Backend, error) {
	const op = "s3.New"

	optFns := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	if cfg.AccessKey != "" {
		optFns = append(optFns, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awscfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, serr.Wrap(serr.InvalidArgument, op, err)
	}

	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	return &Backend{
		client:     client,
		uploader:   manager.NewUploader(client),
		presign:    s3.NewPresignClient(client),
		creds:      awscfg.Credentials,
		bucket:     cfg.Bucket,
		presignMax: cfg.PresignMaxExpiry,
	}, nil
}

// Name reports the backend kind.
func (b *Backend) Name() string { return "s3" }

// Bucket reports the bucket this backend is bound to.
func (b *Backend) Bucket() string { return b.bucket }

// Capabilities reports what this backend honors.
func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		Backend:             "s3",
		ConditionalPut:      true,
		ConditionalCopy:     false, // no atomic destination precondition on CopyObject
		AtomicRename:        false,
		Versions:            true,
		Tags:                false,
		Presign:             true,
		PresignMaxExpiry:    presignHardMax,
		PresignAmbientCreds: true, // SigV4 presign uses the loaded creds; STS creds cap effective lifetime
		BatchDeleteMax:      batchDeleteMax,
		NativeVerbs:         nil,
	}
}

// mapErr normalizes an AWS SDK error into a serr.Error. It prefers typed
// exceptions, then the smithy API error code, then the HTTP status code.
func mapErr(op string, err error) error {
	if err == nil {
		return nil
	}

	var nsk *types.NoSuchKey
	var nf *types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nf) {
		return serr.Wrap(serr.NotFound, op, err)
	}

	// HTTP status, when available, is the most reliable signal for conditionals.
	status := httpStatus(err)

	var apiErr smithy.APIError
	code := ""
	if errors.As(err, &apiErr) {
		code = apiErr.ErrorCode()
	}

	switch {
	case code == "NoSuchKey", code == "NoSuchBucket", code == "NotFound", status == 404:
		return serr.Wrap(serr.NotFound, op, err)
	case status == 304, code == "NotModified":
		return serr.Wrap(serr.NotModified, op, err)
	case code == "PreconditionFailed", status == 412:
		return serr.Wrap(serr.PreconditionFailed, op, err)
	case status == 409:
		return serr.Wrap(serr.AlreadyExists, op, err)
	case code == "AccessDenied", status == 403:
		return serr.Wrap(serr.PermissionDenied, op, err)
	case code == "SlowDown", status == 429, status == 503:
		return serr.Wrap(serr.Throttled, op, err)
	default:
		return serr.Wrap(serr.Internal, op, err)
	}
}

// httpStatus extracts the HTTP status code from an AWS transport error, or 0.
func httpStatus(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// isNotModified reports whether err normalizes to NotModified.
func isNotModified(err error) bool {
	return serr.Is(mapErr("", err), serr.NotModified)
}

// Stat returns object metadata (HEAD).
func (b *Backend) Stat(ctx context.Context, key, versionID string) (*backend.ObjectInfo, error) {
	const op = "s3.Stat"
	in := &s3.HeadObjectInput{Bucket: &b.bucket, Key: &key}
	if versionID != "" {
		in.VersionId = &versionID
	}
	out, err := b.client.HeadObject(ctx, in)
	if err != nil {
		return nil, mapErr(op, err)
	}
	oi := headToObjectInfo(key, out)
	return &oi, nil
}

// Get reads an object, honoring range and conditional headers.
func (b *Backend) Get(ctx context.Context, key string, opts backend.GetOptions) (*backend.GetResult, error) {
	const op = "s3.Get"

	in := &s3.GetObjectInput{Bucket: &b.bucket, Key: &key}
	if r := opts.Range; r != nil {
		var rng string
		if r.Length > 0 {
			// Half-open [Offset, Offset+Length) -> inclusive HTTP byte range.
			rng = fmt.Sprintf("bytes=%d-%d", r.Offset, r.Offset+r.Length-1)
		} else {
			rng = fmt.Sprintf("bytes=%d-", r.Offset)
		}
		in.Range = &rng
	}
	if opts.IfNoneMatch != "" {
		in.IfNoneMatch = &opts.IfNoneMatch
	}
	if opts.IfMatch != "" {
		in.IfMatch = &opts.IfMatch
	}
	if !opts.IfModifiedSince.IsZero() {
		in.IfModifiedSince = &opts.IfModifiedSince
	}
	if opts.VersionID != "" {
		in.VersionId = &opts.VersionID
	}

	out, err := b.client.GetObject(ctx, in)
	if err != nil {
		if isNotModified(err) {
			// Best-effort metadata for the caller; ignore its error.
			res := &backend.GetResult{Info: backend.ObjectInfo{Key: key, ETag: opts.IfNoneMatch}, NotModified: true}
			head := &s3.HeadObjectInput{Bucket: &b.bucket, Key: &key}
			if opts.VersionID != "" {
				head.VersionId = &opts.VersionID
			}
			if hout, herr := b.client.HeadObject(ctx, head); herr == nil {
				res.Info = headToObjectInfo(key, hout)
			}
			return res, nil
		}
		return nil, mapErr(op, err)
	}

	return &backend.GetResult{
		Info: getToObjectInfo(key, out),
		Body: out.Body,
	}, nil
}

// Put writes an object from r using the managed uploader, which hides multipart
// for unknown/large sizes.
func (b *Backend) Put(ctx context.Context, key string, r io.Reader, opts backend.PutOptions) (*backend.PutResult, error) {
	const op = "s3.Put"

	in := &s3.PutObjectInput{
		Bucket: &b.bucket,
		Key:    &key,
		Body:   r,
	}
	if opts.ContentType != "" {
		in.ContentType = &opts.ContentType
	}
	if opts.ContentEncoding != "" {
		in.ContentEncoding = &opts.ContentEncoding
	}
	if opts.CacheControl != "" {
		in.CacheControl = &opts.CacheControl
	}
	if len(opts.UserMetadata) > 0 {
		in.Metadata = opts.UserMetadata
	}
	// The managed uploader copies IfNoneMatch/IfMatch onto CompleteMultipartUpload
	// when it switches to multipart, so the precondition is enforced atomically at
	// the commit point even above the multipart threshold — ConditionalPut holds
	// for large objects too.
	if opts.IfNoneMatch != "" {
		in.IfNoneMatch = &opts.IfNoneMatch // "*" => create-if-absent
	}
	if opts.IfMatch != "" {
		in.IfMatch = &opts.IfMatch // compare-and-swap
	}

	out, err := b.uploader.Upload(ctx, in)
	if err != nil {
		// A failed create-if-absent precondition means the object exists.
		if opts.IfNoneMatch == "*" && serr.Is(mapErr(op, err), serr.PreconditionFailed) {
			return nil, serr.Wrap(serr.AlreadyExists, op, err)
		}
		return nil, mapErr(op, err)
	}

	res := &backend.PutResult{}
	if out.ETag != nil {
		res.ETag = *out.ETag
	}
	if out.VersionID != nil {
		res.VersionID = *out.VersionID
	}
	return res, nil
}

// Delete removes an object (or a specific version); idempotent when
// unconditional.
func (b *Backend) Delete(ctx context.Context, key string, opts backend.DeleteOptions) error {
	const op = "s3.Delete"
	in := &s3.DeleteObjectInput{Bucket: &b.bucket, Key: &key}
	if opts.VersionID != "" {
		in.VersionId = &opts.VersionID
	}
	if opts.IfMatch != "" {
		in.IfMatch = &opts.IfMatch
	}
	if _, err := b.client.DeleteObject(ctx, in); err != nil {
		return mapErr(op, err)
	}
	return nil
}

// DeleteMany removes many keys, chunking into DeleteObjects batches, and returns
// a per-key outcome.
func (b *Backend) DeleteMany(ctx context.Context, keys []string) ([]backend.DeleteEntry, error) {
	const op = "s3.DeleteMany"

	// Default every key to success; DeleteObjects only reports failures.
	entries := make([]backend.DeleteEntry, len(keys))
	idx := make(map[string]int, len(keys))
	for i, k := range keys {
		entries[i] = backend.DeleteEntry{Key: k}
		idx[k] = i
	}

	for start := 0; start < len(keys); start += batchDeleteMax {
		end := start + batchDeleteMax
		if end > len(keys) {
			end = len(keys)
		}
		objs := make([]types.ObjectIdentifier, 0, end-start)
		for i := start; i < end; i++ {
			k := keys[i]
			objs = append(objs, types.ObjectIdentifier{Key: aws.String(k)})
		}

		out, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &b.bucket,
			Delete: &types.Delete{Objects: objs},
		})
		if err != nil {
			// Whole-batch failure: attribute the error to each key in the batch.
			msg := mapErr(op, err).Error()
			for i := start; i < end; i++ {
				entries[i].Error = msg
			}
			continue
		}
		for _, e := range out.Errors {
			if e.Key == nil {
				continue
			}
			i, ok := idx[*e.Key]
			if !ok {
				continue
			}
			if e.Message != nil {
				entries[i].Error = *e.Message
			} else if e.Code != nil {
				entries[i].Error = *e.Code
			} else {
				entries[i].Error = "delete failed"
			}
		}
	}
	return entries, nil
}

// List enumerates objects by prefix with an opaque page token.
func (b *Backend) List(ctx context.Context, opts backend.ListOptions) (*backend.ListResult, error) {
	const op = "s3.List"

	in := &s3.ListObjectsV2Input{Bucket: &b.bucket}
	if opts.Prefix != "" {
		in.Prefix = &opts.Prefix
	}
	if opts.Delimiter != "" {
		in.Delimiter = &opts.Delimiter
	}
	if opts.Limit > 0 {
		in.MaxKeys = aws.Int32(opts.Limit)
	}
	if opts.PageToken != "" {
		in.ContinuationToken = &opts.PageToken
	}

	out, err := b.client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, mapErr(op, err)
	}

	res := &backend.ListResult{}
	for _, o := range out.Contents {
		oi := backend.ObjectInfo{}
		if o.Key != nil {
			oi.Key = *o.Key
		}
		if o.ETag != nil {
			oi.ETag = *o.ETag
		}
		if o.Size != nil {
			oi.Size = *o.Size
		}
		if o.LastModified != nil {
			oi.LastModified = *o.LastModified
		}
		res.Objects = append(res.Objects, oi)
	}
	for _, cp := range out.CommonPrefixes {
		if cp.Prefix != nil {
			res.CommonPrefixes = append(res.CommonPrefixes, *cp.Prefix)
		}
	}
	if out.NextContinuationToken != nil {
		res.NextPageToken = *out.NextContinuationToken
	}
	return res, nil
}

// Copy performs a server-side copy.
func (b *Backend) Copy(ctx context.Context, srcKey, dstKey string, opts backend.CopyOptions) (*backend.PutResult, error) {
	const op = "s3.Copy"

	// CopySource must be URL-path-escaped "bucket/key".
	src := url.PathEscape(b.bucket + "/" + srcKey)
	out, err := b.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     &b.bucket,
		Key:        &dstKey,
		CopySource: &src,
	})
	if err != nil {
		return nil, mapErr(op, err)
	}

	res := &backend.PutResult{}
	if out.CopyObjectResult != nil && out.CopyObjectResult.ETag != nil {
		res.ETag = *out.CopyObjectResult.ETag
	}
	if out.VersionId != nil {
		res.VersionID = *out.VersionId
	}
	return res, nil
}

// Presign returns a time-limited URL, computed locally (SigV4, no server call).
func (b *Backend) Presign(ctx context.Context, key string, method backend.PresignMethod, expiry time.Duration) (*backend.PresignResult, error) {
	const op = "s3.Presign"

	if expiry <= 0 || expiry > presignHardMax {
		expiry = presignHardMax
	}
	if b.presignMax > 0 && expiry > b.presignMax {
		expiry = b.presignMax
	}
	// A URL signed with temporary (STS/session) credentials stops working the
	// moment those credentials expire, regardless of the protocol maximum, so
	// clamp the lifetime — and thus the reported expires_at — to the credential
	// expiration to avoid overstating validity.
	if creds, err := b.creds.Retrieve(ctx); err == nil {
		expiry = clampToCreds(time.Now(), expiry, creds)
	}
	withExpiry := s3.WithPresignExpires(expiry)

	var req *v4.PresignedHTTPRequest
	var err error
	switch method {
	case backend.PresignGet:
		req, err = b.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: &b.bucket, Key: &key}, withExpiry)
	case backend.PresignPut:
		req, err = b.presign.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: &b.bucket, Key: &key}, withExpiry)
	default:
		return nil, serr.New(serr.InvalidArgument, op, fmt.Sprintf("unknown presign method %d", method))
	}
	if err != nil {
		return nil, mapErr(op, err)
	}

	headers := map[string]string{}
	for k := range req.SignedHeader {
		headers[k] = req.SignedHeader.Get(k)
	}

	return &backend.PresignResult{
		Method:    method,
		URL:       req.URL,
		Headers:   headers,
		ExpiresAt: time.Now().Add(expiry),
	}, nil
}

// clampToCreds shortens a presign lifetime so it never outlives the signing
// credentials. Static credentials never expire (CanExpire == false) and pass
// through unchanged; temporary credentials cap the lifetime at their remaining
// validity.
func clampToCreds(now time.Time, expiry time.Duration, creds aws.Credentials) time.Duration {
	if !creds.CanExpire {
		return expiry
	}
	if remaining := creds.Expires.Sub(now); remaining < expiry {
		return remaining
	}
	return expiry
}

// Native is the escape hatch for backend-specific verbs; none are offered.
func (b *Backend) Native(_ context.Context, verb string, _ map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "native", verb)
}

// Close releases resources. The S3 client holds only an HTTP transport with no
// explicit close, so this is a no-op.
func (b *Backend) Close() error { return nil }

// headToObjectInfo maps a HeadObject output into a backend.ObjectInfo.
func headToObjectInfo(key string, out *s3.HeadObjectOutput) backend.ObjectInfo {
	oi := backend.ObjectInfo{Key: key}
	if out == nil {
		return oi
	}
	if out.ETag != nil {
		oi.ETag = *out.ETag
	}
	if out.ContentLength != nil {
		oi.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		oi.LastModified = *out.LastModified
	}
	if out.ContentType != nil {
		oi.ContentType = *out.ContentType
	}
	if out.ContentEncoding != nil {
		oi.ContentEncoding = *out.ContentEncoding
	}
	if out.CacheControl != nil {
		oi.CacheControl = *out.CacheControl
	}
	if out.VersionId != nil {
		oi.VersionID = *out.VersionId
	}
	oi.UserMetadata = copyMeta(out.Metadata)
	return oi
}

// getToObjectInfo maps a GetObject output into a backend.ObjectInfo.
func getToObjectInfo(key string, out *s3.GetObjectOutput) backend.ObjectInfo {
	oi := backend.ObjectInfo{Key: key}
	if out == nil {
		return oi
	}
	if out.ETag != nil {
		oi.ETag = *out.ETag
	}
	if out.ContentLength != nil {
		oi.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		oi.LastModified = *out.LastModified
	}
	if out.ContentType != nil {
		oi.ContentType = *out.ContentType
	}
	if out.ContentEncoding != nil {
		oi.ContentEncoding = *out.ContentEncoding
	}
	if out.CacheControl != nil {
		oi.CacheControl = *out.CacheControl
	}
	if out.VersionId != nil {
		oi.VersionID = *out.VersionId
	}
	oi.UserMetadata = copyMeta(out.Metadata)
	return oi
}

// copyMeta returns a copy of the user-metadata map, or nil when empty.
func copyMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
