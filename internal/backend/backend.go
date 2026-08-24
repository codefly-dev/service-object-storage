// Package backend defines the provider-agnostic contract every storage backend
// implements. The types are Go-native (no proto dependency) so backends stay
// testable and the gRPC layer maps to/from them. The interface is the honest
// intersection across S3, GCS, Azure Blob, and MinIO; anything backend-specific
// is reached only through Native.
package backend

import (
	"context"
	"io"
	"time"
)

// Config is the resolved configuration for opening one backend against one
// bucket/container. A backend uses the subset it needs.
type Config struct {
	// Kind selects the implementation: "minio" | "s3" | "gcs" | "azure".
	Kind string
	// Bucket (S3/GCS) or container (Azure) the server is bound to.
	Bucket string
	Region string
	// Endpoint overrides the service endpoint (MinIO / S3-compatible).
	Endpoint string
	// UsePathStyle forces path-style addressing (required by MinIO).
	UsePathStyle bool

	AccessKey string
	SecretKey string

	// GCSCredentialsFile is a service-account JSON path (empty = ADC).
	GCSCredentialsFile string

	// AzureAccount / AzureKey authenticate Azure Blob (empty key = default creds).
	AzureAccount string
	AzureKey     string

	// PresignMaxExpiry caps presign lifetimes; 0 means the backend default.
	PresignMaxExpiry time.Duration
}

// ObjectInfo is object metadata — and every field the cache needs. ETag is an
// OPAQUE strong validator, never a content hash. VersionID (S3/Azure) and
// Generation (GCS) make version-pinned reads immutable.
type ObjectInfo struct {
	Key             string
	ETag            string
	WeakETag        bool
	Size            int64
	LastModified    time.Time
	VersionID       string
	Generation      int64
	MetaGeneration  int64
	ContentType     string
	ContentEncoding string
	CacheControl    string
	UserMetadata    map[string]string
}

// ByteRange is a half-open [Offset, Offset+Length); Length <= 0 means to end.
type ByteRange struct {
	Offset int64
	Length int64
}

// GetOptions carries conditional-read and range inputs.
type GetOptions struct {
	Range           *ByteRange
	IfNoneMatch     string
	IfMatch         string
	IfModifiedSince time.Time
	VersionID       string
}

// GetResult is a streaming read. Body is nil when NotModified is true.
type GetResult struct {
	Info        ObjectInfo
	Body        io.ReadCloser
	NotModified bool
}

// PutOptions carries content metadata and conditional-write intent. IfNoneMatch
// == "*" is create-if-absent; IfMatch == <etag> is compare-and-swap.
type PutOptions struct {
	ContentType     string
	ContentEncoding string
	CacheControl    string
	UserMetadata    map[string]string
	IfNoneMatch     string
	IfMatch         string
	// Size is a hint; -1 means unknown/streaming.
	Size int64
}

// PutResult reports the stored object's validators.
type PutResult struct {
	ETag       string
	VersionID  string
	Generation int64
}

// DeleteOptions gates a delete on a version and/or an ETag.
type DeleteOptions struct {
	VersionID string
	IfMatch   string
}

// ListOptions is a prefix listing with an opaque page token.
type ListOptions struct {
	Prefix    string
	Delimiter string
	PageToken string
	Limit     int32
}

// ListResult holds objects plus synthetic common prefixes; ordering is not
// guaranteed. NextPageToken is empty at the end.
type ListResult struct {
	Objects        []ObjectInfo
	CommonPrefixes []string
	NextPageToken  string
}

// CopyOptions gates a server-side copy. IfNoneMatch == "*" is copy-if-absent.
type CopyOptions struct {
	IfNoneMatch string
}

// PresignMethod is the HTTP verb a presigned URL authorizes.
type PresignMethod int

const (
	PresignGet PresignMethod = iota
	PresignPut
)

// PresignResult is a time-limited URL the client uses directly over plain HTTP.
type PresignResult struct {
	Method    PresignMethod
	URL       string
	Headers   map[string]string
	ExpiresAt time.Time
}

// Capabilities is the machine-readable feature set a client introspects before
// calling — the OpenDAL model. A false flag means "call it and get Unsupported".
type Capabilities struct {
	Backend             string
	ConditionalPut      bool
	ConditionalCopy     bool
	AtomicRename        bool
	Versions            bool
	Tags                bool
	Presign             bool
	PresignMaxExpiry    time.Duration
	PresignAmbientCreds bool
	BatchDeleteMax      int
	NativeVerbs         []string
}

// Backend is the provider-agnostic object-storage contract. Implementations
// translate to exactly one of S3 / GCS / Azure Blob / MinIO and return
// serr.Error-coded failures.
type Backend interface {
	// Name is the backend kind ("s3", "gcs", "azure", "minio").
	Name() string
	// Bucket is a GLOBALLY UNIQUE identity for the physical location this
	// instance is bound to — the bucket/container plus whatever disambiguates
	// it across deployments (the account for Azure, the endpoint for MinIO and
	// S3-compatible services). The cache folds it into every key, so a bare
	// bucket name is not enough: two containers named "data" in different Azure
	// accounts, or two MinIO clusters with a bucket named "data", must not
	// collide on a shared Redis tier.
	Bucket() string
	// Capabilities reports what this backend honors.
	Capabilities() Capabilities

	// Stat returns object metadata (HEAD). versionID may be empty.
	Stat(ctx context.Context, key, versionID string) (*ObjectInfo, error)
	// Get reads an object (optionally a range / a specific version), honoring
	// conditional headers. On a conditional match it returns NotModified.
	Get(ctx context.Context, key string, opts GetOptions) (*GetResult, error)
	// Put writes an object from r, hiding multipart internally.
	Put(ctx context.Context, key string, r io.Reader, opts PutOptions) (*PutResult, error)
	// Delete removes an object (or a version); idempotent when unconditional.
	Delete(ctx context.Context, key string, opts DeleteOptions) error
	// DeleteMany removes many keys, chunking to the backend batch limit, and
	// returns a per-key outcome (partial failure is normal).
	DeleteMany(ctx context.Context, keys []string) ([]DeleteEntry, error)
	// List enumerates by prefix with an opaque page token.
	List(ctx context.Context, opts ListOptions) (*ListResult, error)
	// Copy performs a server-side copy where supported; may emulate otherwise.
	Copy(ctx context.Context, srcKey, dstKey string, opts CopyOptions) (*PutResult, error)
	// Presign returns a time-limited URL, or serr.Unsupported.
	Presign(ctx context.Context, key string, method PresignMethod, expiry time.Duration) (*PresignResult, error)
	// Native is the escape hatch for backend-specific verbs.
	Native(ctx context.Context, verb string, params map[string]string) (map[string]string, error)
	// Close releases resources.
	Close() error
}

// DeleteEntry is the per-key outcome of a batch delete; Error is empty on
// success.
type DeleteEntry struct {
	Key   string
	Error string
}
