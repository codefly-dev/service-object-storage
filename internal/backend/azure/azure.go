// Package azure implements the backend.Backend contract against Azure Blob
// Storage using the azure-sdk-for-go azblob v1.8.0 client. One instance is
// bound to a single container (cfg.Bucket). Per-blob operations go through the
// service -> container -> blob/blockblob client chain so access conditions can
// be attached uniformly.
package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

func init() { backend.Register("azure", New) }

// maxPresignExpiry is the Azure service-SAS ceiling we clamp to (7 days).
const maxPresignExpiry = 7 * 24 * time.Hour

// Backend is the Azure Blob Storage implementation of backend.Backend.
type Backend struct {
	client    *azblob.Client
	container string
	account   string
	// sharedKey is non-nil only when a shared account key was supplied; it is
	// required for presigning (service SAS) and for authenticating same-account
	// server-side copy sources.
	sharedKey *azblob.SharedKeyCredential
	maxExpiry time.Duration
}

// New opens an Azure Blob backend bound to cfg.Bucket (the container).
func New(ctx context.Context, cfg backend.Config) (backend.Backend, error) {
	const op = "azure.New"
	serviceURL := cfg.Endpoint
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", cfg.AzureAccount)
	}

	b := &Backend{
		container: cfg.Bucket,
		account:   cfg.AzureAccount,
		maxExpiry: cfg.PresignMaxExpiry,
	}

	if cfg.AzureKey != "" {
		cred, err := azblob.NewSharedKeyCredential(cfg.AzureAccount, cfg.AzureKey)
		if err != nil {
			return nil, serr.Wrap(serr.InvalidArgument, op, err)
		}
		client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
		if err != nil {
			return nil, serr.Wrap(serr.Internal, op, err)
		}
		b.client = client
		b.sharedKey = cred
		return b, nil
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, serr.Wrap(serr.PermissionDenied, op, err)
	}
	client, err := azblob.NewClient(serviceURL, cred, nil)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, op, err)
	}
	b.client = client
	return b, nil
}

// Name reports the backend kind.
func (b *Backend) Name() string { return "azure" }

// Bucket reports the container this backend is bound to.
func (b *Backend) Bucket() string { return b.container }

// Capabilities reports the feature set this backend honors.
func (b *Backend) Capabilities() backend.Capabilities {
	return backend.Capabilities{
		Backend:             "azure",
		ConditionalPut:      true,
		ConditionalCopy:     true,
		AtomicRename:        false,
		Versions:            true,
		Tags:                false,
		Presign:             b.sharedKey != nil,
		PresignMaxExpiry:    maxPresignExpiry,
		PresignAmbientCreds: false,
		BatchDeleteMax:      256,
		NativeVerbs:         nil,
	}
}

// containerClient returns the per-container client.
func (b *Backend) containerClient() *container.Client {
	return b.client.ServiceClient().NewContainerClient(b.container)
}

// blobClient returns a blob client for key, pinned to versionID when non-empty.
func (b *Backend) blobClient(key, versionID string) (*blob.Client, error) {
	c := b.containerClient().NewBlobClient(key)
	if versionID == "" {
		return c, nil
	}
	pinned, err := c.WithVersionID(versionID)
	if err != nil {
		return nil, err
	}
	return pinned, nil
}

// Stat returns object metadata (HEAD).
func (b *Backend) Stat(ctx context.Context, key, versionID string) (*backend.ObjectInfo, error) {
	const op = "azure.Stat"
	bc, err := b.blobClient(key, versionID)
	if err != nil {
		return nil, serr.Wrap(serr.InvalidArgument, op, err)
	}
	resp, err := bc.GetProperties(ctx, nil)
	if err != nil {
		return nil, mapErr(op, err)
	}
	info := backend.ObjectInfo{
		Key:             key,
		ETag:            etagStr(resp.ETag),
		Size:            derefInt64(resp.ContentLength),
		LastModified:    derefTime(resp.LastModified),
		VersionID:       derefStr(resp.VersionID),
		ContentType:     derefStr(resp.ContentType),
		ContentEncoding: derefStr(resp.ContentEncoding),
		CacheControl:    derefStr(resp.CacheControl),
		UserMetadata:    fromMetaPtr(resp.Metadata),
	}
	return &info, nil
}

// Get reads an object honoring range and conditional headers.
func (b *Backend) Get(ctx context.Context, key string, opts backend.GetOptions) (*backend.GetResult, error) {
	const op = "azure.Get"
	bc, err := b.blobClient(key, opts.VersionID)
	if err != nil {
		return nil, serr.Wrap(serr.InvalidArgument, op, err)
	}

	dopts := &blob.DownloadStreamOptions{
		AccessConditions: accessConditions(opts.IfNoneMatch, opts.IfMatch, opts.IfModifiedSince, false),
	}
	if opts.Range != nil {
		count := int64(0)
		if opts.Range.Length > 0 {
			count = opts.Range.Length
		}
		dopts.Range = blob.HTTPRange{Offset: opts.Range.Offset, Count: count}
	}

	resp, err := bc.DownloadStream(ctx, dopts)
	if err != nil {
		mapped := mapErr(op, err)
		if serr.CodeOf(mapped) == serr.NotModified {
			res := &backend.GetResult{NotModified: true}
			// Best-effort: fill Info from an unconditional HEAD.
			if props, perr := bc.GetProperties(ctx, nil); perr == nil {
				res.Info = backend.ObjectInfo{
					Key:             key,
					ETag:            etagStr(props.ETag),
					Size:            derefInt64(props.ContentLength),
					LastModified:    derefTime(props.LastModified),
					VersionID:       derefStr(props.VersionID),
					ContentType:     derefStr(props.ContentType),
					ContentEncoding: derefStr(props.ContentEncoding),
					CacheControl:    derefStr(props.CacheControl),
					UserMetadata:    fromMetaPtr(props.Metadata),
				}
			}
			return res, nil
		}
		return nil, mapped
	}

	info := backend.ObjectInfo{
		Key:             key,
		ETag:            etagStr(resp.ETag),
		Size:            derefInt64(resp.ContentLength),
		LastModified:    derefTime(resp.LastModified),
		VersionID:       derefStr(resp.VersionID),
		ContentType:     derefStr(resp.ContentType),
		ContentEncoding: derefStr(resp.ContentEncoding),
		CacheControl:    derefStr(resp.CacheControl),
		UserMetadata:    fromMetaPtr(resp.Metadata),
	}
	return &backend.GetResult{Info: info, Body: resp.Body}, nil
}

// Put writes an object from r, staging blocks internally (hides multipart).
func (b *Backend) Put(ctx context.Context, key string, r io.Reader, opts backend.PutOptions) (*backend.PutResult, error) {
	const op = "azure.Put"
	bbc := b.containerClient().NewBlockBlobClient(key)

	createIfAbsent := opts.IfNoneMatch == "*"
	uopts := &blockblob.UploadStreamOptions{
		AccessConditions: accessConditions(opts.IfNoneMatch, opts.IfMatch, time.Time{}, createIfAbsent),
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType:     ptrIfSet(opts.ContentType),
			BlobContentEncoding: ptrIfSet(opts.ContentEncoding),
			BlobCacheControl:    ptrIfSet(opts.CacheControl),
		},
		Metadata: toMetaPtr(opts.UserMetadata),
	}

	resp, err := bbc.UploadStream(ctx, r, uopts)
	if err != nil {
		mapped := mapErr(op, err)
		// A create-if-absent that lost the race surfaces as 409/412; normalize
		// both to AlreadyExists so the caller sees a single conflict code.
		if createIfAbsent {
			switch serr.CodeOf(mapped) {
			case serr.PreconditionFailed, serr.AlreadyExists:
				return nil, serr.Wrap(serr.AlreadyExists, op, err)
			}
		}
		return nil, mapped
	}

	return &backend.PutResult{
		ETag:      etagStr(resp.ETag),
		VersionID: derefStr(resp.VersionID),
	}, nil
}

// Delete removes an object (or a version); idempotent when unconditional.
func (b *Backend) Delete(ctx context.Context, key string, opts backend.DeleteOptions) error {
	const op = "azure.Delete"
	bc, err := b.blobClient(key, opts.VersionID)
	if err != nil {
		return serr.Wrap(serr.InvalidArgument, op, err)
	}

	conditional := opts.IfMatch != "" || opts.VersionID != ""
	dopts := &blob.DeleteOptions{
		AccessConditions: accessConditions("", opts.IfMatch, time.Time{}, false),
	}
	_, err = bc.Delete(ctx, dopts)
	if err != nil {
		// Unconditional deletes are idempotent: a missing blob is success.
		if !conditional && bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil
		}
		return mapErr(op, err)
	}
	return nil
}

// DeleteMany removes many keys, returning a per-key outcome.
func (b *Backend) DeleteMany(ctx context.Context, keys []string) ([]backend.DeleteEntry, error) {
	out := make([]backend.DeleteEntry, 0, len(keys))
	for _, k := range keys {
		entry := backend.DeleteEntry{Key: k}
		if err := b.Delete(ctx, k, backend.DeleteOptions{}); err != nil {
			entry.Error = err.Error()
		}
		out = append(out, entry)
	}
	return out, nil
}

// List enumerates by prefix with an opaque page token, advancing one page.
func (b *Backend) List(ctx context.Context, opts backend.ListOptions) (*backend.ListResult, error) {
	const op = "azure.List"
	cc := b.containerClient()

	var marker *string
	if opts.PageToken != "" {
		marker = to.Ptr(opts.PageToken)
	}
	var maxResults *int32
	if opts.Limit > 0 {
		maxResults = to.Ptr(opts.Limit)
	}
	var prefix *string
	if opts.Prefix != "" {
		prefix = to.Ptr(opts.Prefix)
	}

	res := &backend.ListResult{}

	if opts.Delimiter != "" {
		pager := cc.NewListBlobsHierarchyPager(opts.Delimiter, &container.ListBlobsHierarchyOptions{
			Prefix:     prefix,
			Marker:     marker,
			MaxResults: maxResults,
		})
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, mapErr(op, err)
		}
		if page.Segment != nil {
			for _, item := range page.Segment.BlobItems {
				res.Objects = append(res.Objects, blobItemInfo(item))
			}
			for _, p := range page.Segment.BlobPrefixes {
				if p != nil {
					res.CommonPrefixes = append(res.CommonPrefixes, derefStr(p.Name))
				}
			}
		}
		res.NextPageToken = derefStr(page.NextMarker)
		return res, nil
	}

	pager := cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:     prefix,
		Marker:     marker,
		MaxResults: maxResults,
	})
	page, err := pager.NextPage(ctx)
	if err != nil {
		return nil, mapErr(op, err)
	}
	if page.Segment != nil {
		for _, item := range page.Segment.BlobItems {
			res.Objects = append(res.Objects, blobItemInfo(item))
		}
	}
	res.NextPageToken = derefStr(page.NextMarker)
	return res, nil
}

// Copy performs a synchronous server-side copy (CopyFromURL) into dstKey.
func (b *Backend) Copy(ctx context.Context, srcKey, dstKey string, opts backend.CopyOptions) (*backend.PutResult, error) {
	const op = "azure.Copy"
	srcURL, err := b.copySourceURL(srcKey)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, op, err)
	}

	dst := b.containerClient().NewBlobClient(dstKey)
	copts := &blob.CopyFromURLOptions{}
	if opts.IfNoneMatch == "*" {
		copts.BlobAccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: to.Ptr(azcore.ETagAny),
			},
		}
	}

	resp, err := dst.CopyFromURL(ctx, srcURL, copts)
	if err != nil {
		mapped := mapErr(op, err)
		if opts.IfNoneMatch == "*" {
			switch serr.CodeOf(mapped) {
			case serr.PreconditionFailed, serr.AlreadyExists:
				return nil, serr.Wrap(serr.AlreadyExists, op, err)
			}
		}
		return nil, mapped
	}
	return &backend.PutResult{
		ETag:      etagStr(resp.ETag),
		VersionID: derefStr(resp.VersionID),
	}, nil
}

// copySourceURL builds a URL for the source blob usable by CopyFromURL. When a
// shared key is available a short-lived read SAS is appended so private
// same-account sources work; otherwise the bare URL is returned (which only
// succeeds for publicly readable sources). Cross-account copies require the
// caller to expose the source via a SAS.
func (b *Backend) copySourceURL(srcKey string) (string, error) {
	base := b.containerClient().NewBlobClient(srcKey).URL()
	if b.sharedKey == nil {
		return base, nil
	}
	now := time.Now().UTC()
	qp, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     now.Add(-5 * time.Minute),
		ExpiryTime:    now.Add(1 * time.Hour),
		Permissions:   (&sas.BlobPermissions{Read: true}).String(),
		ContainerName: b.container,
		BlobName:      srcKey,
	}.SignWithSharedKey(b.sharedKey)
	if err != nil {
		return "", err
	}
	return base + "?" + qp.Encode(), nil
}

// Presign returns a time-limited service-SAS URL (requires a shared key).
func (b *Backend) Presign(ctx context.Context, key string, method backend.PresignMethod, expiry time.Duration) (*backend.PresignResult, error) {
	const op = "azure.Presign"
	if b.sharedKey == nil {
		return nil, serr.New(serr.Unsupported, "presign", "no account key (user-delegation SAS not implemented)")
	}

	clamped := expiry
	if clamped <= 0 || clamped > maxPresignExpiry {
		clamped = maxPresignExpiry
	}
	if b.maxExpiry > 0 && clamped > b.maxExpiry {
		clamped = b.maxExpiry
	}

	var perms sas.BlobPermissions
	switch method {
	case backend.PresignPut:
		perms = sas.BlobPermissions{Write: true, Create: true}
	default:
		perms = sas.BlobPermissions{Read: true}
	}

	now := time.Now().UTC()
	expiresAt := now.Add(clamped)
	qp, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     now.Add(-5 * time.Minute),
		ExpiryTime:    expiresAt,
		Permissions:   perms.String(),
		ContainerName: b.container,
		BlobName:      key,
	}.SignWithSharedKey(b.sharedKey)
	if err != nil {
		return nil, serr.Wrap(serr.Internal, op, err)
	}

	blobURL := b.containerClient().NewBlobClient(key).URL()
	return &backend.PresignResult{
		Method:    method,
		URL:       blobURL + "?" + qp.Encode(),
		ExpiresAt: expiresAt,
	}, nil
}

// Native is the escape hatch for backend-specific verbs; none are implemented.
func (b *Backend) Native(ctx context.Context, verb string, params map[string]string) (map[string]string, error) {
	return nil, serr.New(serr.Unsupported, "native", verb)
}

// Close releases resources (the azblob client holds no closable handles).
func (b *Backend) Close() error { return nil }

// --- helpers ---

// accessConditions builds blob access conditions from the generic option set.
// When createIfAbsent is true, IfNoneMatch is forced to "*" (ETagAny).
func accessConditions(ifNoneMatch, ifMatch string, ifModifiedSince time.Time, createIfAbsent bool) *blob.AccessConditions {
	mac := &blob.ModifiedAccessConditions{}
	set := false

	if createIfAbsent {
		mac.IfNoneMatch = to.Ptr(azcore.ETagAny)
		set = true
	} else if ifNoneMatch != "" {
		mac.IfNoneMatch = to.Ptr(azcore.ETag(ifNoneMatch))
		set = true
	}
	if ifMatch != "" {
		mac.IfMatch = to.Ptr(azcore.ETag(ifMatch))
		set = true
	}
	if !ifModifiedSince.IsZero() {
		mac.IfModifiedSince = to.Ptr(ifModifiedSince)
		set = true
	}
	if !set {
		return nil
	}
	return &blob.AccessConditions{ModifiedAccessConditions: mac}
}

// mapErr normalizes an azblob error into a serr code.
func mapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound):
		return serr.Wrap(serr.NotFound, op, err)
	case bloberror.HasCode(err, bloberror.BlobAlreadyExists):
		return serr.Wrap(serr.AlreadyExists, op, err)
	case bloberror.HasCode(err, bloberror.ConditionNotMet):
		return serr.Wrap(serr.PreconditionFailed, op, err)
	}

	var re *azcore.ResponseError
	if errors.As(err, &re) {
		switch re.StatusCode {
		case http.StatusNotModified: // 304
			return serr.Wrap(serr.NotModified, op, err)
		case http.StatusPreconditionFailed: // 412
			return serr.Wrap(serr.PreconditionFailed, op, err)
		case http.StatusNotFound: // 404
			return serr.Wrap(serr.NotFound, op, err)
		case http.StatusConflict: // 409
			return serr.Wrap(serr.AlreadyExists, op, err)
		case http.StatusForbidden: // 403
			return serr.Wrap(serr.PermissionDenied, op, err)
		case http.StatusTooManyRequests, http.StatusServiceUnavailable: // 429, 503
			return serr.Wrap(serr.Throttled, op, err)
		case http.StatusBadRequest: // 400
			return serr.Wrap(serr.InvalidArgument, op, err)
		}
	}
	return serr.Wrap(serr.Internal, op, err)
}

// blobItemInfo maps a listing item into an ObjectInfo.
func blobItemInfo(item *container.BlobItem) backend.ObjectInfo {
	if item == nil {
		return backend.ObjectInfo{}
	}
	info := backend.ObjectInfo{
		Key:          derefStr(item.Name),
		VersionID:    derefStr(item.VersionID),
		UserMetadata: fromMetaPtr(item.Metadata),
	}
	if p := item.Properties; p != nil {
		info.ETag = etagStr(p.ETag)
		info.Size = derefInt64(p.ContentLength)
		info.LastModified = derefTime(p.LastModified)
		info.ContentType = derefStr(p.ContentType)
		info.ContentEncoding = derefStr(p.ContentEncoding)
		info.CacheControl = derefStr(p.CacheControl)
	}
	return info
}

func etagStr(e *azcore.ETag) string {
	if e == nil {
		return ""
	}
	return strings.Trim(string(*e), `"`)
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func ptrIfSet(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func toMetaPtr(m map[string]string) map[string]*string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*string, len(m))
	for k, v := range m {
		out[k] = to.Ptr(v)
	}
	return out
}

func fromMetaPtr(m map[string]*string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = derefStr(v)
	}
	return out
}
