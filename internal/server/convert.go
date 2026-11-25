package server

import (
	"time"

	storagev1 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v1"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
)

func unixMS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func fromUnixMS(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func toProtoInfo(in *backend.ObjectInfo) *storagev1.ObjectInfo {
	if in == nil {
		return nil
	}
	return &storagev1.ObjectInfo{
		Key:                in.Key,
		Etag:               in.ETag,
		WeakEtag:           in.WeakETag,
		Size:               in.Size,
		LastModifiedUnixMs: unixMS(in.LastModified),
		VersionId:          in.VersionID,
		Generation:         in.Generation,
		Metageneration:     in.MetaGeneration,
		ContentType:        in.ContentType,
		ContentEncoding:    in.ContentEncoding,
		CacheControl:       in.CacheControl,
		UserMetadata:       in.UserMetadata,
	}
}

func toProtoCapabilities(c backend.Capabilities) *storagev1.BackendCapabilities {
	return &storagev1.BackendCapabilities{
		Backend:                 c.Backend,
		ConditionalPut:          c.ConditionalPut,
		ConditionalCopy:         c.ConditionalCopy,
		AtomicRename:            c.AtomicRename,
		Versions:                c.Versions,
		Tags:                    c.Tags,
		Presign:                 c.Presign,
		PresignMaxExpirySeconds: int64(c.PresignMaxExpiry / time.Second),
		PresignAmbientCreds:     c.PresignAmbientCreds,
		BatchDeleteMax:          int32(c.BatchDeleteMax),
		NativeVerbs:             c.NativeVerbs,
	}
}

func toBackendGetOptions(req *storagev1.GetRequest) backend.GetOptions {
	opts := backend.GetOptions{
		IfNoneMatch:     req.GetIfNoneMatch(),
		IfMatch:         req.GetIfMatch(),
		IfModifiedSince: fromUnixMS(req.GetIfModifiedSinceUnixMs()),
		VersionID:       req.GetVersionId(),
	}
	if r := req.GetRange(); r != nil {
		opts.Range = &backend.ByteRange{Offset: r.GetOffset(), Length: r.GetLength()}
	}
	return opts
}

func toProtoEvent(e events.Event) *storagev1.WriteEvent {
	return &storagev1.WriteEvent{
		Key:        e.Key,
		Op:         writeOpToProto(e.Op),
		Etag:       e.ETag,
		VersionId:  e.VersionID,
		TimeUnixMs: unixMS(e.Time),
	}
}

func writeOpToProto(op events.Op) storagev1.WriteOp {
	switch op {
	case events.OpPut:
		return storagev1.WriteOp_WRITE_OP_PUT
	case events.OpDelete:
		return storagev1.WriteOp_WRITE_OP_DELETE
	default:
		return storagev1.WriteOp_WRITE_OP_UNSPECIFIED
	}
}

func presignMethodFromProto(m storagev1.PresignMethod) backend.PresignMethod {
	switch m {
	case storagev1.PresignMethod_PRESIGN_METHOD_PUT:
		return backend.PresignPut
	case storagev1.PresignMethod_PRESIGN_METHOD_POST:
		return backend.PresignPost
	}
	return backend.PresignGet
}

func presignMethodToProto(m backend.PresignMethod) storagev1.PresignMethod {
	switch m {
	case backend.PresignPut:
		return storagev1.PresignMethod_PRESIGN_METHOD_PUT
	case backend.PresignPost:
		return storagev1.PresignMethod_PRESIGN_METHOD_POST
	}
	return storagev1.PresignMethod_PRESIGN_METHOD_GET
}
