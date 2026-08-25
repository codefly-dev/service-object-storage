package server

import (
	"time"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
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

func toProtoInfo(in *backend.ObjectInfo) *storagev0.ObjectInfo {
	if in == nil {
		return nil
	}
	return &storagev0.ObjectInfo{
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

func toProtoCapabilities(c backend.Capabilities) *storagev0.BackendCapabilities {
	return &storagev0.BackendCapabilities{
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

func toBackendGetOptions(req *storagev0.GetRequest) backend.GetOptions {
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

func toProtoEvent(e events.Event) *storagev0.WriteEvent {
	return &storagev0.WriteEvent{
		Key:        e.Key,
		Op:         writeOpToProto(e.Op),
		Etag:       e.ETag,
		VersionId:  e.VersionID,
		TimeUnixMs: unixMS(e.Time),
	}
}

func writeOpToProto(op events.Op) storagev0.WriteOp {
	switch op {
	case events.OpPut:
		return storagev0.WriteOp_WRITE_OP_PUT
	case events.OpDelete:
		return storagev0.WriteOp_WRITE_OP_DELETE
	default:
		return storagev0.WriteOp_WRITE_OP_UNSPECIFIED
	}
}

func presignMethodFromProto(m storagev0.PresignMethod) backend.PresignMethod {
	if m == storagev0.PresignMethod_PRESIGN_METHOD_PUT {
		return backend.PresignPut
	}
	return backend.PresignGet
}

func presignMethodToProto(m backend.PresignMethod) storagev0.PresignMethod {
	if m == backend.PresignPut {
		return storagev0.PresignMethod_PRESIGN_METHOD_PUT
	}
	return storagev0.PresignMethod_PRESIGN_METHOD_GET
}
