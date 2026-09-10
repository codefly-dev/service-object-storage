// Package server implements the ObjectStorage gRPC service over a
// backend.Backend (typically a cache-decorated one). It maps the proto surface
// to the backend contract, streams Get/Put, and normalizes errors to gRPC codes.
package server

import (
	"context"
	"errors"
	"io"
	"time"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/health"
	"github.com/codefly-dev/service-object-storage/internal/serr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// chunkSize bounds each streamed data frame on Get.
const chunkSize = 256 * 1024

// Readiness is the last-probe view Ready reports. The background monitor
// implements it, so Ready and the orchestrator's health check answer from one
// probe rather than each running their own.
type Readiness interface {
	Verdict() health.Verdict
}

// Server is the ObjectStorage service implementation.
type Server struct {
	storagev0.UnimplementedObjectStorageServer
	be        backend.Backend
	hub       *events.Hub
	readiness Readiness
}

// New builds a Server over be, publishing write events to hub and answering
// Ready from readiness.
func New(be backend.Backend, hub *events.Hub, readiness Readiness) *Server {
	return &Server{be: be, hub: hub, readiness: readiness}
}

// Stat returns object metadata, honoring conditional headers by comparing the
// fetched ETag (NotModified is carried in the header, not as an error).
func (s *Server) Stat(ctx context.Context, req *storagev0.StatRequest) (*storagev0.GetHeader, error) {
	info, err := s.be.Stat(ctx, req.GetKey(), req.GetVersionId())
	if err != nil {
		return nil, toStatus(err)
	}
	if inm := req.GetIfNoneMatch(); inm != "" && inm == info.ETag {
		return &storagev0.GetHeader{Info: toProtoInfo(info), NotModified: true}, nil
	}
	if im := req.GetIfMatch(); im != "" && im != info.ETag {
		return nil, status.Error(codes.FailedPrecondition, "if-match: etag mismatch")
	}
	return &storagev0.GetHeader{Info: toProtoInfo(info)}, nil
}

// Get streams an object: a header frame (info / not_modified) followed by data
// frames. This is the proxy byte path; large objects that should skip the proxy
// use the Presign RPC instead.
func (s *Server) Get(req *storagev0.GetRequest, stream storagev0.ObjectStorage_GetServer) error {
	res, err := s.be.Get(stream.Context(), req.GetKey(), toBackendGetOptions(req))
	if err != nil {
		return toStatus(err)
	}
	if res.NotModified {
		return stream.Send(&storagev0.GetResponse{
			Kind: &storagev0.GetResponse_Header{Header: &storagev0.GetHeader{
				Info: toProtoInfo(&res.Info), NotModified: true,
			}},
		})
	}
	defer res.Body.Close()

	if err := stream.Send(&storagev0.GetResponse{
		Kind: &storagev0.GetResponse_Header{Header: &storagev0.GetHeader{Info: toProtoInfo(&res.Info)}},
	}); err != nil {
		return err
	}

	buf := make([]byte, chunkSize)
	for {
		n, rerr := res.Body.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&storagev0.GetResponse{
				Kind: &storagev0.GetResponse_Data{Data: buf[:n]},
			}); sendErr != nil {
				return sendErr
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return toStatus(serrWrapInternal(rerr))
		}
	}
}

// Put assembles a streamed upload and writes it, hiding multipart in the backend.
func (s *Server) Put(stream storagev0.ObjectStorage_PutServer) error {
	first, err := stream.Recv()
	if err != nil {
		return toStatus(serrInvalid("put: empty stream"))
	}
	header := first.GetHeader()
	if header == nil {
		return toStatus(serrInvalid("put: first message must be the header"))
	}

	pr, pw := io.Pipe()
	go func() {
		var werr error
		for {
			msg, rerr := stream.Recv()
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				werr = rerr
				break
			}
			if d := msg.GetData(); len(d) > 0 {
				if _, e := pw.Write(d); e != nil {
					werr = e
					break
				}
			}
		}
		pw.CloseWithError(werr)
	}()

	opts := backend.PutOptions{
		ContentType:     header.GetContentType(),
		ContentEncoding: header.GetContentEncoding(),
		CacheControl:    header.GetCacheControl(),
		UserMetadata:    header.GetUserMetadata(),
		IfNoneMatch:     header.GetIfNoneMatch(),
		IfMatch:         header.GetIfMatch(),
		Size:            header.GetTotalSize(),
	}
	res, err := s.be.Put(stream.Context(), header.GetKey(), pr, opts)
	if err != nil {
		pr.CloseWithError(err)
		return toStatus(err)
	}
	s.hub.Publish(events.Event{Key: header.GetKey(), Op: events.OpPut, ETag: res.ETag, VersionID: res.VersionID})
	return stream.SendAndClose(&storagev0.PutResult{
		Etag:       res.ETag,
		VersionId:  res.VersionID,
		Generation: res.Generation,
	})
}

func (s *Server) Delete(ctx context.Context, req *storagev0.DeleteRequest) (*storagev0.DeleteResult, error) {
	err := s.be.Delete(ctx, req.GetKey(), backend.DeleteOptions{
		VersionID: req.GetVersionId(),
		IfMatch:   req.GetIfMatch(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	s.hub.Publish(events.Event{Key: req.GetKey(), Op: events.OpDelete, VersionID: req.GetVersionId()})
	return &storagev0.DeleteResult{}, nil
}

func (s *Server) DeleteMany(ctx context.Context, req *storagev0.DeleteManyRequest) (*storagev0.DeleteManyResult, error) {
	entries, err := s.be.DeleteMany(ctx, req.GetKeys())
	if err != nil && len(entries) == 0 {
		return nil, toStatus(err)
	}
	out := &storagev0.DeleteManyResult{Entries: make([]*storagev0.DeleteEntry, 0, len(entries))}
	for _, e := range entries {
		if e.Error == "" {
			s.hub.Publish(events.Event{Key: e.Key, Op: events.OpDelete})
		}
		out.Entries = append(out.Entries, &storagev0.DeleteEntry{Key: e.Key, Error: e.Error})
	}
	return out, nil
}

func (s *Server) List(ctx context.Context, req *storagev0.ListRequest) (*storagev0.ListResult, error) {
	res, err := s.be.List(ctx, backend.ListOptions{
		Prefix:    req.GetPrefix(),
		Delimiter: req.GetDelimiter(),
		PageToken: req.GetPageToken(),
		Limit:     req.GetLimit(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	out := &storagev0.ListResult{
		CommonPrefixes: res.CommonPrefixes,
		NextPageToken:  res.NextPageToken,
		Objects:        make([]*storagev0.ObjectInfo, 0, len(res.Objects)),
	}
	for i := range res.Objects {
		out.Objects = append(out.Objects, toProtoInfo(&res.Objects[i]))
	}
	return out, nil
}

func (s *Server) Copy(ctx context.Context, req *storagev0.CopyRequest) (*storagev0.CopyResult, error) {
	res, err := s.be.Copy(ctx, req.GetSourceKey(), req.GetDestKey(), backend.CopyOptions{
		IfNoneMatch: req.GetIfNoneMatch(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	s.hub.Publish(events.Event{Key: req.GetDestKey(), Op: events.OpPut, ETag: res.ETag, VersionID: res.VersionID})
	return &storagev0.CopyResult{Etag: res.ETag, VersionId: res.VersionID}, nil
}

func (s *Server) Presign(ctx context.Context, req *storagev0.PresignRequest) (*storagev0.PresignResult, error) {
	exp := time.Duration(req.GetExpirySeconds()) * time.Second
	res, err := s.be.Presign(ctx, req.GetKey(), presignMethodFromProto(req.GetMethod()), exp)
	if err != nil {
		return nil, toStatus(err)
	}
	return &storagev0.PresignResult{
		Method:          presignMethodToProto(res.Method),
		Url:             res.URL,
		Headers:         res.Headers,
		ExpiresAtUnixMs: unixMS(res.ExpiresAt),
	}, nil
}

// Watch streams write events to the client until it disconnects. A subscriber
// that falls behind is dropped by the hub (its channel closes), which surfaces
// here as RESOURCE_EXHAUSTED so the client reconciles before re-watching.
// Delivery is best-effort — see the WriteEvent proto contract.
func (s *Server) Watch(req *storagev0.WatchRequest, stream storagev0.ObjectStorage_WatchServer) error {
	sub := s.hub.Subscribe(req.GetPrefix())
	defer sub.Close()
	// The header marks the subscription as live. Writes served by this replica
	// after the client observes it are delivered in order (or a slow client is
	// dropped); writes served by other replicas arrive best-effort over the
	// shared tier. A client reconciles anything before this point out of band.
	if err := stream.SendHeader(nil); err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev, ok := <-sub.Events():
			if !ok {
				return status.Error(codes.ResourceExhausted, "watch: consumer fell behind")
			}
			if err := stream.Send(toProtoEvent(ev)); err != nil {
				return err
			}
		}
	}
}

// Capabilities reports the static feature set. It deliberately touches no
// network: what the backend supports is a property of its kind and
// configuration, not of whether it is currently reachable — that is Ready.
func (s *Server) Capabilities(ctx context.Context, _ *storagev0.CapabilitiesRequest) (*storagev0.BackendCapabilities, error) {
	return toProtoCapabilities(s.be.Capabilities()), nil
}

// Ready reports the most recent background probe, and deliberately does not
// probe the store itself. Probing per call made this RPC an unpaced amplifier
// onto the cloud API — a client in a loop became a LIST-per-request stream,
// whose throttling degrades real traffic — and gave it a private timeout that
// SOS_PROBE_TIMEOUT could not reach. checked_at_unix_ms carries how fresh the
// answer is, so a caller can judge staleness rather than be told a cached
// verdict was measured now.
func (s *Server) Ready(ctx context.Context, _ *storagev0.ReadyRequest) (*storagev0.Readiness, error) {
	v := s.readiness.Verdict()
	res := &storagev0.Readiness{Backend: s.be.Name(), Ready: v.Ready}
	if v.CheckedAt.IsZero() {
		// The monitor probes once at startup, so this is the narrow window
		// before that first probe lands. Reporting it as unready with a stated
		// cause beats implying a probe ran and passed.
		res.Code = serr.Unavailable.String()
		res.Detail = "no readiness probe has completed yet"
		return res, nil
	}
	res.CheckedAtUnixMs = unixMS(v.CheckedAt)
	if !v.Ready {
		res.Code = v.Code
		res.Detail = v.Detail
	}
	return res, nil
}

func (s *Server) Native(ctx context.Context, req *storagev0.NativeRequest) (*storagev0.NativeResult, error) {
	values, err := s.be.Native(ctx, req.GetVerb(), req.GetParams())
	if err != nil {
		return nil, toStatus(err)
	}
	return &storagev0.NativeResult{Values: values}, nil
}

func serrWrapInternal(err error) error {
	var e *serr.Error
	if errors.As(err, &e) {
		return err
	}
	return serr.Wrap(serr.Internal, "stream", err)
}

func serrInvalid(msg string) error { return serr.New(serr.InvalidArgument, "request", msg) }
