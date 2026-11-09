package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// PutSizeProbeBytes is how much of a Put whose size the client did not
// declare the gateway buffers to learn it. A body that ends within it is
// written with its exact size: one request on S3/MinIO, so conditional writes
// stay atomic and the store allocates no multipart buffers. A longer body
// streams with an unknown size.
const PutSizeProbeBytes = 16 << 20

// errPutFinished closes a Put's body pipe once the backend has returned, so a
// receiver still writing to it stops.
var errPutFinished = errors.New("put: backend finished reading the body")

// Put assembles a streamed upload and writes it, hiding multipart in the backend.
//
// total_size is a hint the client may leave unset, and proto3 cannot tell
// unset from 0. A positive total_size is a declared length: the backend is
// handed it, and it is enforced on the bytes received, holding the last chunk
// back until the length is confirmed so a backend that reads exactly that
// many bytes never commits a body that turns out to be longer. Anything else
// is unknown: the gateway buffers up to PutSizeProbeBytes to learn the size,
// and streams only a body longer than that.
func (s *Server) Put(stream storagev0.ObjectStorage_PutServer) error {
	first, err := stream.Recv()
	if err != nil {
		return toStatus(serrInvalid("put: empty stream"))
	}
	header := first.GetHeader()
	if header == nil {
		return toStatus(serrInvalid("put: first message must be the header"))
	}

	// The backend runs under a context the body cancels when it refuses, so a
	// store that commits on Close (GCS) aborts instead of committing a
	// truncated object.
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(nil)

	body, size, refusal, err := s.putBody(cancel, stream, header.GetTotalSize())
	if err != nil {
		return err
	}
	opts := backend.PutOptions{
		ContentType:     header.GetContentType(),
		ContentEncoding: header.GetContentEncoding(),
		CacheControl:    header.GetCacheControl(),
		UserMetadata:    header.GetUserMetadata(),
		IfNoneMatch:     header.GetIfNoneMatch(),
		IfMatch:         header.GetIfMatch(),
		Size:            size,
	}
	res, err := s.be.Put(ctx, header.GetKey(), body, opts)
	if closer, ok := body.(interface{ CloseWithError(error) error }); ok {
		// Unblock a receiver still writing; it exits on the failed write.
		_ = closer.CloseWithError(errPutFinished)
	}
	// A body refusal is the cause of whatever the backend made of it. Read it
	// without waiting on the receiver: it may be blocked on a client that has
	// not sent its next message.
	if rerr := refusal(); rerr != nil {
		return rerr
	}
	if err != nil {
		return toStatus(err)
	}
	s.hub.Publish(events.Event{Key: header.GetKey(), Op: events.OpPut, ETag: res.ETag, VersionID: res.VersionID})
	return stream.SendAndClose(&storagev0.PutResult{
		Etag:       res.ETag,
		VersionId:  res.VersionID,
		Generation: res.Generation,
	})
}

// putBody returns the reader the backend consumes, the size to hand it, and
// a function reporting the body's refusal, if any.
func (s *Server) putBody(cancel context.CancelCauseFunc, stream storagev0.ObjectStorage_PutServer, declared int64) (io.Reader, int64, func() error, error) {
	if declared > 0 {
		pr, pw := io.Pipe()
		r := &putReceiver{stream: stream, pw: pw, cancel: cancel, declared: declared}
		go r.run(nil)
		return pr, declared, r.refusal, nil
	}

	// Unknown size: buffer up to PutSizeProbeBytes to learn it.
	var probe bytes.Buffer
	for probe.Len() <= PutSizeProbeBytes {
		msg, err := stream.Recv()
		if err == io.EOF {
			return bytes.NewReader(probe.Bytes()), int64(probe.Len()), func() error { return nil }, nil
		}
		if err != nil {
			return nil, 0, nil, recvStatus(err)
		}
		probe.Write(msg.GetData())
	}
	pr, pw := io.Pipe()
	r := &putReceiver{stream: stream, pw: pw, cancel: cancel}
	go r.run(probe.Bytes())
	return pr, -1, r.refusal, nil
}

// putReceiver moves the rest of a Put's stream into the backend's pipe,
// enforcing a declared length.
type putReceiver struct {
	stream   storagev0.ObjectStorage_PutServer
	pw       *io.PipeWriter
	cancel   context.CancelCauseFunc
	declared int64 // > 0: enforced; otherwise unknown

	mu  sync.Mutex
	err error
}

func (r *putReceiver) refusal() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// fail records why the body is refused, cancels the backend, and closes the
// pipe with it, in that order: the backend sees a cancelled context before
// the failed read, and the handler sees the cause before the backend returns.
func (r *putReceiver) fail(err error) {
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
	r.cancel(err)
	r.pw.CloseWithError(err)
}

func (r *putReceiver) run(prefix []byte) {
	var received int64
	// pending is the last chunk received, written only once the next one (or
	// the end of the stream) confirms it is within the declared length.
	pending := prefix
	write := func(b []byte) bool {
		if len(b) == 0 {
			return true
		}
		_, err := r.pw.Write(b)
		return err == nil // a failed write means the backend has returned
	}
	for {
		msg, err := r.stream.Recv()
		if err == io.EOF {
			if r.declared > 0 && received != r.declared {
				r.fail(putSizeMismatch(r.declared, received))
				return
			}
			if write(pending) {
				r.pw.Close()
			}
			return
		}
		if err != nil {
			r.fail(recvStatus(err))
			return
		}
		data := msg.GetData()
		if len(data) == 0 {
			continue
		}
		received += int64(len(data))
		if r.declared > 0 && received > r.declared {
			r.fail(putSizeMismatch(r.declared, received))
			return
		}
		if r.declared <= 0 {
			// Unknown size: nothing to confirm, so nothing to hold back.
			if !write(pending) || !write(data) {
				return
			}
			pending = nil
			continue
		}
		if !write(pending) {
			return
		}
		pending = data
	}
}

// recvStatus keeps a status the stream already carries (a cancelled or
// expired client) and classifies anything else as internal.
func recvStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

// putSizeMismatch refuses a Put whose body disagrees with its declared
// total_size, before the backend commits anything.
func putSizeMismatch(declared, received int64) error {
	relation := "is longer than"
	if received < declared {
		relation = "ended short of"
	}
	return refuse(codes.Aborted, fmt.Sprintf("put body %s its declared %d bytes (received %d)", relation, declared, received),
		serr.ReasonBodySizeMismatch, map[string]string{"declared_bytes": fmt.Sprint(declared), "sent_bytes": fmt.Sprint(received)}, 0)
}
