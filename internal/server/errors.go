package server

import (
	"github.com/codefly-dev/service-object-storage/internal/serr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toStatus maps a normalized serr code to a gRPC status so clients branch on
// codes, never backend-specific error strings. NotModified is not an error on
// the wire — it is carried in GetHeader.not_modified — so it maps to Internal
// here as a defensive fallback (callers handle it before reaching this).
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var c codes.Code
	switch serr.CodeOf(err) {
	case serr.NotFound:
		c = codes.NotFound
	case serr.AlreadyExists:
		c = codes.AlreadyExists
	case serr.PreconditionFailed:
		c = codes.FailedPrecondition
	case serr.Unsupported:
		c = codes.Unimplemented
	case serr.PermissionDenied:
		c = codes.PermissionDenied
	case serr.Throttled:
		c = codes.ResourceExhausted
	case serr.InvalidArgument:
		c = codes.InvalidArgument
	default:
		c = codes.Internal
	}
	return status.Error(c, err.Error())
}
