package server

import (
	"errors"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// ErrorDomain is the google.rpc.ErrorInfo domain of every refusal the
// gateway names.
const ErrorDomain = "object-storage.codefly.dev"

// defaultRetryDelay is the RetryInfo delay of a throttle whose store gave no
// Retry-After.
const defaultRetryDelay = time.Second

// toStatus maps a normalized serr code to a gRPC status so clients branch on
// codes, never backend-specific error strings. NotModified is not an error on
// the wire — it is carried in GetHeader.not_modified — so it maps to Internal
// here as a defensive fallback (callers handle it before reaching this).
//
// Every status also names its reason (see refuse), since one code covers
// refusals a caller must act on differently.
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
	case serr.Unavailable:
		c = codes.Unavailable
	case serr.TooLarge:
		c = codes.ResourceExhausted
	default:
		c = codes.Internal
	}
	var coded *serr.Error
	var meta map[string]string
	var retryAfter time.Duration
	if errors.As(err, &coded) {
		meta, retryAfter = coded.Meta, coded.RetryAfter
	}
	return refuse(c, err.Error(), serr.ReasonOf(err), meta, retryAfter)
}

// refuse is a status with code and message, naming its reason in a
// google.rpc.ErrorInfo beside the message. A throttle also carries a
// google.rpc.RetryInfo: the store's Retry-After when it gave one (metadata
// retry_delay_source=store), otherwise defaultRetryDelay (=default).
func refuse(c codes.Code, msg, reason string, meta map[string]string, retryAfter time.Duration) error {
	st := status.New(c, msg)
	info := &errdetails.ErrorInfo{Domain: ErrorDomain, Reason: reason, Metadata: map[string]string{}}
	for k, v := range meta {
		info.Metadata[k] = v
	}
	var retry *errdetails.RetryInfo
	if reason == serr.ReasonStoreThrottled {
		info.Metadata["retry_delay_source"] = "store"
		if retryAfter <= 0 {
			retryAfter, info.Metadata["retry_delay_source"] = defaultRetryDelay, "default"
		}
		retry = &errdetails.RetryInfo{RetryDelay: durationpb.New(retryAfter)}
	}
	var detailed *status.Status
	var err error
	if retry != nil {
		detailed, err = st.WithDetails(info, retry)
	} else {
		detailed, err = st.WithDetails(info)
	}
	if err != nil {
		return st.Err()
	}
	return detailed.Err()
}
