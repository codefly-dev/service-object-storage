// Package serr is the normalized error model for the generic storage API.
// Every backend maps its provider-specific errors into these codes so a client
// never parses an S3 XML code or an Azure status string. The set mirrors
// gocloud.dev/gcerrors, the proven denominator.
package serr

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// Code is the backend-independent error class.
type Code int

const (
	// Internal is an unexpected/unclassified failure.
	Internal Code = iota
	// NotFound means the object (or version) does not exist.
	NotFound
	// AlreadyExists means a create-if-absent precondition found an object.
	AlreadyExists
	// PreconditionFailed means a conditional read/write/delete condition (ETag
	// CAS, If-Unmodified-Since) was not met.
	PreconditionFailed
	// NotModified means a conditional read matched (If-None-Match / -Modified).
	NotModified
	// Unsupported means the backend does not offer this operation/option.
	Unsupported
	// PermissionDenied means the credentials lack authorization.
	PermissionDenied
	// Throttled means the backend rate-limited the request; retry with backoff.
	Throttled
	// InvalidArgument means the request was malformed.
	InvalidArgument
	// Unavailable means the request never got an answer from the service: the
	// endpoint could not be reached (dial refused, DNS failure, TLS handshake,
	// transport timeout), or the caller's own deadline expired first. Both are
	// "no answer", which is what a caller branches on; the message distinguishes
	// them.
	Unavailable
	// TooLarge means a response or object exceeds a bound the gateway holds a
	// store to. Unlike Throttled, retrying does not help.
	TooLarge
)

func (c Code) String() string {
	switch c {
	case NotFound:
		return "NotFound"
	case AlreadyExists:
		return "AlreadyExists"
	case PreconditionFailed:
		return "PreconditionFailed"
	case NotModified:
		return "NotModified"
	case Unsupported:
		return "Unsupported"
	case PermissionDenied:
		return "PermissionDenied"
	case Throttled:
		return "Throttled"
	case InvalidArgument:
		return "InvalidArgument"
	case TooLarge:
		return "TooLarge"
	case Unavailable:
		return "Unavailable"
	default:
		return "Internal"
	}
}

// Error carries a normalized Code, the operation that produced it, and the
// underlying cause — and, where the layer that raised it knows why, a Reason
// (one of the Reason constants) with its Meta and, for a throttle, how long
// the store asked the caller to wait. The gateway sends the reason to the
// caller beside the message, so a refusal is classified by machine, not by
// reading prose.
type Error struct {
	Code       Code
	Op         string
	Err        error
	Reason     string
	Meta       map[string]string
	RetryAfter time.Duration
}

// The reasons a refusal names. Each is sent as a google.rpc.ErrorInfo reason
// in the gateway's error domain; a caller acts on the reason, not the code,
// because one code covers refusals that call for different actions — a
// throttle (back off) and a size cap (skip the object) are both
// RESOURCE_EXHAUSTED, a guard refusal (refuse the source) and a rejected
// credential (re-authorise) are both PERMISSION_DENIED.
const (
	ReasonDestinationRefused  = "DESTINATION_REFUSED"
	ReasonCredentialRejected  = "CREDENTIAL_REJECTED"
	ReasonStoreThrottled      = "STORE_THROTTLED"
	ReasonObjectTooLarge      = "OBJECT_TOO_LARGE"
	ReasonResponseTooLarge    = "RESPONSE_TOO_LARGE"
	ReasonListingTooLong      = "LISTING_TOO_LONG"
	ReasonListingIncomplete   = "LISTING_INCOMPLETE"
	ReasonBucketNotFound      = "BUCKET_NOT_FOUND"
	ReasonObjectNotFound      = "OBJECT_NOT_FOUND"
	ReasonAlreadyExists       = "ALREADY_EXISTS"
	ReasonPreconditionFailed  = "PRECONDITION_FAILED"
	ReasonUnsupported         = "UNSUPPORTED"
	ReasonBindingInvalid      = "BINDING_INVALID"
	ReasonInvalidArgument     = "INVALID_ARGUMENT"
	ReasonTooManyItems        = "TOO_MANY_ITEMS"
	ReasonEndpointUnresolved  = "ENDPOINT_UNRESOLVED"
	ReasonStoreUnavailable    = "STORE_UNAVAILABLE"
	ReasonOperationDeadline   = "OPERATION_DEADLINE"
	ReasonBodySizeMismatch    = "BODY_SIZE_MISMATCH"
	ReasonWatchConsumerBehind = "WATCH_CONSUMER_BEHIND"
	ReasonInternal            = "INTERNAL"
)

// WithReason sets the reason a refusal names, and its metadata, on e.
func (e *Error) WithReason(reason string, meta map[string]string) *Error {
	e.Reason, e.Meta = reason, meta
	return e
}

// ReasonOf is the reason err names: its own, or the one its code implies.
func ReasonOf(err error) string {
	var e *Error
	if errors.As(err, &e) && e.Reason != "" {
		return e.Reason
	}
	switch CodeOf(err) {
	case NotFound:
		return ReasonObjectNotFound
	case AlreadyExists:
		return ReasonAlreadyExists
	case PreconditionFailed:
		return ReasonPreconditionFailed
	case Unsupported:
		return ReasonUnsupported
	case PermissionDenied:
		return ReasonCredentialRejected
	case Throttled:
		return ReasonStoreThrottled
	case InvalidArgument:
		return ReasonInvalidArgument
	case Unavailable:
		return ReasonStoreUnavailable
	case TooLarge:
		return ReasonResponseTooLarge
	default:
		return ReasonInternal
	}
}

func (e *Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", e.Op, e.Code)
	}
	return fmt.Sprintf("%s: %s: %v", e.Op, e.Code, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// New builds a normalized error with a message.
func New(code Code, op, msg string) *Error {
	return &Error{Code: code, Op: op, Err: errors.New(msg)}
}

// Wrap wraps a cause under a normalized code.
func Wrap(code Code, op string, err error) *Error {
	return &Error{Code: code, Op: op, Err: err}
}

// ErrBucketNotFound marks a NotFound whose subject is the bucket or container
// itself, not one object in it. A caller that treats a missing object as
// "deleted" must not treat a missing bucket the same way: every object would
// read as deleted.
var ErrBucketNotFound = errors.New("bucket or container does not exist")

// BucketNotFound is NotFound for a missing bucket or container, carrying
// ErrBucketNotFound beside the backend's cause.
func BucketNotFound(op string, err error) *Error {
	return Wrap(NotFound, op, fmt.Errorf("%w: %w", ErrBucketNotFound, err)).WithReason(ReasonBucketNotFound, nil)
}

// CodeOf extracts the normalized code, defaulting to Internal for a plain error
// and to Internal for nil-safe callers (nil returns Internal is never used —
// callers check err != nil first).
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return Internal
}

// Is reports whether err carries the given normalized code.
func Is(err error, code Code) bool {
	return err != nil && CodeOf(err) == code
}

// Unreachable reports whether err means the service never answered, rather
// than a response it produced. Backends use it to separate "no answer" from
// "the store said no", which readiness must not conflate.
//
// A context deadline or cancellation counts: the caller gave up before any
// answer arrived, which is the same absence of a verdict. Both are matched
// explicitly rather than left to the net.Error check — context.DeadlineExceeded
// satisfies net.Error only incidentally, and context.Canceled does not satisfy
// it at all, so a cancelled probe would otherwise be normalized to Internal and
// read as a backend fault.
func Unreachable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
