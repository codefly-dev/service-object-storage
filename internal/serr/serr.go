// Package serr is the normalized error model for the generic storage API.
// Every backend maps its provider-specific errors into these codes so a client
// never parses an S3 XML code or an Azure status string. The set mirrors
// gocloud.dev/gcerrors, the proven denominator.
package serr

import (
	"errors"
	"fmt"
	"net"
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
	// Unavailable means the backend could not be reached at all — the request
	// never got an answer from the service (dial refused, DNS failure, TLS
	// handshake, transport timeout).
	Unavailable
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
	case Unavailable:
		return "Unavailable"
	default:
		return "Internal"
	}
}

// Error carries a normalized Code, the operation that produced it, and the
// underlying cause.
type Error struct {
	Code Code
	Op   string
	Err  error
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

// Unreachable reports whether err is a transport-level failure rather than a
// response the service produced. Backends use it to separate "endpoint is not
// there" from "the store said no", which readiness must not conflate.
func Unreachable(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr)
}
