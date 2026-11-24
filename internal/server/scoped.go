package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
	"github.com/codefly-dev/service-object-storage/internal/sessions"
)

// ScopedMode is whether the gateway runs calls under scoped sessions.
type ScopedMode string

const (
	// ScopedOff refuses a call that names a Scope. It is the default: nothing
	// changes for a caller that names none.
	ScopedOff ScopedMode = "off"
	// ScopedAllow runs a call that names a Scope under its session, and any
	// other with the gateway's own credential, as before.
	ScopedAllow ScopedMode = "allow"
	// ScopedRequire also refuses every call on the configured bucket that
	// names no Scope, so nothing a caller sends runs with the gateway's own
	// credential.
	ScopedRequire ScopedMode = "require"
)

// Scoped is the gateway's scoped-session configuration.
type Scoped struct {
	Mode    ScopedMode
	Manager *sessions.Manager
	// Presign enables the direct mode: a URL signed by a scope's session.
	Presign bool
	// PresignMax is the longest lifetime a scoped URL may be asked for.
	PresignMax time.Duration
}

// DefaultScopedPresignMax is the scoped presign limit when none is set.
const DefaultScopedPresignMax = 5 * time.Minute

// WithScoped turns scoped sessions on. Without it, or with ScopedOff, a call
// that names a Scope is refused.
func (s *Server) WithScoped(scoped Scoped) *Server {
	if scoped.PresignMax <= 0 {
		scoped.PresignMax = DefaultScopedPresignMax
	}
	s.scoped = scoped
	return s
}

func scopeInvalid(msg string) *serr.Error {
	return serr.New(serr.InvalidArgument, "scope", msg).WithReason(serr.ReasonScopeInvalid, nil)
}

func scopeModeFromProto(mode storagev0.ScopeMode) sessions.Mode {
	switch mode {
	case storagev0.ScopeMode_SCOPE_MODE_READ:
		return sessions.Read
	case storagev0.ScopeMode_SCOPE_MODE_INGEST:
		return sessions.Ingest
	case storagev0.ScopeMode_SCOPE_MODE_MAINTENANCE:
		return sessions.Maintenance
	default:
		return 0
	}
}

func scopeFromProto(scope *storagev0.Scope) sessions.Scope {
	return sessions.Scope{Base: scope.GetBase(), Partition: scope.GetPartition(), Scope: scope.GetScope(), Mode: scopeModeFromProto(scope.GetMode())}
}

// scopedCall is the call an RPC that names a Scope runs against: a store
// client holding the scope's session, and only that. Every way of not having
// one is a refusal; nothing here returns the gateway's own backend.
func (s *Server) scopedCall(ctx context.Context, requested *storagev0.Scope) (*call, error) {
	if err := s.scopedAvailable(); err != nil {
		return nil, err
	}
	scope := scopeFromProto(requested)
	session, err := s.scoped.Manager.Session(ctx, scope)
	if err != nil {
		return nil, scopedRefusal(err)
	}
	return &call{Backend: session.Backend, Release: session.Release, scope: &scope, expires: session.Expires}, nil
}

// scopedAvailable refuses a scoped call on a gateway that runs none.
func (s *Server) scopedAvailable() error {
	if s.scoped.Manager != nil && (s.scoped.Mode == ScopedAllow || s.scoped.Mode == ScopedRequire) {
		return nil
	}
	return serr.New(serr.PreconditionFailed, "scope",
		"this gateway does not run calls under scoped sessions; the call was not made with any other credential").
		WithReason(serr.ReasonScopedUnavailable, nil)
}

// scopedRefusal names why no session was had.
func scopedRefusal(err error) error {
	switch {
	case errors.Is(err, sessions.ErrInvalidScope):
		return serr.Wrap(serr.InvalidArgument, "scope", err).WithReason(serr.ReasonScopeInvalid, nil)
	case errors.Is(err, sessions.ErrNotGranted):
		return serr.Wrap(serr.PermissionDenied, "scope", err).WithReason(serr.ReasonScopeNotGranted, nil)
	case errors.Is(err, sessions.ErrPolicyTooLarge):
		return serr.Wrap(serr.PreconditionFailed, "scope", err).WithReason(serr.ReasonScopePolicyTooLarge, nil)
	case errors.Is(err, sessions.ErrUnqualified):
		return serr.Wrap(serr.PreconditionFailed, "scope", err).WithReason(serr.ReasonScopedUnavailable, nil)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return serr.Wrap(serr.Unavailable, "scope", err)
	default:
		return serr.Wrap(serr.Unavailable, "scope", err).WithReason(serr.ReasonScopeIssuerUnavailable, nil)
	}
}

// scopedErr marks a store's refusal of a scoped call as what it is: the
// store, under the scope's session, said no. The store's own code is already
// in the metadata where the backend reports one.
func (c *call) scopedErr(err error) error {
	if serr.CodeOf(err) != serr.PermissionDenied {
		return err
	}
	meta := map[string]string{"scoped": "true", "mode": c.scope.Mode.String()}
	msg := err.Error()
	var coded *serr.Error
	op := "scope"
	if errors.As(err, &coded) {
		for key, value := range coded.Meta {
			meta[key] = value
		}
		if coded.Err != nil {
			op, msg = coded.Op, coded.Err.Error()
		}
	}
	return serr.New(serr.PermissionDenied, op,
		fmt.Sprintf("the store refused the call under the %s session of the scope: %s", c.scope.Mode, msg)).
		WithReason(serr.ReasonCredentialRejected, meta)
}

func presignRefused(msg string) error {
	return serr.New(serr.InvalidArgument, "presign", msg).WithReason(serr.ReasonPresignRefused, nil)
}

// checkScopedPresign is everything that bounds a URL signed by a scope's
// session. Signing calls no store, so unlike every other scoped call nothing
// downstream would refuse these: they are refused here, and never clamped.
func (s *Server) checkScopedPresign(req *storagev0.PresignRequest) error {
	if err := s.scopedAvailable(); err != nil {
		return err
	}
	if !s.scoped.Presign {
		return serr.New(serr.PreconditionFailed, "presign",
			"this gateway does not sign URLs with scoped sessions; read and write through the gateway").
			WithReason(serr.ReasonScopedUnavailable, nil)
	}
	scope := scopeFromProto(req.GetScope())
	want := sessions.Read
	if req.GetMethod() == storagev0.PresignMethod_PRESIGN_METHOD_PUT || req.GetMethod() == storagev0.PresignMethod_PRESIGN_METHOD_POST {
		want = sessions.Ingest
	}
	switch {
	case req.GetMethod() == storagev0.PresignMethod_PRESIGN_METHOD_UNSPECIFIED:
		return presignRefused("a scoped URL names its method")
	case scope.Mode != want:
		return presignRefused(fmt.Sprintf("a %s URL is signed by the scope's %s session, not its %s session", req.GetMethod(), want, scope.Mode))
	case !scope.Contains(req.GetKey()):
		return presignRefused("a scoped URL is for one object of the scope, by its exact key")
	case req.GetExpirySeconds() < 1:
		return presignRefused("a scoped URL states its lifetime in whole seconds")
	case time.Duration(req.GetExpirySeconds())*time.Second > s.scoped.PresignMax:
		return presignRefused(fmt.Sprintf("a scoped URL lives at most %s", s.scoped.PresignMax))
	}
	return nil
}

// presign signs with whichever backend the call resolved to, binding the
// upload's type and size when the request states them.
func presign(ctx context.Context, c *call, req *storagev0.PresignRequest) (*backend.PresignResult, error) {
	method := presignMethodFromProto(req.GetMethod())
	expiry := time.Duration(req.GetExpirySeconds()) * time.Second
	bounded := req.GetContentType() != "" || req.GetContentLengthMax() != 0 || req.GetContentLengthMin() != 0 || method == backend.PresignPost
	if !bounded {
		return c.Presign(ctx, req.GetKey(), method, expiry)
	}
	switch {
	case method == backend.PresignGet:
		return nil, presignRefused("a GET URL has no upload to bound")
	case method == backend.PresignPut && (req.GetContentLengthMax() != 0 || req.GetContentLengthMin() != 0):
		return nil, presignRefused("a PUT URL cannot bound the size of its body; ask for POST")
	case method == backend.PresignPost && req.GetContentLengthMax() <= 0:
		return nil, presignRefused("a POST URL states content_length_max")
	case req.GetContentLengthMin() < 0 || req.GetContentLengthMin() > req.GetContentLengthMax() && method == backend.PresignPost:
		return nil, presignRefused("content_length_min is between 0 and content_length_max")
	}
	signer, ok := c.Backend.(backend.BoundedPresigner)
	if !ok {
		return nil, serr.New(serr.Unsupported, "presign",
			c.Name()+" cannot bind an upload's type or size into a presigned URL; no unbounded URL was signed in its place")
	}
	return signer.PresignBounded(ctx, req.GetKey(), backend.PresignBounds{
		Method: method, Expiry: expiry, ContentType: req.GetContentType(),
		ContentLengthMin: req.GetContentLengthMin(), ContentLengthMax: req.GetContentLengthMax(),
	})
}
