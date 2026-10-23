package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/egress"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// The caps on a bound call. They bound what one caller-named store can make the
// gateway hold or relay, and Capabilities reports them so the caller does not
// keep caps of its own.
const (
	maxBoundObjectBytes = 5 << 20
	maxBoundListLimit   = 1000
)

// boundOperationTimeout bounds each bound operation end to end — a List, a
// Stat, a Get, a Capabilities, each GetMany item — body included, so a store
// that sends headers and then stalls cannot hold the caller. boundBatchTimeout
// bounds a whole GetMany, however many items it carries. Both run from the
// start of the call, so qualifying the endpoint (its DNS resolution) is inside
// them. Variables only so a test can shorten them.
var (
	boundOperationTimeout = 30 * time.Second
	boundBatchTimeout     = 10 * time.Minute
)

// boundCall returns the context a bound RPC runs under from its first step:
// within budget. An unbound RPC's context is ctx.
func boundCall(ctx context.Context, b *storagev0.Binding, budget time.Duration) (context.Context, context.CancelFunc) {
	if b == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, budget)
}

// bindableKinds are the backends a binding may name: every registered kind
// that reaches a remote store. "mem" holds nothing a caller could bind to.
var bindableKinds = map[string]bool{"s3": true, "minio": true, "gcs": true, "azure": true}

// operation returns the context one operation of the call runs under: a
// bound call's carries boundOperationTimeout; an unbound call's is ctx.
func (c *call) operation(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.secrets == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, boundOperationTimeout)
}

// call is the backend one RPC runs against, and how that RPC's errors leave
// the gateway. Release closes a bound backend and its connections, so neither
// the credential nor a connection outlives the call.
type call struct {
	backend.Backend
	Release func()
	// guard and secrets are set for a bound call only.
	guard   *egress.Guard
	secrets []string
}

// backendFor returns the call an RPC runs against. Without a binding it is the
// gateway's configured backend. With one, it is a backend opened for this call
// alone, through a guarded client, from the binding's own credential. A
// returned error has already passed through the call's error policy.
func (s *Server) backendFor(ctx context.Context, b *storagev0.Binding) (*call, error) {
	if b == nil {
		return &call{Backend: s.be, Release: func() {}}, nil
	}
	c := &call{secrets: bindingSecrets(b)}
	cfg, guard, err := boundConfig(ctx, b, s.composed)
	if err != nil {
		return nil, c.err(err)
	}
	c.guard = guard
	be, err := backend.Open(ctx, cfg)
	if err != nil {
		guard.Client.CloseIdleConnections()
		return nil, c.err(err)
	}
	c.Backend = be
	c.Release = func() {
		_ = be.Close()
		guard.Client.CloseIdleConnections()
	}
	return c, nil
}

// err is the error policy of a bound call: a dial the guard refused is
// PermissionDenied however the SDK in between reported it (some format the
// dial error into text and drop its identity), and no credential of the
// binding leaves in a message — an upstream may echo what it was sent. The
// normalized code and the store's own words are otherwise kept. An unbound
// call's errors pass through unchanged.
func (c *call) err(err error) error {
	if err == nil || c.secrets == nil {
		return err
	}
	code := serr.CodeOf(err)
	reason := serr.ReasonOf(err)
	op, msg := "binding", err.Error()
	var meta map[string]string
	var coded *serr.Error
	if errors.As(err, &coded) && coded.Err != nil {
		op, msg, meta = coded.Op, coded.Err.Error(), coded.Meta
	}
	switch {
	case errors.Is(err, egress.ErrNonPublicDestination) || (c.guard != nil && c.guard.Refused()):
		code, reason = serr.PermissionDenied, serr.ReasonDestinationRefused
	case errors.Is(err, egress.ErrResponseTooLarge) || (c.guard != nil && c.guard.TooLarge()):
		code, reason = serr.TooLarge, serr.ReasonResponseTooLarge
		meta = map[string]string{"max_bytes": fmt.Sprint(maxBoundObjectBytes)}
		msg = fmt.Sprintf("a response from the store exceeded %d bytes; refused: %s", maxBoundObjectBytes, msg)
	case errors.Is(err, context.DeadlineExceeded):
		code, reason = serr.Unavailable, serr.ReasonOperationDeadline
		meta = map[string]string{"per_operation": boundOperationTimeout.String(), "per_get_many": boundBatchTimeout.String()}
		msg = fmt.Sprintf("the store did not complete the operation within its deadline (%s per operation, %s per GetMany call): %s",
			boundOperationTimeout, boundBatchTimeout, msg)
	}
	secrets := c.secrets
	if c.guard != nil {
		secrets = append(c.guard.Sent(), secrets...)
	}
	out := serr.New(code, op, egress.Redact(msg, secrets)).WithReason(reason, meta)
	if code == serr.Throttled && c.guard != nil {
		out.RetryAfter = c.guard.RetryAfter()
	}
	return out
}

// bindingSecrets are the credential values a bound call's errors must never
// carry: the secret half of an access key, an access token, and a
// service-account key with its private key. The access key id and the account
// name are identifiers, not secrets.
func bindingSecrets(b *storagev0.Binding) []string {
	secrets := []string{}
	cred := b.GetCredential()
	for _, value := range []string{cred.GetAccessKey().GetSecret(), cred.GetAccessToken(), string(cred.GetServiceAccountKey())} {
		if value != "" {
			secrets = append(secrets, value)
		}
	}
	var key struct {
		PrivateKey string `json:"private_key"`
	}
	if json.Unmarshal(cred.GetServiceAccountKey(), &key) == nil && key.PrivateKey != "" {
		secrets = append(secrets, key.PrivateKey, strings.ReplaceAll(key.PrivateKey, "\n", "\\n"))
	}
	return secrets
}

// boundConfig validates a binding and translates it to a backend.Config and
// the guard its client dials through. The endpoint is qualified before
// anything dials it, against the public-only rule or, for one of the
// gateway's composed endpoints, by exact admission.
func boundConfig(ctx context.Context, b *storagev0.Binding, composed egress.Composed) (backend.Config, *egress.Guard, error) {
	const op = "binding"
	if !bindableKinds[b.GetKind()] {
		return backend.Config{}, nil, bindingInvalid(op,
			fmt.Sprintf("kind %q cannot be bound (want s3, minio, gcs or azure)", b.GetKind()))
	}
	if b.GetBucket() == "" {
		return backend.Config{}, nil, bindingInvalid(op, "bucket is required")
	}
	if b.GetKind() == "minio" && b.GetEndpoint() == "" {
		return backend.Config{}, nil, bindingInvalid(op, "kind minio requires an endpoint")
	}
	cfg := backend.Config{
		Kind:         b.GetKind(),
		Bucket:       b.GetBucket(),
		Prefix:       b.GetPrefix(),
		Region:       b.GetRegion(),
		Endpoint:     b.GetEndpoint(),
		UsePathStyle: true,
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	cred := b.GetCredential()
	switch b.GetKind() {
	case "gcs":
		cfg.GCSCredentialsJSON, cfg.GCSAccessToken = cred.GetServiceAccountKey(), cred.GetAccessToken()
		if len(cfg.GCSCredentialsJSON) == 0 && cfg.GCSAccessToken == "" {
			return backend.Config{}, nil, bindingInvalid(op,
				"kind gcs requires credential.service_account_key or credential.access_token")
		}
	case "azure":
		cfg.AzureAccount, cfg.AzureKey = cred.GetAccessKey().GetId(), cred.GetAccessKey().GetSecret()
		if cfg.AzureAccount == "" || cfg.AzureKey == "" {
			return backend.Config{}, nil, bindingInvalid(op, "kind azure requires credential.access_key (account and shared key)")
		}
	default:
		cfg.AccessKey, cfg.SecretKey = cred.GetAccessKey().GetId(), cred.GetAccessKey().GetSecret()
		if cfg.AccessKey == "" || cfg.SecretKey == "" {
			return backend.Config{}, nil, bindingInvalid(op,
				fmt.Sprintf("kind %s requires credential.access_key", b.GetKind()))
		}
	}
	var admission egress.Admission
	if cfg.Endpoint != "" {
		var err error
		if admission, err = egress.Qualify(ctx, cfg.Endpoint, composed); err != nil {
			var dnsErr *net.DNSError
			switch {
			case errors.Is(err, egress.ErrNonPublicDestination):
				return backend.Config{}, nil, serr.Wrap(serr.PermissionDenied, op, err).WithReason(serr.ReasonDestinationRefused, nil)
			case errors.As(err, &dnsErr):
				return backend.Config{}, nil, serr.Wrap(serr.Unavailable, op, err).WithReason(serr.ReasonEndpointUnresolved, nil)
			default:
				return backend.Config{}, nil, serr.Wrap(serr.InvalidArgument, op, err).WithReason(serr.ReasonBindingInvalid, nil)
			}
		}
	}
	guard := egress.NewGuard(admission, maxBoundObjectBytes+1)
	cfg.HTTPClient = guard.Client
	return cfg, guard, nil
}

// bindingInvalid refuses a binding the gateway cannot use as given.
func bindingInvalid(op, msg string) *serr.Error {
	return serr.New(serr.InvalidArgument, op, msg).WithReason(serr.ReasonBindingInvalid, nil)
}

// capMeta is the metadata of an OBJECT_TOO_LARGE refusal.
var capMeta = map[string]string{"max_bytes": fmt.Sprint(maxBoundObjectBytes)}

// errObjectTooLarge refuses a bound Get past maxBoundObjectBytes.
var errObjectTooLarge = fmt.Errorf("object exceeds the bound-read cap of %d bytes", maxBoundObjectBytes)

// capReader fails a bound Get whose body runs past the cap, so a store that
// understates an object's size still cannot make the gateway relay more.
type capReader struct {
	io.ReadCloser
	left int64
}

func (c *capReader) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if c.left -= int64(n); c.left < 0 {
		return 0, errObjectTooLarge
	}
	return n, err
}
