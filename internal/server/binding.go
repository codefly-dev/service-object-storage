package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"

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

// bindableKinds are the backends a binding may name: every registered kind
// that reaches a remote store. "mem" holds nothing a caller could bind to.
var bindableKinds = map[string]bool{"s3": true, "minio": true, "gcs": true, "azure": true}

// backendFor returns the backend a call runs against and a release func the
// caller defers. Without a binding it is the gateway's configured backend.
// With one, it is a backend opened for this call alone, through a guarded
// client, from the binding's own credential; release closes it and its
// connections, so neither the credential nor a connection outlives the call.
func (s *Server) backendFor(ctx context.Context, b *storagev0.Binding) (backend.Backend, func(), error) {
	if b == nil {
		return s.be, func() {}, nil
	}
	cfg, err := boundConfig(ctx, b, s.composed)
	if err != nil {
		return nil, nil, err
	}
	be, err := backend.Open(ctx, cfg)
	if err != nil {
		cfg.HTTPClient.CloseIdleConnections()
		return nil, nil, err
	}
	return be, func() {
		_ = be.Close()
		cfg.HTTPClient.CloseIdleConnections()
	}, nil
}

// boundConfig validates a binding and translates it to a backend.Config. The
// endpoint is qualified before anything dials it, against the public-only rule
// or, for one of the gateway's composed endpoints, by exact admission.
func boundConfig(ctx context.Context, b *storagev0.Binding, composed egress.Composed) (backend.Config, error) {
	const op = "binding"
	if !bindableKinds[b.GetKind()] {
		return backend.Config{}, serr.New(serr.InvalidArgument, op,
			fmt.Sprintf("kind %q cannot be bound (want s3, minio, gcs or azure)", b.GetKind()))
	}
	if b.GetBucket() == "" {
		return backend.Config{}, serr.New(serr.InvalidArgument, op, "bucket is required")
	}
	if b.GetKind() == "minio" && b.GetEndpoint() == "" {
		return backend.Config{}, serr.New(serr.InvalidArgument, op, "kind minio requires an endpoint")
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
			return backend.Config{}, serr.New(serr.InvalidArgument, op,
				"kind gcs requires credential.service_account_key or credential.access_token")
		}
	case "azure":
		cfg.AzureAccount, cfg.AzureKey = cred.GetAccessKey().GetId(), cred.GetAccessKey().GetSecret()
		if cfg.AzureAccount == "" || cfg.AzureKey == "" {
			return backend.Config{}, serr.New(serr.InvalidArgument, op, "kind azure requires credential.access_key (account and shared key)")
		}
	default:
		cfg.AccessKey, cfg.SecretKey = cred.GetAccessKey().GetId(), cred.GetAccessKey().GetSecret()
		if cfg.AccessKey == "" || cfg.SecretKey == "" {
			return backend.Config{}, serr.New(serr.InvalidArgument, op,
				fmt.Sprintf("kind %s requires credential.access_key", b.GetKind()))
		}
	}
	var admitted []netip.AddrPort
	if cfg.Endpoint != "" {
		var err error
		if admitted, err = egress.Qualify(ctx, cfg.Endpoint, composed); err != nil {
			var dnsErr *net.DNSError
			switch {
			case errors.Is(err, egress.ErrNonPublicDestination):
				return backend.Config{}, serr.Wrap(serr.PermissionDenied, op, err)
			case errors.As(err, &dnsErr):
				return backend.Config{}, serr.Wrap(serr.Unavailable, op, err)
			default:
				return backend.Config{}, serr.Wrap(serr.InvalidArgument, op, err)
			}
		}
	}
	cfg.HTTPClient = egress.Client(admitted)
	return cfg, nil
}

// boundErr classifies a dial the guard refused as PermissionDenied wherever the
// backend surfaced it — after a redirect, or a name that resolved differently
// on the dial than when it was qualified. Any other error keeps its code.
func boundErr(err error) error {
	if err != nil && errors.Is(err, egress.ErrNonPublicDestination) {
		return serr.Wrap(serr.PermissionDenied, "binding", err)
	}
	return err
}

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
