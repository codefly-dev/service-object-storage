// Package config resolves the server configuration from environment variables.
// The default backend is MinIO: local and test contexts always run against
// MinIO, and only a deployed environment names a cloud backend.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codefly-dev/service-object-storage/internal/auth"
	"github.com/codefly-dev/service-object-storage/internal/backend"
)

// Config is the fully resolved server configuration.
type Config struct {
	ListenAddr string
	// AuthToken is the shared secret every caller must present as the
	// auth.MetadataKey metadata header, whitespace-trimmed: a secret delivered
	// through a YAML block scalar or an env file arrives with a trailing
	// newline, and enforcing that byte would reject every client that sends the
	// secret the operator thinks they configured. Empty means the listener is
	// unauthenticated, which FromEnv only resolves when the operator has
	// explicitly accepted it.
	AuthToken string
	Backend   backend.Config
	Health    HealthConfig
}

// HealthConfig paces the background probe that drives the gRPC health service.
// Interval bounds how stale a reported access check can be — a probe that
// succeeded once is never replayed beyond it.
type HealthConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}

// FromEnv resolves configuration from SOS_* environment variables.
func FromEnv() (Config, error) {
	cfg := Config{
		ListenAddr: env("SOS_LISTEN", ":9464"),
		AuthToken:  strings.TrimSpace(os.Getenv("SOS_AUTH_TOKEN")),
		Backend: backend.Config{
			Kind:               env("SOS_BACKEND", "minio"),
			Bucket:             os.Getenv("SOS_BUCKET"),
			Prefix:             os.Getenv("SOS_PREFIX"),
			Region:             env("SOS_REGION", "us-east-1"),
			Endpoint:           os.Getenv("SOS_ENDPOINT"),
			UsePathStyle:       envBool("SOS_USE_PATH_STYLE", false),
			AccessKey:          os.Getenv("SOS_ACCESS_KEY"),
			SecretKey:          os.Getenv("SOS_SECRET_KEY"),
			GCSCredentialsFile: os.Getenv("SOS_GCS_CREDENTIALS_FILE"),
			AzureAccount:       os.Getenv("SOS_AZURE_ACCOUNT"),
			AzureKey:           os.Getenv("SOS_AZURE_KEY"),
			PresignMaxExpiry:   envDuration("SOS_PRESIGN_MAX_EXPIRY", 0),
			ProbeStrategy:      backend.ProbeStrategy(env("SOS_PROBE_STRATEGY", string(backend.ProbeList))),
			ProbeKey:           os.Getenv("SOS_PROBE_KEY"),
		},
		Health: HealthConfig{
			Interval: envDuration("SOS_PROBE_INTERVAL", 10*time.Second),
			Timeout:  envDuration("SOS_PROBE_TIMEOUT", 5*time.Second),
		},
	}
	if cfg.Backend.Bucket == "" {
		return Config{}, fmt.Errorf("SOS_BUCKET is required")
	}
	if err := checkListenerIsAuthenticated(cfg.AuthToken); err != nil {
		return Config{}, err
	}
	switch cfg.Backend.ProbeStrategy {
	case backend.ProbeList:
	case backend.ProbeStat:
		if cfg.Backend.ProbeKey == "" {
			return Config{}, fmt.Errorf("SOS_PROBE_KEY is required when SOS_PROBE_STRATEGY=%s", backend.ProbeStat)
		}
	default:
		return Config{}, fmt.Errorf("unknown SOS_PROBE_STRATEGY %q (want %q or %q)",
			cfg.Backend.ProbeStrategy, backend.ProbeList, backend.ProbeStat)
	}
	// A non-positive interval panics time.NewTicker inside the readiness
	// monitor's goroutine, taking the process down with no way to recover; a
	// non-positive timeout expires every probe context before it is used, so
	// readiness is permanently NOT_SERVING with no cause an operator can see.
	// Both are silent at parse time (envDuration only falls back on empty or
	// unparseable input, so "0s" and "-1s" arrive intact), so they are rejected
	// here where the message can name the variable.
	if cfg.Health.Interval <= 0 {
		return Config{}, fmt.Errorf("SOS_PROBE_INTERVAL must be positive, got %s", cfg.Health.Interval)
	}
	if cfg.Health.Timeout <= 0 {
		return Config{}, fmt.Errorf("SOS_PROBE_TIMEOUT must be positive, got %s", cfg.Health.Timeout)
	}
	if err := checkBackendIsComplete(cfg.Backend); err != nil {
		return Config{}, err
	}
	presign, err := resolvePresignEndpoint(cfg.Backend,
		PresignOrigin(strings.TrimSpace(os.Getenv("SOS_PRESIGN_ORIGIN"))),
		strings.TrimSpace(os.Getenv("SOS_PUBLIC_ENDPOINT")))
	if err != nil {
		return Config{}, err
	}
	cfg.Backend.PresignEndpoint = presign
	prefix, err := backend.NormalizePrefix(cfg.Backend.Prefix)
	if err != nil {
		return Config{}, err
	}
	cfg.Backend.Prefix = prefix
	// MinIO addresses buckets path-style by default.
	if cfg.Backend.Kind == "minio" && os.Getenv("SOS_USE_PATH_STYLE") == "" {
		cfg.Backend.UsePathStyle = true
	}
	return cfg, nil
}

// PresignOrigin names which host the URLs Presign mints are signed for.
type PresignOrigin string

const (
	// PresignFromEndpoint signs against SOS_ENDPOINT, the address the gateway
	// dials. Right when whoever fetches the URL reaches the store at that same
	// address — typically a deployment whose store endpoint is public.
	PresignFromEndpoint PresignOrigin = "endpoint"
	// PresignFromPublic signs against SOS_PUBLIC_ENDPOINT while the gateway keeps
	// dialling SOS_ENDPOINT. Right when the two differ — a gateway container that
	// reaches MinIO over the Docker bridge while a browser on the host reaches it
	// at localhost.
	PresignFromPublic PresignOrigin = "public"
)

// resolvePresignEndpoint turns the caller's explicit presign choice into the
// endpoint the backend signs against. There is no default: a backend that puts
// a configured endpoint's host into its URLs (backend.SignsAgainstEndpoint)
// requires SOS_PRESIGN_ORIGIN, and one that does not refuses it, because a
// setting with no effect reads as one that is in force.
//
// Before this choice existed the gateway always signed against SOS_ENDPOINT.
// Locally that is a container-only name (host.docker.internal) or, where an
// operator substituted one, the machine's LAN address — which the next DHCP
// lease invalidates for every URL and CSP that named it.
func resolvePresignEndpoint(b backend.Config, origin PresignOrigin, public string) (string, error) {
	if !backend.SignsAgainstEndpoint(b.Kind, b.Endpoint) {
		if origin != "" || public != "" {
			return "", fmt.Errorf("SOS_PRESIGN_ORIGIN / SOS_PUBLIC_ENDPOINT are set, but SOS_BACKEND=%s with this SOS_ENDPOINT "+
				"signs against the provider's own host, so they would have no effect: unset them", b.Kind)
		}
		return "", nil
	}
	switch origin {
	case "":
		return "", fmt.Errorf("SOS_PRESIGN_ORIGIN is required for SOS_BACKEND=%s with SOS_ENDPOINT=%s: presigned URLs name a host, "+
			"and the gateway will not guess which one the caller fetches from. "+
			"Set SOS_PRESIGN_ORIGIN=%s to sign against SOS_ENDPOINT, or SOS_PRESIGN_ORIGIN=%s with SOS_PUBLIC_ENDPOINT "+
			"(e.g. http://localhost:<port>) to sign against the host callers use while still dialling SOS_ENDPOINT",
			b.Kind, b.Endpoint, PresignFromEndpoint, PresignFromPublic)
	case PresignFromEndpoint:
		if public != "" {
			return "", fmt.Errorf("SOS_PRESIGN_ORIGIN=%s and SOS_PUBLIC_ENDPOINT are contradictory: "+
				"unset SOS_PUBLIC_ENDPOINT to sign against SOS_ENDPOINT, or set SOS_PRESIGN_ORIGIN=%s to sign against it",
				PresignFromEndpoint, PresignFromPublic)
		}
		return b.Endpoint, nil
	case PresignFromPublic:
		if public == "" {
			return "", fmt.Errorf("SOS_PRESIGN_ORIGIN=%s requires SOS_PUBLIC_ENDPOINT, the exact origin callers fetch presigned URLs from", PresignFromPublic)
		}
		if err := checkPublicEndpoint(b.Kind, public); err != nil {
			return "", err
		}
		return public, nil
	default:
		return "", fmt.Errorf("unknown SOS_PRESIGN_ORIGIN %q (want %q or %q)", origin, PresignFromEndpoint, PresignFromPublic)
	}
}

// checkPublicEndpoint requires an absolute http(s) URL naming a host and
// nothing a signer could not honour. The scheme is required, unlike
// SOS_ENDPOINT's, because it is the one the fetcher uses and cannot be inferred.
func checkPublicEndpoint(kind, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("SOS_PUBLIC_ENDPOINT %q is not a URL: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("SOS_PUBLIC_ENDPOINT %q must start with http:// or https://", raw)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("SOS_PUBLIC_ENDPOINT %q names no host", raw)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("SOS_PUBLIC_ENDPOINT %q must not carry credentials, a query or a fragment", raw)
	}
	// Azure (Azurite) addresses the account as a path segment; the S3 signers
	// take an origin only, and minio-go refuses a path outright.
	if kind != "azure" && u.Path != "" && u.Path != "/" {
		return fmt.Errorf("SOS_PUBLIC_ENDPOINT %q must be an origin (scheme://host[:port]) for SOS_BACKEND=%s", raw, kind)
	}
	return nil
}

// checkBackendIsComplete refuses a backend selection that cannot open a store,
// naming the variable that is missing. Without it the failure surfaces from
// inside the backend's SDK — MinIO with no endpoint reports "Endpoint:  does
// not follow ip address or domain name standards", which names neither the
// variable nor the fact that nothing was configured.
func checkBackendIsComplete(b backend.Config) error {
	switch b.Kind {
	case "minio":
		if strings.TrimSpace(b.Endpoint) == "" {
			return fmt.Errorf("SOS_BACKEND=minio requires SOS_ENDPOINT (the host:port of a MinIO server), and none is set. " +
				"minio is the default backend when SOS_BACKEND is unset, so a gateway deployed without a MinIO to talk to lands here: " +
				"select the store the environment actually provides instead, e.g. SOS_BACKEND=gcs with SOS_BUCKET (and optionally SOS_PREFIX)")
		}
	case "azure":
		if strings.TrimSpace(b.AzureAccount) == "" && strings.TrimSpace(b.Endpoint) == "" {
			return fmt.Errorf("SOS_BACKEND=azure requires SOS_AZURE_ACCOUNT (the storage account name) or SOS_ENDPOINT, and neither is set")
		}
	}
	return nil
}

// checkListenerIsAuthenticated refuses to resolve a configuration that would
// serve bucket-wide read/write/delete/presign to anyone who can reach the
// listen port. The gateway always binds every interface inside its container —
// Docker port publishing cannot forward to a loopback-bound process — so the
// listen address says nothing about who can reach it, and the token is the only
// thing the gateway itself can enforce.
func checkListenerIsAuthenticated(token string) error {
	anonymous := envBool("SOS_ALLOW_ANONYMOUS", false)
	if token != "" {
		// Both set is a contradiction, not a preference: "here is a credential"
		// and "accept callers with no credential" cannot both be the intent, and
		// silently picking one leaves the operator believing the other. It is
		// also the shape a half-finished migration takes — a token added to the
		// Secret while the manifest still carries the opt-out.
		if anonymous {
			return fmt.Errorf("SOS_AUTH_TOKEN and SOS_ALLOW_ANONYMOUS=true are mutually exclusive: " +
				"unset SOS_ALLOW_ANONYMOUS to enforce the token, or unset SOS_AUTH_TOKEN to accept anonymous callers")
		}
		return nil
	}
	if anonymous {
		return nil
	}
	return fmt.Errorf("SOS_AUTH_TOKEN is required: without it every caller that can reach the listener has bucket-wide read/write/delete/presign. "+
		"Set SOS_AUTH_TOKEN to a shared secret and have clients send it as the %q gRPC metadata header, "+
		"or set SOS_ALLOW_ANONYMOUS=true if the listener is confined to a private boundary enforced elsewhere (cluster NetworkPolicy / service-mesh mTLS)",
		auth.MetadataKey)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
