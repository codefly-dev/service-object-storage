// Package config resolves the server configuration from environment variables.
// The default backend is MinIO: local and test contexts always run against
// MinIO, and only a deployed environment names a cloud backend.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codefly-dev/service-object-storage/internal/auth"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/cache"
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
	Cache     CacheConfig
	Health    HealthConfig
}

// HealthConfig paces the background probe that drives the gRPC health service.
// Interval bounds how stale a reported access check can be — a probe that
// succeeded once is never replayed beyond it.
type HealthConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}

// CacheConfig controls the caching layer. When RedisAddr is empty the cache is
// L1-only (single-replica / local); a shared Redis tier enables cross-replica
// coherency.
type CacheConfig struct {
	Enabled       bool
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	Options       cache.Options
}

// FromEnv resolves configuration from SOS_* environment variables.
func FromEnv() (Config, error) {
	cfg := Config{
		ListenAddr: env("SOS_LISTEN", ":9464"),
		AuthToken:  strings.TrimSpace(os.Getenv("SOS_AUTH_TOKEN")),
		Backend: backend.Config{
			Kind:               env("SOS_BACKEND", "minio"),
			Bucket:             os.Getenv("SOS_BUCKET"),
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
		Cache: CacheConfig{
			Enabled:       envBool("SOS_CACHE", true),
			RedisAddr:     os.Getenv("SOS_REDIS_ADDR"),
			RedisPassword: os.Getenv("SOS_REDIS_PASSWORD"),
			RedisDB:       int(envInt("SOS_REDIS_DB", 0)),
			Options: cache.Options{
				MaxCachedObjectBytes: envInt("SOS_CACHE_MAX_OBJECT_BYTES", 1<<20),
				MetaTTL:              envDuration("SOS_CACHE_META_TTL", 0),
				BytesTTL:             envDuration("SOS_CACHE_BYTES_TTL", 0),
				NegativeTTL:          envDuration("SOS_CACHE_NEGATIVE_TTL", 0),
			},
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
	// MinIO addresses buckets path-style by default.
	if cfg.Backend.Kind == "minio" && os.Getenv("SOS_USE_PATH_STYLE") == "" {
		cfg.Backend.UsePathStyle = true
	}
	return cfg, nil
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

func envInt(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
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
