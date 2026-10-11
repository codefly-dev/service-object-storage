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

	"github.com/codefly-dev/core/callertoken"
	"github.com/codefly-dev/service-object-storage/internal/backend"
)

// Config is the fully resolved server configuration.
type Config struct {
	ListenAddr string
	// AuthToken is the shared secret every caller must present as the
	// callertoken.MetadataKey metadata header, whitespace-trimmed: a secret delivered
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
	if err := callertoken.CheckListener(cfg.AuthToken, envBool("SOS_ALLOW_ANONYMOUS", false), callertoken.Listener{
		TokenVar: "SOS_AUTH_TOKEN", AnonymousVar: "SOS_ALLOW_ANONYMOUS",
		Exposure: "has bucket-wide read/write/delete/presign",
	}); err != nil {
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
