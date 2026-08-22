// Package config resolves the server configuration from environment variables.
// The default backend is MinIO: local and test contexts always run against
// MinIO, and only a deployed environment names a cloud backend.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/cache"
)

// Config is the fully resolved server configuration.
type Config struct {
	ListenAddr string
	Backend    backend.Config
	Cache      CacheConfig
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
	}
	if cfg.Backend.Bucket == "" {
		return Config{}, fmt.Errorf("SOS_BUCKET is required")
	}
	// MinIO addresses buckets path-style by default.
	if cfg.Backend.Kind == "minio" && os.Getenv("SOS_USE_PATH_STYLE") == "" {
		cfg.Backend.UsePathStyle = true
	}
	return cfg, nil
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
