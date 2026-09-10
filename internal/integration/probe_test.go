//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"

	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
)

// openProbeBackend opens a real MinIO backend against the live server, letting
// the caller vary the credentials and bucket a probe will be judged on.
func openProbeBackend(t *testing.T, cfg backend.Config) backend.Backend {
	t.Helper()
	cfg.Kind = "minio"
	cfg.Endpoint = mustEnv(t, "MINIO_ENDPOINT")
	cfg.Region = "us-east-1"
	cfg.UsePathStyle = true

	be, err := backend.Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	return be
}

// TestProbeAgainstLiveMinIO qualifies the probe against a real server: declared
// access passes under both strategies, and each way of losing it is reported as
// its own cause rather than a generic failure.
func TestProbeAgainstLiveMinIO(t *testing.T) {
	endpoint := mustEnv(t, "MINIO_ENDPOINT")
	ak := mustEnv(t, "MINIO_ACCESS_KEY")
	sk := mustEnv(t, "MINIO_SECRET_KEY")
	bucket := mustEnv(t, "MINIO_BUCKET")
	ensureBucket(t, endpoint, ak, sk, bucket)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("granted access", func(t *testing.T) {
		for _, strategy := range []backend.ProbeStrategy{backend.ProbeList, backend.ProbeStat} {
			t.Run(string(strategy), func(t *testing.T) {
				be := openProbeBackend(t, backend.Config{
					Bucket: bucket, AccessKey: ak, SecretKey: sk,
					ProbeStrategy: strategy, ProbeKey: ".codefly-readiness",
				})
				require.NoError(t, be.Probe(ctx))
			})
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		be := openProbeBackend(t, backend.Config{
			Bucket:    fmt.Sprintf("%s-absent-%d", bucket, time.Now().UnixNano()),
			AccessKey: ak, SecretKey: sk,
		})
		err := be.Probe(ctx)
		require.Error(t, err)
		require.Equal(t, serr.NotFound, serr.CodeOf(err), "got %v", err)
	})

	t.Run("refused credentials", func(t *testing.T) {
		be := openProbeBackend(t, backend.Config{
			Bucket: bucket, AccessKey: "wrong-key", SecretKey: "wrong-secret-value",
		})
		err := be.Probe(ctx)
		require.Error(t, err)
		require.Equal(t, serr.PermissionDenied, serr.CodeOf(err), "got %v", err)
		require.NotContains(t, err.Error(), "wrong-secret-value", "a probe failure must not leak credentials")
	})
}
