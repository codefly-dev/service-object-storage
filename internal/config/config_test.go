package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/config"
)

// probeEnv sets what every FromEnv call needs before a probe setting is even
// reachable: a bucket, and an auth posture. Resolution refuses an
// unauthenticated listener, and that check runs before the probe checks, so
// without a token these tests would assert against the auth error instead of
// the probe one they are about.
func probeEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SOS_BUCKET", "documents")
	t.Setenv("SOS_AUTH_TOKEN", "a-per-run-secret")
}

// TestFromEnv_RefusesUnauthenticatedListener is the startup guard: the gateway
// must not come up serving bucket-wide read/write/delete to whoever can reach
// its port, and the refusal has to tell an existing user how to move forward.
func TestFromEnv_RefusesUnauthenticatedListener(t *testing.T) {
	t.Setenv("SOS_BUCKET", "documents")

	_, err := config.FromEnv()
	require.Error(t, err)
	for _, want := range []string{"SOS_AUTH_TOKEN", "x-codefly-token", "SOS_ALLOW_ANONYMOUS"} {
		require.Contains(t, err.Error(), want)
	}
}

// TestFromEnv_RefusesBlankToken keeps whitespace from passing as a credential:
// a blank SOS_AUTH_TOKEN would install interceptors nobody can satisfy or, worse,
// read as "configured" while protecting nothing.
func TestFromEnv_RefusesBlankToken(t *testing.T) {
	t.Setenv("SOS_BUCKET", "documents")
	t.Setenv("SOS_AUTH_TOKEN", "   ")

	_, err := config.FromEnv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "SOS_AUTH_TOKEN")
}

// TestFromEnv_TokenIsWhitespaceNormalized is the regression guard for a token
// that validates one way and is enforced another. A Kubernetes Secret written
// with a YAML block scalar delivers "s3cr3t\n"; enforcing that byte rejects
// every client sending "s3cr3t" while the gateway still logs auth=token and
// looks healthy.
func TestFromEnv_TokenIsWhitespaceNormalized(t *testing.T) {
	t.Setenv("SOS_BUCKET", "documents")
	t.Setenv("SOS_AUTH_TOKEN", "s3cr3t\n")

	cfg, err := config.FromEnv()
	require.NoError(t, err)
	require.Equal(t, "s3cr3t", cfg.AuthToken, "the enforced token must be what a client can actually send")
}

// TestFromEnv_RejectsContradictoryAuthSettings covers the half-finished
// migration: a token added to the Secret while the manifest still carries the
// anonymous opt-out. Picking either one silently leaves the operator believing
// the other is in force.
func TestFromEnv_RejectsContradictoryAuthSettings(t *testing.T) {
	t.Setenv("SOS_BUCKET", "documents")
	t.Setenv("SOS_AUTH_TOKEN", "a-per-run-secret")
	t.Setenv("SOS_ALLOW_ANONYMOUS", "true")

	_, err := config.FromEnv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mutually exclusive")
}

func TestFromEnv_TokenEnablesEnforcement(t *testing.T) {
	t.Setenv("SOS_BUCKET", "documents")
	t.Setenv("SOS_AUTH_TOKEN", "a-per-run-secret")

	cfg, err := config.FromEnv()
	require.NoError(t, err)
	require.Equal(t, "a-per-run-secret", cfg.AuthToken)
}

// TestFromEnv_ExplicitOptOut covers the documented escape hatch the deployed
// profile uses, where caller identity is enforced by the cluster instead.
func TestFromEnv_ExplicitOptOut(t *testing.T) {
	t.Setenv("SOS_BUCKET", "documents")
	t.Setenv("SOS_ALLOW_ANONYMOUS", "true")

	cfg, err := config.FromEnv()
	require.NoError(t, err)
	require.Empty(t, cfg.AuthToken)
}

// TestFromEnv_BucketCheckStillRuns pins the ordering: a configuration that is
// broken in more than one way still reports the bucket first, so the auth guard
// did not swallow the pre-existing check.
func TestFromEnv_BucketCheckStillRuns(t *testing.T) {
	_, err := config.FromEnv()
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "SOS_BUCKET"), "got %v", err)
}

func TestFromEnvProbeDefaults(t *testing.T) {
	probeEnv(t)

	cfg, err := config.FromEnv()
	require.NoError(t, err)
	require.Equal(t, backend.ProbeList, cfg.Backend.ProbeStrategy)
	require.Equal(t, 10*time.Second, cfg.Health.Interval)
	require.Equal(t, 5*time.Second, cfg.Health.Timeout)
}

func TestFromEnvProbeOverrides(t *testing.T) {
	probeEnv(t)
	t.Setenv("SOS_PROBE_STRATEGY", "stat")
	t.Setenv("SOS_PROBE_KEY", ".codefly-readiness")
	t.Setenv("SOS_PROBE_INTERVAL", "45s")
	t.Setenv("SOS_PROBE_TIMEOUT", "2s")

	cfg, err := config.FromEnv()
	require.NoError(t, err)
	require.Equal(t, backend.ProbeStat, cfg.Backend.ProbeStrategy)
	require.Equal(t, ".codefly-readiness", cfg.Backend.ProbeKey)
	require.Equal(t, 45*time.Second, cfg.Health.Interval)
	require.Equal(t, 2*time.Second, cfg.Health.Timeout)
}

// TestFromEnvRejectsUnusableProbe keeps a misconfigured probe from turning into
// a permanently unready gateway that nobody can diagnose.
func TestFromEnvRejectsUnusableProbe(t *testing.T) {
	t.Run("unknown strategy", func(t *testing.T) {
		probeEnv(t)
		t.Setenv("SOS_PROBE_STRATEGY", "ping")

		_, err := config.FromEnv()
		require.ErrorContains(t, err, "SOS_PROBE_STRATEGY")
	})

	t.Run("stat without key", func(t *testing.T) {
		probeEnv(t)
		t.Setenv("SOS_PROBE_STRATEGY", "stat")

		_, err := config.FromEnv()
		require.ErrorContains(t, err, "SOS_PROBE_KEY")
	})
}
