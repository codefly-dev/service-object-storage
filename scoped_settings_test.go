package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestScopedSettingsAreDeliveredOnlyWhenAskedFor(t *testing.T) {
	var settings Settings
	require.NoError(t, yaml.Unmarshal([]byte("bucket: docs\n"), &settings))
	envs, err := scopedEnvironment(&settings)
	require.NoError(t, err)
	require.Empty(t, envs, "a spec that names no scoped setting changes nothing about the gateway")
	settings.ScopedSessions = "off"
	envs, err = scopedEnvironment(&settings)
	require.NoError(t, err)
	require.Empty(t, envs)

	require.NoError(t, yaml.Unmarshal([]byte(`
scoped-sessions: require
scoped-presign: true
scoped-grants:
  - base: files
    modes: [read, ingest, maintenance]
  - base: exports
    modes: [read]
    partitions: [tenant-a]
`), &settings))
	envs, err = scopedEnvironment(&settings)
	require.NoError(t, err)
	got := map[string]string{}
	for _, env := range envs {
		got[env.Key] = env.ValueAsString()
	}
	require.Equal(t, map[string]string{
		"SOS_SCOPED_SESSIONS": "require",
		"SOS_SCOPED_PRESIGN":  "true",
		"SOS_SCOPED_GRANTS":   `[{"base":"files","modes":["read","ingest","maintenance"]},{"base":"exports","modes":["read"],"partitions":["tenant-a"]}]`,
	}, got)

	for name, broken := range map[string]Settings{
		"an unknown mode":  {ScopedSessions: "on", ScopedGrants: []ScopedGrant{{Base: "files", Modes: []string{"read"}}}},
		"no grant":         {ScopedSessions: "allow"},
		"an unknown grant": {ScopedSessions: "allow", ScopedGrants: []ScopedGrant{{Base: "files", Modes: []string{"write"}}}},
		"a base path":      {ScopedSessions: "allow", ScopedGrants: []ScopedGrant{{Base: "a/b", Modes: []string{"read"}}}},
	} {
		_, err := scopedEnvironment(&broken)
		require.Error(t, err, name)
	}
}

// A deployment names a store with no qualified issuer and renders no scoped
// setting, so the Builder refuses the spec instead of rendering a gateway
// that would serve everything with its bucket-wide credential.
func TestDeployRejectsScopedSessions(t *testing.T) {
	ctx := context.Background()
	builder := newDeployBuilder(t, ctx)
	builder.ScopedSessions = "require"
	builder.ScopedGrants = []ScopedGrant{{Base: "files", Modes: []string{"read"}}}
	resp, err := builder.Deploy(ctx, deployRequest(t.TempDir(), map[string]string{"SOS_BACKEND": "gcs", "SOS_BUCKET": "b"}))
	require.NoError(t, err)
	require.Contains(t, resp.GetState().GetMessage(), "scoped-sessions: require cannot be delivered")

	builder = newDeployBuilder(t, ctx)
	builder.ScopedSessions = "off"
	resp, err = builder.Deploy(ctx, deployRequest(t.TempDir(), map[string]string{"SOS_BACKEND": "gcs", "SOS_BUCKET": "b"}))
	require.NoError(t, err)
	require.NotContains(t, resp.GetState().GetMessage(), "scoped-sessions")
}
