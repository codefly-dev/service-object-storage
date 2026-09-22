package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestImagePreparationRequiresTheUpcomingVersion(t *testing.T) {
	var workflow struct {
		On struct {
			Dispatch struct {
				Inputs map[string]struct {
					Required bool
				}
			} `yaml:"workflow_dispatch"`
		}
		Jobs map[string]struct {
			Steps []struct {
				ID, Uses, Run string
				Env, With     map[string]string
			}
		}
	}
	require.NoError(t, yaml.Unmarshal([]byte(workflowBody(t, "publish-gateway-image.yml")), &workflow))
	require.True(t, workflow.On.Dispatch.Inputs["version"].Required)
	var validation string
	artifact := false
	for _, step := range workflow.Jobs["publish"].Steps {
		if step.ID == "version" {
			require.Equal(t, "${{ inputs.version }}", step.Env["VERSION"])
			validation = step.Run
		}
		if strings.HasPrefix(step.Uses, "actions/upload-artifact@") {
			require.Equal(t, "gateway-image-lock-${{ steps.version.outputs.version }}", step.With["name"])
			require.Equal(t, "release-evidence/gateway-image.json", step.With["path"])
			require.Equal(t, "error", step.With["if-no-files-found"])
			artifact = true
		}
	}
	require.NotEmpty(t, validation)
	require.True(t, artifact)
	for _, tc := range []struct {
		version string
		valid   bool
	}{
		{"9.8.7", true},
		{"0.0.0", true},
		{"", false},
		{"v9.8.7", false},
		{"9.8", false},
		{"09.8.7", false},
		{"9.8.7\nother=value", false},
	} {
		t.Run(tc.version, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "output")
			command := exec.Command("bash", "-c", validation)
			command.Env = append(os.Environ(), "VERSION="+tc.version, "GITHUB_OUTPUT="+output)
			payload, err := command.CombinedOutput()
			require.Equal(t, tc.valid, err == nil, "%s: %s", err, payload)
			if tc.valid {
				actual, err := os.ReadFile(output)
				require.NoError(t, err)
				require.Equal(t, "version="+tc.version+"\n", string(actual))
			}
		})
	}
}
