package main

import (
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
			Uses                       string
			With, Secrets, Permissions map[string]string
		}
	}
	require.NoError(t, yaml.Unmarshal([]byte(workflowBody(t, "publish-gateway-image.yml")), &workflow))
	require.True(t, workflow.On.Dispatch.Inputs["version"].Required)
	job := workflow.Jobs["publish"]
	// Core owns input validation and lock writing. This repo owns the wiring:
	// the version must reach that workflow and the artifact must name our lock.
	require.Equal(t, "codefly-dev/core/.github/workflows/publish-service-image.yml@ea49e7a968ef5f2fd325b1c41f823a47eb20141c", job.Uses)
	require.Equal(t, "${{ inputs.version }}", job.With["version"])
	require.Equal(t, "gateway-image.json", job.With["lock-file"])
	require.Equal(t, "${{ secrets.GITHUB_TOKEN }}", job.Secrets["registry-token"])
	require.Equal(t, "read", job.Permissions["contents"])
	require.Equal(t, "write", job.Permissions["packages"])
}
