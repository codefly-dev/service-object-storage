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
			require.Equal(t, "gateway-image.json-${{ steps.version.outputs.version }}", step.With["name"])
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

func TestImagePreparationRefusesUnmergedCommitsBeforePublishing(t *testing.T) {
	var workflow struct {
		On   map[string]interface{}
		Jobs map[string]struct {
			Permissions map[string]string
			Steps       []struct {
				Name, Uses, Run string
				Env, With       map[string]string
			}
		}
	}
	require.NoError(t, yaml.Unmarshal([]byte(workflowBody(t, "publish-gateway-image.yml")), &workflow))
	require.Len(t, workflow.On, 1, "publishing must remain dispatch-only")
	require.Contains(t, workflow.On, "workflow_dispatch")
	job := workflow.Jobs["publish"]
	require.Equal(t, map[string]string{"contents": "read", "packages": "write"}, job.Permissions)
	require.GreaterOrEqual(t, len(job.Steps), 2)
	require.True(t, strings.HasPrefix(job.Steps[0].Uses, "actions/checkout@"))
	require.Equal(t, "false", job.Steps[0].With["persist-credentials"])
	require.Equal(t, "0", job.Steps[0].With["fetch-depth"])
	refusal := job.Steps[1]
	require.Equal(t, "require the commit to be on the default branch, or refuse", refusal.Name)
	require.Equal(t, "${{ github.token }}", refusal.Env["GH_TOKEN"])
	require.NotEmpty(t, refusal.Run)
	// Checkout and refusal must precede every login or build action.
	for i, step := range job.Steps {
		if strings.HasPrefix(step.Uses, "docker/") {
			require.Greater(t, i, 1)
		}
	}

	git := func(dir string, args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = dir
		command.Env = append(os.Environ(), "GIT_AUTHOR_NAME=publisher-test", "GIT_AUTHOR_EMAIL=test@example.invalid",
			"GIT_COMMITTER_NAME=publisher-test", "GIT_COMMITTER_EMAIL=test@example.invalid")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return strings.TrimSpace(string(output))
	}
	// Use a real remote with a default branch other than main. A similarly
	// named tag must not let an unmerged commit masquerade as reviewed history.
	origin := t.TempDir()
	git(origin, "init", "-b", "release")
	git(origin, "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "reviewed")
	reviewed := git(origin, "rev-parse", "HEAD")
	git(origin, "checkout", "-b", "unmerged")
	git(origin, "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "unreviewed")
	unmerged := git(origin, "rev-parse", "HEAD")
	git(origin, "checkout", "release")
	checkout := t.TempDir()
	git(checkout, "clone", "--no-hardlinks", origin, ".")
	git(checkout, "-c", "tag.gpgsign=false", "tag", "origin/release", unmerged)

	for _, tc := range []struct {
		name, sha string
		accepted  bool
	}{
		{"reviewed", reviewed, true},
		{"unmerged", unmerged, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command("bash", "-c", refusal.Run)
			command.Dir = checkout
			command.Env = append(os.Environ(), "GH_TOKEN=publisher-test-token", "GITHUB_SHA="+tc.sha, "GITHUB_REF_NAME="+tc.name)
			output, err := command.CombinedOutput()
			if tc.accepted {
				require.NoError(t, err, "%s", output)
			} else {
				require.Error(t, err, "%s", output)
				require.Contains(t, string(output), "not reachable from refs/remotes/origin/release; refusing to publish it")
			}
			config := git(checkout, "config", "--local", "--list")
			require.NotContains(t, config, "extraheader", "authentication must not persist into the Docker build context")
		})
	}
}
