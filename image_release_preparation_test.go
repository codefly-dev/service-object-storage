package main

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// coreActionPin is the pin the publish job must hold: the composite action's
// path, at a full commit SHA. A tag or branch would let an upstream change
// rewrite a job that runs with this repository's registry credential, and the
// action's own guards would be the thing rewritten.
var coreActionPin = regexp.MustCompile(
	`^codefly-dev/core/\.github/actions/publish-service-image@[0-9a-f]{40}$`)

// publishWorkflow is publish-gateway-image.yml, in file order so a test can
// assert what runs before what.
type publishWorkflow struct {
	On   map[string]interface{} `yaml:"on"`
	Jobs map[string]struct {
		If             string            `yaml:"if"`
		TimeoutMinutes int               `yaml:"timeout-minutes"`
		Permissions    map[string]string `yaml:"permissions"`
		Steps          []struct {
			Name string            `yaml:"name"`
			ID   string            `yaml:"id"`
			Uses string            `yaml:"uses"`
			Run  string            `yaml:"run"`
			With map[string]string `yaml:"with"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func publishJob(t *testing.T) publishWorkflow {
	t.Helper()
	var workflow publishWorkflow
	require.NoError(t, yaml.Unmarshal([]byte(workflowBody(t, "publish-gateway-image.yml")), &workflow))
	return workflow
}

// The version check, the default-branch refusal, the build, the digest and the
// lock artifact are core's — one implementation, exercised by core's own test
// caller, instead of a copy here that drifts. What stays this repository's
// obligation is the job the action is a step in: GHCR authorizes the repository
// that defines the job, and a composite action cannot declare job policy.
func TestImagePreparationDelegatesToCoresPublishAction(t *testing.T) {
	workflow := publishJob(t)

	require.Len(t, workflow.On, 1, "publishing must remain dispatch-only")
	require.Contains(t, workflow.On, "workflow_dispatch")

	job := workflow.Jobs["publish"]
	require.Equal(t, "github.event_name == 'workflow_dispatch'", job.If,
		"the caller owns the event gate; the action refuses anything else but cannot declare it")
	require.Equal(t, 30, job.TimeoutMinutes)
	require.Equal(t, map[string]string{"contents": "read", "packages": "write"}, job.Permissions,
		"packages: write has to be granted by this repository, which is the whole reason the job lives here")

	// Exactly two steps: a checkout, then the action. The action proves the
	// build context is the clean triggering commit, so a step that writes
	// anything before it — generated code, a version stamp, a restored cache —
	// is refused, and a step after it would run under a registry credential
	// for no reason.
	require.Len(t, job.Steps, 2, "no step may run before or after core's action")

	checkout := job.Steps[0]
	require.True(t, strings.HasPrefix(checkout.Uses, "actions/checkout@"))
	require.Equal(t, "false", checkout.With["persist-credentials"],
		"a persisted credential lands in the local git config, which the action refuses")
	require.Equal(t, "0", checkout.With["fetch-depth"],
		"the ancestry proof reads real history")
	require.NotContains(t, checkout.With, "ref",
		"the checkout must select the triggering commit, not a ref the dispatcher chose")

	publish := job.Steps[1]
	require.Regexp(t, coreActionPin, publish.Uses)
	require.Equal(t, "publish", publish.ID)
	require.Equal(t, "${{ inputs.version }}", publish.With["version"])
	require.Equal(t, "${{ secrets.GITHUB_TOKEN }}", publish.With["registry-token"])
}

// Everything the old local job did has to be gone rather than merely unused: a
// second build, login or upload path is a second thing that can publish, and
// the one that publishes without the action's guards is the one that gets used
// in a hurry.
func TestImagePreparationKeepsNoLocalPublishingPath(t *testing.T) {
	workflow := publishJob(t)
	for _, step := range workflow.Jobs["publish"].Steps {
		require.NotContains(t, step.Uses, "docker/",
			"building, logging in and pushing are the action's; %q is a local copy", step.Uses)
		require.NotContains(t, step.Uses, "actions/upload-artifact@",
			"the action uploads the lock artifact")
		require.Empty(t, step.Run, "the publish job runs no shell of its own")
	}

	body := workflowBody(t, "publish-gateway-image.yml")
	for _, dead := range []string{
		"STOPGAP", "stopgap", "core#770", "issues/770",
		"release-evidence/", "push-by-digest", "GITHUB_STEP_SUMMARY",
	} {
		require.NotContains(t, body, dead,
			"the stopgap publisher and its explanation are superseded, not retained")
	}
}

// What a coordinator is told to download has to be what the action uploads.
// The action names the artifact <lock-file>-<version> and defaults lock-file to
// gateway-image.json; overriding either input silently invalidates the runbook,
// so this asserts the caller leaves the defaults the docs describe.
func TestImagePreparationArtifactMatchesTheDocumentedRunbook(t *testing.T) {
	publish := publishJob(t).Jobs["publish"].Steps[1]

	if lock, overridden := publish.With["lock-file"]; overridden {
		require.Equal(t, "gateway-image.json", lock,
			"the lock file names the artifact and the committed file the release reads")
	}
	if name, overridden := publish.With["image-name"]; overridden {
		require.Equal(t, "service-object-storage", name,
			"release.yml and gateway-image.json both require ghcr.io/codefly-dev/service-object-storage")
	}

	for _, documentation := range []string{"README.md", "AGENTS.md"} {
		require.Contains(t, readRepoFile(t, documentation), "gateway-image.json-<version>",
			"%s must name the artifact core's action actually uploads", documentation)
	}
}
