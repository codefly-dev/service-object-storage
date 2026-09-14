package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/codefly-dev/core/agents/services/sbom"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	agentv0 "github.com/codefly-dev/core/generated/go/codefly/services/agent/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"gopkg.in/yaml.v3"

	"github.com/codefly-dev/service-object-storage/internal/imageevidence"
)

// testService is the identity newSBOMBuilder loads, and therefore the identity
// every subject it produces carries.
const testService = "module/object-storage"

// newSBOMBuilder returns a Builder wired for a headless SBOM call.
func newSBOMBuilder(t *testing.T) *Builder {
	t.Helper()
	builder := NewBuilder()
	identity := &basev0.ServiceIdentity{
		Workspace: "workspace", Module: "module", Name: "object-storage", Version: "1.2.3",
		WorkspacePath: t.TempDir(), RelativeToWorkspace: ".",
	}
	if err := builder.Base.HeadlessLoad(context.Background(), identity); err != nil {
		t.Fatalf("HeadlessLoad: %v", err)
	}
	return builder
}

func TestImageSubjectsCoverEveryPublishedPlatform(t *testing.T) {
	// The published image is what this asserts, so the override an e2e run
	// exports for the whole job must not decide the subjects instead.
	t.Setenv(gatewayImageOverrideEnv, "")
	builder := newSBOMBuilder(t)

	subjects, source := builder.imageSubjects()

	if source != sbom.SourceRegistry {
		t.Errorf("published image source = %v, want the registry", source)
	}
	if len(subjects) != len(imageevidence.Platforms) {
		t.Fatalf("got %d subjects for %d shipped platforms", len(subjects), len(imageevidence.Platforms))
	}
	covered := map[string]bool{}
	for _, subject := range subjects {
		if got := subject.GetReference(); got != gatewayImage.FullName() {
			t.Errorf("subject reference = %q, want the published gateway %q", got, gatewayImage.FullName())
		}
		if got := subject.GetRole(); got != imageevidence.Role {
			t.Errorf("subject role = %q, want %q", got, imageevidence.Role)
		}
		if got := subject.GetService(); got != testService {
			t.Errorf("subject service = %q, want the loaded service identity", got)
		}
		covered[subject.GetPlatform()] = true
	}
	for _, platform := range imageevidence.Platforms {
		if !covered[platform] {
			t.Errorf("no subject covers shipped platform %s", platform)
		}
	}
}

func TestImageEvidenceSatisfiesTheCoverageContract(t *testing.T) {
	t.Setenv(gatewayImageOverrideEnv, "")
	builder := newSBOMBuilder(t)
	subjects, _ := builder.imageSubjects()

	// The expectation is built from the platforms the release pipeline actually
	// publishes, never from imageSubjects: deriving both sides from the code
	// under test compares it against itself, and would validate however many
	// platforms it dropped.
	expected := expectedFromReleasePipeline(t, gatewayImage.FullName())

	complete := imageResponse(evidenceFor(subjects))
	if err := sbom.ValidateCoverage(expected, complete); err != nil {
		t.Fatalf("evidence for every published platform must validate as coverage: %v", err)
	}

	partial := imageResponse(evidenceFor(subjects[:1]))
	if err := sbom.ValidateCoverage(expected, partial); err == nil {
		t.Error("evidence omitting a published platform must not validate as coverage")
	}
}

func TestGatewayOverrideIsInventoriedThroughTheDaemon(t *testing.T) {
	const override = "service-object-storage:e2e"
	t.Setenv(gatewayImageOverrideEnv, override)
	builder := newSBOMBuilder(t)

	subjects, source := builder.imageSubjects()

	if source != sbom.SourceDockerDaemon {
		t.Errorf("override source = %v, want the Docker daemon", source)
	}
	if len(subjects) != 1 {
		t.Fatalf("got %d subjects, want the single platform the daemon holds", len(subjects))
	}
	if got := subjects[0].GetReference(); got != override {
		t.Errorf("subject reference = %q, want the override %q", got, override)
	}
	if got := subjects[0].GetPlatform(); got != "" {
		t.Errorf("subject platform = %q, want it read from the image rather than assumed", got)
	}
}

func TestImageScopeReportsFailureRatherThanCoverage(t *testing.T) {
	t.Setenv(gatewayImageOverrideEnv, "not a valid image reference")
	builder := newSBOMBuilder(t)
	subjects, _ := builder.imageSubjects()

	resp, err := builder.SBOM(context.Background(), &builderv0.SBOMRequest{Scope: builderv0.SBOMScope_SBOM_SCOPE_IMAGE})
	if err != nil {
		t.Fatalf("SBOM: %v", err)
	}

	if state := resp.GetState().GetState(); state != builderv0.SBOMStatus_ERROR {
		t.Errorf("state = %v, want ERROR for an unresolvable image", state)
	}
	if scope := resp.GetScope(); scope != builderv0.SBOMScope_SBOM_SCOPE_IMAGE {
		t.Errorf("scope = %v, want the failure attributed to the image request", scope)
	}
	if len(resp.GetImages()) != 0 {
		t.Errorf("a failed scan carried %d inventories", len(resp.GetImages()))
	}
	if err := sbom.ValidateCoverage(subjects, resp); err == nil {
		t.Error("a failed scan must not validate as coverage")
	}
}

func TestSourceScopeKeepsTheExistingInventory(t *testing.T) {
	t.Setenv(gatewayImageOverrideEnv, "")
	builder := newSBOMBuilder(t)
	subjects, _ := builder.imageSubjects()

	// A cancelled context stops the scan before it reaches the network, which is
	// enough to tell the two paths apart: the image path attributes even its
	// failures to SBOM_SCOPE_IMAGE, so an unspecified scope that does not come
	// back image-scoped was answered by the source inventory.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := builder.SBOM(ctx, &builderv0.SBOMRequest{})
	if err != nil {
		t.Fatalf("SBOM: %v", err)
	}

	if scope := resp.GetScope(); scope == builderv0.SBOMScope_SBOM_SCOPE_IMAGE {
		t.Error("an unspecified scope must not be answered with image evidence")
	}
	if err := sbom.ValidateCoverage(subjects, resp); err == nil {
		t.Error("a source inventory must not validate as image coverage")
	}
}

func TestShippedPlatformsMatchTheReleasePipeline(t *testing.T) {
	published := append([]string(nil), publishedPlatforms(t)...)
	declared := append([]string(nil), imageevidence.Platforms...)
	// Compare as sets: the pipeline and the code have to name the same
	// platforms, but neither owns the order the other writes them in.
	sort.Strings(published)
	sort.Strings(declared)

	if strings.Join(published, ",") != strings.Join(declared, ",") {
		t.Errorf("the pipeline publishes %v but evidence covers %v; a platform shipped without a subject ships uninventoried", published, declared)
	}
}

// The release once looped over a platform list written into the workflow and
// called the scanner itself, which is a second list to keep in sync and a
// second scanner to drift from the contract. Evidence comes from the release
// command, which reads the same platform list the agent does.
func TestReleaseEvidenceComesFromTheSharedCommand(t *testing.T) {
	body := releaseWorkflow(t)

	if !strings.Contains(body, "cmd/image-sbom") {
		t.Error("the release workflow does not generate evidence through cmd/image-sbom")
	}
	if strings.Contains(body, "for platform in") {
		t.Error("the release workflow loops over its own platform list; it must read imageevidence.Platforms through cmd/image-sbom")
	}
}

// expectedFromReleasePipeline builds the coverage expectation from the release
// workflow, independently of the code that produces the evidence.
func expectedFromReleasePipeline(t *testing.T, reference string) []*builderv0.ImageSubject {
	t.Helper()
	platforms := publishedPlatforms(t)
	expected := make([]*builderv0.ImageSubject, 0, len(platforms))
	for _, platform := range platforms {
		expected = append(expected, &builderv0.ImageSubject{
			Reference: reference,
			Platform:  platform,
			Role:      imageevidence.Role,
			Service:   testService,
		})
	}
	return expected
}

// publishedPlatforms reads the platforms the release actually builds, from the
// step that pushes the image rather than from any step that happens to carry a
// platform list.
func publishedPlatforms(t *testing.T) []string {
	t.Helper()
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string `yaml:"uses"`
				With struct {
					Platforms string `yaml:"platforms"`
				} `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(releaseWorkflow(t)), &workflow); err != nil {
		t.Fatalf("parse release workflow: %v", err)
	}
	var published []string
	for _, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if !strings.Contains(step.Uses, "docker/build-push-action") {
				continue
			}
			for _, platform := range strings.Split(step.With.Platforms, ",") {
				if platform = strings.TrimSpace(platform); platform != "" {
					published = append(published, platform)
				}
			}
		}
	}
	if len(published) == 0 {
		t.Fatal("the release workflow's build-push step publishes no platforms")
	}
	return published
}

func releaseWorkflow(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(".github", "workflows", "release-image.yml"))
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	return string(data)
}

// releaseStep is one step of the release workflow, kept in file order so a test
// can assert what happens before what.
type releaseStep struct {
	Name string `yaml:"name"`
	Uses string `yaml:"uses"`
	Run  string `yaml:"run"`
	With struct {
		Platforms string `yaml:"platforms"`
		Tags      string `yaml:"tags"`
		Outputs   string `yaml:"outputs"`
	} `yaml:"with"`
}

func releaseSteps(t *testing.T) []releaseStep {
	t.Helper()
	var workflow struct {
		Jobs map[string]struct {
			Steps []releaseStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(releaseWorkflow(t)), &workflow); err != nil {
		t.Fatalf("parse release workflow: %v", err)
	}
	var steps []releaseStep
	for _, job := range workflow.Jobs {
		steps = append(steps, job.Steps...)
	}
	if len(steps) == 0 {
		t.Fatal("the release workflow has no steps")
	}
	return steps
}

// The release used to push the tags consumers resolve and inventory the image
// afterwards. A failure anywhere in the evidence step then turned the run red
// with a pullable, uninventoried image already published — and a red run reads
// as "nothing shipped", so nobody goes looking for it. The build now pushes by
// digest, and the tags are created only once evidence exists.
func TestReleasePublishesTagsOnlyAfterEvidence(t *testing.T) {
	steps := releaseSteps(t)

	build, evidence, publish := -1, -1, -1
	for i, step := range steps {
		if strings.Contains(step.Uses, "docker/build-push-action") {
			build = i
		}
		if strings.Contains(step.Run, "cmd/image-sbom") {
			evidence = i
		}
		if strings.Contains(step.Run, "imagetools create") {
			publish = i
		}
	}

	if build < 0 {
		t.Fatal("the release workflow does not build the gateway image")
	}
	if evidence < 0 {
		t.Fatal("the release workflow does not generate image evidence")
	}
	if publish < 0 {
		t.Fatal("the release workflow never creates the tags consumers resolve")
	}
	if publish < evidence {
		t.Errorf("release tags are created at step %d, before evidence at step %d: a failed scan would leave a pullable image with no evidence",
			publish, evidence)
	}
	if tags := strings.TrimSpace(steps[build].With.Tags); tags != "" {
		t.Errorf("the build step publishes tags %q; it must push by digest so no tag resolves until evidence succeeds", tags)
	}
	if !strings.Contains(steps[build].With.Outputs, "push-by-digest=true") {
		t.Error("the build step does not push by digest, so its image is resolvable by tag before evidence exists")
	}
}

// The published image tag is derived from the git tag while the agent resolves
// its gateway from the embedded agent.codefly.yaml. Nothing reconciles them at
// run time, and a tag one release ahead of the file makes the agent pull the
// previous release's image — which exists, so the mismatch is silent. This runs
// the workflow's own guard against the real file rather than trusting that it
// is present.
func TestReleaseGuardRejectsTagThatDisagreesWithAgentVersion(t *testing.T) {
	var guard releaseStep
	for _, step := range releaseSteps(t) {
		if strings.Contains(step.Run, "agent.codefly.yaml") {
			guard = step
			break
		}
	}
	if guard.Run == "" {
		t.Fatal("no release step reconciles the pushed tag with agent.codefly.yaml")
	}

	run := func(version string) error {
		command := exec.Command("bash", "-c", guard.Run)
		command.Env = append(os.Environ(), "VERSION="+version)
		return command.Run()
	}

	if err := run(agent.Version); err != nil {
		t.Errorf("guard rejected %s, the version this repo actually carries: %v", agent.Version, err)
	}
	if err := run(agent.Version + "9"); err == nil {
		t.Error("guard accepted a tag that disagrees with agent.codefly.yaml; such a tag ships an agent that silently pulls the previous release's image")
	}
}

// imageResponse wraps evidence in the complete image-scope response an agent
// returns.
func imageResponse(images []*builderv0.ImageSBOM) *builderv0.SBOMResponse {
	return &builderv0.SBOMResponse{
		State:  &builderv0.SBOMStatus{State: builderv0.SBOMStatus_COMPLETE},
		Scope:  builderv0.SBOMScope_SBOM_SCOPE_IMAGE,
		Images: images,
	}
}

// evidenceFor builds one digest-bound inventory per subject, each with a
// distinct digest so coverage is matched per platform.
func evidenceFor(subjects []*builderv0.ImageSubject) []*builderv0.ImageSBOM {
	images := make([]*builderv0.ImageSBOM, 0, len(subjects))
	for i, subject := range subjects {
		images = append(images, &builderv0.ImageSBOM{
			Digest:   fmt.Sprintf("sha256:%064d", i),
			Platform: subject.GetPlatform(),
			Subjects: []*builderv0.ImageSubject{subject},
			Bom:      &agentv0.Bom{Components: []*agentv0.Component{{Name: "libc", Version: "2.36"}}},
			Tool:     "syft",
			Sha256:   fmt.Sprintf("%064d", i),
		})
	}
	return images
}
