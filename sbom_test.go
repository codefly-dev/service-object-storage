package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codefly-dev/core/agents/services/sbom"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	agentv0 "github.com/codefly-dev/core/generated/go/codefly/services/agent/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"gopkg.in/yaml.v3"
)

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
	if len(subjects) != len(gatewayPlatforms) {
		t.Fatalf("got %d subjects for %d shipped platforms", len(subjects), len(gatewayPlatforms))
	}
	covered := map[string]bool{}
	for _, subject := range subjects {
		if got := subject.GetReference(); got != gatewayImage.FullName() {
			t.Errorf("subject reference = %q, want the published gateway %q", got, gatewayImage.FullName())
		}
		if got := subject.GetRole(); got != gatewayImageRole {
			t.Errorf("subject role = %q, want %q", got, gatewayImageRole)
		}
		if got := subject.GetService(); got != "module/object-storage" {
			t.Errorf("subject service = %q, want the loaded service identity", got)
		}
		covered[subject.GetPlatform()] = true
	}
	for _, platform := range gatewayPlatforms {
		if !covered[platform] {
			t.Errorf("no subject covers shipped platform %s", platform)
		}
	}
}

func TestImageEvidenceSatisfiesTheCoverageContract(t *testing.T) {
	// Several platforms are what make the omitted-platform case meaningful, so
	// pin to the published image rather than whatever the environment names.
	t.Setenv(gatewayImageOverrideEnv, "")
	builder := newSBOMBuilder(t)
	subjects, _ := builder.imageSubjects()

	complete := imageResponse(evidenceFor(subjects))
	if err := sbom.ValidateCoverage(subjects, complete); err != nil {
		t.Fatalf("evidence for every subject must validate as coverage: %v", err)
	}

	partial := imageResponse(evidenceFor(subjects[:1]))
	if err := sbom.ValidateCoverage(subjects, partial); err == nil {
		t.Error("evidence omitting a shipped platform must not validate as coverage")
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
	data, err := os.ReadFile(filepath.Join(".github", "workflows", "release-image.yml"))
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				With struct {
					Platforms string `yaml:"platforms"`
				} `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse release workflow: %v", err)
	}

	var published []string
	for _, job := range workflow.Jobs {
		for _, step := range job.Steps {
			if step.With.Platforms == "" {
				continue
			}
			for _, platform := range strings.Split(step.With.Platforms, ",") {
				published = append(published, strings.TrimSpace(platform))
			}
		}
	}
	if len(published) == 0 {
		t.Fatal("the release workflow publishes no platforms")
	}

	if strings.Join(published, ",") != strings.Join(gatewayPlatforms, ",") {
		t.Errorf("the pipeline publishes %v but evidence covers %v; a platform shipped without a subject ships uninventoried", published, gatewayPlatforms)
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
