//go:build e2e

// This file drives the agent's image-SBOM path against a real gateway image,
// inventorying it exactly as a release does and checking the result against the
// shared coverage contract. Run with:
//
//	SOS_GATEWAY_IMAGE=<locally-built gateway image> go test -tags e2e -run TestImageSBOM .
//
// It needs syft on PATH: an image the local daemon holds and no registry serves
// is only reachable by a scanner with Docker access, which the managed
// containerized scanner deliberately does not have.
//
// Writing the evidence out is the release command's job (cmd/image-sbom), not
// this test's.

package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/codefly-dev/core/agents/services/sbom"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
)

func TestImageSBOMInventoriesTheGatewayImage(t *testing.T) {
	if os.Getenv(gatewayImageOverrideEnv) == "" {
		t.Skipf("set %s to a locally built gateway image", gatewayImageOverrideEnv)
	}
	if _, err := exec.LookPath("syft"); err != nil {
		t.Skip("syft is required to inventory an image held only by the local daemon")
	}
	builder := newSBOMBuilder(t)
	subjects, err := builder.imageSubjects(context.Background())
	if err != nil {
		t.Fatalf("imageSubjects: %v", err)
	}

	// The daemon resolves the override to its own image ID, so that ID is what a
	// scan of it binds evidence to and what the subject has to name.
	if len(subjects) != 1 {
		t.Fatalf("got %d subjects, want the single platform the daemon holds", len(subjects))
	}
	if source := sbom.SourceOf(subjects[0]); source != sbom.SourceDockerDaemon {
		t.Errorf("override source = %v, want the Docker daemon", source)
	}
	if got := subjects[0].GetReference(); got != os.Getenv(gatewayImageOverrideEnv) {
		t.Errorf("subject reference = %q, want the override", got)
	}
	if got := subjects[0].GetPlatform(); got != "" {
		t.Errorf("subject platform = %q, want it read from the image rather than assumed", got)
	}
	if err := sbom.RequirePinned(subjects[0]); err != nil {
		t.Errorf("override subject is not pinned: %v", err)
	}

	resp, err := builder.SBOM(context.Background(), &builderv0.SBOMRequest{Scope: builderv0.SBOMScope_SBOM_SCOPE_IMAGE})
	if err != nil {
		t.Fatalf("SBOM: %v", err)
	}
	if state := resp.GetState().GetState(); state != builderv0.SBOMStatus_COMPLETE {
		t.Fatalf("state = %v: %s", state, resp.GetState().GetMessage())
	}
	if err := sbom.ValidateCoverage(testService, subjects, resp); err != nil {
		t.Fatalf("ValidateCoverage: %v", err)
	}

	if len(resp.GetImages()) != 1 {
		t.Fatalf("got %d inventories for one image", len(resp.GetImages()))
	}
	evidence := resp.GetImages()[0]

	// The subject named the local image ID before the scan ran; evidence that
	// resolved to any other identity describes an image nobody asked for.
	if got, want := evidence.GetDigest(), subjects[0].GetDigest(); got != want {
		t.Errorf("evidence is bound to %s, but the subject named %s", got, want)
	}

	// The digest and platform are read from the image, not echoed from the
	// request: evidence that names neither cannot be matched to what ships.
	if digest := evidence.GetDigest(); !strings.HasPrefix(digest, "sha256:") {
		t.Errorf("evidence digest = %q, want it bound to a sha256 digest", digest)
	}
	if evidence.GetPlatform() == "" {
		t.Error("evidence names no platform")
	}
	if evidence.GetSha256() == "" {
		t.Error("evidence carries no document checksum")
	}

	var goModules, osPackages int
	for _, component := range evidence.GetBom().GetComponents() {
		switch {
		case strings.HasPrefix(component.GetPurl(), "pkg:golang/"):
			goModules++
		case isOSPackage(component.GetPurl()):
			osPackages++
		}
	}
	if goModules == 0 {
		t.Error("inventory found no Go modules: the gateway's application dependencies are missing")
	}
	if osPackages == 0 {
		t.Error("inventory found no OS packages: an image inventory is not satisfied by application dependencies alone")
	}

	t.Logf("inventoried %s on %s (%d Go modules, %d OS packages)",
		evidence.GetDigest(), evidence.GetPlatform(), goModules, osPackages)
}

// osPackageTypes are the package-URL namespaces an OS inventory reports under.
// Most of what syft finds in an image carries no package URL at all, so counting
// everything that is not a Go module would let an image with no OS inventory
// pass as covered.
var osPackageTypes = []string{"pkg:deb/", "pkg:apk/", "pkg:rpm/"}

func isOSPackage(purl string) bool {
	for _, prefix := range osPackageTypes {
		if strings.HasPrefix(purl, prefix) {
			return true
		}
	}
	return false
}
