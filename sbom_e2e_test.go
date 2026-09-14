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
// Setting SOS_SBOM_OUTPUT writes the CycloneDX document to that path, which is
// how CI keeps the evidence retrievable alongside the image it describes.

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

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
	subjects, _ := builder.imageSubjects()

	resp, err := builder.SBOM(context.Background(), &builderv0.SBOMRequest{Scope: builderv0.SBOMScope_SBOM_SCOPE_IMAGE})
	if err != nil {
		t.Fatalf("SBOM: %v", err)
	}
	if state := resp.GetState().GetState(); state != builderv0.SBOMStatus_COMPLETE {
		t.Fatalf("state = %v: %s", state, resp.GetState().GetMessage())
	}
	if err := sbom.ValidateCoverage(subjects, resp); err != nil {
		t.Fatalf("ValidateCoverage: %v", err)
	}

	if len(resp.GetImages()) != 1 {
		t.Fatalf("got %d inventories for one image", len(resp.GetImages()))
	}
	evidence := resp.GetImages()[0]

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

	if output := os.Getenv("SOS_SBOM_OUTPUT"); output != "" {
		writeEvidence(t, output, evidence)
	}
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

// writeEvidence exports the CycloneDX document so the scan a release gates on
// is also the artifact it publishes.
func writeEvidence(t *testing.T, path string, evidence *builderv0.ImageSBOM) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create evidence directory: %v", err)
	}
	document, err := protojson.MarshalOptions{Multiline: true}.Marshal(evidence.GetBom())
	if err != nil {
		t.Fatalf("marshal inventory: %v", err)
	}
	if err := os.WriteFile(path, document, 0o644); err != nil {
		t.Fatalf("write inventory: %v", err)
	}
	t.Logf("wrote %s for %s on %s (sha256 %s)", path, evidence.GetDigest(), evidence.GetPlatform(), evidence.GetSha256())
}
