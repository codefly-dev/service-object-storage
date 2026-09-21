package main

import (
	"os"
	"testing"
)

func TestParseGatewayImageLock(t *testing.T) {
	image, err := parseGatewayImageLock([]byte(`{
		"name": "ghcr.io/codefly-dev/service-object-storage",
		"digest": "sha256:1205da0743e56b68c12c754438476e8fdd0f05cdb08ce1c9687ddb8d8f0e1669"
	}`))
	if err != nil {
		t.Fatalf("parseGatewayImageLock: %v", err)
	}
	const want = "ghcr.io/codefly-dev/service-object-storage@sha256:1205da0743e56b68c12c754438476e8fdd0f05cdb08ce1c9687ddb8d8f0e1669"
	if got := image.FullName(); got != want {
		t.Errorf("FullName() = %q, want %q", got, want)
	}
}

// A lock the agent accepts but that names no immutable image is the failure
// pinning exists to prevent: it produces subjects whose evidence describes
// whatever the registry served, so every malformed shape is refused at load
// rather than carried into a scan.
func TestParseGatewayImageLockRejectsAnythingButASHA256Digest(t *testing.T) {
	for name, lock := range map[string]string{
		"no name":         `{"digest": "sha256:1205da0743e56b68c12c754438476e8fdd0f05cdb08ce1c9687ddb8d8f0e1669"}`,
		"no digest":       `{"name": "ghcr.io/codefly-dev/service-object-storage"}`,
		"tag as digest":   `{"name": "ghcr.io/codefly-dev/service-object-storage", "digest": "0.0.3"}`,
		"other algorithm": `{"name": "ghcr.io/codefly-dev/service-object-storage", "digest": "sha512:1205da07"}`,
		"truncated":       `{"name": "ghcr.io/codefly-dev/service-object-storage", "digest": "sha256:1205da07"}`,
		"not hex":         `{"name": "ghcr.io/codefly-dev/service-object-storage", "digest": "sha256:zzzzda0743e56b68c12c754438476e8fdd0f05cdb08ce1c9687ddb8d8f0e1669"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseGatewayImageLock([]byte(lock)); err == nil {
				t.Error("accepted a lock that pins no image")
			}
		})
	}
}

// The embedded copy is what the agent runs; the file on disk is what the
// release reads to tag and to inventory. They are the same bytes, and a test
// reading the file keeps the embed from silently going stale.
func TestGatewayImageMatchesTheLockOnDisk(t *testing.T) {
	lock, err := os.ReadFile("gateway-image.json")
	if err != nil {
		t.Fatalf("read gateway-image.json: %v", err)
	}
	expected, err := parseGatewayImageLock(lock)
	if err != nil {
		t.Fatalf("parseGatewayImageLock: %v", err)
	}
	if got, want := gatewayImage.FullName(), expected.FullName(); got != want {
		t.Errorf("gateway image = %q, want the recorded %q", got, want)
	}
}
