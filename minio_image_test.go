package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestParseMinioImageLock(t *testing.T) {
	image, err := parseMinioImageLock([]byte(`{
		"name": "ghcr.io/codefly-dev/minio",
		"digest": "sha256:6db9ae5fd307001ad5bb1899a8a4b8f69aebe982f95b7085416953b5f4cc22a5",
		"release": "RELEASE.2025-04-22T22-12-26Z"
	}`))
	if err != nil {
		t.Fatalf("parseMinioImageLock: %v", err)
	}
	const want = "ghcr.io/codefly-dev/minio@sha256:6db9ae5fd307001ad5bb1899a8a4b8f69aebe982f95b7085416953b5f4cc22a5"
	if got := image.FullName(); got != want {
		t.Errorf("FullName() = %q, want %q", got, want)
	}
}

// A MinIO pinned by tag runs whatever the registry serves under it, so every
// shape that names no immutable image is refused at load.
func TestParseMinioImageLockRejectsAnythingButASHA256Digest(t *testing.T) {
	for name, lock := range map[string]string{
		"no name":       `{"digest": "sha256:6db9ae5fd307001ad5bb1899a8a4b8f69aebe982f95b7085416953b5f4cc22a5", "release": "RELEASE.2025-04-22T22-12-26Z"}`,
		"no digest":     `{"name": "ghcr.io/codefly-dev/minio", "release": "RELEASE.2025-04-22T22-12-26Z"}`,
		"tag as digest": `{"name": "ghcr.io/codefly-dev/minio", "digest": "RELEASE.2025-04-22T22-12-26Z", "release": "RELEASE.2025-04-22T22-12-26Z"}`,
		"truncated":     `{"name": "ghcr.io/codefly-dev/minio", "digest": "sha256:6db9ae5f", "release": "RELEASE.2025-04-22T22-12-26Z"}`,
		"no release":    `{"name": "ghcr.io/codefly-dev/minio", "digest": "sha256:6db9ae5fd307001ad5bb1899a8a4b8f69aebe982f95b7085416953b5f4cc22a5"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseMinioImageLock([]byte(lock)); err == nil {
				t.Error("accepted a lock that pins no image")
			}
		})
	}
}

// minio-image.json is the one record of which MinIO this repository runs. The
// agent embeds it; CI, the docs and the build recipe restate it because a
// shell step or a README cannot read JSON. Every restatement has to agree, or
// CI tests one MinIO while the agent runs another.
func TestMinioImageIsTheOneTheLockRecords(t *testing.T) {
	content, err := os.ReadFile("minio-image.json")
	if err != nil {
		t.Fatalf("read minio-image.json: %v", err)
	}
	expected, err := parseMinioImageLock(content)
	if err != nil {
		t.Fatalf("parseMinioImageLock: %v", err)
	}
	pinned := expected.FullName()
	if got := minioImage.FullName(); got != pinned {
		t.Errorf("minio image = %q, want the recorded %q", got, pinned)
	}

	for _, file := range []string{
		".github/workflows/ci.yml",
		"AGENTS.md",
		"README.md",
		".claude/skills/local-test-suites/SKILL.md",
	} {
		body := readRepoFile(t, file)
		if !strings.Contains(body, pinned) {
			t.Errorf("%s does not name %s", file, pinned)
		}
		for _, closed := range []string{"quay.io/minio/minio@", "quay.io/minio/minio:", "minio/minio:RELEASE"} {
			if strings.Contains(body, closed) {
				t.Errorf("%s still pulls %s…, which no longer serves anonymous pulls", file, closed)
			}
		}
	}

	var lock minioImageLock
	if err := json.Unmarshal(content, &lock); err != nil {
		t.Fatalf("parse minio-image.json: %v", err)
	}
	if lock.Revision == "" {
		t.Fatal("minio-image.json records no upstream revision")
	}
	for _, file := range []string{"images/minio/Dockerfile", ".github/workflows/publish-minio-image.yml"} {
		body := readRepoFile(t, file)
		for _, pin := range []string{lock.Release, lock.Revision} {
			if !strings.Contains(body, pin) {
				t.Errorf("%s does not build %s from the recorded source", file, pin)
			}
		}
	}
}

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}
