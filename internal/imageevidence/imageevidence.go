// Package imageevidence holds the one description of the images this service
// ships: the platforms the release pipeline publishes, the subjects that
// describe them, and the CycloneDX documents inventorying them.
//
// The agent's Builder.SBOM, the release command, and the drift test all read it
// from here. A second copy of the platform list is how a platform gets added to
// the pipeline and silently shipped with no evidence, so there is only one.
package imageevidence

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/codefly-dev/core/agents/services/sbom"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
)

// Platforms are the platforms the release pipeline publishes the gateway image
// for, and therefore the platforms evidence has to cover: a multi-architecture
// image is not inventoried by scanning one of its children. It is asserted
// against .github/workflows/release.yml.
var Platforms = []string{"linux/amd64", "linux/arm64"}

// Role is the gateway's purpose within the service, carried on every subject.
const Role = "runtime"

// Subjects describes the images the service ships. A published image is
// multi-architecture and contributes one subject per shipped platform. A local
// image was built and never pushed, so the daemon holds exactly one platform of
// it and the subject names none: the platform is read back from the image
// rather than asserted by the caller.
func Subjects(service, reference string, local bool) ([]*builderv0.ImageSubject, sbom.ImageSource) {
	if local {
		return []*builderv0.ImageSubject{{
			Reference: reference,
			Role:      Role,
			Service:   service,
		}}, sbom.SourceDockerDaemon
	}
	subjects := make([]*builderv0.ImageSubject, 0, len(Platforms))
	for _, platform := range Platforms {
		subjects = append(subjects, &builderv0.ImageSubject{
			Reference: reference,
			Platform:  platform,
			Role:      Role,
			Service:   service,
		})
	}
	return subjects, sbom.SourceRegistry
}

// Document is one platform's inventory, written to disk and named by the digest
// it is bound to.
type Document struct {
	Reference string
	Digest    string
	Platform  string
	Path      string
	SHA256    string
}

// Collect inventories every subject through the shared scanner and writes one
// CycloneDX document each. The document carries the scanned digest in its own
// root component, so an exported artifact still names the image it describes
// rather than relying on the filename to say so.
//
// A single failed scan fails the whole call: publishing evidence for some
// platforms while silently omitting others is the false coverage claim this
// evidence exists to prevent.
func Collect(ctx context.Context, dir string, subjects []*builderv0.ImageSubject, source sbom.ImageSource) ([]Document, error) {
	if len(subjects) == 0 {
		return nil, fmt.Errorf("no image subjects to inventory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create evidence directory: %w", err)
	}
	documents := make([]Document, 0, len(subjects))
	for _, subject := range subjects {
		result, err := sbom.Image(ctx, sbom.ImageRequest{
			Reference: subject.GetReference(),
			Platform:  subject.GetPlatform(),
			Source:    source,
		})
		if err != nil {
			return nil, fmt.Errorf("inventory %s: %w", describe(subject), err)
		}
		encoded, err := sbom.MarshalCycloneDXJSON(result.Bom)
		if err != nil {
			return nil, fmt.Errorf("encode %s: %w", describe(subject), err)
		}
		path := filepath.Join(dir, documentName(result.Platform, result.Digest))
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			return nil, fmt.Errorf("write %s: %w", path, err)
		}
		documents = append(documents, Document{
			Reference: subject.GetReference(),
			Digest:    result.Digest,
			Platform:  result.Platform,
			Path:      path,
			SHA256:    result.SHA256,
		})
	}
	return documents, nil
}

// WriteIndex records which image and platform each document covers, so a
// release asset is readable without opening every document.
func WriteIndex(dir string, documents []Document) error {
	var index strings.Builder
	for _, document := range documents {
		fmt.Fprintf(&index, "%s %s@%s %s %s\n",
			document.Platform, referenceName(document.Reference), document.Digest,
			filepath.Base(document.Path), document.SHA256)
	}
	return os.WriteFile(filepath.Join(dir, "index.txt"), []byte(index.String()), 0o644)
}

func documentName(platform, digest string) string {
	return fmt.Sprintf("object-storage-%s-%s.cdx.json",
		strings.ReplaceAll(platform, "/", "-"),
		strings.TrimPrefix(digest, "sha256:"))
}

func describe(subject *builderv0.ImageSubject) string {
	if platform := subject.GetPlatform(); platform != "" {
		return subject.GetReference() + " on " + platform
	}
	return subject.GetReference()
}

// referenceName strips any tag or digest so the index names the repository and
// the digest actually scanned, rather than the tag that was asked for.
func referenceName(reference string) string {
	if base, _, found := strings.Cut(reference, "@"); found {
		reference = base
	}
	if slash := strings.LastIndex(reference, "/"); slash >= 0 {
		if colon := strings.LastIndex(reference[slash:], ":"); colon >= 0 {
			return reference[:slash+colon]
		}
		return reference
	}
	if colon := strings.LastIndex(reference, ":"); colon >= 0 {
		return reference[:colon]
	}
	return reference
}
