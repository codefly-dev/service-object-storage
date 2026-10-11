// Package imageevidence declares the images this service ships and their
// platforms. Core owns subject derivation, scanning and evidence indexing.
//
// The agent's Builder.SBOM, the release command, and the drift test all read it
// from here. A second copy of the platform list is how a platform gets added to
// the pipeline and silently shipped with no evidence, so there is only one.
package imageevidence

import (
	"context"

	"github.com/codefly-dev/core/agents/services/sbom"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
)

// Platforms are the platforms the release pipeline publishes the gateway image
// for, and therefore the platforms evidence has to cover: a multi-architecture
// image is not inventoried by scanning one of its children. It is asserted
// against the platforms publish-gateway-image.yml passes to core's publish action.
var Platforms = []string{"linux/amd64", "linux/arm64"}

// Role is the gateway's purpose within the service, carried on every subject.
const Role = "runtime"

// Subjects supplies this service's image declaration to core's shared evidence
// path. A local image omits Platforms: core reads its platform from the daemon.
func Subjects(ctx context.Context, service, reference string, local bool) ([]*builderv0.ImageSubject, error) {
	image := sbom.PublishedImage{Reference: reference, Service: service, Role: Role}
	if !local {
		image.Platforms = Platforms
	}
	return sbom.ImageSubjects(ctx, image, local)
}
