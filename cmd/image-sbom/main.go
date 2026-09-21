// Command image-sbom writes CycloneDX evidence for the images this service
// ships, through the same shared scanner Builder.SBOM uses.
//
// The release pipeline runs it so published evidence comes from the agent's own
// code path. Generating it with a separate scanner invocation produced
// documents that were not bound to the image digest and that drifted from the
// contract the agent implements.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/codefly-dev/service-object-storage/internal/imageevidence"
)

func main() {
	image := flag.String("image", "", "image reference to inventory")
	out := flag.String("out", "sbom", "directory to write CycloneDX documents into")
	local := flag.Bool("local", false, "inventory an image held by the local Docker daemon rather than a registry image")
	service := flag.String("service", "codefly.dev/object-storage", "service identity recorded on each subject")
	flag.Parse()

	if *image == "" {
		fmt.Fprintln(os.Stderr, "image-sbom: -image is required")
		os.Exit(2)
	}

	ctx := context.Background()
	subjects, err := imageevidence.Subjects(ctx, *service, *image, *local)
	if err != nil {
		fmt.Fprintf(os.Stderr, "image-sbom: %v\n", err)
		os.Exit(1)
	}
	documents, err := imageevidence.Collect(ctx, *out, subjects)
	if err != nil {
		fmt.Fprintf(os.Stderr, "image-sbom: %v\n", err)
		os.Exit(1)
	}
	if err := imageevidence.WriteIndex(*out, documents); err != nil {
		fmt.Fprintf(os.Stderr, "image-sbom: %v\n", err)
		os.Exit(1)
	}
	for _, document := range documents {
		fmt.Printf("%s %s %s\n", document.Platform, document.Digest, document.Path)
	}
}
