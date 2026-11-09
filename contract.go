package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	"github.com/codefly-dev/core/standards"
	"github.com/codefly-dev/core/wool"
)

// storageContract is the gateway's gRPC API, the codefly/storage/v0 proto the
// gateway binary is built from. It is published into every object-storage
// service so dependents can generate a client.
//
//go:embed proto/codefly/storage/v0/storage.proto
var storageContract []byte

// writeDependencyContract publishes the API this agent serves at the service's
// dependency contract path (standards.DependencyProtoPath). Core's
// Service.LoadEndpoints advertises that file as the gRPC endpoint's API, and a
// dependent's `codefly sync` generates its client from it. The gateway's API
// lives in this agent, not in the service directory, so without the file the
// endpoint is advertised with no proto and dependents get an empty client.
//
// It rewrites the file only when its content differs, so a sync after an agent
// upgrade refreshes the contract and an unchanged one leaves it untouched.
func writeDependencyContract(serviceDir string) (bool, error) {
	if serviceDir == "" {
		return false, fmt.Errorf("service directory is empty")
	}
	path := filepath.Join(serviceDir, filepath.FromSlash(standards.DependencyProtoPath))
	current, err := os.ReadFile(path)
	if err == nil && bytes.Equal(current, storageContract) {
		return false, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("cannot read dependency contract %s: %w", path, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, fmt.Errorf("cannot create dependency contract directory: %w", err)
	}
	if err := os.WriteFile(path, storageContract, 0o644); err != nil {
		return false, fmt.Errorf("cannot write dependency contract %s: %w", path, err)
	}
	return true, nil
}

// publishContract writes the contract into this service's directory.
func (s *Builder) publishContract() error {
	written, err := writeDependencyContract(s.Location)
	if err != nil {
		return err
	}
	if written {
		s.Wool.Info("published storage API contract", wool.Field("path", standards.DependencyProtoPath))
	}
	return nil
}
