package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/shared"
	"github.com/codefly-dev/core/standards"
	"github.com/stretchr/testify/require"
)

func TestWriteDependencyContractPublishesTheStorageAPI(t *testing.T) {
	dir := t.TempDir()
	written, err := writeDependencyContract(dir)
	require.NoError(t, err)
	require.True(t, written)

	path := filepath.Join(dir, standards.DependencyProtoPath)
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	source, err := os.ReadFile(filepath.Join("proto", "codefly", "storage", "v0", "storage.proto"))
	require.NoError(t, err)
	require.Equal(t, source, content, "the published contract is the proto the gateway is built from")

	// Core advertises this file as the endpoint's API: it must parse, with
	// the package and every RPC a dependent's client needs.
	api, err := resources.LoadGrpcAPI(context.Background(), shared.Pointer(path))
	require.NoError(t, err)
	require.Equal(t, "codefly.storage.v0", api.GetPackage())
	require.Equal(t, content, api.GetProto())
	rpcs := map[string]bool{}
	for _, rpc := range api.GetRpcs() {
		require.Equal(t, "ObjectStorage", rpc.GetServiceName())
		rpcs[rpc.GetName()] = true
	}
	for _, name := range []string{"Stat", "Get", "Put", "Delete", "List", "Presign", "Watch", "Capabilities", "Ready"} {
		require.True(t, rpcs[name], "contract is missing rpc %s", name)
	}
}

func TestWriteDependencyContractLeavesACurrentContractUntouched(t *testing.T) {
	dir := t.TempDir()
	_, err := writeDependencyContract(dir)
	require.NoError(t, err)
	path := filepath.Join(dir, standards.DependencyProtoPath)
	past := time.Now().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(path, past, past))

	written, err := writeDependencyContract(dir)
	require.NoError(t, err)
	require.False(t, written)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, info.ModTime().Equal(past), "an unchanged contract is not rewritten")
}

func TestWriteDependencyContractRefreshesAStaleContract(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, standards.DependencyProtoPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(`syntax = "proto3"; package codefly.storage.v0;`), 0o644))

	written, err := writeDependencyContract(dir)
	require.NoError(t, err)
	require.True(t, written)
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, storageContract, content)
}

func TestWriteDependencyContractRefusesAnEmptyServiceDirectory(t *testing.T) {
	_, err := writeDependencyContract("")
	require.Error(t, err)
}
