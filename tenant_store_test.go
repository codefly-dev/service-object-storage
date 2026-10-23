package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/standards"
)

// Only a tcp endpoint named "minio" is the tenant store; the gRPC endpoint and
// any other tcp endpoint are not.
func TestTenantStoreEndpointIsTheDeclaredMinioEndpoint(t *testing.T) {
	ctx := context.Background()
	grpc := &basev0.Endpoint{Name: "grpc", Api: standards.GRPC}
	other := &basev0.Endpoint{Name: "metrics", Api: standards.TCP}
	store := &basev0.Endpoint{Name: tenantStoreEndpointName, Api: standards.TCP}
	if got := resolveTenantStoreEndpoint(ctx, []*basev0.Endpoint{grpc, other}); got != nil {
		t.Fatalf("no minio endpoint declared, got %v", got)
	}
	if got := resolveTenantStoreEndpoint(ctx, []*basev0.Endpoint{grpc, other, store}); got != store {
		t.Fatalf("got %v, want the declared minio endpoint", got)
	}
}

func connectionValues(t *testing.T, svc *Service, instance *basev0.NetworkInstance) map[string]*basev0.ConfigurationValue {
	t.Helper()
	conf, err := svc.CreateConnectionConfiguration(context.Background(), nil, instance)
	if err != nil {
		t.Fatalf("CreateConnectionConfiguration: %v", err)
	}
	values := map[string]*basev0.ConfigurationValue{}
	for _, info := range conf.Infos {
		for _, value := range info.ConfigurationValues {
			values[value.Key] = value
		}
	}
	return values
}

// A dependent of a tenant-store service is handed the store as it reaches it:
// the address for its own access kind, and the key with its secret half
// marked secret. A service that did not opt in hands over nothing of it.
func TestConnectionConfigurationCarriesTheTenantStore(t *testing.T) {
	ctx := context.Background()
	svc := NewService()
	identity := &basev0.ServiceIdentity{Workspace: "workspace", Module: "module", Name: "tenant", Version: "1.2.3", WorkspacePath: t.TempDir(), RelativeToWorkspace: "."}
	if err := svc.Base.HeadlessLoad(ctx, identity); err != nil {
		t.Fatalf("HeadlessLoad: %v", err)
	}
	native := &basev0.NetworkInstance{Address: "localhost:9464", Access: resources.NewNativeNetworkAccess()}
	container := &basev0.NetworkInstance{Address: "host.docker.internal:9464", Access: resources.NewContainerNetworkAccess()}

	for _, key := range []string{"minio_endpoint", "minio_access_key", "minio_secret_key"} {
		if _, ok := connectionValues(t, svc, container)[key]; ok {
			t.Fatalf("%s exported by a service with no minio endpoint", key)
		}
	}

	storeNative := resources.NewNetworkInstance("localhost", 45420)
	storeNative.Access = resources.NewNativeNetworkAccess()
	storeContainer := resources.NewNetworkInstance("host.docker.internal", 45420)
	storeContainer.Access = resources.NewContainerNetworkAccess()
	svc.tenantStore = &tenantStore{
		mapping:   &basev0.NetworkMapping{Instances: []*basev0.NetworkInstance{storeNative, storeContainer}},
		accessKey: localMinioUser, secret: "per-run-secret",
	}
	for instance, want := range map[*basev0.NetworkInstance]string{
		native:    "http://localhost:45420",
		container: "http://host.docker.internal:45420",
	} {
		values := connectionValues(t, svc, instance)
		if got := values["minio_endpoint"].GetValue(); got != want {
			t.Errorf("minio_endpoint for %s = %q, want %q", instance.GetAccess().GetKind(), got, want)
		}
		if values["minio_access_key"].GetValue() != localMinioUser {
			t.Errorf("minio_access_key = %q", values["minio_access_key"].GetValue())
		}
		if secret := values["minio_secret_key"]; secret.GetValue() != "per-run-secret" || !secret.GetSecret() {
			t.Errorf("minio_secret_key = %+v, want the per-run secret marked secret", secret)
		}
	}
}

// A deployment starts no MinIO, so a service that declares the tenant-store
// endpoint is refused before anything is rendered.
func TestDeployRefusesATenantStoreEndpoint(t *testing.T) {
	ctx := context.Background()
	builder := newDeployBuilder(t, ctx)
	builder.TenantStoreEndpoint = &basev0.Endpoint{Name: tenantStoreEndpointName, Api: standards.TCP}
	destination := t.TempDir()

	resp, err := builder.Deploy(ctx, deployRequest(destination, map[string]string{"SOS_BACKEND": "s3", "SOS_BUCKET": "prod-docs"}))
	if err != nil {
		t.Fatalf("Deploy returned transport error: %v", err)
	}
	if got := resp.GetState().GetState(); got != builderv0.DeploymentStatus_ERROR {
		t.Fatalf("deploy state = %v, want ERROR", got)
	}
	if msg := resp.GetState().GetMessage(); !strings.Contains(msg, `endpoint "minio" serves the MinIO a local run starts`) {
		t.Errorf("error message = %q", msg)
	}
	if _, err := os.Stat(filepath.Join(destination, "base", "deployment.yaml")); err == nil {
		t.Errorf("refused deploy still wrote a manifest")
	}
}
