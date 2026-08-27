package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codefly-dev/core/agents/services"
	agenttesting "github.com/codefly-dev/core/agents/testing"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"github.com/codefly-dev/core/resources"
	"gopkg.in/yaml.v3"
)

func TestNewService_EmbedsBase(t *testing.T) {
	svc := NewService()
	if svc == nil || svc.Base == nil {
		t.Fatal("services.Base embedding broken")
	}
	if svc.Settings == nil {
		t.Fatal("Service.Settings is nil")
	}
}

func TestLoadConfigurationDefaults(t *testing.T) {
	svc := NewService()
	if err := svc.LoadConfiguration(context.Background(), nil); err != nil {
		t.Fatalf("LoadConfiguration: %v", err)
	}
	if svc.conf.bucket != "documents" {
		t.Errorf("bucket = %q, want documents", svc.conf.bucket)
	}
	if svc.conf.region != "us-east-1" {
		t.Errorf("region = %q, want us-east-1", svc.conf.region)
	}
	if svc.conf.backend != "" {
		t.Errorf("backend = %q, want empty (local minio default)", svc.conf.backend)
	}
}

func TestLoadConfigurationRuntimeOverrides(t *testing.T) {
	svc := NewService()
	conf := &basev0.Configuration{Infos: []*basev0.ConfigurationInformation{{
		Name: "object-storage",
		ConfigurationValues: []*basev0.ConfigurationValue{
			{Key: "SOS_BACKEND", Value: "s3"},
			{Key: "SOS_BUCKET", Value: "prod-docs"},
			{Key: "SOS_REGION", Value: "eu-west-1"},
			{Key: "SOS_ACCESS_KEY", Value: "AKIA"},
			{Key: "SOS_SECRET_KEY", Value: "shhh"},
		},
	}}}
	if err := svc.LoadConfiguration(context.Background(), conf); err != nil {
		t.Fatalf("LoadConfiguration: %v", err)
	}
	if svc.conf.backend != "s3" || svc.conf.bucket != "prod-docs" || svc.conf.region != "eu-west-1" {
		t.Fatalf("runtime overrides not applied: %+v", svc.conf)
	}
	if svc.conf.accessKey != "AKIA" || svc.conf.secretKey != "shhh" {
		t.Fatalf("credentials not applied: %+v", svc.conf)
	}
}

func TestRunsLocalMinIO(t *testing.T) {
	rt := NewRuntime()
	rt.conf.backend = ""
	if !rt.runsLocalMinIO() {
		t.Error("empty backend should run local minio")
	}
	rt.conf.backend = "minio"
	if !rt.runsLocalMinIO() {
		t.Error("minio backend should run local minio")
	}
	rt.conf.backend = "s3"
	if rt.runsLocalMinIO() {
		t.Error("s3 backend must not run local minio")
	}
}

func TestResolveServingGRPCEndpoint(t *testing.T) {
	endpoints := []*basev0.Endpoint{{Name: "grpc", Api: "grpc"}}
	endpoint, err := resolveServingGRPCEndpoint(context.Background(), endpoints)
	if err != nil {
		t.Fatalf("resolveServingGRPCEndpoint: %v", err)
	}
	if endpoint.Name != "grpc" {
		t.Fatalf("endpoint = %q, want grpc", endpoint.Name)
	}
	if _, err := resolveServingGRPCEndpoint(context.Background(), nil); err == nil {
		t.Fatal("expected error when no grpc endpoint is present")
	}
}

func TestConnectionString(t *testing.T) {
	svc := NewService()
	if got := svc.createConnectionString("localhost:9464"); got != "grpc://localhost:9464" {
		t.Fatalf("connection string = %q", got)
	}
}

func TestSettings_YAMLRoundTrip(t *testing.T) {
	var s Settings
	if err := yaml.Unmarshal([]byte("backend: s3\nbucket: docs\nregion: us-east-2\n"), &s); err != nil {
		t.Fatalf("yaml unmarshal: %v", err)
	}
	if s.Backend != "s3" || s.Bucket != "docs" || s.Region != "us-east-2" {
		t.Fatalf("settings = %+v", s)
	}
}

func TestDeploymentTemplates(t *testing.T) {
	cases := []struct {
		name        string
		params      deploymentTemplateParameters
		wantGCSFile bool
	}{
		{"s3", deploymentTemplateParameters{Backend: "s3", Bucket: "documents", Region: "us-east-1"}, false},
		{"azure", deploymentTemplateParameters{Backend: "azure", Bucket: "documents", Region: "us-east-1"}, false},
		// GCS with no key file authenticates via Workload Identity — no
		// credentials-file env, and crucially no required secret mount that
		// would wedge the pod when the operator runs keyless.
		{"gcs-adc", deploymentTemplateParameters{Backend: "gcs", Bucket: "documents", Region: "us-east-1"}, false},
		// GCS with an explicit key-file path emits SOS_GCS_CREDENTIALS_FILE.
		{"gcs-file", deploymentTemplateParameters{Backend: "gcs", Bucket: "documents", Region: "us-east-1", GCSCredentialsFile: "/var/secrets/gcs/key.json"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := tc.params
			dir := agenttesting.AssertKustomizeTemplates(t, deploymentFS, &params)
			manifest, err := os.ReadFile(filepath.Join(dir, "base", "deployment.yaml"))
			if err != nil {
				t.Fatalf("read base deployment: %v", err)
			}
			body := string(manifest)
			if !strings.Contains(body, `name: SOS_BACKEND`) || !strings.Contains(body, `value: "`+tc.params.Backend+`"`) {
				t.Errorf("SOS_BACKEND not set to %q:\n%s", tc.params.Backend, body)
			}
			if gcsWired := strings.Contains(body, "SOS_GCS_CREDENTIALS_FILE"); gcsWired != tc.wantGCSFile {
				t.Errorf("SOS_GCS_CREDENTIALS_FILE present=%v, want %v:\n%s", gcsWired, tc.wantGCSFile, body)
			}
			// The removed mount must not linger: a required secret key would
			// block startup when no key is supplied.
			if strings.Contains(body, "gcs-credentials") {
				t.Errorf("unexpected gcs-credentials volume wiring:\n%s", body)
			}
		})
	}
}

// TestDeployResolvesBackendFromConfiguration drives the real Builder.Deploy path
// with a gcs configuration and asserts the rendered manifest names it. This is
// the regression guard for the bug where Deploy never loaded the configuration,
// leaving SOS_BACKEND pinned to s3 regardless of the environment.
func TestDeployResolvesBackendFromConfiguration(t *testing.T) {
	ctx := context.Background()
	builder := NewBuilder()
	identity := &basev0.ServiceIdentity{Workspace: "workspace", Module: "module", Name: "object-storage", Version: "1.2.3", WorkspacePath: t.TempDir(), RelativeToWorkspace: "."}
	if err := builder.Base.HeadlessLoad(ctx, identity); err != nil {
		t.Fatalf("HeadlessLoad: %v", err)
	}
	builder.Base.Information = &services.Information{Service: resources.ToServiceWithCase(resources.ServiceIdentityFromProto(identity))}
	builder.Base.EnvironmentVariables.SetIdentity(identity)
	builder.Base.SetDockerImage(resources.NewDockerImage("example/service:1.2.3"))

	destination := t.TempDir()
	req := &builderv0.DeploymentRequest{
		Environment: &basev0.Environment{Name: "test", Fixture: "dev-admin"},
		Deployment: &builderv0.Deployment{Kind: &builderv0.Deployment_Kubernetes{
			Kubernetes: &builderv0.KubernetesDeployment{
				Namespace:   "codefly",
				Destination: destination,
				Profile:     builderv0.KubernetesOutputProfile_KUBERNETES_OUTPUT_PROFILE_EPHEMERAL_LOCAL_APPLY_V1,
			},
		}},
		Configuration: &basev0.Configuration{Infos: []*basev0.ConfigurationInformation{{
			Name: "object-storage",
			ConfigurationValues: []*basev0.ConfigurationValue{
				{Key: "SOS_BACKEND", Value: "gcs"},
				{Key: "SOS_BUCKET", Value: "prod-docs"},
				{Key: "SOS_GCS_CREDENTIALS_FILE", Value: "/var/secrets/gcs/key.json"},
			},
		}}},
	}

	resp, err := builder.Deploy(ctx, req)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if got := resp.GetState().GetState(); got != builderv0.DeploymentStatus_SUCCESS {
		t.Fatalf("deploy state = %v, want SUCCESS", got)
	}

	manifest, err := os.ReadFile(filepath.Join(destination, "base", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read base deployment: %v", err)
	}
	body := string(manifest)
	for _, want := range []string{
		"name: SOS_BACKEND",
		`value: "gcs"`,
		`value: "prod-docs"`,
		"name: SOS_GCS_CREDENTIALS_FILE",
		`value: "/var/secrets/gcs/key.json"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("rendered manifest missing %q:\n%s", want, body)
		}
	}
}

func TestGatewayImageTracksAgentVersion(t *testing.T) {
	if !strings.HasSuffix(gatewayImage.FullName(), agent.Version) {
		t.Fatalf("gateway image %q should be tagged with agent version %q", gatewayImage.FullName(), agent.Version)
	}
}
