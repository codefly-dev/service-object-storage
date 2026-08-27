package main

import (
	"context"
	"strings"
	"testing"

	agenttesting "github.com/codefly-dev/core/agents/testing"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
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
	agenttesting.AssertKustomizeTemplates(t, deploymentFS, &deploymentTemplateParameters{
		Bucket: "documents",
		Region: "us-east-1",
	})
}

func TestGatewayImageTracksAgentVersion(t *testing.T) {
	if !strings.HasSuffix(gatewayImage.FullName(), agent.Version) {
		t.Fatalf("gateway image %q should be tagged with agent version %q", gatewayImage.FullName(), agent.Version)
	}
}
