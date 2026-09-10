package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/codefly-dev/core/agents/services"
	agenttesting "github.com/codefly-dev/core/agents/testing"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"github.com/codefly-dev/core/resources"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/auth"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/server"
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

// TestLoadConfigurationNormalizesAuthToken guards the agent side of the
// whitespace trap: a configured token carrying a trailing newline would be
// handed to consumers in one form and enforced by the gateway in another.
func TestLoadConfigurationNormalizesAuthToken(t *testing.T) {
	svc := NewService()
	conf := &basev0.Configuration{Infos: []*basev0.ConfigurationInformation{{
		Name: "object-storage",
		ConfigurationValues: []*basev0.ConfigurationValue{
			{Key: "SOS_AUTH_TOKEN", Value: "pinned-secret\n"},
		},
	}}}
	if err := svc.LoadConfiguration(context.Background(), conf); err != nil {
		t.Fatalf("LoadConfiguration: %v", err)
	}
	if svc.conf.authToken != "pinned-secret" {
		t.Fatalf("authToken = %q, want it whitespace-normalized", svc.conf.authToken)
	}
}

// TestResolveGatewayToken covers both arms of the choice the Runtime makes: a
// pinned token is honored rather than silently overridden, and its absence
// yields a fresh per-run secret.
func TestResolveGatewayToken(t *testing.T) {
	rt := NewRuntime()
	rt.conf.authToken = "pinned-secret"
	token, err := rt.resolveGatewayToken()
	if err != nil {
		t.Fatalf("resolveGatewayToken: %v", err)
	}
	if token != "pinned-secret" {
		t.Errorf("token = %q, want the configured one", token)
	}

	rt.conf.authToken = ""
	first, err := rt.resolveGatewayToken()
	if err != nil {
		t.Fatalf("resolveGatewayToken: %v", err)
	}
	second, err := rt.resolveGatewayToken()
	if err != nil {
		t.Fatalf("resolveGatewayToken: %v", err)
	}
	if first == "" || first == second {
		t.Errorf("generated tokens must be non-empty and per-run: %q, %q", first, second)
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

// TestWaitForReadyReportsRejectedCredential is the regression guard for a
// readiness probe that swallowed its own error: a token mismatch used to retry
// for 30 seconds and then report "not ready", pointing the operator at the
// backend instead of at the credential.
func TestWaitForReadyReportsRejectedCredential(t *testing.T) {
	address := startAuthenticatedGateway(t, "server-token")
	rt := newProbeRuntime(t, address)

	rt.gatewayToken = "a-different-token"
	start := time.Now()
	err := rt.WaitForReady(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("WaitForReady must fail when the gateway rejects the agent's credential")
	}
	if !strings.Contains(err.Error(), "rejected the agent's credential") {
		t.Errorf("error = %v, want it to name the credential rejection", err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("took %v: a rejected credential must not be retried as if transient", elapsed)
	}

	rt.gatewayToken = "server-token"
	if err := rt.WaitForReady(context.Background()); err != nil {
		t.Fatalf("WaitForReady with the right token: %v", err)
	}
}

// startAuthenticatedGateway serves the real ObjectStorage implementation behind
// the auth interceptors and returns its address.
func startAuthenticatedGateway(t *testing.T, token string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	be, err := mem.New(context.Background(), backend.Config{})
	if err != nil {
		t.Fatalf("mem backend: %v", err)
	}
	hub := events.NewHub(be.Name(), be.Identity(), nil)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(token)),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(token)),
	)
	storagev0.RegisterObjectStorageServer(srv, server.New(be, hub))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); hub.Close(); _ = be.Close() })
	return lis.Addr().String()
}

// newProbeRuntime wires a Runtime with just enough network state for
// WaitForReady to resolve address.
func newProbeRuntime(t *testing.T, address string) *Runtime {
	t.Helper()
	rt := NewRuntime()
	endpoint := &basev0.Endpoint{Name: "grpc", Api: "grpc"}
	rt.GrpcEndpoint = endpoint
	rt.Runtime.WithContext(resources.NewRuntimeContextNative())
	rt.NetworkMappings = []*basev0.NetworkMapping{{
		Endpoint:  endpoint,
		Instances: []*basev0.NetworkInstance{{Address: address, Access: resources.NewNativeNetworkAccess()}},
	}}
	return rt
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

// TestConnectionConfigurationCarriesTokenAsSecret pins how a consumer receives
// the gateway credential: as its own secret-marked value, never folded into the
// connection string or endpoint that get logged and templated everywhere.
func TestConnectionConfigurationCarriesTokenAsSecret(t *testing.T) {
	ctx := context.Background()
	svc := NewService()
	identity := &basev0.ServiceIdentity{Workspace: "workspace", Module: "module", Name: "object-storage", Version: "1.2.3", WorkspacePath: t.TempDir(), RelativeToWorkspace: "."}
	if err := svc.Base.HeadlessLoad(ctx, identity); err != nil {
		t.Fatalf("HeadlessLoad: %v", err)
	}
	const token = "b6f1a0c9d8e7f6a5b4c3d2e1f0a9b8c7"
	svc.gatewayToken = token

	conf, err := svc.CreateConnectionConfiguration(ctx, nil, &basev0.NetworkInstance{Address: "localhost:9464", Access: resources.NewNativeNetworkAccess()})
	if err != nil {
		t.Fatalf("CreateConnectionConfiguration: %v", err)
	}

	values := map[string]*basev0.ConfigurationValue{}
	for _, info := range conf.Infos {
		for _, value := range info.ConfigurationValues {
			values[value.Key] = value
		}
	}
	tokenValue, ok := values["token"]
	if !ok {
		t.Fatalf("no token value in connection configuration: %+v", values)
	}
	if tokenValue.Value != token {
		t.Errorf("token = %q, want the generated one", tokenValue.Value)
	}
	if !tokenValue.Secret {
		t.Error("token must be marked secret so it is never logged or displayed")
	}
	for _, key := range []string{"connection", "endpoint"} {
		if strings.Contains(values[key].GetValue(), token) {
			t.Errorf("%s value leaks the token: %q", key, values[key].GetValue())
		}
	}
}

// TestConnectionConfigurationWithoutTokenOmitsIt covers the Builder, which
// shares the Service but never generates a token: it must not advertise an
// empty credential a consumer would then try to present.
func TestConnectionConfigurationWithoutTokenOmitsIt(t *testing.T) {
	ctx := context.Background()
	svc := NewService()
	identity := &basev0.ServiceIdentity{Workspace: "workspace", Module: "module", Name: "object-storage", Version: "1.2.3", WorkspacePath: t.TempDir(), RelativeToWorkspace: "."}
	if err := svc.Base.HeadlessLoad(ctx, identity); err != nil {
		t.Fatalf("HeadlessLoad: %v", err)
	}

	conf, err := svc.CreateConnectionConfiguration(ctx, nil, &basev0.NetworkInstance{Address: "localhost:9464", Access: resources.NewNativeNetworkAccess()})
	if err != nil {
		t.Fatalf("CreateConnectionConfiguration: %v", err)
	}
	for _, info := range conf.Infos {
		for _, value := range info.ConfigurationValues {
			if value.Key == "token" {
				t.Fatalf("unexpected token value %+v", value)
			}
		}
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
		name             string
		params           deploymentTemplateParameters
		wantAzureAccount string
	}{
		{"s3", deploymentTemplateParameters{Backend: "s3", Bucket: "documents", Region: "us-east-1"}, ""},
		// Azure with no account name emits no SOS_AZURE_ACCOUNT env.
		{"azure", deploymentTemplateParameters{Backend: "azure", Bucket: "documents", Region: "us-east-1"}, ""},
		// Azure with an account name emits the non-sensitive SOS_AZURE_ACCOUNT.
		{"azure-account", deploymentTemplateParameters{Backend: "azure", Bucket: "documents", Region: "us-east-1", AzureAccount: "acmestorage"}, "acmestorage"},
		// GCS authenticates via Workload Identity — no credentials-file env, and
		// crucially no required secret mount that would wedge the pod when the
		// operator runs keyless.
		{"gcs", deploymentTemplateParameters{Backend: "gcs", Bucket: "documents", Region: "us-east-1"}, ""},
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
			// No rendering names a key-file path: nothing in this manifest
			// mounts one, so an emitted path would point at a file the pod does
			// not have. Deploy rejects a configured path instead.
			if strings.Contains(body, "name: SOS_GCS_CREDENTIALS_FILE") {
				t.Errorf("manifest names an unmounted GCS key file:\n%s", body)
			}
			azureWired := strings.Contains(body, "SOS_AZURE_ACCOUNT")
			if azureWired != (tc.wantAzureAccount != "") {
				t.Errorf("SOS_AZURE_ACCOUNT present=%v, want %v:\n%s", azureWired, tc.wantAzureAccount != "", body)
			}
			if tc.wantAzureAccount != "" && !strings.Contains(body, `value: "`+tc.wantAzureAccount+`"`) {
				t.Errorf("SOS_AZURE_ACCOUNT not set to %q:\n%s", tc.wantAzureAccount, body)
			}
			// The removed mount must not linger: a required secret key would
			// block startup when no key is supplied.
			if strings.Contains(body, "gcs-credentials") {
				t.Errorf("unexpected gcs-credentials volume wiring:\n%s", body)
			}
			// The gateway refuses an unauthenticated listener unless the
			// manifest says so, so a deployment that stops emitting this opt-out
			// without also delivering SOS_AUTH_TOKEN would CrashLoop.
			if !strings.Contains(body, "name: SOS_ALLOW_ANONYMOUS") {
				t.Errorf("deployed profile must declare its unauthenticated listener:\n%s", body)
			}
			if strings.Contains(body, "name: SOS_AUTH_TOKEN") {
				t.Errorf("manifest must not carry the gateway credential:\n%s", body)
			}
		})
	}
}

// newDeployBuilder returns a Builder wired for a headless Deploy call.
func newDeployBuilder(t *testing.T, ctx context.Context) *Builder {
	t.Helper()
	builder := NewBuilder()
	identity := &basev0.ServiceIdentity{Workspace: "workspace", Module: "module", Name: "object-storage", Version: "1.2.3", WorkspacePath: t.TempDir(), RelativeToWorkspace: "."}
	if err := builder.Base.HeadlessLoad(ctx, identity); err != nil {
		t.Fatalf("HeadlessLoad: %v", err)
	}
	builder.Base.Information = &services.Information{Service: resources.ToServiceWithCase(resources.ServiceIdentityFromProto(identity))}
	builder.Base.EnvironmentVariables.SetIdentity(identity)
	builder.Base.SetDockerImage(resources.NewDockerImage("example/service:1.2.3"))
	return builder
}

// deployRequest builds a Kubernetes DeploymentRequest carrying the given
// object-storage configuration values, rendering into destination.
func deployRequest(destination string, values map[string]string) *builderv0.DeploymentRequest {
	var cfgValues []*basev0.ConfigurationValue
	for k, v := range values {
		cfgValues = append(cfgValues, &basev0.ConfigurationValue{Key: k, Value: v})
	}
	return &builderv0.DeploymentRequest{
		Environment: &basev0.Environment{Name: "test", Fixture: "dev-admin"},
		Deployment: &builderv0.Deployment{Kind: &builderv0.Deployment_Kubernetes{
			Kubernetes: &builderv0.KubernetesDeployment{
				Namespace:   "codefly",
				Destination: destination,
				Profile:     builderv0.KubernetesOutputProfile_KUBERNETES_OUTPUT_PROFILE_EPHEMERAL_LOCAL_APPLY_V1,
			},
		}},
		Configuration: &basev0.Configuration{Infos: []*basev0.ConfigurationInformation{{
			Name:                "object-storage",
			ConfigurationValues: cfgValues,
		}}},
	}
}

// TestDeployResolvesBackendFromConfiguration drives the real Builder.Deploy path
// and asserts the rendered manifest names the configured backend. This is the
// regression guard for the bug where Deploy never loaded the configuration,
// leaving SOS_BACKEND pinned to s3 regardless of the environment. Each backend
// exercises the full config-key -> resolved -> template wiring end to end, so a
// typo'd map key (e.g. SOS_AZURE_ACCOUNT) fails here rather than shipping green.
func TestDeployResolvesBackendFromConfiguration(t *testing.T) {
	cases := []struct {
		name     string
		config   map[string]string
		want     []string
		unwanted []string
	}{
		{
			name: "gcs-keyless",
			config: map[string]string{
				"SOS_BACKEND": "gcs",
				"SOS_BUCKET":  "prod-docs",
			},
			want: []string{
				"name: SOS_BACKEND", `value: "gcs"`, `value: "prod-docs"`,
			},
			// Deployed GCS authenticates via Workload Identity. A key-file path
			// would name a file nothing mounts, so none is rendered.
			unwanted: []string{"name: SOS_GCS_CREDENTIALS_FILE"},
		},
		{
			name: "azure-with-account",
			config: map[string]string{
				"SOS_BACKEND":       "azure",
				"SOS_BUCKET":        "prod-docs",
				"SOS_AZURE_ACCOUNT": "acmestorage",
			},
			want: []string{
				"name: SOS_BACKEND", `value: "azure"`, `value: "prod-docs"`,
				"name: SOS_AZURE_ACCOUNT", `value: "acmestorage"`,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			builder := newDeployBuilder(t, ctx)
			destination := t.TempDir()

			resp, err := builder.Deploy(ctx, deployRequest(destination, tc.config))
			if err != nil {
				t.Fatalf("Deploy: %v", err)
			}
			if got := resp.GetState().GetState(); got != builderv0.DeploymentStatus_SUCCESS {
				t.Fatalf("deploy state = %v (%s), want SUCCESS", got, resp.GetState().GetMessage())
			}

			manifest, err := os.ReadFile(filepath.Join(destination, "base", "deployment.yaml"))
			if err != nil {
				t.Fatalf("read base deployment: %v", err)
			}
			body := string(manifest)
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Errorf("rendered manifest missing %q:\n%s", want, body)
				}
			}
			for _, unwanted := range tc.unwanted {
				if strings.Contains(body, unwanted) {
					t.Errorf("rendered manifest carries %q:\n%s", unwanted, body)
				}
			}
		})
	}
}

// TestDeployRejectsBrokenBackendConfiguration guards the validation that keeps a
// misconfigured deploy from being reported as a success and then crashing the
// gateway at runtime: an unknown backend kind, azure without its account name,
// or a credential this deployment has no channel to deliver.
func TestDeployRejectsBrokenBackendConfiguration(t *testing.T) {
	cases := []struct {
		name    string
		config  map[string]string
		wantMsg string
	}{
		{
			name:    "unsupported-backend",
			config:  map[string]string{"SOS_BACKEND": "gcs2", "SOS_BUCKET": "prod-docs"},
			wantMsg: "unsupported deploy backend",
		},
		{
			name:    "azure-without-account",
			config:  map[string]string{"SOS_BACKEND": "azure", "SOS_BUCKET": "prod-docs"},
			wantMsg: "requires SOS_AZURE_ACCOUNT",
		},
		{
			// Rendering this would name a key path nothing mounts: the
			// configuration channel carries the path, never the service-account
			// JSON, so the gateway would look for a file the pod does not have.
			name: "gcs-with-key-file",
			config: map[string]string{
				"SOS_BACKEND":              "gcs",
				"SOS_BUCKET":               "prod-docs",
				"SOS_GCS_CREDENTIALS_FILE": "/var/secrets/gcs/key.json",
			},
			wantMsg: "SOS_GCS_CREDENTIALS_FILE cannot be delivered",
		},
		{
			// Rendering this would start a gateway enforcing a credential no
			// consumer holds: the manifest carries no secret values, and only the
			// Runtime ever emits a token to consumers.
			name:    "undeliverable-auth-token",
			config:  map[string]string{"SOS_BACKEND": "s3", "SOS_BUCKET": "prod-docs", "SOS_AUTH_TOKEN": "operator-set"},
			wantMsg: "SOS_AUTH_TOKEN cannot be delivered",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			builder := newDeployBuilder(t, ctx)
			destination := t.TempDir()

			resp, err := builder.Deploy(ctx, deployRequest(destination, tc.config))
			if err != nil {
				t.Fatalf("Deploy returned transport error: %v", err)
			}
			if got := resp.GetState().GetState(); got != builderv0.DeploymentStatus_ERROR {
				t.Fatalf("deploy state = %v, want ERROR", got)
			}
			if msg := resp.GetState().GetMessage(); !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("error message = %q, want it to contain %q", msg, tc.wantMsg)
			}
			// A rejected deploy must not leave a half-written manifest behind.
			if _, err := os.Stat(filepath.Join(destination, "base", "deployment.yaml")); err == nil {
				t.Errorf("rejected deploy still wrote a manifest at %s", destination)
			}
		})
	}
}

func TestGatewayImageTracksAgentVersion(t *testing.T) {
	if !strings.HasSuffix(gatewayImage.FullName(), agent.Version) {
		t.Fatalf("gateway image %q should be tagged with agent version %q", gatewayImage.FullName(), agent.Version)
	}
}
