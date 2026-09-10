package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codefly-dev/core/agents/services"
	agenttesting "github.com/codefly-dev/core/agents/testing"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/shared"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"gopkg.in/yaml.v3"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/auth"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
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

// TestLoadConfigurationNormalizesGCSCredentialsFile guards the same whitespace
// trap on the key-file path. A path delivered through a YAML block scalar or an
// env file arrives with a trailing newline: untrimmed it reaches the gateway as
// a path that cannot open, and the Builder interpolates a raw newline into its
// rejection message. A whitespace-only value is not a configured path at all and
// must read as unset rather than as a path naming nothing.
func TestLoadConfigurationNormalizesGCSCredentialsFile(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  string
	}{
		{"trailing-newline", "/var/secrets/gcs/key.json\n", "/var/secrets/gcs/key.json"},
		{"whitespace-only-reads-as-unset", "   ", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := NewService()
			conf := &basev0.Configuration{Infos: []*basev0.ConfigurationInformation{{
				Name: "object-storage",
				ConfigurationValues: []*basev0.ConfigurationValue{
					{Key: "SOS_GCS_CREDENTIALS_FILE", Value: tc.value},
				},
			}}}
			if err := svc.LoadConfiguration(context.Background(), conf); err != nil {
				t.Fatalf("LoadConfiguration: %v", err)
			}
			if svc.conf.gcsCredentialsFile != tc.want {
				t.Fatalf("gcsCredentialsFile = %q, want %q", svc.conf.gcsCredentialsFile, tc.want)
			}
		})
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
	storagev0.RegisterObjectStorageServer(srv, server.New(be, hub, probetest.Monitor(t, be)))
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
			// not have. Deploy rejects a configured path instead. Matched on the
			// bare name rather than "name: ..." so that explaining the absence in
			// a rendered YAML comment fails here too: that prose shipped into
			// every s3/azure manifest, which is why it is a template comment now.
			if strings.Contains(body, "SOS_GCS_CREDENTIALS_FILE") {
				t.Errorf("manifest mentions an unmounted GCS key file:\n%s", body)
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
			requireProbeSemantics(t, body)
		})
	}
}

// requireProbeSemantics pins what the rendered probes attest. A TCP probe
// passes as soon as the gRPC port is bound, which says nothing about the store
// behind it, so STARTUP names the ObjectStorage service and gates rollout on
// real backend access. Readiness and liveness stay on the overall service:
// readiness that follows the backend probe is a global kill switch, since every
// replica shares one bucket and one credential set and would drain together,
// emptying the Service's endpoints over an outage the gateway could partly ride
// out.
func requireProbeSemantics(t *testing.T, body string) {
	t.Helper()
	if strings.Contains(body, "tcpSocket") {
		t.Errorf("probes must not settle for a bound socket:\n%s", body)
	}

	var deployment struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						StartupProbe   map[string]any `yaml:"startupProbe"`
						ReadinessProbe map[string]any `yaml:"readinessProbe"`
						LivenessProbe  map[string]any `yaml:"livenessProbe"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(body), &deployment); err != nil {
		t.Fatalf("parse deployment: %v", err)
	}
	containers := deployment.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("want one container, got %d", len(containers))
	}
	c := containers[0]

	startup, ok := c.StartupProbe["grpc"].(map[string]any)
	if !ok {
		t.Fatalf("startupProbe is not a grpc probe: %v", c.StartupProbe)
	}
	if startup["service"] != storagev0.ObjectStorage_ServiceDesc.ServiceName {
		t.Errorf("startupProbe must check %q, got %v",
			storagev0.ObjectStorage_ServiceDesc.ServiceName, startup["service"])
	}

	// Naming the backend-gated service here would drain every replica at once on
	// a shared-backend outage, so both must stay on the overall service.
	for name, probe := range map[string]map[string]any{
		"readinessProbe": c.ReadinessProbe,
		"livenessProbe":  c.LivenessProbe,
	} {
		grpc, isGRPC := probe["grpc"].(map[string]any)
		if !isGRPC {
			t.Errorf("%s is not a grpc probe: %v", name, probe)
			continue
		}
		if _, named := grpc["service"]; named {
			t.Errorf("%s must check the overall service, got %v", name, grpc["service"])
		}
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
			// would name a file nothing mounts, so none is rendered — and the
			// bare name is matched so that explaining the absence in a rendered
			// YAML comment fails here too.
			unwanted: []string{"SOS_GCS_CREDENTIALS_FILE"},
		},
		{
			// The guard is gcs-only, so this configuration deploys. It is the
			// one case that exercises the rendering path with a key file
			// actually resolved: the manifest still must not carry it.
			name: "non-gcs-backend-ignores-key-file",
			config: map[string]string{
				"SOS_BACKEND":              "s3",
				"SOS_BUCKET":               "prod-docs",
				"SOS_GCS_CREDENTIALS_FILE": "/var/secrets/gcs/key.json",
			},
			want:     []string{"name: SOS_BACKEND", `value: "s3"`},
			unwanted: []string{"SOS_GCS_CREDENTIALS_FILE", "/var/secrets/gcs/key.json"},
		},
		{
			// A whitespace-only path is not a configured path: trimmed at the
			// LoadConfiguration boundary, it reads as unset and deploys keyless
			// rather than being rejected for naming nothing.
			name: "gcs-whitespace-only-key-file-is-unset",
			config: map[string]string{
				"SOS_BACKEND":              "gcs",
				"SOS_BUCKET":               "prod-docs",
				"SOS_GCS_CREDENTIALS_FILE": "   ",
			},
			want:     []string{"name: SOS_BACKEND", `value: "gcs"`},
			unwanted: []string{"SOS_GCS_CREDENTIALS_FILE"},
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
		// wantMsgAlso is a second required fragment, for messages that have to
		// stay actionable on more than one kind of cluster.
		wantMsgAlso string
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
			// Workload Identity is GKE-only. On EKS/AKS/on-prem there is no
			// metadata server, so a message naming only WI prescribes a remedy
			// the operator cannot perform; it must also name the mount route.
			wantMsgAlso: "mount the key from an overlay of your own",
		},
		{
			// The same path carrying the trailing newline a YAML block scalar or
			// an env file adds. It is still rejected, and the message must not
			// interpolate the raw newline mid-sentence.
			name: "gcs-with-key-file-trailing-newline",
			config: map[string]string{
				"SOS_BACKEND":              "gcs",
				"SOS_BUCKET":               "prod-docs",
				"SOS_GCS_CREDENTIALS_FILE": "/var/secrets/gcs/key.json\n",
			},
			wantMsg: "nothing here mounts /var/secrets/gcs/key.json into the pod",
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
			if tc.wantMsgAlso != "" {
				if msg := resp.GetState().GetMessage(); !strings.Contains(msg, tc.wantMsgAlso) {
					t.Errorf("error message = %q, want it to contain %q", msg, tc.wantMsgAlso)
				}
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

// newProjectionRuntime returns a Runtime configured for a local gcs run whose
// credential projection lands under a throwaway CODEFLY_HOME.
func newProjectionRuntime(t *testing.T, serviceDir, credentialsFile string) *Runtime {
	t.Helper()
	t.Setenv(resources.CodeflyHomeEnv, t.TempDir())
	rt := NewRuntime()
	identity := &basev0.ServiceIdentity{
		Workspace: "workspace", Module: "module", Name: "object-storage", Version: "1.2.3",
		WorkspacePath: serviceDir, RelativeToWorkspace: ".",
	}
	if err := rt.Base.HeadlessLoad(context.Background(), identity); err != nil {
		t.Fatalf("HeadlessLoad: %v", err)
	}
	rt.Base.Environment = shared.Must(resources.LocalEnvironment().Proto())
	rt.conf.backend = "gcs"
	rt.conf.gcsCredentialsFile = credentialsFile
	return rt
}

func TestProjectGCSCredentials(t *testing.T) {
	key := []byte(`{"type":"service_account","private_key":"projection-sentinel"}`)
	serviceDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(serviceDir, "gcs.json"), key, 0o600))

	rt := newProjectionRuntime(t, serviceDir, "gcs.json")
	require.NoError(t, rt.projectGCSCredentials())

	projected := rt.gcsCredentialsHostFile
	require.NotEmpty(t, projected)
	require.NotEqual(t, filepath.Join(serviceDir, "gcs.json"), projected,
		"the operator's file must not be handed to the container directly")

	got, err := os.ReadFile(projected)
	require.NoError(t, err)
	require.Equal(t, key, got)

	info, err := os.Stat(projected)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o444), info.Mode().Perm(),
		"the projected key must carry no write bit for the container user")

	dir, err := os.Stat(filepath.Dir(projected))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dir.Mode().Perm(),
		"the projection directory must not be reachable by other host users")

	rt.discardProjectedCredentials()
	_, err = os.Stat(projected)
	require.True(t, os.IsNotExist(err), "Destroy must remove the projected key")
}

// TestProjectGCSCredentialsRelativeToServiceIgnoresCwd pins the resolution base:
// the agent process runs from wherever the CLI started it, so a relative
// configuration value must not follow it.
func TestProjectGCSCredentialsRelativeToServiceIgnoresCwd(t *testing.T) {
	serviceDir := t.TempDir()
	key := []byte(`{"type":"service_account"}`)
	require.NoError(t, os.WriteFile(filepath.Join(serviceDir, "gcs.json"), key, 0o600))

	// A decoy of the same relative name next to the process working directory.
	decoy := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(decoy, "gcs.json"), []byte(`{"decoy":true}`), 0o600))
	t.Chdir(decoy)

	rt := newProjectionRuntime(t, serviceDir, "gcs.json")
	require.NoError(t, rt.projectGCSCredentials())

	got, err := os.ReadFile(rt.gcsCredentialsHostFile)
	require.NoError(t, err)
	require.Equal(t, key, got)
}

func TestProjectGCSCredentialsRejects(t *testing.T) {
	serviceDir := t.TempDir()

	unreadable := filepath.Join(serviceDir, "unreadable.json")
	require.NoError(t, os.WriteFile(unreadable, []byte(`{}`), 0o600))
	require.NoError(t, os.Chmod(unreadable, 0o000))

	malformed := filepath.Join(serviceDir, "malformed.json")
	require.NoError(t, os.WriteFile(malformed, []byte(`{"type":"service_ac`), 0o600))

	for _, tc := range []struct {
		name      string
		file      string
		wantMsg   string
		modeGated bool
	}{
		{name: "no credentials configured", wantMsg: "Workload Identity are deployment-only"},
		{name: "missing file", file: filepath.Join(serviceDir, "absent.json"), wantMsg: "absent.json"},
		{name: "directory", file: serviceDir, wantMsg: "not a regular file"},
		{name: "unreadable file", file: unreadable, wantMsg: "unreadable.json", modeGated: true},
		{name: "malformed json", file: malformed, wantMsg: "not valid JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.modeGated && os.Geteuid() == 0 {
				t.Skip("root reads any file regardless of mode")
			}
			rt := newProjectionRuntime(t, serviceDir, tc.file)
			err := rt.projectGCSCredentials()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantMsg)
			require.Empty(t, rt.gcsCredentialsHostFile,
				"a rejected projection must leave nothing for the gateway to mount")
		})
	}
}

// TestProjectGCSCredentialsSkipsOtherBackends keeps the projection out of the
// MinIO path, where SOS_GCS_CREDENTIALS_FILE is meaningless.
func TestProjectGCSCredentialsSkipsOtherBackends(t *testing.T) {
	rt := newProjectionRuntime(t, t.TempDir(), "")
	rt.conf.backend = "minio"
	require.NoError(t, rt.projectGCSCredentials())
	require.Empty(t, rt.gcsCredentialsHostFile)
}

// TestProjectGCSCredentialsRotationChangesMountSource guards the mount source
// against dockerrun's container reuse: it fingerprints mounts, so a projected
// path that stayed constant across a key rotation would let a running gateway
// keep serving the superseded key while the host copy showed the new one.
func TestProjectGCSCredentialsRotationChangesMountSource(t *testing.T) {
	serviceDir := t.TempDir()
	keyFile := filepath.Join(serviceDir, "gcs.json")

	require.NoError(t, os.WriteFile(keyFile, []byte(`{"key":"first"}`), 0o600))
	rt := newProjectionRuntime(t, serviceDir, keyFile)
	require.NoError(t, rt.projectGCSCredentials())
	first := rt.gcsCredentialsHostFile

	require.NoError(t, os.WriteFile(keyFile, []byte(`{"key":"second"}`), 0o600))
	require.NoError(t, rt.projectGCSCredentials())
	require.NotEqual(t, first, rt.gcsCredentialsHostFile,
		"a rotated key must change the mount source so the container is recreated")

	require.NoError(t, os.WriteFile(keyFile, []byte(`{"key":"first"}`), 0o600))
	require.NoError(t, rt.projectGCSCredentials())
	require.Equal(t, first, rt.gcsCredentialsHostFile,
		"an unchanged key must not churn the mount source and force a recreation")
}

// TestProjectGCSCredentialsUnderRestrictiveUmask pins the read bit the container
// depends on: os.WriteFile's mode is masked, and the gateway user is not the
// file's owner, so a masked other-read bit locks the gateway out of its own key.
func TestProjectGCSCredentialsUnderRestrictiveUmask(t *testing.T) {
	previous := syscall.Umask(0o077)
	t.Cleanup(func() { syscall.Umask(previous) })

	serviceDir := t.TempDir()
	keyFile := filepath.Join(serviceDir, "gcs.json")
	require.NoError(t, os.WriteFile(keyFile, []byte(`{"type":"service_account"}`), 0o600))

	rt := newProjectionRuntime(t, serviceDir, keyFile)
	require.NoError(t, rt.projectGCSCredentials())

	info, err := os.Stat(rt.gcsCredentialsHostFile)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o444), info.Mode().Perm())
}

// stubEnvironment stands in for a container environment whose teardown fails —
// the state a live Docker daemon cannot be asked to produce on demand.
type stubEnvironment struct {
	err       error
	shutdowns int
}

func (e *stubEnvironment) ContainerID() (string, error) { return "stub", nil }

func (e *stubEnvironment) Shutdown(context.Context) error {
	e.shutdowns++
	return e.err
}

// newProjectedRuntime returns a Runtime holding a real projected credential, so
// teardown assertions run against a file that actually exists on disk.
func newProjectedRuntime(t *testing.T) *Runtime {
	t.Helper()
	serviceDir := t.TempDir()
	keyFile := filepath.Join(serviceDir, "gcs.json")
	require.NoError(t, os.WriteFile(keyFile, []byte(`{"type":"service_account"}`), 0o600))
	rt := newProjectionRuntime(t, serviceDir, keyFile)
	require.NoError(t, rt.projectGCSCredentials())
	return rt
}

// TestTeardownDiscardsCredentialsWhenShutdownFails covers the path a broken
// Docker daemon takes: the projected key must not outlive the run just because
// the containers could not be stopped.
func TestTeardownDiscardsCredentialsWhenShutdownFails(t *testing.T) {
	rt := newProjectedRuntime(t)
	projected := rt.gcsCredentialsHostFile

	gateway := &stubEnvironment{err: errors.New("daemon unreachable")}
	minio := &stubEnvironment{err: errors.New("daemon unreachable")}
	rt.gatewayEnv = gateway
	rt.minioEnv = minio

	err := rt.teardown(context.Background())
	require.Error(t, err)

	require.Equal(t, 1, minio.shutdowns,
		"a gateway that cannot be shut down must not strand the minio container")
	_, statErr := os.Stat(projected)
	require.True(t, os.IsNotExist(statErr), "the projected key must not survive a failed teardown")
	require.Empty(t, rt.gcsCredentialsHostFile)

	// A failed shutdown keeps its reference so the next attempt retries it.
	require.NotNil(t, rt.gatewayEnv)
	require.NotNil(t, rt.minioEnv)
	gateway.err, minio.err = nil, nil
	require.NoError(t, rt.teardown(context.Background()))
	require.Equal(t, 2, gateway.shutdowns)
	require.Nil(t, rt.gatewayEnv)
	require.Nil(t, rt.minioEnv)
}

// TestDestroyReportsTeardownFailure keeps the daemon error visible to the caller
// rather than swallowed by the cleanup that now always runs.
func TestDestroyReportsTeardownFailure(t *testing.T) {
	rt := newProjectedRuntime(t)
	projected := rt.gcsCredentialsHostFile
	rt.gatewayEnv = &stubEnvironment{err: errors.New("daemon unreachable")}

	resp, err := rt.Destroy(context.Background(), &runtimev0.DestroyRequest{})
	require.NoError(t, err, "Destroy reports failure in its response, not as a transport error")
	require.Equal(t, runtimev0.DestroyStatus_ERROR, resp.GetStatus().GetState())
	require.Contains(t, resp.GetStatus().GetMessage(), "daemon unreachable")

	_, statErr := os.Stat(projected)
	require.True(t, os.IsNotExist(statErr))
}

// TestRollbackInitReleasesWhatInitOwns is the guard for a startup that fails
// between containers: whatever Init already started has to go back down.
func TestRollbackInitReleasesWhatInitOwns(t *testing.T) {
	rt := newProjectedRuntime(t)
	projected := rt.gcsCredentialsHostFile
	minio := &stubEnvironment{}
	rt.minioEnv = minio

	rt.rollbackInit(context.Background())

	require.Equal(t, 1, minio.shutdowns)
	require.Nil(t, rt.minioEnv)
	_, statErr := os.Stat(projected)
	require.True(t, os.IsNotExist(statErr))
}
