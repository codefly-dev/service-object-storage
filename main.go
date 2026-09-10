// Command object-storage is the codefly service agent that runs the
// object-storage gateway as a real service: a container speaking the
// codefly/storage/v0 gRPC contract, backed by MinIO locally and a cloud object
// store (S3/GCS/Azure) when deployed. Consumers depend on it as a gRPC service
// and never link a cloud SDK — the gateway absorbs every backend difference.
//
// The agent mirrors the shape of the other codefly infrastructure agents
// (redis, postgres): a Service advertises its configuration surface, a Runtime
// brings the containers up for `codefly run` and tests, and a Builder emits the
// Kubernetes deployment. Locally the Runtime also starts a MinIO container and
// points the gateway at it — "test on MinIO, ship on S3", decided by config.
package main

import (
	"context"
	"embed"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/codefly-dev/core/agents"
	"github.com/codefly-dev/core/agents/services"
	"github.com/codefly-dev/core/builders"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	agentv0 "github.com/codefly-dev/core/generated/go/codefly/services/agent/v0"
	"github.com/codefly-dev/core/resources"
	runnersbase "github.com/codefly-dev/core/runners/base"
	"github.com/codefly-dev/core/shared"
	"github.com/codefly-dev/core/standards"
	"github.com/codefly-dev/core/templates"
)

// agent is loaded from the embedded agent.codefly.yaml (name + version).
var agent = shared.Must(resources.LoadFromFs[resources.Agent](shared.Embed(infoFS)))

var requirements = builders.NewDependencies(agent.Name,
	builders.NewDependency("service.codefly.yaml"),
)

// gatewayImage is the object-storage gateway container the agent runs. It is
// built and published by this repo's release pipeline; the tag tracks the agent
// version. SOS_GATEWAY_IMAGE overrides it (tests use a locally-built image).
var gatewayImage = &resources.DockerImage{
	Repository: "ghcr.io/codefly-dev",
	Name:       "service-object-storage",
	Tag:        agent.Version,
}

// minioImage backs the gateway for local and test runs. Deployed environments
// name a cloud backend instead and never start MinIO.
var minioImage = &resources.DockerImage{
	Name: "minio/minio",
	Tag:  "RELEASE.2025-04-22T22-12-26Z",
}

// Settings are the service.codefly.yaml knobs. Local runs need none — the
// defaults stand up a MinIO-backed gateway. Deployed runs name a cloud backend
// and its bucket; credentials arrive as runtime configuration, never here.
type Settings struct {
	// Backend selects the object store: empty means the local MinIO default.
	// A deployed environment sets "s3", "gcs", or "azure".
	Backend string `yaml:"backend"`
	// Bucket is the container objects live in. Defaults to "documents".
	Bucket string `yaml:"bucket"`
	// Region is the backend region (S3/GCS). Defaults to us-east-1.
	Region string `yaml:"region"`
}

// resolved holds the effective configuration after defaults and runtime
// configuration are applied.
type resolved struct {
	backend            string
	bucket             string
	region             string
	endpoint           string
	accessKey          string
	secretKey          string
	gcsCredentialsFile string
	azureAccount       string
}

type Service struct {
	*services.Base
	*Settings

	conf resolved

	// gatewayToken is the bearer every caller must present to the gateway. The
	// Runtime generates one per run and hands it to consumers through the
	// configuration channel as a secret value — never inside the connection
	// string. It lives outside resolved because LoadConfiguration rebuilds
	// resolved from the incoming configuration on every call.
	gatewayToken string

	GrpcEndpoint *basev0.Endpoint
}

func NewService() *Service {
	return &Service{
		Base:     services.NewServiceBase(context.Background(), agent.Of(resources.ServiceAgent)),
		Settings: &Settings{},
	}
}

func (s *Service) GetAgentInformation(ctx context.Context, _ *agentv0.AgentInformationRequest) (*agentv0.AgentInformation, error) {
	readme, err := templates.ApplyTemplateFrom(ctx, shared.Embed(readmeFS), "templates/agent/README.md", s.Information)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return services.Advertisement{
		Backends: runnersbase.BackendSupport{Docker: true},
		ReadMe:   readme,
		Config: []*agentv0.ConfigurationValueDetail{
			{
				Name: "object-storage", Description: "object-storage gateway endpoint",
				Fields: []*agentv0.ConfigurationValueInformation{
					{Name: "connection", Description: "grpc connection string"},
					{Name: "endpoint", Description: "host:port of the gRPC endpoint"},
					{Name: "token", Description: "bearer sent as the x-codefly-token grpc metadata header"},
				},
			},
		},
	}.Build(), nil
}

// resolveServingGRPCEndpoint selects the single gRPC endpoint the agent binds
// its runtime and deployment to.
func resolveServingGRPCEndpoint(ctx context.Context, endpoints []*basev0.Endpoint) (*basev0.Endpoint, error) {
	grpc := resources.FindEndpointsByAPI(ctx, standards.GRPC, endpoints)
	if len(grpc) == 0 {
		return nil, fmt.Errorf("no grpc endpoint found")
	}
	return grpc[0], nil
}

// LoadConfiguration resolves the effective backend configuration. Settings are
// the local defaults; runtime configuration (deployed credentials and endpoint)
// takes precedence.
func (s *Service) LoadConfiguration(ctx context.Context, conf *basev0.Configuration) error {
	r := resolved{
		backend: s.Backend,
		bucket:  s.Bucket,
		region:  s.Region,
	}
	if r.bucket == "" {
		r.bucket = "documents"
	}
	if r.region == "" {
		r.region = "us-east-1"
	}
	if conf != nil {
		for key, dst := range map[string]*string{
			"SOS_BACKEND":              &r.backend,
			"SOS_BUCKET":               &r.bucket,
			"SOS_REGION":               &r.region,
			"SOS_ENDPOINT":             &r.endpoint,
			"SOS_ACCESS_KEY":           &r.accessKey,
			"SOS_SECRET_KEY":           &r.secretKey,
			"SOS_GCS_CREDENTIALS_FILE": &r.gcsCredentialsFile,
			"SOS_AZURE_ACCOUNT":        &r.azureAccount,
		} {
			v, err := resources.GetConfigurationValue(ctx, conf, "object-storage", key)
			if err == nil && v != "" {
				*dst = v
			}
		}
	}
	s.conf = r
	return nil
}

func (s *Service) createConnectionString(address string) string {
	return fmt.Sprintf("grpc://%s", address)
}

func (s *Service) CreateConnectionConfiguration(ctx context.Context, conf *basev0.Configuration, instance *basev0.NetworkInstance) (*basev0.Configuration, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	if err := s.LoadConfiguration(ctx, conf); err != nil {
		return nil, s.Wool.Wrapf(err, "cannot load configuration")
	}
	values := []*basev0.ConfigurationValue{
		{Key: "connection", Value: s.createConnectionString(instance.Address)},
		{Key: "endpoint", Value: instance.Address},
	}
	// The token travels as its own secret value: putting it in the connection
	// string would leak it into every log line and manifest that carries an
	// endpoint.
	if s.gatewayToken != "" {
		values = append(values, &basev0.ConfigurationValue{Key: "token", Value: s.gatewayToken, Secret: true})
	}
	return &basev0.Configuration{
		Origin:         s.Base.Unique(),
		RuntimeContext: resources.RuntimeContextFromInstance(instance),
		Infos: []*basev0.ConfigurationInformation{
			{Name: "object-storage", ConfigurationValues: values},
		},
	}, nil
}

func main() {
	svc := NewService()
	agents.Serve(agents.PluginRegistration{
		Agent:   svc,
		Runtime: NewRuntime(),
		Builder: NewBuilder(),
	})
}

//go:embed agent.codefly.yaml
var infoFS embed.FS

//go:embed templates/agent
var readmeFS embed.FS
