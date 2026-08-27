package main

import (
	"context"
	"embed"

	"github.com/codefly-dev/core/agents/communicate"
	"github.com/codefly-dev/core/agents/services"
	v0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	builderv0 "github.com/codefly-dev/core/generated/go/codefly/services/builder/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/standards"
	"github.com/codefly-dev/core/wool"
)

type Builder struct {
	*services.DefaultBuilder
	*Service
}

// deploymentTemplateParameters carries the values the Kubernetes templates need
// beyond the resolved configuration.
type deploymentTemplateParameters struct {
	Backend string
	Bucket  string
	Region  string
	// GCSCredentialsFile is the path to a GCS service-account key file. Empty
	// means Application Default Credentials / Workload Identity.
	GCSCredentialsFile string
}

func NewBuilder() *Builder {
	service := NewService()
	return &Builder{
		DefaultBuilder: services.NewDefaultBuilder(service.Builder),
		Service:        service,
	}
}

func (s *Builder) Load(ctx context.Context, req *builderv0.LoadRequest) (*builderv0.LoadResponse, error) {
	defer s.Wool.Catch()
	return s.Builder.LoadService(ctx, req, services.BuilderLoad{
		Settings:         s.Settings,
		Requirements:     requirements,
		FactoryTemplates: factoryFS,
		ResolveEndpoints: func(ctx context.Context, endpoints []*v0.Endpoint) error {
			endpoint, err := resolveServingGRPCEndpoint(ctx, endpoints)
			if err != nil {
				return err
			}
			s.GrpcEndpoint = endpoint
			return nil
		},
	})
}

func (s *Builder) Audit(ctx context.Context, req *builderv0.AuditRequest) (*builderv0.AuditResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	return s.Builder.AuditContainer(ctx, req, gatewayImage.FullName())
}

func (s *Builder) SBOM(ctx context.Context, _ *builderv0.SBOMRequest) (*builderv0.SBOMResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	return s.Builder.SBOMContainer(ctx, gatewayImage.FullName())
}

func (s *Builder) Deploy(ctx context.Context, req *builderv0.DeploymentRequest) (*builderv0.DeploymentResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	s.Base.SetDockerImage(gatewayImage)

	// Resolve the effective backend and credentials from the deployment
	// configuration; without this the template only ever sees the zero-value
	// config and can never name gcs/azure.
	if err := s.LoadConfiguration(ctx, req.GetConfiguration()); err != nil {
		return s.Builder.DeployError(err)
	}

	parameters := &deploymentTemplateParameters{
		Backend:            s.conf.backend,
		Bucket:             s.conf.bucket,
		Region:             s.conf.region,
		GCSCredentialsFile: s.conf.gcsCredentialsFile,
	}
	// Local runs default to MinIO; a deployment that names no backend targets
	// S3. An explicit backend (including minio-against-an-endpoint) is honored.
	if parameters.Backend == "" {
		parameters.Backend = "s3"
	}
	if parameters.Bucket == "" {
		parameters.Bucket = "documents"
	}
	if parameters.Region == "" {
		parameters.Region = "us-east-1"
	}
	return s.Builder.DeployKustomize(ctx, req, services.KustomizeDeployment{
		EnvironmentVariables: s.EnvironmentVariables,
		Templates:            deploymentFS,
		Parameters:           parameters,
	})
}

func (s *Builder) Create(ctx context.Context, req *builderv0.CreateRequest) (*builderv0.CreateResponse, error) {
	defer s.Wool.Catch()
	if err := s.Templates(ctx, s.Information, services.WithFactory(factoryFS)); err != nil {
		return s.Builder.CreateError(err)
	}
	if err := s.CreateEndpoints(ctx); err != nil {
		return s.Builder.CreateErrorf(err, "cannot create endpoints")
	}
	return s.Builder.CreateResponse(ctx, s.Settings)
}

func (s *Builder) CreateEndpoints(ctx context.Context) error {
	grpc, err := resources.LoadGrpcAPI(ctx, nil)
	if err != nil {
		return s.Wool.Wrapf(err, "cannot load grpc api")
	}
	endpoint := s.Base.BaseEndpoint(standards.GRPC)
	endpoint.Visibility = resources.VisibilityExternal
	s.GrpcEndpoint, err = resources.NewAPI(ctx, endpoint, resources.ToGrpcAPI(grpc))
	if err != nil {
		return s.Wool.Wrapf(err, "cannot create grpc endpoint")
	}
	s.Endpoints = []*v0.Endpoint{s.GrpcEndpoint}
	s.Wool.Debug("created endpoints", wool.Field("endpoints", resources.MakeManyEndpointSummary(s.Endpoints)))
	return nil
}

func (s *Builder) Communicate(stream builderv0.Builder_CommunicateServer) error {
	asker := communicate.NewQuestionAsker(stream)
	_, err := asker.RunSequence(nil)
	return err
}

//go:embed templates/factory
var factoryFS embed.FS

//go:embed templates/deployment
var deploymentFS embed.FS
