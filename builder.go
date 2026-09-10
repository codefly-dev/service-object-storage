package main

import (
	"context"
	"embed"
	"fmt"

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
	// AzureAccount is the Azure Blob storage account name. It is non-sensitive
	// (it forms the public blob endpoint host); the shared key that pairs with
	// it is sensitive and travels via the Secret, not the manifest.
	AzureAccount string
}

// supportedDeployBackends is the set of backend kinds Deploy knows how to
// template. It mirrors the gateway's registered backends (minio is honored for
// deployments that point it at an external endpoint); an out-of-set value is a
// misconfiguration the builder rejects rather than passes through.
var supportedDeployBackends = map[string]bool{
	"s3":    true,
	"gcs":   true,
	"azure": true,
	"minio": true,
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
		Backend:      s.conf.backend,
		Bucket:       s.conf.bucket, // LoadConfiguration already defaults bucket/region.
		Region:       s.conf.region,
		AzureAccount: s.conf.azureAccount,
	}
	// Local runs default to MinIO; a deployment that names no backend targets
	// S3. An explicit backend (including minio-against-an-endpoint) is honored.
	if parameters.Backend == "" {
		parameters.Backend = "s3"
	}
	// Validate the resolved backend before templating. The builder reports the
	// deploy as a success once the manifest is written, so an unknown backend
	// kind or an azure deployment missing its (non-secret) account name would
	// otherwise ship a manifest that passes every probe and then fails on the
	// gateway's first storage call. Reject it here, at the layer that owns the
	// manifest, with an actionable error instead of a silently broken deploy.
	if !supportedDeployBackends[parameters.Backend] {
		return s.Builder.DeployError(fmt.Errorf("unsupported deploy backend %q (want one of s3, gcs, azure, minio)", parameters.Backend))
	}
	if parameters.Backend == "azure" && parameters.AzureAccount == "" {
		return s.Builder.DeployError(fmt.Errorf("backend azure requires SOS_AZURE_ACCOUNT (the storage account name)"))
	}
	// A key-file path cannot be honored by this deployment: nothing projects the
	// key into the pod. The configuration channel carries the path, never the
	// service-account JSON, and the only file seam the manifest has is
	// ConfigMap-backed — the wrong home for a private key. Emitting the env var
	// anyway names a path that does not exist, and the gateway then fails on its
	// first storage call after passing every probe. Keyless auth is the remedy
	// on GKE; off GKE (EKS, AKS, on-prem) there is no Workload Identity and no
	// metadata server, so the key has to be mounted by an overlay this agent
	// does not own and the env var patched on there. Either way, reject the
	// path rather than render a manifest that lies about it.
	if parameters.Backend == "gcs" && s.conf.gcsCredentialsFile != "" {
		return s.Builder.DeployError(fmt.Errorf("SOS_GCS_CREDENTIALS_FILE cannot be delivered by this deployment: "+
			"nothing here mounts %s into the pod, so the gateway would name a path that does not exist; "+
			"on GKE, unset it and bind the workload's Kubernetes ServiceAccount to a GCP service account "+
			"(Workload Identity); elsewhere, mount the key from an overlay of your own and patch "+
			"SOS_GCS_CREDENTIALS_FILE onto the container there "+
			"(see README \"Running as a codefly service\")", s.conf.gcsCredentialsFile))
	}
	// A configured gateway token cannot be honored by this deployment: the
	// manifest carries no secret values (DeployKustomize is not wired for
	// configuration inputs here, the same gap SOS_SECRET_KEY / SOS_AZURE_KEY sit
	// behind), and CreateConnectionConfiguration only emits a token the Runtime
	// generated, so consumers would receive none. Accepting it would render a
	// manifest that either crash-loops the gateway or, worse, starts a gateway
	// enforcing a credential every consumer lacks. Reject it where the manifest
	// is owned, as with an unsupported backend.
	if s.conf.authToken != "" {
		return s.Builder.DeployError(fmt.Errorf("SOS_AUTH_TOKEN cannot be delivered by this deployment: " +
			"the emitted Secret carries no configuration values and consumers would receive no credential; " +
			"caller identity in the deployed profile is enforced by the cluster (see README \"Authentication\")"))
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
