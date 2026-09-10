package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"

	"github.com/codefly-dev/core/agents/services"
	"github.com/codefly-dev/core/resources"
	dockerrun "github.com/codefly-dev/core/runners/dockerrun"
	"github.com/codefly-dev/core/wool"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/auth"
)

// gatewayContainerPort is the port the gateway listens on inside its container
// (SOS_LISTEN default). The agent maps the assigned host port onto it.
const gatewayContainerPort = 9464

// minioContainerPort is MinIO's S3 API port inside its container.
const minioContainerPort = 9000

// gcsCredentialsContainerPath is where the gateway container reads its GCS
// service-account key. The agent projects the operator's host file there and
// points SOS_GCS_CREDENTIALS_FILE at this path, so the value the gateway sees
// is always a container path.
const gcsCredentialsContainerPath = "/codefly/credentials/gcs-service-account.json"

// localMinioUser is the root user for the agent-managed local MinIO. The
// password is generated per run (see startLocalMinIO): the container publishes
// on all interfaces so the gateway can reach it on Linux, so a fixed password
// would be a known credential on the host's network.
const localMinioUser = "minioadmin"

type Runtime struct {
	*services.DefaultRuntime
	*Service

	minioEnv   *dockerrun.DockerEnvironment
	gatewayEnv *dockerrun.DockerEnvironment

	// minioHostPort is the host port MinIO is mapped to; the agent reaches it at
	// localhost and the gateway container at host.docker.internal.
	minioHostPort uint16

	// minioPassword is the MinIO root password generated for this run.
	minioPassword string

	// gcsCredentialsHostFile is the projected copy of the configured GCS
	// service-account key that the gateway container mounts.
	gcsCredentialsHostFile string
}

func NewRuntime() *Runtime {
	service := NewService()
	return &Runtime{
		DefaultRuntime: services.NewDefaultRuntime(service.Runtime),
		Service:        service,
	}
}

func (s *Runtime) Load(ctx context.Context, req *runtimev0.LoadRequest) (*runtimev0.LoadResponse, error) {
	defer s.Wool.Catch()
	return s.Runtime.LoadService(ctx, req, services.RuntimeLoad{
		Settings:     s.Settings,
		Requirements: requirements,
		ResolveEndpoints: func(ctx context.Context, endpoints []*basev0.Endpoint) error {
			endpoint, err := resolveServingGRPCEndpoint(ctx, endpoints)
			if err != nil {
				return s.Wool.Wrapf(err, "cannot find grpc endpoint")
			}
			s.GrpcEndpoint = endpoint
			return nil
		},
	})
}

// runsLocalMinIO reports whether the agent should stand up a MinIO backend for
// the gateway. A deployed environment names a cloud backend and reaches it
// directly, so no MinIO is started.
func (s *Runtime) runsLocalMinIO() bool {
	return s.conf.backend == "" || s.conf.backend == "minio"
}

func (s *Runtime) Init(ctx context.Context, req *runtimev0.InitRequest) (*runtimev0.InitResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)

	s.Runtime.LogInitRequest(req)
	s.Runtime.WithContext(req.GetRuntimeContext())
	s.NetworkMappings = req.ProposedNetworkMappings

	w := s.Wool.In("runtime::init")
	configuration := req.GetConfiguration()

	net, err := resources.FindNetworkMapping(ctx, s.NetworkMappings, s.GrpcEndpoint)
	if err != nil {
		return s.Runtime.InitError(err)
	}
	instance, err := resources.FindNetworkInstanceInNetworkMappings(ctx, s.NetworkMappings, s.GrpcEndpoint, s.Runtime.NetworkAccess())
	if err != nil {
		return s.Runtime.InitError(err)
	}

	if err = s.LoadConfiguration(ctx, configuration); err != nil {
		return s.Runtime.InitError(err)
	}

	// Project credentials before any container exists: a missing or unreadable
	// key file must fail with its own cause rather than as a gateway that comes
	// up and cannot authenticate.
	if err = s.projectGCSCredentials(); err != nil {
		return s.Runtime.InitError(err)
	}

	if s.runsLocalMinIO() {
		if err = s.startLocalMinIO(ctx); err != nil {
			return s.Runtime.InitError(s.abortInit(ctx, err))
		}
	}

	s.gatewayToken, err = s.resolveGatewayToken()
	if err != nil {
		return s.Runtime.InitError(s.abortInit(ctx, err))
	}

	if err = s.startGateway(ctx, uint16(instance.Port)); err != nil {
		return s.Runtime.InitError(s.abortInit(ctx, err))
	}

	for _, inst := range net.Instances {
		conf, errConn := s.CreateConnectionConfiguration(ctx, configuration, inst)
		if errConn != nil {
			return s.Runtime.InitError(errConn)
		}
		s.Runtime.RuntimeConfigurations = append(s.Runtime.RuntimeConfigurations, conf)
	}

	w.Debug("init successful")
	return s.Runtime.InitResponse()
}

// resolveGatewayToken returns the bearer the gateway will enforce and consumers
// will present. A configured token is honored so an operator who pins one is not
// silently overridden; otherwise a fresh one is generated per run, which is what
// stands between the all-interface published port and bucket-wide
// read/write/delete/presign, and which revokes every token a previous run handed
// out.
//
// Generating a new token changes the gateway container's environment, and
// dockerrun fingerprints the environment, so each run deliberately recreates the
// container rather than reattaching to the previous one. Making the token stable
// to recover that reattach would silently give up the revocation property.
func (s *Runtime) resolveGatewayToken() (string, error) {
	if s.conf.authToken != "" {
		return s.conf.authToken, nil
	}
	return randomSecret()
}

// startLocalMinIO brings up a MinIO container, maps it to a free host port, and
// creates the bucket the gateway will address.
func (s *Runtime) startLocalMinIO(ctx context.Context) error {
	port, err := freeHostPort()
	if err != nil {
		return s.Wool.Wrapf(err, "cannot allocate minio port")
	}
	s.minioHostPort = port

	password, err := randomSecret()
	if err != nil {
		return s.Wool.Wrapf(err, "cannot generate minio credentials")
	}
	s.minioPassword = password

	runner, err := dockerrun.NewDockerHeadlessEnvironment(ctx, minioImage, s.UniqueWithWorkspace()+"-minio")
	if err != nil {
		return s.Wool.Wrapf(err, "cannot create minio environment")
	}
	runner.WithOutput(os.Stdout)
	runner.WithPortMapping(ctx, s.minioHostPort, minioContainerPort)
	// The gateway container reaches MinIO through host.docker.internal, which on
	// Linux is the bridge gateway (172.17.0.1). A port bound only to 127.0.0.1 is
	// unreachable there, so publish on all interfaces.
	runner.WithPublicPorts()
	runner.WithEnvironmentVariables(ctx,
		resources.Env("MINIO_ROOT_USER", localMinioUser),
		resources.Env("MINIO_ROOT_PASSWORD", s.minioPassword),
	)
	runner.WithCommand("server", "/data")
	if err = runner.Init(ctx); err != nil {
		return s.Wool.Wrapf(err, "cannot start minio")
	}
	s.minioEnv = runner

	// The agent reaches MinIO on the host; the gateway resolves the same store
	// through host.docker.internal (dockerrun injects it into the container).
	s.conf.endpoint = fmt.Sprintf("http://host.docker.internal:%d", s.minioHostPort)
	s.conf.backend = "minio"
	s.conf.accessKey = localMinioUser
	s.conf.secretKey = s.minioPassword

	return s.ensureBucket(ctx)
}

// ensureBucket creates the configured bucket if it does not yet exist, retrying
// while MinIO finishes coming up.
func (s *Runtime) ensureBucket(ctx context.Context) error {
	endpoint := fmt.Sprintf("localhost:%d", s.minioHostPort)
	var lastErr error
	for retry := 0; retry < 30; retry++ {
		cl, err := miniogo.New(endpoint, &miniogo.Options{
			Creds:  miniocreds.NewStaticV4(s.conf.accessKey, s.conf.secretKey, ""),
			Secure: false,
		})
		if err == nil {
			exists, existErr := cl.BucketExists(ctx, s.conf.bucket)
			switch {
			case existErr != nil:
				lastErr = existErr
			case exists:
				return nil
			default:
				if mkErr := cl.MakeBucket(ctx, s.conf.bucket, miniogo.MakeBucketOptions{}); mkErr == nil {
					return nil
				} else {
					lastErr = mkErr
				}
			}
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	return s.Wool.Wrapf(lastErr, "minio bucket %q not ready", s.conf.bucket)
}

// projectGCSCredentials makes the configured GCS service-account key available
// to the gateway container. The configured value names a file on this machine —
// relative to the service directory when it is not absolute — and the gateway
// reads it at gcsCredentialsContainerPath.
//
// The key is copied into an invocation-owned directory instead of being mounted
// from where the operator keeps it: the gateway image runs as the distroless
// nonroot user, which cannot read a 0600 key owned by the operator. The copy is
// mode 0444 in a 0700 directory, so the container user can read it and — not
// being its owner, and not root — cannot write to it, while no other host user
// can reach it. The gateway therefore never has a path to the operator's file.
func (s *Runtime) projectGCSCredentials() error {
	if s.conf.backend != "gcs" {
		return nil
	}
	if s.conf.gcsCredentialsFile == "" {
		return s.Wool.NewError("backend gcs has no usable credentials for a local run: set SOS_GCS_CREDENTIALS_FILE in the object-storage configuration to a service-account JSON key on this machine. Application Default Credentials and Workload Identity are deployment-only modes — the gateway container inherits no host identity")
	}

	source := s.conf.gcsCredentialsFile
	if !filepath.IsAbs(source) {
		source = filepath.Join(s.Location, source)
	}
	info, err := os.Stat(source)
	if err != nil {
		return s.Wool.Wrapf(err, "cannot use SOS_GCS_CREDENTIALS_FILE %q", source)
	}
	if !info.Mode().IsRegular() {
		return s.Wool.NewError("SOS_GCS_CREDENTIALS_FILE %q is not a regular file", source)
	}
	key, err := os.ReadFile(source)
	if err != nil {
		return s.Wool.Wrapf(err, "cannot read SOS_GCS_CREDENTIALS_FILE %q", source)
	}

	dir := s.projectedCredentialsDir()
	if err = os.RemoveAll(dir); err != nil {
		return s.Wool.Wrapf(err, "cannot reset credential projection")
	}
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return s.Wool.Wrapf(err, "cannot create credential projection")
	}
	projected := filepath.Join(dir, filepath.Base(gcsCredentialsContainerPath))
	if err = os.WriteFile(projected, key, 0o444); err != nil {
		return s.Wool.Wrapf(err, "cannot project gcs credentials")
	}
	s.gcsCredentialsHostFile = projected
	return nil
}

// projectedCredentialsDir is where this service's projected credentials live.
func (s *Runtime) projectedCredentialsDir() string {
	return filepath.Join(resources.CodeflyHomeDir(), "runtime-credentials", s.UniqueWithWorkspace())
}

// discardProjectedCredentials removes the projected copy from the host.
func (s *Runtime) discardProjectedCredentials() {
	if s.gcsCredentialsHostFile == "" {
		return
	}
	_ = os.RemoveAll(s.projectedCredentialsDir())
	s.gcsCredentialsHostFile = ""
}

// abortInit tears down what Init already owns and returns cause unchanged, so a
// failure part-way through startup leaves neither a stray container nor a
// projected credential behind.
func (s *Runtime) abortInit(ctx context.Context, cause error) error {
	if s.gatewayEnv != nil {
		if err := s.gatewayEnv.Shutdown(ctx); err != nil {
			s.Wool.Warn("cannot shut down gateway after failed init", wool.ErrField(err))
		}
		s.gatewayEnv = nil
	}
	if s.minioEnv != nil {
		if err := s.minioEnv.Shutdown(ctx); err != nil {
			s.Wool.Warn("cannot shut down minio after failed init", wool.ErrField(err))
		}
		s.minioEnv = nil
	}
	s.discardProjectedCredentials()
	return cause
}

// startGateway runs the gateway container, mapping the assigned host port onto
// the gateway's listen port and pointing it at the resolved backend.
func (s *Runtime) startGateway(ctx context.Context, hostPort uint16) error {
	image := gatewayImage
	if override := os.Getenv("SOS_GATEWAY_IMAGE"); override != "" {
		image = &resources.DockerImage{Name: override}
	}

	runner, err := dockerrun.NewDockerHeadlessEnvironment(ctx, image, s.UniqueWithWorkspace()+"-gateway")
	if err != nil {
		return s.Wool.Wrapf(err, "cannot create gateway environment")
	}
	runner.WithOutput(os.Stdout)
	runner.WithPortMapping(ctx, hostPort, gatewayContainerPort)
	// Consumers running in their own container reach the gateway through
	// host.docker.internal (the Container network instance the agent advertises),
	// which resolves to the bridge gateway on Linux — unreachable if the port is
	// bound only to 127.0.0.1. Publish on all interfaces, as MinIO does; what
	// makes that safe is SOS_AUTH_TOKEN below, not the binding.
	runner.WithPublicPorts()
	envs := []*resources.EnvironmentVariable{
		resources.Env("SOS_LISTEN", fmt.Sprintf(":%d", gatewayContainerPort)),
		resources.Env("SOS_BACKEND", s.conf.backend),
		resources.Env("SOS_BUCKET", s.conf.bucket),
		resources.Env("SOS_REGION", s.conf.region),
		resources.Env("SOS_CACHE", "false"),
		resources.Env("SOS_AUTH_TOKEN", s.gatewayToken),
	}
	if s.conf.endpoint != "" {
		envs = append(envs, resources.Env("SOS_ENDPOINT", s.conf.endpoint))
	}
	if s.conf.accessKey != "" {
		envs = append(envs,
			resources.Env("SOS_ACCESS_KEY", s.conf.accessKey),
			resources.Env("SOS_SECRET_KEY", s.conf.secretKey),
		)
	}
	// The configured key file lives on the host, so the gateway is given the
	// projected copy and the container path it appears at — never the host path,
	// which does not exist inside the container.
	if s.gcsCredentialsHostFile != "" {
		runner.WithMount(s.gcsCredentialsHostFile, gcsCredentialsContainerPath)
		envs = append(envs, resources.Env("SOS_GCS_CREDENTIALS_FILE", gcsCredentialsContainerPath))
	}
	// The account name is not a credential; the shared key that pairs with it has
	// no local delivery path, so an azure backend only authenticates deployed.
	if s.conf.azureAccount != "" {
		envs = append(envs, resources.Env("SOS_AZURE_ACCOUNT", s.conf.azureAccount))
	}
	runner.WithEnvironmentVariables(ctx, envs...)
	if err = runner.Init(ctx); err != nil {
		return s.Wool.Wrapf(err, "cannot start gateway")
	}
	s.gatewayEnv = runner
	return nil
}

func (s *Runtime) Start(ctx context.Context, req *runtimev0.StartRequest) (*runtimev0.StartResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	if err := s.WaitForReady(ctx); err != nil {
		return s.Runtime.StartError(err)
	}
	return s.Runtime.StartResponse()
}

// WaitForReady dials the gateway's gRPC endpoint and calls Capabilities until it
// answers — the honest readiness signal that the gateway opened its backend.
func (s *Runtime) WaitForReady(ctx context.Context) error {
	instance, err := resources.FindNetworkInstanceInNetworkMappings(ctx, s.NetworkMappings, s.GrpcEndpoint, s.Runtime.NetworkAccess())
	if err != nil {
		return s.Wool.Wrapf(err, "cannot find network instance")
	}
	address := instance.Address
	s.Wool.Debug("waiting for object-storage gateway", wool.Field("address", address))

	var lastErr error
	for retry := 0; retry < 30; retry++ {
		conn, dialErr := grpc.NewClient(address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			auth.DialOption(s.gatewayToken))
		if dialErr != nil {
			lastErr = dialErr
		} else {
			client := storagev0.NewObjectStorageClient(conn)
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, capErr := client.Capabilities(cctx, &storagev0.CapabilitiesRequest{})
			cancel()
			_ = conn.Close()
			if capErr == nil {
				return nil
			}
			lastErr = capErr
			// A rejected credential is not a gateway that is still coming up:
			// retrying cannot change the answer, and doing so for 30 seconds
			// reports the failure as backend readiness instead of as the
			// mismatched token it is.
			if status.Code(capErr) == codes.Unauthenticated {
				return s.Wool.Wrapf(capErr, "object-storage gateway rejected the agent's credential")
			}
		}
		time.Sleep(time.Second)
	}
	return s.Wool.Wrapf(lastErr, "object-storage gateway not ready")
}

func (s *Runtime) Stop(ctx context.Context, req *runtimev0.StopRequest) (*runtimev0.StopResponse, error) {
	defer s.Wool.Catch()
	return s.Runtime.StopResponse()
}

func (s *Runtime) Destroy(ctx context.Context, req *runtimev0.DestroyRequest) (*runtimev0.DestroyResponse, error) {
	defer s.Wool.Catch()
	ctx = s.Wool.Inject(ctx)
	if s.gatewayEnv != nil {
		if err := s.gatewayEnv.Shutdown(ctx); err != nil {
			return s.Runtime.DestroyError(err)
		}
	}
	if s.minioEnv != nil {
		if err := s.minioEnv.Shutdown(ctx); err != nil {
			return s.Runtime.DestroyError(err)
		}
	}
	s.discardProjectedCredentials()
	return s.Runtime.DestroyResponse()
}

func (s *Runtime) Test(ctx context.Context, req *runtimev0.TestRequest) (*runtimev0.TestResponse, error) {
	return s.Runtime.TestResponse()
}

// freeHostPort asks the kernel for an unused TCP port and returns it.
func freeHostPort() (uint16, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return uint16(l.Addr().(*net.TCPAddr).Port), nil
}

// randomSecret returns an unguessable hex secret. Both the MinIO root password
// and the gateway bearer are generated this way, so neither published port is
// reachable with a well-known credential.
func randomSecret() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
