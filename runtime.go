package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	runtimev0 "github.com/codefly-dev/core/generated/go/codefly/services/runtime/v0"

	"github.com/codefly-dev/core/agents/services"
	"github.com/codefly-dev/core/resources"
	dockerrun "github.com/codefly-dev/core/runners/dockerrun"
	"github.com/codefly-dev/core/wool"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
)

// gatewayContainerPort is the port the gateway listens on inside its container
// (SOS_LISTEN default). The agent maps the assigned host port onto it.
const gatewayContainerPort = 9464

// minioContainerPort is MinIO's S3 API port inside its container.
const minioContainerPort = 9000

// localMinioUser / localMinioPassword are the fixed credentials for the
// agent-managed local MinIO. They never leave the developer's machine.
const (
	localMinioUser     = "minioadmin"
	localMinioPassword = "minioadmin"
)

type Runtime struct {
	*services.DefaultRuntime
	*Service

	minioEnv   *dockerrun.DockerEnvironment
	gatewayEnv *dockerrun.DockerEnvironment

	// minioHostPort is the host port MinIO is mapped to; the agent reaches it at
	// localhost and the gateway container at host.docker.internal.
	minioHostPort uint16
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

	if s.runsLocalMinIO() {
		if err = s.startLocalMinIO(ctx); err != nil {
			return s.Runtime.InitError(err)
		}
	}

	if err = s.startGateway(ctx, uint16(instance.Port)); err != nil {
		return s.Runtime.InitError(err)
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

// startLocalMinIO brings up a MinIO container, maps it to a free host port, and
// creates the bucket the gateway will address.
func (s *Runtime) startLocalMinIO(ctx context.Context) error {
	port, err := freeHostPort()
	if err != nil {
		return s.Wool.Wrapf(err, "cannot allocate minio port")
	}
	s.minioHostPort = port

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
		resources.Env("MINIO_ROOT_PASSWORD", localMinioPassword),
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
	s.conf.secretKey = localMinioPassword

	return s.ensureBucket(ctx)
}

// ensureBucket creates the configured bucket if it does not yet exist, retrying
// while MinIO finishes coming up.
func (s *Runtime) ensureBucket(ctx context.Context) error {
	endpoint := fmt.Sprintf("localhost:%d", s.minioHostPort)
	var lastErr error
	for retry := 0; retry < 30; retry++ {
		cl, err := miniogo.New(endpoint, &miniogo.Options{
			Creds:  miniocreds.NewStaticV4(localMinioUser, localMinioPassword, ""),
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
	envs := []*resources.EnvironmentVariable{
		resources.Env("SOS_LISTEN", fmt.Sprintf(":%d", gatewayContainerPort)),
		resources.Env("SOS_BACKEND", s.conf.backend),
		resources.Env("SOS_BUCKET", s.conf.bucket),
		resources.Env("SOS_REGION", s.conf.region),
		resources.Env("SOS_CACHE", "false"),
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

	for retry := 0; retry < 30; retry++ {
		conn, dialErr := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			client := storagev0.NewObjectStorageClient(conn)
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, capErr := client.Capabilities(cctx, &storagev0.CapabilitiesRequest{})
			cancel()
			_ = conn.Close()
			if capErr == nil {
				return nil
			}
		}
		time.Sleep(time.Second)
	}
	return s.Wool.NewError("object-storage gateway not ready")
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
