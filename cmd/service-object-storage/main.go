// Command service-object-storage runs the object-storage gateway: it resolves
// its configuration from SOS_* environment variables, opens the configured
// backend, and serves the uniform ObjectStorage gRPC API. Every cloud backend is compiled in and selected by
// config; the client speaks only the gRPC contract.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/auth"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/config"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/health"
	"github.com/codefly-dev/service-object-storage/internal/server"

	// Backends register themselves via init(); importing them compiles each into
	// the single binary and makes its kind selectable by config.
	_ "github.com/codefly-dev/service-object-storage/internal/backend/azure"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/gcs"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/mem"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/s3"
)

// shutdownGrace bounds how long a graceful stop waits for in-flight RPCs before
// the server is forced down. Object streams (Get/Put) can otherwise keep
// GracefulStop blocked indefinitely, leaving the process un-interruptible.
const shutdownGrace = 15 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("service-object-storage: %v", err)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	store, err := backend.Open(ctx, cfg.Backend)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}

	hub := events.NewHub()
	defer hub.Close()

	// Config resolution refuses an empty token unless the operator accepted an
	// unauthenticated listener, so the empty case here is that choice.
	var options []grpc.ServerOption
	authMode := "none"
	if cfg.AuthToken != "" {
		authMode = "token"
		options = append(options,
			grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(cfg.AuthToken)),
			grpc.ChainStreamInterceptor(auth.StreamInterceptor(cfg.AuthToken)),
		)
	}

	grpcServer := grpc.NewServer(options...)

	monitorCtx, stopMonitor := context.WithCancel(ctx)
	defer stopMonitor()
	// The monitor is the single prober: the health service and the Ready RPC
	// both read its verdict, so it must exist before the storage service that
	// answers from it.
	monitor := serveHealth(monitorCtx, grpcServer, store, cfg.Health)
	storagev0.RegisterObjectStorageServer(grpcServer, server.New(store, hub, monitor))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down")
		gracefulStop(grpcServer, shutdownGrace)
	}()

	log.Printf("object-storage gateway listening on %s (backend=%s bucket=%s prefix=%q auth=%s)",
		cfg.ListenAddr, cfg.Backend.Kind, cfg.Backend.Bucket, cfg.Backend.Prefix, authMode)
	return grpcServer.Serve(lis)
}

// serveHealth registers the gRPC health service, starts the backend-access
// monitor behind it, and returns the monitor so the storage service can answer
// Ready from the same probe.
//
// The overall ("") service carries liveness AND readiness: it stays SERVING for
// as long as the process serves. The ObjectStorage service follows the backend
// probe and is what a startupProbe gates on, so a replica must prove access
// once before joining rotation — while a backend outage later, which would fail
// every replica at once, is reported per request instead of emptying the
// Service's endpoints.
func serveHealth(ctx context.Context, srv *grpc.Server, store backend.Backend, cfg config.HealthConfig) *health.Monitor {
	hs := grpchealth.NewServer()
	healthv1.RegisterHealthServer(srv, hs)
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)

	monitor := health.New(store, hs, storagev0.ObjectStorage_ServiceDesc.ServiceName,
		cfg.Interval, cfg.Timeout)
	go monitor.Run(ctx)
	return monitor
}

// gracefulStop drains in-flight RPCs, but escalates to a hard Stop if they do
// not finish within grace, so a stuck stream can never make shutdown hang.
func gracefulStop(server *grpc.Server, grace time.Duration) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		server.Stop()
		<-done
	}
}
