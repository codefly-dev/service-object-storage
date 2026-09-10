// Command service-object-storage runs the object-storage gateway: it resolves
// its configuration from SOS_* environment variables, opens the configured
// backend, optionally wraps it in the two-tier cache, and serves the uniform
// ObjectStorage gRPC API. Every cloud backend is compiled in and selected by
// config; the client speaks only the gRPC contract.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/auth"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/cache"
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

// redisPingTimeout bounds the startup connectivity check for the shared cache.
const redisPingTimeout = 5 * time.Second

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
	store, rdb, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = store.Close()
		if rdb != nil {
			_ = rdb.Close()
		}
	}()

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}

	hub := events.NewHub(store.Name(), store.Identity(), rdb)
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
	storagev0.RegisterObjectStorageServer(grpcServer, server.New(store, hub))

	monitorCtx, stopMonitor := context.WithCancel(ctx)
	defer stopMonitor()
	serveHealth(monitorCtx, grpcServer, store, cfg.Health)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down")
		gracefulStop(grpcServer, shutdownGrace)
	}()

	log.Printf("object-storage gateway listening on %s (backend=%s bucket=%s auth=%s)",
		cfg.ListenAddr, cfg.Backend.Kind, cfg.Backend.Bucket, authMode)
	return grpcServer.Serve(lis)
}

// openStore opens the configured backend and, when caching is enabled, wraps it
// in the cache. A configured shared-cache (Redis) tier is verified at startup:
// a misconfigured address must fail loudly here, because the cache silently
// swallows Redis errors at request time — an unreachable tier would otherwise
// degrade to L1-only with no signal, silently dropping cross-replica
// invalidation. The returned redis client (if any) is owned by the caller.
func openStore(ctx context.Context, cfg config.Config) (backend.Backend, redis.UniversalClient, error) {
	be, err := backend.Open(ctx, cfg.Backend)
	if err != nil {
		return nil, nil, err
	}
	if !cfg.Cache.Enabled {
		return be, nil, nil
	}

	var rdb redis.UniversalClient
	if cfg.Cache.RedisAddr != "" {
		rdb = redis.NewClient(&redis.Options{
			Addr:     cfg.Cache.RedisAddr,
			Password: cfg.Cache.RedisPassword,
			DB:       cfg.Cache.RedisDB,
		})
		pctx, cancel := context.WithTimeout(ctx, redisPingTimeout)
		defer cancel()
		if err := rdb.Ping(pctx).Err(); err != nil {
			_ = rdb.Close()
			_ = be.Close()
			return nil, nil, fmt.Errorf("shared cache unreachable at %s: %w", cfg.Cache.RedisAddr, err)
		}
	}
	return cache.New(be, rdb, cfg.Cache.Options), rdb, nil
}

// serveHealth registers the gRPC health service and starts the readiness
// monitor behind it.
//
// The overall ("") service is liveness: it stays SERVING for as long as the
// process serves, so a backend outage never gets the container restarted. The
// ObjectStorage service is readiness and follows the backend probe, so access
// lost after startup drains this replica out of rotation and access regained
// puts it back.
func serveHealth(ctx context.Context, srv *grpc.Server, store backend.Backend, cfg config.HealthConfig) {
	hs := grpchealth.NewServer()
	healthv1.RegisterHealthServer(srv, hs)
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)

	go health.New(store, hs, storagev0.ObjectStorage_ServiceDesc.ServiceName,
		cfg.Interval, cfg.Timeout).Run(ctx)
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
