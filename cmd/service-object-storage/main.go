// Command service-object-storage runs the object-storage gateway: it resolves
// its configuration from SOS_* environment variables, opens the configured
// backend, optionally wraps it in the two-tier cache, and serves the uniform
// ObjectStorage gRPC API. Every cloud backend is compiled in and selected by
// config; the client speaks only the gRPC contract.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/cache"
	"github.com/codefly-dev/service-object-storage/internal/config"
	"github.com/codefly-dev/service-object-storage/internal/server"

	// Backends register themselves via init(); importing them compiles each into
	// the single binary and makes its kind selectable by config.
	_ "github.com/codefly-dev/service-object-storage/internal/backend/azure"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/gcs"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/mem"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/s3"
)

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
	be, err := backend.Open(ctx, cfg.Backend)
	if err != nil {
		return err
	}

	store := be
	var rdb redis.UniversalClient
	if cfg.Cache.Enabled {
		if cfg.Cache.RedisAddr != "" {
			rdb = redis.NewClient(&redis.Options{
				Addr:     cfg.Cache.RedisAddr,
				Password: cfg.Cache.RedisPassword,
				DB:       cfg.Cache.RedisDB,
			})
		}
		store = cache.New(be, rdb, cfg.Cache.Options)
	}

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		store.Close()
		return err
	}

	grpcServer := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(grpcServer, server.New(store))
	reflection.Register(grpcServer)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down")
		grpcServer.GracefulStop()
	}()

	log.Printf("object-storage gateway listening on %s (backend=%s bucket=%s)",
		cfg.ListenAddr, cfg.Backend.Kind, cfg.Backend.Bucket)
	serveErr := grpcServer.Serve(lis)

	// store.Close closes the cache and, through it, the backend; rdb is owned here.
	store.Close()
	if rdb != nil {
		rdb.Close()
	}
	return serveErr
}
