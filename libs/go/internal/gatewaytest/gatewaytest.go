// Package gatewaytest runs the real gateway server in process for the
// library's own tests: over the in-memory backend, or over a live MinIO when
// the integration suite's environment names one.
package gatewaytest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"testing"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"

	_ "github.com/codefly-dev/service-object-storage/internal/backend/mem"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
)

func serve(t *testing.T, be backend.Backend) storagev0.ObjectStorageClient {
	t.Helper()
	hub := events.NewHub()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(grpcServer, server.New(be, hub, probetest.Monitor(t, be)))
	go func() { _ = grpcServer.Serve(listener) }()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); grpcServer.Stop(); hub.Close(); _ = be.Close() })
	return storagev0.NewObjectStorageClient(conn)
}

// Memory is a gateway over the in-memory backend.
func Memory(t *testing.T) storagev0.ObjectStorageClient {
	t.Helper()
	be, err := backend.Open(context.Background(), backend.Config{Kind: "mem", Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return serve(t, be)
}

// MinIO is where a live MinIO is, and how its root reaches it.
type MinIO struct {
	Endpoint, AccessKey, SecretKey, Bucket string
	// Prefix is the prefix the gateway of this test was confined to, as it
	// appears in the bucket: every key the gateway sees is under it.
	Prefix string
}

// Raw is a client on the store itself, past the gateway.
func (m MinIO) Raw(t *testing.T, accessKey, secretKey, sessionToken string) *miniogo.Client {
	t.Helper()
	client, err := miniogo.New(m.Endpoint, &miniogo.Options{Creds: miniocreds.NewStaticV4(accessKey, secretKey, sessionToken), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// LiveMinIO is a gateway over the MinIO the environment names, confined to a
// prefix of this test's own, or a skip when the environment names none.
func LiveMinIO(t *testing.T) (storagev0.ObjectStorageClient, MinIO) {
	t.Helper()
	store := MinIO{}
	for name, into := range map[string]*string{
		"MINIO_ENDPOINT": &store.Endpoint, "MINIO_ACCESS_KEY": &store.AccessKey,
		"MINIO_SECRET_KEY": &store.SecretKey, "MINIO_BUCKET": &store.Bucket,
	} {
		if *into = os.Getenv(name); *into == "" {
			t.Skipf("%s not set; skipping the MinIO integration test", name)
		}
	}
	root := store.Raw(t, store.AccessKey, store.SecretKey, "")
	exists, err := root.BucketExists(context.Background(), store.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		if err := root.MakeBucket(context.Background(), store.Bucket, miniogo.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	token := make([]byte, 6)
	_, _ = rand.Read(token)
	store.Prefix = "run-" + hex.EncodeToString(token) + "/"
	be, err := backend.Open(context.Background(), backend.Config{
		Kind: "minio", Endpoint: store.Endpoint, Region: "us-east-1",
		AccessKey: store.AccessKey, SecretKey: store.SecretKey,
		Bucket: store.Bucket, Prefix: store.Prefix, UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return serve(t, be), store
}
