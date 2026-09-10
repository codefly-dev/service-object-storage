// Package probetest builds backends and monitors in known access states, so
// readiness tests in different packages share one definition of "a store that
// is not there" rather than drifting copies of it.
package probetest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpchealth "google.golang.org/grpc/health"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	miniobe "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	"github.com/codefly-dev/service-object-storage/internal/health"
)

// UnreachableMinIO opens the real MinIO client against an endpoint nothing
// listens on — a port bound and released, so dials are refused rather than
// blackholed. name seeds the bucket and credentials so a caller can assert that
// a failure detail never echoes the secret.
func UnreachableMinIO(t *testing.T, name string) backend.Backend {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	be, err := miniobe.New(context.Background(), backend.Config{
		Endpoint:  "http://" + addr,
		Bucket:    name,
		AccessKey: name,
		SecretKey: name + "-secret",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	return be
}

// Monitor returns a running monitor over be whose first probe has already
// landed, so a test reading its verdict is not racing startup.
func Monitor(t *testing.T, be backend.Backend) *health.Monitor {
	t.Helper()
	m := health.New(be, grpchealth.NewServer(), "codefly.storage.v0.ObjectStorage",
		50*time.Millisecond, 2*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go m.Run(ctx)

	require.Eventually(t, func() bool { return !m.Verdict().CheckedAt.IsZero() },
		15*time.Second, 10*time.Millisecond, "first probe never landed")
	return m
}
