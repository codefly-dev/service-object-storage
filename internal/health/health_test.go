package health_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpchealth "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	miniobe "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	"github.com/codefly-dev/service-object-storage/internal/health"
)

const serviceName = "codefly.storage.v0.ObjectStorage"

const emptyListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>probe</Name><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`

const accessDenied = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>AccessDenied</Message></Error>`

// revocableStore is a real S3-speaking endpoint whose answer can be switched
// from granted to denied while the monitor is running.
type revocableStore struct {
	*httptest.Server
	denied chan bool
}

func newRevocableStore(t *testing.T) *revocableStore {
	t.Helper()
	s := &revocableStore{denied: make(chan bool, 1)}
	s.denied <- false
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		denied := <-s.denied
		s.denied <- denied
		w.Header().Set("Content-Type", "application/xml")
		if denied {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(accessDenied))
			return
		}
		_, _ = w.Write([]byte(emptyListing))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *revocableStore) revoke(denied bool) {
	<-s.denied
	s.denied <- denied
}

// servingStatus reads one service's status, reporting UNKNOWN for a service the
// monitor has not published yet.
func servingStatus(t *testing.T, hs *grpchealth.Server, service string) healthv1.HealthCheckResponse_ServingStatus {
	t.Helper()
	res, err := hs.Check(context.Background(), &healthv1.HealthCheckRequest{Service: service})
	if err != nil {
		require.Equal(t, codes.NotFound, status.Code(err))
		return healthv1.HealthCheckResponse_UNKNOWN
	}
	return res.GetStatus()
}

func requireStatus(t *testing.T, hs *grpchealth.Server, service string, want healthv1.HealthCheckResponse_ServingStatus) {
	t.Helper()
	require.Eventually(t, func() bool {
		return servingStatus(t, hs, service) == want
	}, 5*time.Second, 20*time.Millisecond, "want %s for %q", want, service)
}

// TestMonitorFollowsAccess covers the whole backend-access lifecycle against a
// real endpoint: the named service starts serving, stops when access is revoked
// mid-run, and comes back when access returns — while the overall service is
// never touched.
func TestMonitorFollowsAccess(t *testing.T) {
	store := newRevocableStore(t)

	be, err := miniobe.New(context.Background(), backend.Config{
		Endpoint:  store.URL,
		Bucket:    "probe",
		AccessKey: "probe",
		SecretKey: "probe-secret",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	hs := grpchealth.NewServer()
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go health.New(be, hs, serviceName, 50*time.Millisecond, time.Second).Run(ctx)

	requireStatus(t, hs, serviceName, healthv1.HealthCheckResponse_SERVING)

	store.revoke(true)
	requireStatus(t, hs, serviceName, healthv1.HealthCheckResponse_NOT_SERVING)
	require.Equal(t, healthv1.HealthCheckResponse_SERVING, servingStatus(t, hs, ""),
		"the overall service carries liveness AND readiness: draining every replica "+
			"on a shared-backend outage is a global kill switch, not load shedding")

	store.revoke(false)
	requireStatus(t, hs, serviceName, healthv1.HealthCheckResponse_SERVING)
}

// TestMonitorStartsUnready pins startup: a store that never answers must leave
// readiness NOT_SERVING rather than defaulting to serving.
func TestMonitorStartsUnready(t *testing.T) {
	be, err := miniobe.New(context.Background(), backend.Config{
		Endpoint:  "http://127.0.0.1:1",
		Bucket:    "probe",
		AccessKey: "probe",
		SecretKey: "probe-secret",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	hs := grpchealth.NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go health.New(be, hs, serviceName, time.Second, 500*time.Millisecond).Run(ctx)

	requireStatus(t, hs, serviceName, healthv1.HealthCheckResponse_NOT_SERVING)
}

// TestMonitorRecordsVerdict pins the verdict the Ready RPC reads, so readiness
// is answered from this one probe instead of each surface running its own.
func TestMonitorRecordsVerdict(t *testing.T) {
	store := newRevocableStore(t)

	be, err := miniobe.New(context.Background(), backend.Config{
		Endpoint:  store.URL,
		Bucket:    "probe",
		AccessKey: "probe",
		SecretKey: "probe-secret",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })

	hs := grpchealth.NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := health.New(be, hs, serviceName, 50*time.Millisecond, time.Second)
	go m.Run(ctx)

	require.Eventually(t, func() bool { return m.Verdict().Ready },
		5*time.Second, 20*time.Millisecond, "a reachable store must record a ready verdict")
	require.False(t, m.Verdict().CheckedAt.IsZero(), "a recorded verdict must carry when it was measured")

	store.revoke(true)
	require.Eventually(t, func() bool {
		v := m.Verdict()
		return !v.Ready && v.Code == "PermissionDenied"
	}, 5*time.Second, 20*time.Millisecond, "the verdict must carry the normalized cause")
	require.NotContains(t, m.Verdict().Detail, "probe-secret", "a verdict must not leak credentials")
}
