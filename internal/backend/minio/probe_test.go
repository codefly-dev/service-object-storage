package minio_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	miniobe "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// s3Response is one canned S3 wire answer the fake endpoint returns.
type s3Response struct {
	status int
	body   string
}

const emptyListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>probe</Name><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`

func s3Error(code string) string {
	return `<?xml version="1.0" encoding="UTF-8"?><Error><Code>` + code + `</Code><Message>` + code + `</Message></Error>`
}

// fakeStore is a real HTTP server speaking just enough S3 for a probe. The
// answer it gives is swappable so a test can revoke access mid-flight.
type fakeStore struct {
	*httptest.Server
	answer chan s3Response
}

func newFakeStore(t *testing.T, initial s3Response) *fakeStore {
	t.Helper()
	f := &fakeStore{answer: make(chan s3Response, 1)}
	f.answer <- initial
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := <-f.answer
		f.answer <- res
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(res.status)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(res.body))
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeStore) serve(res s3Response) {
	<-f.answer
	f.answer <- res
}

func openBackend(t *testing.T, cfg backend.Config) backend.Backend {
	t.Helper()
	cfg.Bucket = "probe"
	cfg.AccessKey = "probe"
	cfg.SecretKey = "probe-secret"
	be, err := miniobe.New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	return be
}

// deadEndpoint returns an address nothing listens on: a port bound and released,
// so a dial is refused rather than blackholed.
func deadEndpoint(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// TestProbeUnreachableEndpoint is the acceptance case behind readiness: a client
// constructed against an endpoint that is not there answers Capabilities from
// its static table, and only the probe discovers the truth.
func TestProbeUnreachableEndpoint(t *testing.T) {
	for _, strategy := range []backend.ProbeStrategy{backend.ProbeList, backend.ProbeStat} {
		t.Run(string(strategy), func(t *testing.T) {
			be := openBackend(t, backend.Config{
				Endpoint:      "http://" + deadEndpoint(t),
				ProbeStrategy: strategy,
				ProbeKey:      "sentinel",
			})
			require.Equal(t, "minio", be.Capabilities().Backend)

			err := be.Probe(context.Background())
			require.Error(t, err)
			require.True(t, serr.Is(err, serr.Unavailable), "want Unavailable, got %v (%s)", err, serr.CodeOf(err))
		})
	}
}

func TestProbeReachableBucket(t *testing.T) {
	store := newFakeStore(t, s3Response{status: http.StatusOK, body: emptyListing})
	be := openBackend(t, backend.Config{Endpoint: store.URL})
	require.NoError(t, be.Probe(context.Background()))
}

func TestProbeNormalizesRefusals(t *testing.T) {
	cases := []struct {
		name     string
		strategy backend.ProbeStrategy
		answer   s3Response
		want     serr.Code
	}{
		{"denied-credentials", backend.ProbeList, s3Response{http.StatusForbidden, s3Error("AccessDenied")}, serr.PermissionDenied},
		{"missing-bucket", backend.ProbeList, s3Response{http.StatusNotFound, s3Error("NoSuchBucket")}, serr.NotFound},
		{"throttled", backend.ProbeList, s3Response{http.StatusServiceUnavailable, s3Error("SlowDown")}, serr.Throttled},
		{"stat-denied", backend.ProbeStat, s3Response{http.StatusForbidden, s3Error("AccessDenied")}, serr.PermissionDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore(t, tc.answer)
			be := openBackend(t, backend.Config{
				Endpoint:      store.URL,
				ProbeStrategy: tc.strategy,
				ProbeKey:      "sentinel",
			})
			err := be.Probe(context.Background())
			require.Error(t, err)
			require.Equal(t, tc.want, serr.CodeOf(err), "got %v", err)
		})
	}
}

// TestProbeStatToleratesMissingObject pins the least-privilege contract: the
// sentinel need not exist, because a 404 already proves the endpoint answered
// and accepted the credentials.
func TestProbeStatToleratesMissingObject(t *testing.T) {
	store := newFakeStore(t, s3Response{status: http.StatusNotFound, body: s3Error("NoSuchKey")})
	be := openBackend(t, backend.Config{
		Endpoint:      store.URL,
		ProbeStrategy: backend.ProbeStat,
		ProbeKey:      "sentinel",
	})
	require.NoError(t, be.Probe(context.Background()))
}

// TestProbeHonorsDeadline holds the response open so the probe can only return
// by its own deadline.
func TestProbeHonorsDeadline(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	be := openBackend(t, backend.Config{Endpoint: srv.URL})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := be.Probe(ctx)
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second, "probe must return on its deadline, not hang")
}
