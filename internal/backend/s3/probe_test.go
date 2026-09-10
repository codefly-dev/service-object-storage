package s3

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

const emptyListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>probe</Name><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`

func s3Error(code string) string {
	return `<?xml version="1.0" encoding="UTF-8"?><Error><Code>` + code + `</Code><Message>` + code + `</Message></Error>`
}

// openProbe opens a real S3 client against endpoint with static credentials.
func openProbe(t *testing.T, endpoint string, strategy backend.ProbeStrategy) backend.Backend {
	t.Helper()
	be, err := New(context.Background(), backend.Config{
		Bucket:        "probe",
		Region:        "us-east-1",
		Endpoint:      endpoint,
		UsePathStyle:  true,
		AccessKey:     "probe",
		SecretKey:     "probe-secret",
		ProbeStrategy: strategy,
		ProbeKey:      "sentinel",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	return be
}

func fakeS3(t *testing.T, status int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestProbeReachableBucket(t *testing.T) {
	be := openProbe(t, fakeS3(t, http.StatusOK, emptyListing), backend.ProbeList)
	require.NoError(t, be.Probe(context.Background()))
}

// TestProbeUnreachableEndpoint pins the distinction readiness turns on: a client
// against an endpoint that is not there still reports its capabilities, and only
// the probe says the store cannot be reached.
func TestProbeUnreachableEndpoint(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	be := openProbe(t, "http://"+addr, backend.ProbeList)
	require.Equal(t, "s3", be.Capabilities().Backend)

	err = be.Probe(context.Background())
	require.Error(t, err)
	require.Equal(t, serr.Unavailable, serr.CodeOf(err), "got %v", err)
}

func TestProbeNormalizesRefusals(t *testing.T) {
	cases := []struct {
		name     string
		strategy backend.ProbeStrategy
		status   int
		body     string
		want     serr.Code
	}{
		{"denied-credentials", backend.ProbeList, http.StatusForbidden, s3Error("AccessDenied"), serr.PermissionDenied},
		{"missing-bucket", backend.ProbeList, http.StatusNotFound, s3Error("NoSuchBucket"), serr.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			be := openProbe(t, fakeS3(t, tc.status, tc.body), tc.strategy)
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
	be := openProbe(t, fakeS3(t, http.StatusNotFound, ""), backend.ProbeStat)
	require.NoError(t, be.Probe(context.Background()))
}

// TestProbeStatToleratesForbidden is the regression this strategy was shipped
// broken on. S3 answers 403, not 404, for a key that does not exist when the
// caller lacks s3:ListBucket — precisely the least-privilege grant `stat`
// exists to serve. Failing the probe there left readiness permanently red and
// the pod never Ready, so a bodyless 403 must pass exactly as a 404 does.
func TestProbeStatToleratesForbidden(t *testing.T) {
	be := openProbe(t, fakeS3(t, http.StatusForbidden, ""), backend.ProbeStat)
	require.NoError(t, be.Probe(context.Background()))
}

// TestProbeStatStillReportsUnreachable guards the line the tolerance must not
// cross: accepting 403 and 404 widens what `stat` forgives, but an endpoint
// that never answered is still a failure under every strategy.
func TestProbeStatStillReportsUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	be := openProbe(t, "http://"+addr, backend.ProbeStat)
	err = be.Probe(context.Background())
	require.Error(t, err)
	require.Equal(t, serr.Unavailable, serr.CodeOf(err), "got %v", err)
}
