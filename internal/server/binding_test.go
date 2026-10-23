package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/s3"
	"github.com/codefly-dev/service-object-storage/internal/egress"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

func s3Binding(endpoint string) *storagev0.Binding {
	return &storagev0.Binding{
		Kind: "s3", Endpoint: endpoint, Bucket: "tenant", Prefix: "docs/",
		Credential: &storagev0.Credential{Kind: &storagev0.Credential_AccessKey{
			AccessKey: &storagev0.AccessKey{Id: "id", Secret: "secret"},
		}},
	}
}

// A loopback endpoint is refused before anything dials it, on every bound RPC,
// and the refusal names the endpoint the caller sent.
func TestBoundCallRefusesALoopbackEndpointByName(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	b := s3Binding("http://localhost:9000")

	_, err := c.Stat(ctx, &storagev0.StatRequest{Key: "a", Binding: b})
	requireRefused(t, err)
	_, err = c.List(ctx, &storagev0.ListRequest{Binding: b})
	requireRefused(t, err)
	_, err = c.Capabilities(ctx, &storagev0.CapabilitiesRequest{Binding: b})
	requireRefused(t, err)
	_, _, err = getObject(t, c, &storagev0.GetRequest{Key: "a", Binding: b})
	requireRefused(t, err)
}

func requireRefused(t *testing.T, err error) {
	t.Helper()
	require.Equal(t, codes.PermissionDenied, status.Code(err), "got %v", err)
	require.Contains(t, status.Convert(err).Message(), `"localhost"`)
	require.Contains(t, status.Convert(err).Message(), "not a public address")
}

// A bound Capabilities answers for the bound kind and reports the caps a bound
// call runs under; the configured bucket reports none.
func TestCapabilitiesReportsTheBoundKindAndCaps(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	bound, err := c.Capabilities(ctx, &storagev0.CapabilitiesRequest{Binding: s3Binding("")})
	require.NoError(t, err)
	require.Equal(t, "s3", bound.GetBackend())
	require.True(t, bound.GetVersions())
	require.False(t, bound.GetPresign(), "a bound call cannot presign")
	require.False(t, bound.GetConditionalPut(), "a bound call cannot write")
	require.EqualValues(t, 5<<20, bound.GetMaxObjectBytes())
	require.EqualValues(t, 1000, bound.GetMaxListLimit())
	require.EqualValues(t, 1000, bound.GetMaxGetManyItems())

	own, err := c.Capabilities(ctx, &storagev0.CapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, "mem", own.GetBackend())
	require.Zero(t, own.GetMaxObjectBytes())
	require.Zero(t, own.GetMaxListLimit())
}

func TestBindingRefusesAKindWithNoRemoteStore(t *testing.T) {
	c := newTestClient(t)
	b := s3Binding("")
	b.Kind = "mem"
	_, err := c.List(context.Background(), &storagev0.ListRequest{Binding: b})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "got %v", err)
}

// A bound call reaches a non-public endpoint only when the server was told it
// is a composed dependency, and then it is served; the same endpoint is
// refused by a server that was not.
func TestBoundCallReachesOnlyAComposedNonPublicEndpoint(t *testing.T) {
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<ListBucketResult><Name>tenant</Name><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	defer store.Close()
	composed, err := egress.ParseComposed(strings.TrimPrefix(store.URL, "http://"))
	require.NoError(t, err)

	be, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	admitting := newTestClientForServer(t, server.New(be, events.NewHub(), probetest.Monitor(t, be)).AdmitComposed(composed))
	_, err = admitting.List(context.Background(), &storagev0.ListRequest{Binding: s3Binding(store.URL)})
	require.NoError(t, err)

	_, err = newTestClient(t).List(context.Background(), &storagev0.ListRequest{Binding: s3Binding(store.URL)})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "got %v", err)
}

func gcsTokenBinding(endpoint, token string) *storagev0.Binding {
	return &storagev0.Binding{
		Kind: "gcs", Endpoint: endpoint, Bucket: "tenant",
		Credential: &storagev0.Credential{Kind: &storagev0.Credential_AccessToken{AccessToken: token}},
	}
}

// A gcs access token is sent as the bearer exactly as given — nothing
// refreshes or exchanges it — so a token the store no longer accepts comes
// back as the store's own refusal, PERMISSION_DENIED. A missing token is
// refused before anything is dialed: there is no identity to fall back to.
func TestBoundGCSAccessTokenIsTheBearerAndTheStoreJudgesIt(t *testing.T) {
	var bearers []string
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearers = append(bearers, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":401,"message":"Invalid Credentials","errors":[{"reason":"authError","message":"Invalid Credentials"}]}}`))
	}))
	defer store.Close()
	composed, err := egress.ParseComposed(strings.TrimPrefix(store.URL, "http://"))
	require.NoError(t, err)
	be, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	c := newTestClientForServer(t, server.New(be, events.NewHub(), probetest.Monitor(t, be)).AdmitComposed(composed))

	_, err = c.List(context.Background(), &storagev0.ListRequest{Binding: gcsTokenBinding(store.URL, "ya29.expired")})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "got %v", err)
	require.Contains(t, status.Convert(err).Message(), "Invalid Credentials")
	require.NotEmpty(t, bearers)
	for _, bearer := range bearers {
		require.Equal(t, "Bearer ya29.expired", bearer)
	}

	seen := len(bearers)
	_, err = c.List(context.Background(), &storagev0.ListRequest{Binding: gcsTokenBinding(store.URL, "")})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "got %v", err)
	require.Len(t, bearers, seen, "a missing token must not reach the store")
}
