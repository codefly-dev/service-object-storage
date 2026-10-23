package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/azure"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/gcs"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/s3"
)

// recorder is a transport that answers 404 to everything and records the hosts
// it was asked for, so a test can see which requests a backend sent through it.
type recorder struct {
	mu    sync.Mutex
	hosts []string
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.hosts = append(r.hosts, req.URL.Host)
	r.mu.Unlock()
	return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func serviceAccountKey(t *testing.T, tokenURI string) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	out, err := json.Marshal(map[string]string{
		"type":         "service_account",
		"client_email": "tenant@example.iam.gserviceaccount.com",
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"token_uri":    tokenURI,
	})
	require.NoError(t, err)
	return out
}

// Every bindable backend sends its requests — and, for gcs, its token exchange
// — through the client the binding hands it, which is what puts each of them
// behind the dial guard. A backend that built its own client would send
// nothing here and fail this test.
func TestEveryBindableBackendSendsThroughTheBoundClient(t *testing.T) {
	cases := []backend.Config{
		{Kind: "s3", Endpoint: "https://s3.tenant.example", AccessKey: "id", SecretKey: "secret"},
		{Kind: "minio", Endpoint: "https://minio.tenant.example", AccessKey: "id", SecretKey: "secret"},
		{Kind: "azure", Endpoint: "https://azure.tenant.example/", AzureAccount: "acct", AzureKey: "c2VjcmV0"},
		{Kind: "gcs", Endpoint: "https://gcs.tenant.example/storage/v1/", GCSCredentialsJSON: serviceAccountKey(t, "https://token.tenant.example/token")},
	}
	require.Len(t, cases, len(bindableKinds))
	for _, cfg := range cases {
		t.Run(cfg.Kind, func(t *testing.T) {
			rec := &recorder{}
			cfg.Bucket, cfg.Region, cfg.UsePathStyle = "data", "us-east-1", true
			cfg.HTTPClient = &http.Client{Transport: rec}
			be, err := backend.Open(context.Background(), cfg)
			require.NoError(t, err)
			defer be.Close()
			_, _ = be.List(context.Background(), backend.ListOptions{Limit: 1})
			rec.mu.Lock()
			defer rec.mu.Unlock()
			require.NotEmpty(t, rec.hosts, "%s sent nothing through the bound client", cfg.Kind)
			for _, host := range rec.hosts {
				require.True(t, strings.HasSuffix(host, ".tenant.example"), "%s sent a request to %q", cfg.Kind, host)
			}
		})
	}
}

// A bound call authenticates with the binding's credential or not at all: an
// absent one is refused rather than replaced by the gateway's own identity.
func TestBindingRequiresItsOwnCredential(t *testing.T) {
	for kind := range bindableKinds {
		_, err := boundConfig(context.Background(), &storagev0.Binding{Kind: kind, Bucket: "data", Endpoint: "https://8.8.8.8"})
		require.ErrorContains(t, err, "requires", kind)
	}
	cfg, err := boundConfig(context.Background(), &storagev0.Binding{
		Kind: "s3", Bucket: "data",
		Credential: &storagev0.Credential{Kind: &storagev0.Credential_AccessKey{AccessKey: &storagev0.AccessKey{Id: "id", Secret: "secret"}}},
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.HTTPClient, "a bound config must carry the guarded client")
}

func TestCapReaderRefusesPastTheCap(t *testing.T) {
	under := &capReader{ReadCloser: io.NopCloser(strings.NewReader("12345")), left: 5}
	got, err := io.ReadAll(under)
	require.NoError(t, err)
	require.Equal(t, "12345", string(got))

	over := &capReader{ReadCloser: io.NopCloser(strings.NewReader("123456")), left: 5}
	_, err = io.ReadAll(over)
	require.True(t, errors.Is(err, errObjectTooLarge), "got %v", err)
}
