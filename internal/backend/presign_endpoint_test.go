package backend_test

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"

	_ "github.com/codefly-dev/service-object-storage/internal/backend/azure"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/gcs"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/mem"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/s3"
)

// dialled is a container-only name, the address the local agent hands the
// gateway: a host process cannot resolve it, so a URL naming it is dead on
// arrival for a browser.
const dialled = "http://host.docker.internal:9000"

// TestPresignNamesThePresignEndpointHost is the regression for URLs signed with
// the address the gateway dials. Each backend that signs against a configured
// endpoint must put the PresignEndpoint's host into the URL, for GET and PUT,
// and still dial the other one; signing with the dialling client (the old
// behaviour) names host.docker.internal and fails here.
func TestPresignNamesThePresignEndpointHost(t *testing.T) {
	for _, tc := range []struct {
		kind     string
		cfg      backend.Config
		wantHost string
		wantPath string
	}{
		{
			kind:     "minio",
			cfg:      backend.Config{Endpoint: dialled, PresignEndpoint: "http://localhost:9000", AccessKey: "ak", SecretKey: "sk-synthetic", UsePathStyle: true},
			wantHost: "localhost:9000",
			wantPath: "/bkt/dir/obj.xlsx",
		},
		{
			kind:     "s3",
			cfg:      backend.Config{Endpoint: dialled, PresignEndpoint: "https://objects.example.test", AccessKey: "ak", SecretKey: "sk-synthetic", UsePathStyle: true},
			wantHost: "objects.example.test",
			wantPath: "/bkt/dir/obj.xlsx",
		},
		{
			kind: "azure",
			cfg: backend.Config{
				Endpoint: "http://azurite:10000/acct/", PresignEndpoint: "http://localhost:10000/acct/",
				AzureAccount: "acct", AzureKey: base64.StdEncoding.EncodeToString([]byte("synthetic-shared-key-for-tests")),
			},
			wantHost: "localhost:10000",
			wantPath: "/acct/bkt/dir/obj.xlsx",
		},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Kind = tc.kind
			cfg.Bucket = "bkt"
			cfg.Region = "us-east-1"
			be, err := backend.Open(context.Background(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = be.Close() })

			for _, method := range []backend.PresignMethod{backend.PresignGet, backend.PresignPut} {
				res, err := be.Presign(context.Background(), "dir/obj.xlsx", method, time.Minute)
				require.NoError(t, err)
				u, err := url.Parse(res.URL)
				require.NoError(t, err)
				require.Equal(t, tc.wantHost, u.Host, "presigned URL must name the presign endpoint, not the dialled one: %s", res.URL)
				require.Equal(t, tc.wantPath, u.Path)
				require.NotContains(t, res.URL, "host.docker.internal")
				require.NotContains(t, res.URL, "azurite")
			}
		})
	}
}

// TestPresignEndpointIsRequiredExactlyWhereItMatters: a backend that signs
// against its configured endpoint refuses to open without being told which
// host its URLs name, and one that does not refuses a presign endpoint it
// would silently ignore. Both are refused at New, so no URL is ever minted
// for the dialled address by default.
func TestPresignEndpointIsRequiredExactlyWhereItMatters(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte("synthetic-shared-key-for-tests"))
	for _, tc := range []struct {
		name string
		cfg  backend.Config
		want string
	}{
		{"minio-without", backend.Config{Kind: "minio", Endpoint: dialled}, "no presign endpoint was given"},
		{"s3-compatible-without", backend.Config{Kind: "s3", Endpoint: dialled}, "no presign endpoint was given"},
		{"azurite-without", backend.Config{Kind: "azure", Endpoint: "http://azurite:10000/acct/", AzureAccount: "acct", AzureKey: key}, "no presign endpoint was given"},
		{"aws-s3-with", backend.Config{Kind: "s3", PresignEndpoint: "http://localhost:9000"}, "has no effect"},
		{"azure-account-with", backend.Config{Kind: "azure", AzureAccount: "acct", AzureKey: key, PresignEndpoint: "http://localhost:10000/acct/"}, "has no effect"},
		{"gcs-with", backend.Config{Kind: "gcs", PresignEndpoint: "http://localhost:4443"}, "has no effect"},
		{"mem-with", backend.Config{Kind: "mem", PresignEndpoint: "http://localhost:1"}, "has no effect"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Bucket = "bkt"
			cfg.Region = "us-east-1"
			cfg.AccessKey, cfg.SecretKey = "ak", "sk-synthetic"
			be, err := backend.Open(context.Background(), cfg)
			if be != nil {
				_ = be.Close()
			}
			require.Error(t, err)
			require.Equal(t, serr.InvalidArgument, serr.CodeOf(err), "got %v", err)
			require.True(t, strings.Contains(err.Error(), tc.want), "got %v", err)
		})
	}
}
