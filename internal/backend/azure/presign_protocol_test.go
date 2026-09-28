package azure

import (
	"context"
	"encoding/base64"
	"net/url"
	"testing"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/stretchr/testify/require"
)

func TestCopySourceSASProtocolUsesDialEndpoint(t *testing.T) {
	for _, tc := range []struct{ name, dial, public, protocol string }{
		{"http-dial", "http://azurite:10000/acct", "https://public.example.test/acct", "https,http"},
		{"https-dial", "https://private.example.test/acct", "http://localhost:10000/acct", "https"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			be, err := New(context.Background(), backend.Config{Endpoint: tc.dial, PresignEndpoint: tc.public, AzureAccount: "acct", AzureKey: base64.StdEncoding.EncodeToString([]byte("synthetic-shared-key-for-tests")), Bucket: "bkt"})
			require.NoError(t, err)
			t.Cleanup(func() { _ = be.Close() })
			raw, err := be.(*Backend).copySourceURL("object")
			require.NoError(t, err)
			u, err := url.Parse(raw)
			require.NoError(t, err)
			dial, err := url.Parse(tc.dial)
			require.NoError(t, err)
			require.Equal(t, dial.Host, u.Host)
			require.Equal(t, tc.protocol, u.Query().Get("spr"))
		})
	}
}
