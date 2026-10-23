package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// TestReviewGCSCredentialRestrictions is review r1's reproduction for r11,
// kept: an external-account key is refused before anything is dialed, and a
// service-account key whose token_uri is non-public is PERMISSION_DENIED —
// the token exchange is guarded, and its refusal is classified as such even
// though the oauth2 SDK reports the dial error as text — with no request
// reaching the listener either way.
func TestReviewGCSCredentialRestrictions(t *testing.T) {
	var reached atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer srv.Close()
	for _, tc := range []struct {
		name   string
		key    []byte
		code   codes.Code
		reason string
	}{
		{"external", []byte(`{"type":"external_account","credential_source":{"file":"/does-not-exist"}}`), codes.InvalidArgument, serr.ReasonBindingInvalid},
		// A refusal at dial time — here the token exchange, after the
		// endpoint was qualified — still names the guard, not the credential.
		{"token_exchange", serviceAccountKey(t, srv.URL), codes.PermissionDenied, serr.ReasonDestinationRefused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &storagev0.Binding{Kind: "gcs", Bucket: "tenant", Credential: &storagev0.Credential{
				Kind: &storagev0.Credential_ServiceAccountKey{ServiceAccountKey: tc.key}}}
			_, err := (&Server{}).Stat(context.Background(), &storagev0.StatRequest{Binding: b, Key: "key"})
			if status.Code(err) != tc.code || reached.Load() != 0 {
				t.Fatalf("status %v (want %v), listener reached %d times: %v", status.Code(err), tc.code, reached.Load(), err)
			}
			var reason string
			for _, detail := range status.Convert(err).Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok {
					reason = info.GetReason()
				}
			}
			if reason != tc.reason {
				t.Fatalf("reason %q, want %q: %v", reason, tc.reason, err)
			}
		})
	}
}
