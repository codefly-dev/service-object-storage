package server_test

// The independent review of PR #111 (lazybox report r1, at 33ba749) shipped a
// reproduction for each finding. Each is kept here under its TestReview name
// and its original scenario, asserting the invariant the fix restored.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/azure"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/gcs"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
	"github.com/codefly-dev/service-object-storage/internal/egress"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

// composedStore serves handler on loopback and returns the store's endpoint by
// its composed name and a client of a server that admits it.
func composedStore(t *testing.T, handler http.HandlerFunc) (string, storagev0.ObjectStorageClient) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	endpoint := named(srv)
	composed, err := egress.ParseComposed(strings.TrimPrefix(endpoint, "http://"))
	require.NoError(t, err)
	be, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	return endpoint, newTestClientForServer(t, server.New(be, events.NewHub(), probetest.Monitor(t, be)).AdmitComposed(composed))
}

// r4: an upstream that echoes the credential it was sent cannot carry it out
// in the status the caller gets — for a bearer and for an access-key secret.
func TestReviewBearerReturnedInStatus(t *testing.T) {
	const token = "synthetic-review-secret"
	endpoint, c := composedStore(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": 401, "message": "invalid credential: " + r.Header.Get("Authorization")}})
	})
	_, err := c.List(context.Background(), &storagev0.ListRequest{Binding: gcsTokenBinding(endpoint, token)})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "got %v", err)
	require.NotContains(t, status.Convert(err).Message(), token)
	require.Contains(t, status.Convert(err).Message(), "invalid credential: [redacted]", "the store's own words are kept")
	// The refusal names its reason beside the message (it used to carry no
	// details at all), and no detail carries the credential either.
	requireReason(t, err, codes.PermissionDenied, "CREDENTIAL_REJECTED")
	for _, detail := range status.Convert(err).Details() {
		require.NotContains(t, fmt.Sprint(detail), token)
	}

	const secret = "synthetic-access-secret"
	endpoint, c = composedStore(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprintf(w, `<Error><Code>AccessDenied</Code><Message>key %s refused</Message></Error>`, secret)
	})
	b := s3Binding(endpoint)
	b.Credential.GetAccessKey().Secret = secret
	_, err = c.List(context.Background(), &storagev0.ListRequest{Binding: b})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "got %v", err)
	require.NotContains(t, status.Convert(err).Message(), secret)
}

// r4: GOOGLE_SDK_GO_LOGGING_LEVEL=debug cannot write a bound call's traffic,
// which here echoes the bearer, to the gateway's stderr.
func TestReviewSDKDebugLoggingReflectsCredential(t *testing.T) {
	t.Setenv("GOOGLE_SDK_GO_LOGGING_LEVEL", "debug")
	const token = "synthetic-review-log-secret"
	endpoint, c := composedStore(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Echo-Credential", r.Header.Get("Authorization"))
		_, _ = fmt.Fprint(w, `{"kind":"storage#objects","items":[]}`)
	})
	f, err := os.CreateTemp(t.TempDir(), "stderr")
	require.NoError(t, err)
	old := os.Stderr
	os.Stderr = f
	_, callErr := c.List(context.Background(), &storagev0.ListRequest{Binding: gcsTokenBinding(endpoint, token)})
	os.Stderr = old
	require.NoError(t, f.Close())
	require.NoError(t, callErr)
	logged, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	require.NotContains(t, string(logged), token)
}

// r9: protobuf-go does not implement debug_redact — String, prototext and
// protojson all print a marked field — so the gateway never relies on it: a
// bound call's credential reaches no diagnostic it emits except through the
// error policy above. This pins the runtime's behaviour the docs describe; if
// a future protobuf-go starts redacting, it fails and the docs are updated.
func TestReviewDebugRedaction(t *testing.T) {
	b := gcsTokenBinding("", "synthetic-review-secret")
	require.Contains(t, b.String(), "synthetic-review-secret",
		"protobuf-go now honours debug_redact: update the README and proto comments that say it does not")
	// The marking itself is in place, for the runtimes and tools that do.
	for _, field := range []struct {
		msg  protoreflect.MessageDescriptor
		name protoreflect.Name
	}{
		{(&storagev0.AccessKey{}).ProtoReflect().Descriptor(), "secret"},
		{(&storagev0.Credential{}).ProtoReflect().Descriptor(), "service_account_key"},
		{(&storagev0.Credential{}).ProtoReflect().Descriptor(), "access_token"},
	} {
		options := field.msg.Fields().ByName(field.name).Options().(*descriptorpb.FieldOptions)
		require.True(t, options.GetDebugRedact(), "%s.%s is not marked debug_redact", field.msg.Name(), field.name)
	}
}

// r5: a replacement between GCS's metadata read and its media read never
// delivers the replacement's bytes under the old ETag as OK. The media read
// is pinned to the observed generation: a store that honours the pin answers
// 412, the item is CHANGED; one that ignores it is caught by the exact-size
// accounting, and the stream ends.
func TestReviewGCSMetadataBodyRace(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	for _, honours := range []bool{true, false} {
		t.Run(fmt.Sprintf("store honours the generation pin=%v", honours), func(t *testing.T) {
			var pinned atomic.Int32
			endpoint, c := composedStore(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("alt") == "media" || !strings.HasPrefix(r.URL.Path, "/storage/v1/") {
					pin := r.Header.Get("X-Goog-If-Generation-Match") + r.URL.Query().Get("ifGenerationMatch")
					if pin == "1" {
						pinned.Add(1)
					}
					if honours && pin != "" && pin != "2" {
						w.WriteHeader(http.StatusPreconditionFailed)
						return
					}
					w.Header().Set("Content-Type", "application/octet-stream")
					w.Header().Set("X-Goog-Generation", "2")
					_, _ = fmt.Fprint(w, "NEWER")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"bucket":"tenant","name":"a","etag":"old-etag","size":"3","generation":"1","metageneration":"1","updated":"2026-01-01T00:00:00Z"}`)
			})
			items, err := getMany(t, c, &storagev0.GetManyRequest{Binding: gcsTokenBinding(endpoint, "synthetic-review-token"),
				Items: []*storagev0.GetManyItem{{Key: "a", IfMatch: "old-etag"}, {Key: "b"}}})
			require.NotZero(t, pinned.Load(), "the media read carried no generation pin")
			for _, item := range items {
				require.NotContains(t, string(item.body), "NEWER", "the replacement's bytes were delivered under the old ETag")
			}
			if honours {
				require.NoError(t, err)
				require.Len(t, items, 2)
				require.Equal(t, storagev0.GetManyOutcome_GET_MANY_OUTCOME_CHANGED, items[0].header.GetOutcome())
				require.Empty(t, items[0].body)
				return
			}
			require.Equal(t, codes.Aborted, status.Code(err), "got %v", err)
		})
	}
}

// r6: a store that answers without a length (chunked) cannot produce an OK
// item whose bytes differ from its size; nor can one whose body is shorter
// than the length it declared.
func TestReviewUnknownSizeBatch(t *testing.T) {
	endpoint, c := composedStore(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		_, _ = fmt.Fprint(w, "alpha")
	})
	items, err := getMany(t, c, &storagev0.GetManyRequest{Binding: s3Binding(endpoint), Items: []*storagev0.GetManyItem{{Key: "a"}, {Key: "b"}}})
	require.NoError(t, err)
	require.Len(t, items, 2)
	for _, item := range items {
		require.Equal(t, storagev0.GetManyOutcome_GET_MANY_OUTCOME_OK, item.header.GetOutcome())
		require.EqualValues(t, 5, item.header.GetInfo().GetSize())
		require.Equal(t, "alpha", string(item.body))
	}

	// The short-body branch (round two, n2). Over S3 the declared size is the
	// same response's Content-Length, so a short body is the HTTP layer's
	// ErrUnexpectedEOF before the accounting runs. On GCS the size comes from
	// the metadata read and the bytes from the media read, so a store whose
	// metadata says 10 bytes and whose media read ends cleanly after 5 reaches
	// the accounting itself — which must end the stream, not complete the item.
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	endpoint, c = composedStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("alt") == "media" || !strings.HasPrefix(r.URL.Path, "/storage/v1/") {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", "5")
			_, _ = fmt.Fprint(w, "alpha")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"bucket":"tenant","name":"a","etag":"e1","size":"10","generation":"1","metageneration":"1","updated":"2026-01-01T00:00:00Z"}`)
	})
	items, err = getMany(t, c, &storagev0.GetManyRequest{Binding: gcsTokenBinding(endpoint, "synthetic-token"),
		Items: []*storagev0.GetManyItem{{Key: "a"}, {Key: "b"}}})
	require.Equal(t, codes.Aborted, status.Code(err), "a body shorter than its declared size completed: %v", err)
	require.Contains(t, status.Convert(err).Message(), "ended at 5 of its declared 10 bytes")
	info, _ := requireReason(t, err, codes.Aborted, "BODY_SIZE_MISMATCH")
	require.Equal(t, map[string]string{"declared_bytes": "10", "sent_bytes": "5"}, info.GetMetadata())
	require.Len(t, items, 1, "the batch moved on past an incomplete item")
}

// r7: a missing bucket is not every object deleted. The stream ends with
// NOT_FOUND before any item outcome; a missing key still continues the batch.
func TestReviewMissingBucketContinuesBatch(t *testing.T) {
	endpoint, c := composedStore(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>`)
	})
	items, err := getMany(t, c, &storagev0.GetManyRequest{Binding: s3Binding(endpoint), Items: []*storagev0.GetManyItem{{Key: "a"}, {Key: "b"}}})
	require.Equal(t, codes.NotFound, status.Code(err), "got %v", err)
	require.Contains(t, status.Convert(err).Message(), "bucket or container does not exist")
	requireReason(t, err, codes.NotFound, "BUCKET_NOT_FOUND")
	require.Empty(t, items)

	endpoint, c = composedStore(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
	})
	items, err = getMany(t, c, &storagev0.GetManyRequest{Binding: s3Binding(endpoint), Items: []*storagev0.GetManyItem{{Key: "a"}, {Key: "b"}}})
	require.NoError(t, err)
	require.Len(t, items, 2)
	for _, item := range items {
		require.Equal(t, storagev0.GetManyOutcome_GET_MANY_OUTCOME_NOT_FOUND, item.header.GetOutcome())
	}
}

// A bound List of a missing bucket fails with NOT_FOUND on every bindable
// kind, never an empty listing: a host diffing listings would read an empty
// one as every object deleted (module-saas-starter#1211).
func TestBoundListOfAMissingBucketFailsOnEveryKind(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	s3Missing := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>`)
	}
	cases := map[string]struct {
		handler http.HandlerFunc
		binding func(endpoint string) *storagev0.Binding
	}{
		"s3": {s3Missing, s3Binding},
		"minio": {s3Missing, func(endpoint string) *storagev0.Binding {
			b := s3Binding(endpoint)
			b.Kind = "minio"
			return b
		}},
		"gcs": {func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":{"code":404,"message":"The specified bucket does not exist.","errors":[{"reason":"notFound"}]}}`)
		}, func(endpoint string) *storagev0.Binding { return gcsTokenBinding(endpoint, "synthetic-token") }},
		"azure": {func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			w.Header().Set("x-ms-error-code", "ContainerNotFound")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>ContainerNotFound</Code><Message>The specified container does not exist.</Message></Error>`)
		}, func(endpoint string) *storagev0.Binding {
			return &storagev0.Binding{Kind: "azure", Endpoint: endpoint, Bucket: "tenant", Credential: &storagev0.Credential{
				Kind: &storagev0.Credential_AccessKey{AccessKey: &storagev0.AccessKey{Id: "tenant", Secret: "c2VjcmV0"}}}}
		}},
	}
	for kind, tc := range cases {
		t.Run(kind, func(t *testing.T) {
			endpoint, c := composedStore(t, tc.handler)
			res, err := c.List(context.Background(), &storagev0.ListRequest{Binding: tc.binding(endpoint)})
			require.Equal(t, codes.NotFound, status.Code(err), "%s listed a missing bucket as %v, err %v", kind, res, err)
		})
	}
}

// r1: a bound GCS call authenticates with the caller's credential only. The
// gateway's ambient identity — GOOGLE_APPLICATION_CREDENTIALS, the metadata
// server — is never consulted, for an access token or a service-account key,
// opening a client (Capabilities) or reading through one (List).
//
// Round two (n1): GCP_STORAGE_GO_ENABLE_OTEL_METRICS=true would start the
// SDK's metrics exporter, which runs the same discovery; it is set here so
// the invariant holds with it on.
func TestReviewBoundGCSReadsAmbientCredentials(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	t.Setenv("GCP_STORAGE_GO_ENABLE_OTEL_METRICS", "true")
	fifo := filepath.Join(t.TempDir(), "ambient-credentials.json")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", fifo)
	var metadata atomic.Int32
	md := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { metadata.Add(1) }))
	defer md.Close()
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(md.URL, "http://"))

	opened := make(chan struct{})
	go func() {
		f, err := os.OpenFile(fifo, os.O_WRONLY, 0o600) // blocks until something reads the file
		if err == nil {
			close(opened)
			_, _ = f.Write([]byte(`{"type":"authorized_user","client_id":"ambient","client_secret":"ambient","refresh_token":"ambient"}`))
			_ = f.Close()
		}
	}()
	t.Cleanup(func() {
		if f, err := os.OpenFile(fifo, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
			_ = f.Close() // release the writer if nothing else did
		}
	})

	endpoint, c := composedStore(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"kind":"storage#objects","items":[]}`)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := &storagev0.Binding{Kind: "gcs", Endpoint: endpoint, Bucket: "tenant",
		Credential: &storagev0.Credential{Kind: &storagev0.Credential_AccessToken{AccessToken: "synthetic-caller-token"}}}
	_, err := c.Capabilities(ctx, &storagev0.CapabilitiesRequest{Binding: gcsTokenBinding("", "synthetic-caller-token")})
	require.NoError(t, err)
	_, err = c.List(ctx, &storagev0.ListRequest{Binding: key})
	require.NoError(t, err)

	select {
	case <-opened:
		t.Fatal("a bound GCS call opened GOOGLE_APPLICATION_CREDENTIALS")
	case <-time.After(200 * time.Millisecond):
	}
	require.Zero(t, metadata.Load(), "a bound GCS call contacted the metadata server")
}

// r8: the ETag a bound Azure List reports is accepted back as if_match: it is
// sent as a quoted entity tag, and the unchanged object is delivered.
func TestReviewAzureListedETagRoundTrip(t *testing.T) {
	var gotIfMatch string
	endpoint, c := composedStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("comp") == "list" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="http://example/" ContainerName="tenant"><Blobs><Blob><Name>a</Name><Properties><Last-Modified>Wed, 01 Jan 2025 00:00:00 GMT</Last-Modified><Etag>"0x8DFA"</Etag><Content-Length>5</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob></Blobs><NextMarker /></EnumerationResults>`)
			return
		}
		gotIfMatch = r.Header.Get("If-Match")
		if gotIfMatch == `"0x8DFA"` {
			w.Header().Set("ETag", `"0x8DFA"`)
			w.Header().Set("Content-Length", "5")
			_, _ = fmt.Fprint(w, "alpha")
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("x-ms-error-code", "ConditionNotMet")
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = fmt.Fprint(w, `<Error><Code>ConditionNotMet</Code><Message>invalid entity tag</Message></Error>`)
	})
	b := &storagev0.Binding{Kind: "azure", Endpoint: endpoint, Bucket: "tenant", Credential: &storagev0.Credential{
		Kind: &storagev0.Credential_AccessKey{AccessKey: &storagev0.AccessKey{Id: "tenant", Secret: "c2VjcmV0"}}}}
	listed, err := c.List(context.Background(), &storagev0.ListRequest{Binding: b})
	require.NoError(t, err)
	require.Len(t, listed.GetObjects(), 1)
	items, err := getMany(t, c, &storagev0.GetManyRequest{Binding: b, Items: []*storagev0.GetManyItem{{Key: "a", IfMatch: listed.GetObjects()[0].GetEtag()}}})
	require.NoError(t, err)
	require.Equal(t, `"0x8DFA"`, gotIfMatch)
	require.Equal(t, storagev0.GetManyOutcome_GET_MANY_OUTCOME_OK, items[0].header.GetOutcome())
	require.Equal(t, "alpha", string(items[0].body))
}
