package server_test

// The independent review of module-saas-starter#1235 found four defects in
// this service the host connector cannot work around. Its probes are kept
// here, under TestReview1235 names and their original scenarios, asserting
// the invariant each fix restored.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

// (B) A binding's prefix is a literal key prefix: "/reports/" lists keys
// beginning "/reports/", and "/" is not the whole bucket. A bound read's
// result can authorize deletions, so its scope is never rewritten.
func TestReview1235LeadingSlashChangesListedPrefix(t *testing.T) {
	for _, prefix := range []string{"/reports/", "/", "reports", "a//b/"} {
		t.Run(prefix, func(t *testing.T) {
			sent := make(chan string, 1)
			endpoint, c := composedStore(t, func(w http.ResponseWriter, r *http.Request) {
				sent <- r.URL.Query().Get("prefix")
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			})
			b := s3Binding(endpoint)
			b.Prefix = prefix
			_, err := c.List(context.Background(), &storagev0.ListRequest{Binding: b})
			require.NoError(t, err)
			require.Equal(t, prefix, <-sent, "the binding's prefix was rewritten before it reached the store")
		})
	}
}

// (A) A page marked truncated with no continuation token is an incomplete
// listing, and is refused rather than read as the last, empty page — on the
// s3 backend, and on minio, whose SDK refuses it too.
func TestReview1235TruncatedWithoutTokenAcceptedComplete(t *testing.T) {
	for _, kind := range []string{"s3", "minio"} {
		t.Run(kind, func(t *testing.T) {
			endpoint, c := composedStore(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>`)
			})
			b := s3Binding(endpoint)
			b.Kind = kind
			res, err := c.List(context.Background(), &storagev0.ListRequest{Binding: b})
			require.Error(t, err, "an incomplete listing was answered as %v", res)
			require.NotEqual(t, codes.OK, status.Code(err))
			require.Contains(t, strings.ToLower(status.Convert(err).Message()), "truncated")
		})
	}
}

// (C) A store cannot make the gateway hold an unbounded listing: a response
// body past the bound fails, and so does a page with more entries than it was
// asked for.
func TestReview1235ListBodyExceedsOldFiveMiBLimit(t *testing.T) {
	endpoint, c := composedStore(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "<ListBucketResult>"+strings.Repeat(" ", 6<<20)+"<IsTruncated>false</IsTruncated></ListBucketResult>")
	})
	_, err := c.List(context.Background(), &storagev0.ListRequest{Binding: s3Binding(endpoint)})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "got %v", err)
	require.Contains(t, status.Convert(err).Message(), "exceeded")
}

func TestReview1235ListDoesNotEnforceReturnedObjectLimit(t *testing.T) {
	endpoint, c := composedStore(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "<ListBucketResult>")
		for i := 0; i < 1001; i++ {
			_, _ = fmt.Fprintf(w, "<Contents><Key>docs/%d</Key><ETag>x</ETag><Size>1</Size></Contents>", i)
		}
		_, _ = fmt.Fprint(w, "<IsTruncated>false</IsTruncated></ListBucketResult>")
	})
	res, err := c.List(context.Background(), &storagev0.ListRequest{Binding: s3Binding(endpoint)})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "got %d objects, %v", len(res.GetObjects()), err)
	require.Contains(t, status.Convert(err).Message(), "1001 entries for a page of at most 1000")
}

// (D) Every bound operation has a finite end-to-end deadline, body included:
// a store that sends headers and then stalls fails the operation instead of
// holding the caller. Shown for a List body and a GetMany item body.
func TestReview1235SlowBodyOutlivesOldTimeout(t *testing.T) {
	defer server.SetBoundOperationTimeout(300 * time.Millisecond)()
	stall := func(w http.ResponseWriter, r *http.Request, head string) {
		_, _ = fmt.Fprint(w, head)
		w.(http.Flusher).Flush()
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	}
	endpoint, c := composedStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") == "2" {
			stall(w, r, "<ListBucketResult>")
			return
		}
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", "10")
		stall(w, r, "alpha")
	})
	start := time.Now()
	_, err := c.List(context.Background(), &storagev0.ListRequest{Binding: s3Binding(endpoint)})
	require.Error(t, err)
	require.Less(t, time.Since(start), 3*time.Second, "a stalled listing body held the call")
	require.Contains(t, status.Convert(err).Message(), "did not complete the operation within")

	start = time.Now()
	_, err = getMany(t, c, &storagev0.GetManyRequest{Binding: s3Binding(endpoint), Items: []*storagev0.GetManyItem{{Key: "a"}}})
	require.Error(t, err)
	require.Less(t, time.Since(start), 3*time.Second, "a stalled object body held the batch")
}
