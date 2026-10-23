package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
)

// slowDown answers every request the way S3 throttles, with Retry-After when
// given one.
func slowDown(retryAfter string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`)
	}
}

// A store's throttle names STORE_THROTTLED and carries a google.rpc.RetryInfo:
// the store's own Retry-After when it sent one, a marked default otherwise.
func TestThrottleNamesItsReasonAndRetryDelay(t *testing.T) {
	endpoint, c := composedStore(t, slowDown("7"))
	_, err := c.List(context.Background(), &storagev0.ListRequest{Binding: s3Binding(endpoint)})
	info, retry := requireReason(t, err, codes.ResourceExhausted, "STORE_THROTTLED")
	require.NotNil(t, retry, "a throttle carries no retry information")
	require.Equal(t, 7*time.Second, retry.GetRetryDelay().AsDuration())
	require.Equal(t, "store", info.GetMetadata()["retry_delay_source"])

	endpoint, c = composedStore(t, slowDown(""))
	_, err = c.List(context.Background(), &storagev0.ListRequest{Binding: s3Binding(endpoint)})
	info, retry = requireReason(t, err, codes.ResourceExhausted, "STORE_THROTTLED")
	require.NotNil(t, retry)
	require.Equal(t, time.Second, retry.GetRetryDelay().AsDuration())
	require.Equal(t, "default", info.GetMetadata()["retry_delay_source"])
}

// The reason exists because the code cannot tell these apart: a throttle and
// the gateway's own size cap are both RESOURCE_EXHAUSTED — one says back off,
// the other skip the object — and only the reason, not the prose, separates
// them. The message is unchanged by the reason beside it.
func TestTheSameCodeNamesDifferentReasons(t *testing.T) {
	endpoint, throttled := composedStore(t, slowDown("2"))
	_, throttle := throttled.Stat(context.Background(), &storagev0.StatRequest{Key: "a", Binding: s3Binding(endpoint)})

	endpoint, c := composedStore(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", fmt.Sprint(6<<20))
		_, _ = io.WriteString(w, strings.Repeat("x", 6<<20))
	})
	_, _, capped := getObject(t, c, &storagev0.GetRequest{Key: "big.bin", Binding: s3Binding(endpoint)})

	require.Equal(t, status.Code(throttle), status.Code(capped), "both are RESOURCE_EXHAUSTED")
	_, retry := requireReason(t, throttle, codes.ResourceExhausted, "STORE_THROTTLED")
	require.NotNil(t, retry)
	info, retry := requireReason(t, capped, codes.ResourceExhausted, "OBJECT_TOO_LARGE")
	require.Nil(t, retry, "a cap is not retried")
	require.Equal(t, fmt.Sprint(5<<20), info.GetMetadata()["max_bytes"])
	require.Equal(t, "object exceeds the bound-read cap of 5242880 bytes", status.Convert(capped).Message())
}

// A store that refuses the credential names CREDENTIAL_REJECTED (re-authorise)
// where the guard's refusal names DESTINATION_REFUSED (refuse the source) —
// both PERMISSION_DENIED.
func TestStoreRejectedCredentialNamesItsReason(t *testing.T) {
	endpoint, c := composedStore(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<Error><Code>InvalidAccessKeyId</Code><Message>The access key you provided does not exist.</Message></Error>`)
	})
	_, err := c.List(context.Background(), &storagev0.ListRequest{Binding: s3Binding(endpoint)})
	requireReason(t, err, codes.PermissionDenied, "CREDENTIAL_REJECTED")

	_, err = newTestClient(t).List(context.Background(), &storagev0.ListRequest{Binding: s3Binding("http://localhost:9000")})
	requireReason(t, err, codes.PermissionDenied, "DESTINATION_REFUSED")
}

// Refusals on the gateway's own bucket name their reasons too.
func TestUnboundRefusalsNameTheirReasons(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	_, err := c.Stat(ctx, &storagev0.StatRequest{Key: "missing"})
	requireReason(t, err, codes.NotFound, "OBJECT_NOT_FOUND")

	_, err = putObject(t, c, "a", []byte("alpha"), nil)
	require.NoError(t, err)
	_, err = c.Stat(ctx, &storagev0.StatRequest{Key: "a", IfMatch: "stale"})
	requireReason(t, err, codes.FailedPrecondition, "PRECONDITION_FAILED")
	require.Equal(t, "if-match: etag mismatch", status.Convert(err).Message())
}

// A Watch consumer that falls behind is dropped with RESOURCE_EXHAUSTED,
// naming WATCH_CONSUMER_BEHIND — not a throttle, and not to be retried as one.
func TestWatchDropNamesItsReason(t *testing.T) {
	c := newTestClient(t)
	stream, _ := startWatch(t, c, "")
	keys := make([]string, 20000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	_, err := c.DeleteMany(context.Background(), &storagev0.DeleteManyRequest{Keys: keys})
	require.NoError(t, err)
	for {
		if _, err = stream.Recv(); err != nil {
			break
		}
	}
	info, retry := requireReason(t, err, codes.ResourceExhausted, "WATCH_CONSUMER_BEHIND")
	require.NotNil(t, info)
	require.Nil(t, retry)
	require.Equal(t, "watch: consumer fell behind", status.Convert(err).Message())
}
