package server_test

import (
	"context"
	"fmt"
	"io"
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
	"github.com/codefly-dev/service-object-storage/internal/egress"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
)

// manyItem is one item as a GetMany stream delivered it.
type manyItem struct {
	header *storagev0.GetManyHeader
	body   []byte
}

func getMany(t *testing.T, c storagev0.ObjectStorageClient, req *storagev0.GetManyRequest) ([]manyItem, error) {
	t.Helper()
	st, err := c.GetMany(context.Background(), req)
	require.NoError(t, err)
	var items []manyItem
	for {
		msg, rerr := st.Recv()
		if rerr == io.EOF {
			return items, nil
		}
		if rerr != nil {
			return items, rerr
		}
		if h := msg.GetHeader(); h != nil {
			items = append(items, manyItem{header: h})
			continue
		}
		require.NotEmpty(t, items, "data before any header")
		items[len(items)-1].body = append(items[len(items)-1].body, msg.GetData()...)
	}
}

// One call returns every item in request order, each body exactly its size,
// and an item that changed or vanished since it was listed is reported in its
// own header while the rest are still delivered.
func TestGetManyDeliversEachItemInOrderAtItsListedETag(t *testing.T) {
	c := newTestClient(t)
	a, err := putObject(t, c, "docs/a.md", []byte("alpha"), nil)
	require.NoError(t, err)
	_, err = putObject(t, c, "docs/b.md", []byte("bravo"), nil)
	require.NoError(t, err)
	big := []byte(strings.Repeat("x", 600*1024)) // spans several chunks
	bigPut, err := putObject(t, c, "docs/big.bin", big, nil)
	require.NoError(t, err)

	items, err := getMany(t, c, &storagev0.GetManyRequest{Items: []*storagev0.GetManyItem{
		{Key: "docs/big.bin", IfMatch: bigPut.GetEtag()},
		{Key: "docs/b.md", IfMatch: "stale-etag"},
		{Key: "docs/gone.md"},
		{Key: "docs/a.md", IfMatch: a.GetEtag()},
	}})
	require.NoError(t, err)
	require.Len(t, items, 4)
	ok, changed, missing := storagev0.GetManyOutcome_GET_MANY_OUTCOME_OK,
		storagev0.GetManyOutcome_GET_MANY_OUTCOME_CHANGED, storagev0.GetManyOutcome_GET_MANY_OUTCOME_NOT_FOUND
	for i, want := range []storagev0.GetManyOutcome{ok, changed, missing, ok} {
		require.EqualValues(t, i, items[i].header.GetIndex())
		require.Equal(t, want, items[i].header.GetOutcome(), "item %d", i)
	}
	require.Equal(t, big, items[0].body)
	require.EqualValues(t, len(big), items[0].header.GetInfo().GetSize())
	require.Empty(t, items[1].body)
	require.Empty(t, items[2].body)
	require.Equal(t, "alpha", string(items[3].body))
	require.Equal(t, a.GetEtag(), items[3].header.GetInfo().GetEtag())
}

func TestGetManyRefusesMoreItemsThanCapabilitiesAllows(t *testing.T) {
	c := newTestClient(t)
	caps, err := c.Capabilities(context.Background(), &storagev0.CapabilitiesRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 1000, caps.GetMaxGetManyItems())

	items := make([]*storagev0.GetManyItem, caps.GetMaxGetManyItems()+1)
	for i := range items {
		items[i] = &storagev0.GetManyItem{Key: fmt.Sprintf("k%d", i)}
	}
	_, err = getMany(t, c, &storagev0.GetManyRequest{Items: items})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "got %v", err)
	info, _ := requireReason(t, err, codes.InvalidArgument, "TOO_MANY_ITEMS")
	require.Equal(t, "1000", info.GetMetadata()["max_items"])
}

// fakeS3 answers GetObject path-style the way S3 does for the cases a bound
// GetMany distinguishes: a match, an If-Match mismatch (412), a missing key
// (404), and an object over the bound cap.
func fakeS3(t *testing.T) *httptest.Server {
	t.Helper()
	objects := map[string]string{"/tenant/docs/a.md": "alpha", "/tenant/docs/huge.bin": strings.Repeat("x", 5<<20+1)}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := objects[r.URL.Path]
		switch {
		case !ok:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
		case r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != `"etag-1"`:
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
		default:
			w.Header().Set("ETag", `"etag-1"`)
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			_, _ = w.Write([]byte(body))
		}
	}))
}

// The bound path runs through a real SDK: its 404 and 412 become the item
// outcomes, and an object over the bound cap is TOO_LARGE without its bytes
// being relayed.
func TestBoundGetManyMapsStoreAnswersToOutcomes(t *testing.T) {
	store := fakeS3(t)
	defer store.Close()
	endpoint := named(store)
	composed, err := egress.ParseComposed(strings.TrimPrefix(endpoint, "http://"))
	require.NoError(t, err)
	be, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	c := newTestClientForServer(t, server.New(be, events.NewHub(), probetest.Monitor(t, be)).AdmitComposed(composed))

	items, err := getMany(t, c, &storagev0.GetManyRequest{Binding: s3Binding(endpoint), Items: []*storagev0.GetManyItem{
		{Key: "a.md", IfMatch: `"etag-1"`},
		{Key: "a.md", IfMatch: `"etag-0"`},
		{Key: "gone.md"},
		{Key: "huge.bin"},
	}})
	require.NoError(t, err)
	require.Len(t, items, 4)
	require.Equal(t, storagev0.GetManyOutcome_GET_MANY_OUTCOME_OK, items[0].header.GetOutcome())
	require.Equal(t, "alpha", string(items[0].body))
	require.Equal(t, storagev0.GetManyOutcome_GET_MANY_OUTCOME_CHANGED, items[1].header.GetOutcome())
	require.Equal(t, storagev0.GetManyOutcome_GET_MANY_OUTCOME_NOT_FOUND, items[2].header.GetOutcome())
	require.Equal(t, storagev0.GetManyOutcome_GET_MANY_OUTCOME_TOO_LARGE, items[3].header.GetOutcome())
	require.Empty(t, items[3].body)

	// The guard still decides the endpoint: the same request without the
	// composed admission ends the stream before any item.
	_, err = getMany(t, newTestClient(t), &storagev0.GetManyRequest{Binding: s3Binding(endpoint), Items: []*storagev0.GetManyItem{{Key: "a.md"}}})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "got %v", err)
}
