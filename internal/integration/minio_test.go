//go:build integration

// Package integration exercises the full stack — gRPC server → real
// backend — against a live MinIO, the universal local/test backend. Run with:
//
//	go test -tags integration ./internal/integration/...
//
// It requires a running MinIO reachable via env:
//
//	MINIO_ENDPOINT (host:port), MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
//
// The test never mocks — this is the "test on MinIO, ship on S3" guarantee in
// practice: the same gRPC surface a client uses in production, over real infra.
package integration

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	objectstorage "github.com/codefly-dev/service-object-storage/libs/go"
	"github.com/codefly-dev/service-object-storage/libs/go/miniosession"
	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/egress"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"

	_ "github.com/codefly-dev/service-object-storage/internal/backend/minio"
)

func mustEnv(t *testing.T, key string) string {
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s not set; skipping MinIO integration test", key)
	}
	return v
}

func ensureBucket(t *testing.T, endpoint, ak, sk, bucket string) {
	t.Helper()
	cl, err := miniogo.New(endpoint, &miniogo.Options{
		Creds:  miniocreds.NewStaticV4(ak, sk, ""),
		Secure: false,
	})
	require.NoError(t, err)
	exists, err := cl.BucketExists(context.Background(), bucket)
	require.NoError(t, err)
	if !exists {
		require.NoError(t, cl.MakeBucket(context.Background(), bucket, miniogo.MakeBucketOptions{}))
	}
}

func newStack(t *testing.T) storagev0.ObjectStorageClient {
	t.Helper()
	endpoint := mustEnv(t, "MINIO_ENDPOINT")
	ak := mustEnv(t, "MINIO_ACCESS_KEY")
	sk := mustEnv(t, "MINIO_SECRET_KEY")
	bucket := mustEnv(t, "MINIO_BUCKET")
	ensureBucket(t, endpoint, ak, sk, bucket)

	be, err := backend.Open(context.Background(), backend.Config{
		Kind:         "minio",
		Endpoint:     endpoint,
		Region:       "us-east-1",
		AccessKey:    ak,
		SecretKey:    sk,
		Bucket:       bucket,
		UsePathStyle: true,
	})
	require.NoError(t, err)
	hub := events.NewHub()
	// The MinIO is on loopback, so a bound call can reach it only as a
	// composed endpoint; the configured-bucket tests never consult this.
	composed, err := egress.ParseComposed(byName(endpoint))
	require.NoError(t, err)

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(s, server.New(be, hub, probetest.Monitor(t, be)).AdmitComposed(composed))
	go func() { _ = s.Serve(lis) }()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); s.Stop(); hub.Close(); _ = be.Close() })
	return storagev0.NewObjectStorageClient(conn)
}

func put(t *testing.T, c storagev0.ObjectStorageClient, key string, data []byte, h *storagev0.PutHeader) (*storagev0.PutResult, error) {
	if h == nil {
		h = &storagev0.PutHeader{}
	}
	h.Key = key
	h.TotalSize = int64(len(data))
	st, err := c.Put(context.Background())
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: h}}))
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: data}}))
	return st.CloseAndRecv()
}

func get(t *testing.T, c storagev0.ObjectStorageClient, key string) ([]byte, *storagev0.GetHeader) {
	st, err := c.Get(context.Background(), &storagev0.GetRequest{Key: key})
	require.NoError(t, err)
	var body []byte
	var hdr *storagev0.GetHeader
	for {
		msg, rerr := st.Recv()
		if rerr == io.EOF {
			break
		}
		require.NoError(t, rerr)
		if h := msg.GetHeader(); h != nil {
			hdr = h
		}
		body = append(body, msg.GetData()...)
	}
	return body, hdr
}

func TestMinIOFullStack(t *testing.T) {
	c := newStack(t)
	key := "integration/hello.txt"
	payload := []byte("real minio, real bytes")

	pr, err := put(t, c, key, payload, &storagev0.PutHeader{ContentType: "text/plain"})
	require.NoError(t, err)
	require.NotEmpty(t, pr.GetEtag())

	body, hdr := get(t, c, key)
	require.Equal(t, payload, body)
	require.Equal(t, "text/plain", hdr.GetInfo().GetContentType())
	require.Equal(t, int64(len(payload)), hdr.GetInfo().GetSize())

	// A second read must match.
	body2, _ := get(t, c, key)
	require.Equal(t, payload, body2)

	// Presign returns a usable URL.
	ps, err := c.Presign(context.Background(), &storagev0.PresignRequest{
		Key: key, Method: storagev0.PresignMethod_PRESIGN_METHOD_GET, ExpirySeconds: 600,
	})
	require.NoError(t, err)
	require.Contains(t, ps.GetUrl(), key)

	// Copy then list under the prefix.
	_, err = c.Copy(context.Background(), &storagev0.CopyRequest{SourceKey: key, DestKey: "integration/copy.txt"})
	require.NoError(t, err)
	lr, err := c.List(context.Background(), &storagev0.ListRequest{Prefix: "integration/"})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(lr.GetObjects()), 2)

	// Cleanup.
	_, err = c.DeleteMany(context.Background(), &storagev0.DeleteManyRequest{
		Keys: []string{key, "integration/copy.txt"},
	})
	require.NoError(t, err)
}

// putUnsized streams a body without declaring total_size, as a client that
// leaves the proto3 field unset does.
func putUnsized(t *testing.T, c storagev0.ObjectStorageClient, h *storagev0.PutHeader, chunks ...[]byte) (*storagev0.PutResult, error) {
	st, err := c.Put(context.Background())
	require.NoError(t, err)
	require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: h}}))
	for _, chunk := range chunks {
		require.NoError(t, st.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: chunk}}))
	}
	return st.CloseAndRecv()
}

// TestMinIOPutWithoutTotalSize is the regression against the store where it
// happened: an unset total_size reached minio-go as an exact length of 0, and
// the object was stored empty with no error.
func TestMinIOPutWithoutTotalSize(t *testing.T) {
	c := newStack(t)
	key := "integration/unsized.bin"
	payload := bytes.Repeat([]byte("unsized body "), 20_000)
	_, err := putUnsized(t, c, &storagev0.PutHeader{Key: key}, payload[:100_000], payload[100_000:])
	require.NoError(t, err)
	body, hdr := get(t, c, key)
	require.Equal(t, int64(len(payload)), hdr.GetInfo().GetSize())
	require.True(t, bytes.Equal(payload, body))
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: key})
	require.NoError(t, err)
}

// TestMinIOCreateIfAbsentWithoutTotalSize: a body of unknown size that fits
// the gateway's size probe is written in one request, so create-if-absent
// stays atomic and is enforced.
func TestMinIOCreateIfAbsentWithoutTotalSize(t *testing.T) {
	c := newStack(t)
	key := "integration/create-once.txt"
	_, err := putUnsized(t, c, &storagev0.PutHeader{Key: key, IfNoneMatch: "*"}, []byte("first"))
	require.NoError(t, err)
	_, err = putUnsized(t, c, &storagev0.PutHeader{Key: key, IfNoneMatch: "*"}, []byte("second"))
	require.Error(t, err, "create-if-absent must refuse an existing key")
	body, _ := get(t, c, key)
	require.Equal(t, []byte("first"), body)
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: key})
	require.NoError(t, err)
}

// TestMinIOConditionalDelete asserts the compare-and-delete precondition is
// honored against MinIO — a wrong If-Match must refuse and preserve the object,
// not silently delete it unconditionally.
func TestMinIOConditionalDelete(t *testing.T) {
	c := newStack(t)
	key := "integration/conditional-delete.txt"

	pr, err := put(t, c, key, []byte("v1"), nil)
	require.NoError(t, err)
	etag := pr.GetEtag()
	require.NotEmpty(t, etag)

	// A compare-and-delete against a missing object fails its precondition.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: "integration/never-existed", IfMatch: etag})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	// A stale precondition must fail and leave the object intact.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: key, IfMatch: `"00000000000000000000000000000000"`})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, hdr := get(t, c, key)
	require.Equal(t, etag, hdr.GetInfo().GetEtag(), "object must survive a failed conditional delete")

	// The matching precondition deletes.
	_, err = c.Delete(context.Background(), &storagev0.DeleteRequest{Key: key, IfMatch: etag})
	require.NoError(t, err)

	st, err := c.Get(context.Background(), &storagev0.GetRequest{Key: key})
	require.NoError(t, err)
	_, rerr := st.Recv()
	require.Equal(t, codes.NotFound, status.Code(rerr), "object must be gone after a matching conditional delete")
}

// TestMinIOBoundGetMany lists and batch-reads the same MinIO through a
// per-call binding: the ETags MinIO lists are accepted back as if_match, the
// batch comes back in request order, and a stale ETag is CHANGED.
func TestMinIOBoundGetMany(t *testing.T) {
	c := newStack(t)
	ctx := context.Background()
	prefix := "bound-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "/"
	for _, key := range []string{"a.md", "b.md"} {
		_, err := put(t, c, prefix+key, []byte("# "+key), nil)
		require.NoError(t, err)
	}
	binding := &storagev0.Binding{Kind: "minio", Endpoint: "http://" + byName(mustEnv(t, "MINIO_ENDPOINT")),
		Bucket: mustEnv(t, "MINIO_BUCKET"), Prefix: prefix,
		Credential: &storagev0.Credential{Kind: &storagev0.Credential_AccessKey{AccessKey: &storagev0.AccessKey{
			Id: mustEnv(t, "MINIO_ACCESS_KEY"), Secret: mustEnv(t, "MINIO_SECRET_KEY")}}}}

	lr, err := c.List(ctx, &storagev0.ListRequest{Binding: binding})
	require.NoError(t, err)
	etags := map[string]string{}
	for _, o := range lr.GetObjects() {
		etags[o.GetKey()] = o.GetEtag()
	}
	require.Len(t, etags, 2)

	ms, err := c.GetMany(ctx, &storagev0.GetManyRequest{Binding: binding, Items: []*storagev0.GetManyItem{
		{Key: "b.md", IfMatch: etags["b.md"]},
		{Key: "a.md", IfMatch: "stale"},
		{Key: "a.md", IfMatch: etags["a.md"]},
	}})
	require.NoError(t, err)
	var outcomes []storagev0.GetManyOutcome
	var payloads []string
	for {
		msg, rerr := ms.Recv()
		if rerr == io.EOF {
			break
		}
		require.NoError(t, rerr)
		if h := msg.GetHeader(); h != nil {
			outcomes, payloads = append(outcomes, h.GetOutcome()), append(payloads, "")
			continue
		}
		payloads[len(payloads)-1] += string(msg.GetData())
	}
	require.Equal(t, []storagev0.GetManyOutcome{
		storagev0.GetManyOutcome_GET_MANY_OUTCOME_OK,
		storagev0.GetManyOutcome_GET_MANY_OUTCOME_CHANGED,
		storagev0.GetManyOutcome_GET_MANY_OUTCOME_OK,
	}, outcomes)
	require.Equal(t, []string{"# b.md", "", "# a.md"}, payloads)
}

// byName is a loopback host:port as a composed endpoint names it: by
// localhost, never by address.
func byName(endpoint string) string {
	return "localhost:" + endpoint[strings.LastIndex(endpoint, ":")+1:]
}

func TestMinIOIssuedSessionConfinesGatewayReads(t *testing.T) {
	c := newStack(t)
	ctx := context.Background()
	prefix := "issued-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "/"
	issuer := miniosession.Issuer{
		Endpoint: "http://" + byName(mustEnv(t, "MINIO_ENDPOINT")), Region: "us-east-1", Bucket: mustEnv(t, "MINIO_BUCKET"),
		Prefix: prefix, AccessKey: mustEnv(t, "MINIO_ACCESS_KEY"), SecretKey: mustEnv(t, "MINIO_SECRET_KEY"),
	}
	binding := objectstorage.Binding{Partition: "org", Principal: "human:amy", Reach: []string{"east", "west"}, Mode: objectstorage.Strict, Access: objectstorage.ReadWrite}
	_, err := issuer.Issue(ctx, "files", binding, "elsewhere")
	require.ErrorIs(t, err, objectstorage.ErrOutsideReach)
	bound, err := issuer.Issue(ctx, "files", binding, "east")
	require.NoError(t, err)
	codec, err := objectkey.New("files")
	require.NoError(t, err)
	store, err := objectstorage.NewStore(c, "files", objectstorage.FixedBinder(binding), objectstorage.WithReaderSessions(issuer))
	require.NoError(t, err)
	for _, row := range []struct {
		scope, principal string
		allowed          bool
	}{
		{"east", "human:amy", true}, {"east", "human:bob", false}, {"west", "human:amy", false},
	} {
		key, err := codec.Key(objectkey.Key{Partition: "org", Scope: row.scope, Slot: objectkey.Principal(row.principal), Name: "test.txt"})
		require.NoError(t, err)
		_, err = put(t, c, prefix+key, []byte("stored"), nil)
		require.NoError(t, err)
		_, err = c.Stat(ctx, &storagev0.StatRequest{Binding: bound, Key: key})
		if row.allowed {
			require.NoError(t, err)
		} else {
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		}
	}
	ref := objectstorage.Ref{Scope: "east", Name: "test.txt"}
	_, err = store.Head(ctx, ref)
	require.NoError(t, err)
	body, _, err := store.Get(ctx, ref)
	require.NoError(t, err)
	defer body.Close()
	content, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, "stored", string(content))
	page, err := store.List(ctx, objectstorage.Ref{Scope: "east"}, objectstorage.ListOptions{})
	require.NoError(t, err)
	require.Len(t, page.Objects, 1)

}

// boundGet reads key through the gateway's bound Get with the caller's
// credential, and answers what the store answered.
func boundGet(ctx context.Context, c storagev0.ObjectStorageClient, bound *storagev0.Binding, key string) ([]byte, error) {
	stream, err := c.Get(ctx, &storagev0.GetRequest{Binding: bound, Key: key})
	if err != nil {
		return nil, err
	}
	var body []byte
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return body, nil
		}
		if err != nil {
			return nil, err
		}
		body = append(body, message.GetData()...)
	}
}

// Evidence for MinIO only: the pinned MinIO and miniosession's AssumeRole
// session policy. It says nothing of S3, GCS or Azure, and nothing of a
// consumer's delivery path: Document Store reads and presigns with the
// gateway's own bucket-wide credential and issues no session today, so this
// shows what a session over its key layout would reach, beside its route and
// not on it.
//
// The layout is Document Store's own (its storage.ContentScope and
// storage.DeriveKey): base "documents-content", partition = tenant, scope =
// installation, a Strict binding of the principal "service:documents",
// content by digest in the content slot, and the legacy copy every write
// still makes at "<tenant>/blobs/<digest>". The session is issued by the real
// issuer for one installation. It reads and stats the installation's content
// by digest and lists its own slot; it is refused Get, Stat and List in a
// sibling installation of the same tenant (the same bytes there included),
// in another tenant and at the legacy keys, and no parent prefix lists for
// it, so no sibling is enumerated. The digest is the capability inside the
// scope: the content slot itself does not list. A session is read-only, so
// held by a client on MinIO itself it is refused Put, Copy (as source or as
// destination) and Delete everywhere, its own scope included, and every
// object is afterwards as it was written.
func TestMinIOIssuedSessionCannotLeaveADocumentsContentScope(t *testing.T) {
	c := newStack(t)
	ctx := context.Background()
	const base, principal = "documents-content", "service:documents"
	endpoint, bucket := mustEnv(t, "MINIO_ENDPOINT"), mustEnv(t, "MINIO_BUCKET")
	run := strconv.FormatInt(time.Now().UnixNano(), 36)
	tenant, otherTenant := "tenant-a-"+run, "tenant-b-"+run
	const installation, sibling = "installation-a", "installation-b"
	codec, err := objectkey.New(base)
	require.NoError(t, err)

	// Written as Document Store writes: PutContent through a Store bound as
	// its PutScoped binds, then the legacy copy.
	stored := map[string]string{}
	content := func(tenant, scope, body string) string {
		t.Helper()
		binding := objectstorage.Binding{Partition: tenant, Principal: principal, Reach: []string{scope}, Mode: objectstorage.Strict, Access: objectstorage.ReadWrite}
		store, err := objectstorage.NewStore(c, base, objectstorage.FixedBinder(binding))
		require.NoError(t, err)
		info, err := store.PutContent(ctx, scope, strings.NewReader(body), objectstorage.PutOptions{})
		require.NoError(t, err)
		key, err := codec.Key(objectkey.Key{Partition: tenant, Scope: scope, Slot: objectkey.Content(), Name: info.Ref.Name})
		require.NoError(t, err)
		legacy := tenant + "/blobs/" + info.Ref.Name
		_, err = put(t, c, legacy, []byte(body), nil)
		require.NoError(t, err)
		stored[key], stored[legacy] = body, body
		return key
	}
	first := content(tenant, installation, "first document of installation a "+run)
	second := content(tenant, installation, "second document of installation a "+run)
	inSibling := content(tenant, sibling, "document of installation b "+run)
	// The same bytes in the sibling installation: the same digest, another key.
	sameBytes := content(tenant, sibling, "first document of installation a "+run)
	inOtherTenant := content(otherTenant, installation, "first document of installation a "+run)
	digest := first[strings.LastIndex(first, "/")+1:]
	require.Equal(t, digest, sameBytes[strings.LastIndex(sameBytes, "/")+1:])
	require.NotEqual(t, first, sameBytes)
	legacy, otherLegacy := tenant+"/blobs/"+digest, otherTenant+"/blobs/"+digest
	slot := func(tenant, scope string, slot objectkey.Slot) string {
		prefix, err := codec.SlotPrefix(tenant, scope, slot)
		require.NoError(t, err)
		return prefix
	}
	own := slot(tenant, installation, objectkey.Principal(principal))
	_, err = put(t, c, own+"note.txt", []byte("own"), nil)
	require.NoError(t, err)
	stored[own+"note.txt"] = "own"
	scopeOf := func(tenant, scope string) string {
		prefix, err := codec.ScopePrefix(tenant, scope)
		require.NoError(t, err)
		return prefix
	}
	partition, err := codec.PartitionPrefix(tenant)
	require.NoError(t, err)
	otherPartition, err := codec.PartitionPrefix(otherTenant)
	require.NoError(t, err)

	issuer := miniosession.Issuer{
		Endpoint: "http://" + byName(endpoint), Region: "us-east-1", Bucket: bucket,
		AccessKey: mustEnv(t, "MINIO_ACCESS_KEY"), SecretKey: mustEnv(t, "MINIO_SECRET_KEY"),
	}
	binding := objectstorage.Binding{Partition: tenant, Principal: principal, Reach: []string{installation}, Mode: objectstorage.Strict, Access: objectstorage.ReadWrite}
	_, err = issuer.Issue(ctx, base, binding, sibling)
	require.ErrorIs(t, err, objectstorage.ErrOutsideReach)
	bound, err := issuer.Issue(ctx, base, binding, installation)
	require.NoError(t, err)

	// Inside the scope, through the gateway's bound reads.
	for _, key := range []string{first, second} {
		info, err := c.Stat(ctx, &storagev0.StatRequest{Binding: bound, Key: key})
		require.NoError(t, err, "stat %s", key)
		require.Equal(t, int64(len(stored[key])), info.GetInfo().GetSize())
		body, err := boundGet(ctx, c, bound, key)
		require.NoError(t, err, "get %s", key)
		require.Equal(t, stored[key], string(body))
	}
	listed, err := c.List(ctx, &storagev0.ListRequest{Binding: bound, Prefix: own})
	require.NoError(t, err)
	var names []string
	for _, object := range listed.GetObjects() {
		names = append(names, object.GetKey())
	}
	require.Contains(t, names, own+"note.txt")
	for _, name := range names {
		require.True(t, strings.HasPrefix(name, own), "the own slot listed %s", name)
	}

	// Outside it, through the same bound reads.
	denied := func(what string, err error) {
		t.Helper()
		require.Equal(t, codes.PermissionDenied, status.Code(err), "%s: %v", what, err)
	}
	for what, key := range map[string]string{
		"a sibling installation's content":                 inSibling,
		"the same bytes in a sibling installation":         sameBytes,
		"another tenant's content":                         inOtherTenant,
		"the legacy copy of its own content":               legacy,
		"another tenant's legacy copy":                     otherLegacy,
		"a key of the sibling's own slot":                  slot(tenant, sibling, objectkey.Principal(principal)) + "note.txt",
		"its own digest under the sibling's scope by name": scopeOf(tenant, sibling) + "_sha256/" + second[strings.LastIndex(second, "/")+1:],
	} {
		_, err := c.Stat(ctx, &storagev0.StatRequest{Binding: bound, Key: key})
		denied("stat "+what, err)
		body, err := boundGet(ctx, c, bound, key)
		denied("get "+what, err)
		require.Empty(t, body, "get %s delivered bytes", what)
	}
	for what, prefix := range map[string]string{
		"its own content slot":                     slot(tenant, installation, objectkey.Content()),
		"its own scope":                            scopeOf(tenant, installation),
		"the sibling installation":                 scopeOf(tenant, sibling),
		"the sibling's content slot":               slot(tenant, sibling, objectkey.Content()),
		"the sibling's own slot":                   slot(tenant, sibling, objectkey.Principal(principal)),
		"the tenant, parent of both":               partition,
		"another tenant":                           otherPartition,
		"another tenant's same installation":       scopeOf(otherTenant, installation),
		"the base":                                 codec.BasePrefix(),
		"the bucket":                               "",
		"the legacy prefix":                        tenant + "/blobs/",
		"the legacy tenant":                        tenant + "/",
		"another tenant's legacy prefix":           otherTenant + "/blobs/",
		"its own slot without the separator":       strings.TrimSuffix(own, "/"),
		"the scope its installation's name begins": strings.TrimSuffix(scopeOf(tenant, installation), "/"),
	} {
		for _, delimiter := range []string{"", "/"} {
			page, err := c.List(ctx, &storagev0.ListRequest{Binding: bound, Prefix: prefix, Delimiter: delimiter})
			denied("list "+what, err)
			require.Empty(t, page.GetObjects(), "list %s enumerated objects", what)
			require.Empty(t, page.GetCommonPrefixes(), "list %s enumerated prefixes", what)
		}
	}

	// The same credential on MinIO itself, past the gateway: it reads what it
	// may, so a refusal below is the policy's and not a credential MinIO does
	// not know, and it writes nothing anywhere.
	key := bound.GetCredential().GetAccessKey()
	raw, err := miniogo.New(endpoint, &miniogo.Options{Creds: miniocreds.NewStaticV4(key.GetId(), key.GetSecret(), key.GetSessionToken()), Region: "us-east-1"})
	require.NoError(t, err)
	_, err = raw.StatObject(ctx, bucket, first, miniogo.StatObjectOptions{})
	require.NoError(t, err)
	refused := func(what string, err error) {
		t.Helper()
		require.Error(t, err, "%s was allowed", what)
		require.Equal(t, "AccessDenied", miniogo.ToErrorResponse(err).Code, "%s: %v", what, err)
	}
	read := func(key string) error {
		object, err := raw.GetObject(ctx, bucket, key, miniogo.GetObjectOptions{})
		if err != nil {
			return err
		}
		defer object.Close()
		_, err = io.ReadAll(object)
		return err
	}
	write := func(key string) error {
		_, err := raw.PutObject(ctx, bucket, key, strings.NewReader("overwritten"), int64(len("overwritten")), miniogo.PutObjectOptions{})
		return err
	}
	copyTo := func(source, destination string) error {
		_, err := raw.CopyObject(ctx, miniogo.CopyDestOptions{Bucket: bucket, Object: destination}, miniogo.CopySrcOptions{Bucket: bucket, Object: source})
		return err
	}
	remove := func(key string) error { return raw.RemoveObject(ctx, bucket, key, miniogo.RemoveObjectOptions{}) }
	list := func(prefix string) error {
		for object := range raw.ListObjects(ctx, bucket, miniogo.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if object.Err != nil {
				return object.Err
			}
		}
		return nil
	}
	fresh := slot(tenant, installation, objectkey.Principal(principal)) + "written-by-the-session.txt"
	for what, target := range map[string]string{
		"a sibling installation's content": inSibling,
		"the same bytes in the sibling":    sameBytes,
		"another tenant's content":         inOtherTenant,
		"the legacy copy":                  legacy,
		"another tenant's legacy copy":     otherLegacy,
	} {
		refused("raw get of "+what, read(target))
		_, err := raw.StatObject(ctx, bucket, target, miniogo.StatObjectOptions{})
		require.Error(t, err, "raw stat of %s was allowed", what)
		refused("put over "+what, write(target))
		refused("delete of "+what, remove(target))
		refused("copy from "+what+" into its own scope", copyTo(target, fresh))
		refused("copy of its own content over "+what, copyTo(first, target))
	}
	for what, prefix := range map[string]string{
		"the sibling installation": scopeOf(tenant, sibling), "the tenant": partition, "another tenant": otherPartition,
		"the legacy prefix": tenant + "/blobs/", "the bucket": "", "its own content slot": slot(tenant, installation, objectkey.Content()),
	} {
		refused("raw list of "+what, list(prefix))
	}
	// Read-only inside its own scope too.
	refused("put into its own slot", write(fresh))
	refused("put over its own content", write(first))
	refused("delete of its own content", remove(first))
	refused("delete of its own object", remove(own+"note.txt"))
	refused("copy within its own scope", copyTo(first, fresh))
	refused("put of a new sibling key", write(scopeOf(tenant, sibling)+"_sha256/"+strings.Repeat("0", 64)))

	// Nothing moved: every object is as it was written, and nothing was added.
	for key, body := range stored {
		got, _ := get(t, c, key)
		require.Equal(t, body, string(got), "%s changed", key)
	}
	for _, added := range []string{fresh, scopeOf(tenant, sibling) + "_sha256/" + strings.Repeat("0", 64)} {
		_, err := c.Stat(ctx, &storagev0.StatRequest{Key: added})
		require.Equal(t, codes.NotFound, status.Code(err), "%s was written", added)
	}
}
