//go:build integration

package integration

// Evidence for MinIO only.
//
// Every test here runs the gateway's scoped path against the pinned MinIO
// with the real issuer (sessions.IssuerFor, MinIO's STS AssumeRole under an
// inline session policy), issuing from the MinIO root credential the suite is
// given. A refusal is asserted as MinIO's own: the HTTP exchange between the
// gateway and MinIO is recorded, and a refused call must show a 403 from
// MinIO on a request signed by the scope's session, never by the gateway's
// own key. Nothing here says anything about S3, GCS or Azure, whose issuers
// are unqualified and issue nothing, nor about a non-root issuing identity.
// The gateway runs in process, not as its built image.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
	"github.com/codefly-dev/service-object-storage/internal/sessions"
	objectstorage "github.com/codefly-dev/service-object-storage/libs/go"
	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

// exchange is one request a session's client made to MinIO, and its answer.
type exchange struct {
	method, path, accessKey string
	status                  int
}

// wire records what the gateway's scoped clients exchange with MinIO.
type wire struct {
	mu   sync.Mutex
	seen []exchange
}

var signedBy = regexp.MustCompile(`Credential=([^/]+)/`)

func (w *wire) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	key := ""
	if match := signedBy.FindStringSubmatch(request.Header.Get("Authorization")); match != nil {
		key = match[1]
	}
	w.mu.Lock()
	w.seen = append(w.seen, exchange{method: request.Method, path: request.URL.Path, accessKey: key, status: response.StatusCode})
	w.mu.Unlock()
	return response, nil
}

func (w *wire) mark() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.seen)
}

func (w *wire) since(mark int) []exchange {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]exchange(nil), w.seen[mark:]...)
}

// testClock is the gateway's clock, which a test moves to age a session.
type testClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

func (c *testClock) Advance(by time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += by
}

// onClock dates a session by the gateway's clock, so that moving the clock
// ages the sessions already issued and not the ones issued after.
type onClock struct {
	sessions.Issuer
	clock *testClock
}

func (i onClock) Issue(ctx context.Context, scope sessions.Scope, ttl time.Duration) (sessions.Credential, error) {
	credential, err := i.Issuer.Issue(ctx, scope, ttl)
	i.clock.mu.Lock()
	credential.Expires = credential.Expires.Add(i.clock.offset)
	i.clock.mu.Unlock()
	return credential, err
}

// byCaller authorizes by the grants and names the caller from the request,
// as an authorizer of a verified per-request identity would. The gateway's
// own authorizer names one caller; this one lets a test be two.
type byCaller struct{ grants sessions.Grants }

func (a byCaller) Authorize(ctx context.Context, scope sessions.Scope) (string, error) {
	if _, err := a.grants.Authorize(ctx, scope); err != nil {
		return "", err
	}
	if values := metadata.ValueFromIncomingContext(ctx, "x-test-caller"); len(values) == 1 {
		return values[0], nil
	}
	return "the-caller", nil
}

func asCaller(name string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "x-test-caller", name)
}

type scopedStack struct {
	client          storagev0.ObjectStorageClient
	wire            *wire
	clock           *testClock
	manager         *sessions.Manager
	config          backend.Config
	endpoint        string
	bucket, prefix  string
	rootKey, secret string
}

const scopedBase = "documents-content"

// newScopedStack is a gateway over the live MinIO, confined to a prefix of
// this test's own, running scoped sessions from the real issuer.
func newScopedStack(t *testing.T, mode server.ScopedMode, prefix string, presign bool) *scopedStack {
	t.Helper()
	stack := &scopedStack{
		wire: &wire{}, clock: &testClock{}, endpoint: mustEnv(t, "MINIO_ENDPOINT"), bucket: mustEnv(t, "MINIO_BUCKET"),
		rootKey: mustEnv(t, "MINIO_ACCESS_KEY"), secret: mustEnv(t, "MINIO_SECRET_KEY"), prefix: prefix,
	}
	ensureBucket(t, stack.endpoint, stack.rootKey, stack.secret, stack.bucket)
	if stack.prefix == "" {
		token := make([]byte, 6)
		_, _ = rand.Read(token)
		stack.prefix = "scoped-" + hex.EncodeToString(token) + "/"
	}
	stack.config = backend.Config{
		Kind: "minio", Endpoint: stack.endpoint, Region: "us-east-1", AccessKey: stack.rootKey, SecretKey: stack.secret,
		Bucket: stack.bucket, Prefix: stack.prefix, UsePathStyle: true,
	}
	own, err := backend.Open(context.Background(), stack.config)
	require.NoError(t, err)
	issuer, err := sessions.IssuerFor(stack.config)
	require.NoError(t, err)
	require.Equal(t, sessions.Qualified, issuer.Qualification())
	grants, err := sessions.ParseGrants(`[{"base":"documents-content","modes":["read","ingest","maintenance"]}]`)
	require.NoError(t, err)
	stack.manager, err = sessions.NewManager(sessions.Options{
		Backend: stack.config, Issuer: onClock{issuer, stack.clock}, Authorizer: byCaller{grants}, Now: stack.clock.Now,
		// The session's client is the gateway's own MinIO client with the
		// session's credential; only its transport is replaced, to record.
		Open: func(ctx context.Context, cfg backend.Config) (backend.Backend, error) {
			cfg.HTTPClient = &http.Client{Transport: stack.wire}
			return backend.Open(ctx, cfg)
		},
	})
	require.NoError(t, err)
	hub := events.NewHub()
	srv := server.New(own, hub, probetest.Monitor(t, own)).WithScoped(server.Scoped{Mode: mode, Manager: stack.manager, Presign: presign})
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	storagev0.RegisterObjectStorageServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); grpcServer.Stop(); hub.Close(); _ = own.Close() })
	stack.client = storagev0.NewObjectStorageClient(conn)
	return stack
}

// root is a client on MinIO itself with the root credential: how a test sees
// what is really in the bucket.
func (s *scopedStack) root(t *testing.T) *miniogo.Client {
	t.Helper()
	client, err := miniogo.New(s.endpoint, &miniogo.Options{Creds: miniocreds.NewStaticV4(s.rootKey, s.secret, ""), Region: "us-east-1"})
	require.NoError(t, err)
	return client
}

func (s *scopedStack) stored(t *testing.T, key string) (string, bool) {
	t.Helper()
	object, err := s.root(t).GetObject(context.Background(), s.bucket, s.prefix+key, miniogo.GetObjectOptions{})
	require.NoError(t, err)
	defer object.Close()
	body, err := io.ReadAll(object)
	if err != nil {
		require.Equal(t, "NoSuchKey", miniogo.ToErrorResponse(err).Code, "%s: %v", key, err)
		return "", false
	}
	return string(body), true
}

func in(tenant, installation string, mode storagev0.ScopeMode) *storagev0.Scope {
	return &storagev0.Scope{Base: scopedBase, Partition: tenant, Scope: installation, Mode: mode}
}

const (
	readMode        = storagev0.ScopeMode_SCOPE_MODE_READ
	ingestMode      = storagev0.ScopeMode_SCOPE_MODE_INGEST
	maintenanceMode = storagev0.ScopeMode_SCOPE_MODE_MAINTENANCE
)

func reasonOf(err error) (string, map[string]string) {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetReason(), info.GetMetadata()
		}
	}
	return "", nil
}

// refusedByMinIO requires that call was refused, and that the refusal is
// MinIO's: a 403 from MinIO on a request signed by a session, with MinIO's
// AccessDenied carried to the caller. A gateway-side check would leave no 403
// on the wire, and a call made with the gateway's own key would not be
// refused at all.
func (s *scopedStack) refusedByMinIO(t *testing.T, what string, mode storagev0.ScopeMode, call func() error) {
	t.Helper()
	mark := s.wire.mark()
	err := call()
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%s: %v", what, err)
	reason, meta := reasonOf(err)
	require.Equal(t, "CREDENTIAL_REJECTED", reason, what)
	require.Equal(t, "true", meta["scoped"], what)
	require.Equal(t, strings.ToLower(strings.TrimPrefix(mode.String(), "SCOPE_MODE_")), meta["mode"], what)
	require.Equal(t, "AccessDenied", meta["store_code"], "%s: the refusal does not carry MinIO's code", what)
	exchanges := s.wire.since(mark)
	require.NotEmpty(t, exchanges, "%s was refused without asking MinIO", what)
	denied := false
	for _, exchange := range exchanges {
		require.NotEqual(t, s.rootKey, exchange.accessKey, "%s reached MinIO with the gateway's own credential", what)
		require.NotEmpty(t, exchange.accessKey, what)
		denied = denied || exchange.status == http.StatusForbidden
	}
	require.True(t, denied, "%s: MinIO answered no 403: %+v", what, exchanges)
}

func drain(stream storagev0.ObjectStorage_GetClient, err error) ([]byte, error) {
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

func putScoped(c storagev0.ObjectStorageClient, with *storagev0.Scope, key string, body []byte, declared bool) error {
	stream, err := c.Put(context.Background())
	if err != nil {
		return err
	}
	header := &storagev0.PutHeader{Key: key, Scope: with, TotalSize: -1}
	if declared {
		header.TotalSize = int64(len(body))
	}
	if err := stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: header}}); err == nil {
		for offset := 0; offset < len(body); offset += 1 << 20 {
			end := min(offset+1<<20, len(body))
			if stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: body[offset:end]}}) != nil {
				break
			}
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

// layout is two tenants, each with two installations, written with the
// gateway's own credential the way a content-by-digest consumer lays its
// objects out: content under the scope's content slot, an object of the
// service's own slot, and the older copy at "<tenant>/blobs/<digest>" outside
// the base.
type layout struct {
	codec                        objectkey.Codec
	tenant, otherTenant          string
	installation, sibling        string
	own, second, note            string // in (tenant, installation)
	inSibling, sameBytes         string // in (tenant, sibling)
	inOtherTenant                string // in (otherTenant, installation)
	legacy, otherLegacy          string // outside the base
	bodies                       map[string]string
	scopePrefix, siblingPrefix   string
	partitionPrefix, otherPrefix string
}

func writeLayout(t *testing.T, stack *scopedStack) *layout {
	t.Helper()
	run := hex.EncodeToString(func() []byte { b := make([]byte, 4); _, _ = rand.Read(b); return b }())
	codec, err := objectkey.New(scopedBase)
	require.NoError(t, err)
	l := &layout{codec: codec, tenant: "tenant-a-" + run, otherTenant: "tenant-b-" + run,
		installation: "installation-a", sibling: "installation-b", bodies: map[string]string{}}
	write := func(key, body string) string {
		require.NoError(t, putScoped(stack.client, nil, key, []byte(body), true))
		l.bodies[key] = body
		return key
	}
	content := func(tenant, installation, body string) string {
		digest := sha256.Sum256([]byte(body))
		key, err := codec.Key(objectkey.Key{Partition: tenant, Scope: installation, Slot: objectkey.Content(), Name: hex.EncodeToString(digest[:])})
		require.NoError(t, err)
		write(tenant+"/blobs/"+hex.EncodeToString(digest[:]), body)
		return write(key, body)
	}
	l.own = content(l.tenant, l.installation, "first document of installation a "+run)
	l.second = content(l.tenant, l.installation, "second document of installation a "+run)
	l.inSibling = content(l.tenant, l.sibling, "document of installation b "+run)
	l.sameBytes = content(l.tenant, l.sibling, "first document of installation a "+run)
	l.inOtherTenant = content(l.otherTenant, l.installation, "first document of installation a "+run)
	digest := l.own[strings.LastIndex(l.own, "/")+1:]
	l.legacy, l.otherLegacy = l.tenant+"/blobs/"+digest, l.otherTenant+"/blobs/"+digest
	slot, err := codec.SlotPrefix(l.tenant, l.installation, objectkey.Principal("service:documents"))
	require.NoError(t, err)
	l.note = write(slot+"note.txt", "own")
	l.scopePrefix, _ = codec.ScopePrefix(l.tenant, l.installation)
	l.siblingPrefix, _ = codec.ScopePrefix(l.tenant, l.sibling)
	l.partitionPrefix, _ = codec.PartitionPrefix(l.tenant)
	l.otherPrefix, _ = codec.PartitionPrefix(l.otherTenant)
	return l
}

func (l *layout) outside() map[string]string {
	return map[string]string{
		"a sibling installation's content":         l.inSibling,
		"the same bytes in a sibling installation": l.sameBytes,
		"another tenant's content":                 l.inOtherTenant,
		"the legacy copy of its own content":       l.legacy,
		"another tenant's legacy copy":             l.otherLegacy,
	}
}

func (l *layout) unchanged(t *testing.T, stack *scopedStack) {
	t.Helper()
	for key, body := range l.bodies {
		got, found := stack.stored(t, key)
		require.True(t, found, "%s is gone", key)
		require.Equal(t, body, got, "%s changed", key)
	}
}

// Through the gateway's scoped path a read session reads, stats, ranges and
// lists its own tenant and installation, and MinIO refuses it the same four
// in a sibling installation, another tenant and the legacy prefix. The caller
// holds no credential at any point: it names a scope and a key.
func TestMinIOScopedReadSessionIsConfinedToItsTenantAndInstallationByMinIO(t *testing.T) {
	stack := newScopedStack(t, server.ScopedAllow, "", false)
	l := writeLayout(t, stack)
	ctx := context.Background()
	c := stack.client
	scope := in(l.tenant, l.installation, readMode)

	// Inside the scope.
	mark := stack.wire.mark()
	for _, key := range []string{l.own, l.second, l.note} {
		header, err := c.Stat(ctx, &storagev0.StatRequest{Key: key, Scope: scope})
		require.NoError(t, err, "stat %s", key)
		require.Equal(t, int64(len(l.bodies[key])), header.GetInfo().GetSize())
		require.Equal(t, key, header.GetInfo().GetKey())
		body, err := drain(c.Get(ctx, &storagev0.GetRequest{Key: key, Scope: scope}))
		require.NoError(t, err, "get %s", key)
		require.Equal(t, l.bodies[key], string(body))
		part, err := drain(c.Get(ctx, &storagev0.GetRequest{Key: key, Scope: scope, Range: &storagev0.ByteRange{Offset: 1, Length: 2}}))
		require.NoError(t, err, "range %s", key)
		require.Equal(t, l.bodies[key][1:3], string(part))
	}
	listed, err := c.List(ctx, &storagev0.ListRequest{Prefix: l.scopePrefix, Scope: scope})
	require.NoError(t, err)
	var names []string
	for _, object := range listed.GetObjects() {
		names = append(names, object.GetKey())
	}
	require.ElementsMatch(t, []string{l.own, l.second, l.note}, names, "a scope lists its own objects and no other's")
	sessionKeys := map[string]bool{}
	for _, exchange := range stack.wire.since(mark) {
		require.NotEqual(t, stack.rootKey, exchange.accessKey, "a scoped read reached MinIO with the gateway's own credential")
		require.Less(t, exchange.status, 300, "%+v", exchange)
		sessionKeys[exchange.accessKey] = true
	}
	require.Len(t, sessionKeys, 1, "one cached session served every read of the scope")

	// Outside it: MinIO refuses each verb.
	for what, key := range l.outside() {
		stack.refusedByMinIO(t, "stat of "+what, readMode, func() error {
			_, err := c.Stat(ctx, &storagev0.StatRequest{Key: key, Scope: scope})
			return err
		})
		stack.refusedByMinIO(t, "get of "+what, readMode, func() error {
			body, err := drain(c.Get(ctx, &storagev0.GetRequest{Key: key, Scope: scope}))
			require.Empty(t, body, "get of %s delivered bytes", what)
			return err
		})
		stack.refusedByMinIO(t, "range of "+what, readMode, func() error {
			body, err := drain(c.Get(ctx, &storagev0.GetRequest{Key: key, Scope: scope, Range: &storagev0.ByteRange{Offset: 0, Length: 1}}))
			require.Empty(t, body, "range of %s delivered bytes", what)
			return err
		})
		stack.refusedByMinIO(t, "get-many of "+what, readMode, func() error {
			stream, err := c.GetMany(ctx, &storagev0.GetManyRequest{Items: []*storagev0.GetManyItem{{Key: key}}, Scope: scope})
			require.NoError(t, err)
			_, err = stream.Recv()
			return err
		})
	}
	for what, prefix := range map[string]string{
		"the sibling installation":               l.siblingPrefix,
		"the tenant, parent of both":             l.partitionPrefix,
		"another tenant":                         l.otherPrefix,
		"the base":                               l.codec.BasePrefix(),
		"the gateway's whole prefix":             "",
		"the legacy prefix":                      l.tenant + "/blobs/",
		"the legacy tenant":                      l.tenant + "/",
		"another tenant's legacy prefix":         l.otherTenant + "/blobs/",
		"its own scope without the separator":    strings.TrimSuffix(l.scopePrefix, "/"),
		"the installation its name is a part of": strings.TrimSuffix(l.scopePrefix, "-a/"),
	} {
		for _, delimiter := range []string{"", "/"} {
			stack.refusedByMinIO(t, "list of "+what, readMode, func() error {
				page, err := c.List(ctx, &storagev0.ListRequest{Prefix: prefix, Delimiter: delimiter, Scope: scope})
				require.Empty(t, page.GetObjects(), "list of %s enumerated objects", what)
				require.Empty(t, page.GetCommonPrefixes(), "list of %s enumerated prefixes", what)
				return err
			})
		}
	}

	// The control: each refused key is served to the scope that holds it, on
	// the same path, so the refusals above are the scope's and not the key's.
	body, err := drain(c.Get(ctx, &storagev0.GetRequest{Key: l.inSibling, Scope: in(l.tenant, l.sibling, readMode)}))
	require.NoError(t, err)
	require.Equal(t, l.bodies[l.inSibling], string(body))
	body, err = drain(c.Get(ctx, &storagev0.GetRequest{Key: l.inOtherTenant, Scope: in(l.otherTenant, l.installation, readMode)}))
	require.NoError(t, err)
	require.Equal(t, l.bodies[l.inOtherTenant], string(body))
	// And the gateway's own credential, which a scoped call never uses, would
	// have read them all: without the session nothing is confined.
	for _, key := range l.outside() {
		_, err := c.Stat(ctx, &storagev0.StatRequest{Key: key})
		require.NoError(t, err)
	}
}

// A read session writes nothing: MinIO refuses Put, Copy and Delete under
// it, its own scope included, and every object stays as it was written.
func TestMinIOScopedReadSessionCannotWrite(t *testing.T) {
	stack := newScopedStack(t, server.ScopedAllow, "", false)
	l := writeLayout(t, stack)
	ctx := context.Background()
	c := stack.client
	scope := in(l.tenant, l.installation, readMode)
	fresh := l.scopePrefix + "service.documents/written-by-a-read-session.txt"

	targets := l.outside()
	targets["its own content"] = l.own
	targets["a new key of its own scope"] = fresh
	for what, key := range targets {
		stack.refusedByMinIO(t, "put over "+what, readMode, func() error {
			return putScoped(c, scope, key, []byte("overwritten"), true)
		})
		stack.refusedByMinIO(t, "streamed put over "+what, readMode, func() error {
			return putScoped(c, scope, key, []byte("overwritten"), false)
		})
		stack.refusedByMinIO(t, "delete of "+what, readMode, func() error {
			_, err := c.Delete(ctx, &storagev0.DeleteRequest{Key: key, Scope: scope})
			return err
		})
		stack.refusedByMinIO(t, "copy of its own content over "+what, readMode, func() error {
			_, err := c.Copy(ctx, &storagev0.CopyRequest{SourceKey: l.second, DestKey: key, Scope: scope})
			return err
		})
	}
	// A batch delete answers per key; MinIO refuses each one.
	result, err := c.DeleteMany(ctx, &storagev0.DeleteManyRequest{Keys: []string{l.own, l.inSibling}, Scope: scope})
	require.NoError(t, err)
	require.Len(t, result.GetEntries(), 2)
	for _, entry := range result.GetEntries() {
		require.Contains(t, entry.GetError(), "Access Denied", "batch delete of %s", entry.GetKey())
	}
	l.unchanged(t, stack)
	_, found := stack.stored(t, fresh)
	require.False(t, found, "a read session wrote an object")
}

// An ingest session writes inside its scope, single request and multipart,
// and MinIO refuses it every read, list and delete, its own writes included,
// and every write outside the scope.
func TestMinIOScopedIngestSessionWritesAndCannotReadOrDelete(t *testing.T) {
	stack := newScopedStack(t, server.ScopedAllow, "", false)
	l := writeLayout(t, stack)
	ctx := context.Background()
	c := stack.client
	scope := in(l.tenant, l.installation, ingestMode)
	staged := l.scopePrefix + "service.documents/_staging/" + strings.Repeat("0", 32)

	mark := stack.wire.mark()
	require.NoError(t, putScoped(c, scope, staged, []byte("uploaded"), true))
	body, found := stack.stored(t, staged)
	require.True(t, found)
	require.Equal(t, "uploaded", body)
	// A body longer than the gateway's size probe streams as a multipart
	// upload: initiate, part, complete, all under the ingest session.
	large := bytes.Repeat([]byte("multipart "), (server.PutSizeProbeBytes+(1<<20))/10)
	multipartKey := staged[:len(staged)-1] + "1"
	require.NoError(t, putScoped(c, scope, multipartKey, large, false))
	info, err := stack.root(t).StatObject(ctx, stack.bucket, stack.prefix+multipartKey, miniogo.StatObjectOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(large)), info.Size)
	multipartStarted := false
	for _, exchange := range stack.wire.since(mark) {
		require.NotEqual(t, stack.rootKey, exchange.accessKey)
		require.Less(t, exchange.status, 300, "%+v", exchange)
		multipartStarted = multipartStarted || exchange.method == http.MethodPost
	}
	require.True(t, multipartStarted, "the large body was not a multipart upload")

	// It reads nothing, not even what it just wrote.
	for what, key := range map[string]string{"what it just wrote": staged, "content of its own scope": l.own, "a sibling's content": l.inSibling} {
		stack.refusedByMinIO(t, "stat of "+what, ingestMode, func() error {
			_, err := c.Stat(ctx, &storagev0.StatRequest{Key: key, Scope: scope})
			return err
		})
		stack.refusedByMinIO(t, "get of "+what, ingestMode, func() error {
			body, err := drain(c.Get(ctx, &storagev0.GetRequest{Key: key, Scope: scope}))
			require.Empty(t, body)
			return err
		})
		stack.refusedByMinIO(t, "delete of "+what, ingestMode, func() error {
			_, err := c.Delete(ctx, &storagev0.DeleteRequest{Key: key, Scope: scope})
			return err
		})
		stack.refusedByMinIO(t, "copy from "+what, ingestMode, func() error {
			_, err := c.Copy(ctx, &storagev0.CopyRequest{SourceKey: key, DestKey: staged + "x", Scope: scope})
			return err
		})
	}
	for what, prefix := range map[string]string{"its own scope": l.scopePrefix, "the sibling": l.siblingPrefix, "the gateway's whole prefix": ""} {
		stack.refusedByMinIO(t, "list of "+what, ingestMode, func() error {
			page, err := c.List(ctx, &storagev0.ListRequest{Prefix: prefix, Scope: scope})
			require.Empty(t, page.GetObjects())
			return err
		})
	}
	// It writes nowhere else.
	for what, key := range l.outside() {
		stack.refusedByMinIO(t, "put over "+what, ingestMode, func() error {
			return putScoped(c, scope, key, []byte("overwritten"), true)
		})
	}
	stack.refusedByMinIO(t, "put of a new key in the sibling", ingestMode, func() error {
		return putScoped(c, scope, l.siblingPrefix+"_sha256/"+strings.Repeat("0", 64), []byte("planted"), true)
	})
	stack.refusedByMinIO(t, "streamed put of a new legacy key", ingestMode, func() error {
		return putScoped(c, scope, l.tenant+"/blobs/"+strings.Repeat("0", 64), []byte("planted"), false)
	})
	l.unchanged(t, stack)
	for _, planted := range []string{l.siblingPrefix + "_sha256/" + strings.Repeat("0", 64), l.tenant + "/blobs/" + strings.Repeat("0", 64), staged + "x"} {
		_, found := stack.stored(t, planted)
		require.False(t, found, "%s was written", planted)
	}
}

// A maintenance session copies and deletes inside its scope, and MinIO
// refuses it both across the scope's edge, in either direction.
func TestMinIOScopedMaintenanceSessionCopiesAndDeletesOnlyInsideItsScope(t *testing.T) {
	stack := newScopedStack(t, server.ScopedAllow, "", false)
	l := writeLayout(t, stack)
	ctx := context.Background()
	c := stack.client
	scope := in(l.tenant, l.installation, maintenanceMode)
	promoted := l.scopePrefix + "_sha256/" + strings.Repeat("c", 64)

	_, err := c.Copy(ctx, &storagev0.CopyRequest{SourceKey: l.note, DestKey: promoted, Scope: scope})
	require.NoError(t, err)
	body, found := stack.stored(t, promoted)
	require.True(t, found)
	require.Equal(t, "own", body)
	_, err = c.Copy(ctx, &storagev0.CopyRequest{SourceKey: l.note, DestKey: promoted, IfNoneMatch: "*", Scope: scope})
	require.Equal(t, codes.AlreadyExists, status.Code(err), "copy-if-absent stats its destination under the same session")
	_, err = c.Delete(ctx, &storagev0.DeleteRequest{Key: promoted, Scope: scope})
	require.NoError(t, err)
	_, found = stack.stored(t, promoted)
	require.False(t, found)
	batch := []string{l.scopePrefix + "service.documents/batch-1", l.scopePrefix + "service.documents/batch-2"}
	for _, key := range batch {
		_, err = c.Copy(ctx, &storagev0.CopyRequest{SourceKey: l.note, DestKey: key, Scope: scope})
		require.NoError(t, err)
	}
	result, err := c.DeleteMany(ctx, &storagev0.DeleteManyRequest{Keys: append(batch, l.inSibling), Scope: scope})
	require.NoError(t, err)
	require.Empty(t, result.GetEntries()[0].GetError())
	require.Empty(t, result.GetEntries()[1].GetError())
	require.Contains(t, result.GetEntries()[2].GetError(), "Access Denied", "a batch delete reached the sibling installation")

	for what, key := range l.outside() {
		stack.refusedByMinIO(t, "copy from "+what+" into its own scope", maintenanceMode, func() error {
			_, err := c.Copy(ctx, &storagev0.CopyRequest{SourceKey: key, DestKey: promoted, Scope: scope})
			return err
		})
		stack.refusedByMinIO(t, "copy of its own content over "+what, maintenanceMode, func() error {
			_, err := c.Copy(ctx, &storagev0.CopyRequest{SourceKey: l.own, DestKey: key, Scope: scope})
			return err
		})
		stack.refusedByMinIO(t, "delete of "+what, maintenanceMode, func() error {
			_, err := c.Delete(ctx, &storagev0.DeleteRequest{Key: key, Scope: scope})
			return err
		})
	}
	l.unchanged(t, stack)
	_, found = stack.stored(t, promoted)
	require.False(t, found, "a refused copy wrote its destination")
}

// A scope whose session policy would exceed MinIO's 2,048 bytes is refused
// before anything is issued. The same scope under a gateway prefix that
// leaves room is served, so the refusal is the size and nothing else.
func TestMinIOScopedSessionRefusesAnOverlargePolicy(t *testing.T) {
	long := strings.Repeat("p", 600) + "/"
	stack := newScopedStack(t, server.ScopedAllow, long, false)
	ctx := context.Background()
	tenant, installation := strings.Repeat("t", 240), strings.Repeat("i", 240)
	mark := stack.wire.mark()
	for _, mode := range []storagev0.ScopeMode{readMode, maintenanceMode} {
		_, err := stack.client.Stat(ctx, &storagev0.StatRequest{Key: "any", Scope: in(tenant, installation, mode)})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		reason, _ := reasonOf(err)
		require.Equal(t, "SCOPE_POLICY_TOO_LARGE", reason)
	}
	require.Empty(t, stack.wire.since(mark), "a call was made although no session could be issued")
	require.Zero(t, stack.manager.Held())

	fits := newScopedStack(t, server.ScopedAllow, "", false)
	_, err := fits.client.Stat(ctx, &storagev0.StatRequest{Key: "any", Scope: in(tenant, installation, readMode)})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "served, and MinIO answered for a key outside the scope: %v", err)
	codec, _ := objectkey.New(scopedBase)
	prefix, _ := codec.ScopePrefix(tenant, installation)
	listed, err := fits.client.List(ctx, &storagev0.ListRequest{Prefix: prefix, Scope: in(tenant, installation, readMode)})
	require.NoError(t, err)
	require.Empty(t, listed.GetObjects())
}

// A session is reused until a minute before it expires, then a new one is
// issued and the old one is used no more. The gateway's clock is moved; the
// sessions are MinIO's. What MinIO does with a session past its own expiry is
// TestMinIOScopedSessionExpiresAtMinIO.
func TestMinIOScopedSessionIsReissuedBeforeItExpires(t *testing.T) {
	stack := newScopedStack(t, server.ScopedAllow, "", false)
	l := writeLayout(t, stack)
	ctx := context.Background()
	scope := in(l.tenant, l.installation, readMode)
	keyOf := func() string {
		mark := stack.wire.mark()
		_, err := stack.client.Stat(ctx, &storagev0.StatRequest{Key: l.own, Scope: scope})
		require.NoError(t, err)
		exchanges := stack.wire.since(mark)
		require.Len(t, exchanges, 1)
		return exchanges[0].accessKey
	}
	first := keyOf()
	require.Equal(t, first, keyOf(), "the session is reused")
	stack.clock.Advance(sessions.DefaultTTL - 2*time.Minute)
	require.Equal(t, first, keyOf(), "and still two minutes before its expiry")
	stack.clock.Advance(90 * time.Second)
	second := keyOf()
	require.NotEqual(t, first, second, "inside its last minute a session is replaced")
	require.NotEqual(t, stack.rootKey, second)
	require.Equal(t, second, keyOf())
	require.Equal(t, 1, stack.manager.Held())
	// The new session is confined as the first was.
	stack.refusedByMinIO(t, "stat of a sibling under the re-issued session", readMode, func() error {
		_, err := stack.client.Stat(ctx, &storagev0.StatRequest{Key: l.inSibling, Scope: scope})
		return err
	})

	// MinIO issues no session shorter than the gateway's minimum, which is
	// why the minimum is what it is.
	issuer, err := sessions.IssuerFor(stack.config)
	require.NoError(t, err)
	_, err = issuer.Issue(ctx, sessions.Scope{Base: scopedBase, Partition: l.tenant, Scope: l.installation, Mode: sessions.Read}, time.Minute)
	require.ErrorIs(t, err, sessions.ErrIssuerUnavailable)
}

// MinIO stops honouring a session at its expiry: the credential that read an
// object is refused the same object sixteen minutes later, and the gateway,
// asked again, has issued another. It waits out a real 15-minute session, so
// it runs only when SOS_TEST_SESSION_EXPIRY=1.
func TestMinIOScopedSessionExpiresAtMinIO(t *testing.T) {
	if os.Getenv("SOS_TEST_SESSION_EXPIRY") != "1" {
		t.Skip("SOS_TEST_SESSION_EXPIRY is not 1; skipping the 15-minute wait for a real session to expire")
	}
	stack := newScopedStack(t, server.ScopedAllow, "", false)
	l := writeLayout(t, stack)
	ctx := context.Background()
	issuer, err := sessions.IssuerFor(stack.config)
	require.NoError(t, err)
	credential, err := issuer.Issue(ctx, sessions.Scope{Base: scopedBase, Partition: l.tenant, Scope: l.installation, Mode: sessions.Read}, sessions.MinTTL)
	require.NoError(t, err)
	raw, err := miniogo.New(stack.endpoint, &miniogo.Options{Creds: miniocreds.NewStaticV4(credential.AccessKey, credential.SecretKey, credential.SessionToken), Region: "us-east-1"})
	require.NoError(t, err)
	_, err = raw.StatObject(ctx, stack.bucket, stack.prefix+l.own, miniogo.StatObjectOptions{})
	require.NoError(t, err)
	scope := in(l.tenant, l.installation, readMode)
	mark := stack.wire.mark()
	_, err = stack.client.Stat(ctx, &storagev0.StatRequest{Key: l.own, Scope: scope})
	require.NoError(t, err)
	first := stack.wire.since(mark)[0].accessKey

	time.Sleep(time.Until(credential.Expires) + 20*time.Second)

	_, err = raw.StatObject(ctx, stack.bucket, stack.prefix+l.own, miniogo.StatObjectOptions{})
	require.Error(t, err, "MinIO still honours a session past its expiry")
	require.Equal(t, http.StatusForbidden, miniogo.ToErrorResponse(err).StatusCode, "%v", err)
	mark = stack.wire.mark()
	_, err = stack.client.Stat(ctx, &storagev0.StatRequest{Key: l.own, Scope: scope})
	require.NoError(t, err, "the gateway did not replace its expired session")
	require.NotEqual(t, first, stack.wire.since(mark)[0].accessKey)
}

// Two callers of one scope are two sessions, and so are two tenants of one
// caller: no request reaches MinIO signed by a session issued for another.
func TestMinIOScopedSessionCacheNeverCrossesCallers(t *testing.T) {
	stack := newScopedStack(t, server.ScopedAllow, "", false)
	l := writeLayout(t, stack)
	type request struct {
		caller string
		scope  *storagev0.Scope
		key    string
	}
	requests := []request{
		{"alpha", in(l.tenant, l.installation, readMode), l.own},
		{"beta", in(l.tenant, l.installation, readMode), l.own},
		{"alpha", in(l.tenant, l.sibling, readMode), l.inSibling},
		{"alpha", in(l.otherTenant, l.installation, readMode), l.inOtherTenant},
		{"alpha", in(l.tenant, l.installation, maintenanceMode), l.own},
	}
	owner := map[string]int{}
	for round := 0; round < 3; round++ {
		for index, request := range requests {
			mark := stack.wire.mark()
			_, err := stack.client.Stat(asCaller(request.caller), &storagev0.StatRequest{Key: request.key, Scope: request.scope})
			require.NoError(t, err)
			exchanges := stack.wire.since(mark)
			require.Len(t, exchanges, 1)
			key := exchanges[0].accessKey
			if seen, ok := owner[key]; ok {
				require.Equal(t, seen, index, "session %s served two different callers or scopes", key)
			}
			owner[key] = index
		}
	}
	require.Len(t, owner, len(requests), "one session each, reused across the rounds")
	require.Equal(t, len(requests), stack.manager.Held())

	// A caller that asks for another caller's tenant gets a session for the
	// tenant it named, which MinIO holds to that tenant: beta's session for
	// tenant A is refused alpha's object in tenant B.
	stack.refusedByMinIO(t, "another tenant's object under beta's session", readMode, func() error {
		_, err := stack.client.Stat(asCaller("beta"), &storagev0.StatRequest{Key: l.inOtherTenant, Scope: in(l.tenant, l.installation, readMode)})
		return err
	})
}

// A gateway that requires scopes serves a Store built with gateway sessions
// end to end, and refuses the same client everything without a scope: the
// consumer holds no bucket-wide path at all.
func TestMinIOStoreWithGatewaySessionsOnAGatewayThatRequiresThem(t *testing.T) {
	stack := newScopedStack(t, server.ScopedRequire, "", false)
	ctx := context.Background()
	run := hex.EncodeToString(func() []byte { b := make([]byte, 4); _, _ = rand.Read(b); return b }())
	tenant := "tenant-" + run
	binding := func(installation string) objectstorage.Binder {
		return objectstorage.FixedBinder(objectstorage.Binding{Partition: tenant, Principal: "service:documents",
			Reach: []string{installation}, Mode: objectstorage.Strict, Access: objectstorage.ReadWrite})
	}
	store, err := objectstorage.NewStore(stack.client, scopedBase, binding("installation-a"), objectstorage.WithGatewaySessions())
	require.NoError(t, err)
	sibling, err := objectstorage.NewStore(stack.client, scopedBase, binding("installation-b"), objectstorage.WithGatewaySessions())
	require.NoError(t, err)

	mark := stack.wire.mark()
	written, err := store.PutContent(ctx, "installation-a", strings.NewReader("a document"), objectstorage.PutOptions{ContentType: "text/plain"})
	require.NoError(t, err)
	again, err := store.PutContent(ctx, "installation-a", strings.NewReader("a document"), objectstorage.PutOptions{})
	require.NoError(t, err)
	require.Equal(t, written.Ref, again.Ref, "the same bytes are one object")
	body, info, err := store.Get(ctx, written.Ref)
	require.NoError(t, err)
	content, err := io.ReadAll(body)
	require.NoError(t, err)
	_ = body.Close()
	require.Equal(t, "a document", string(content))
	require.Equal(t, int64(len("a document")), info.Size)
	_, err = store.Head(ctx, written.Ref)
	require.NoError(t, err)
	_, err = store.Put(ctx, objectstorage.Own("notes/one.txt"), strings.NewReader("one"), objectstorage.PutOptions{})
	require.NoError(t, err)
	_, err = store.Copy(ctx, objectstorage.Own("notes/one.txt"), objectstorage.Own("notes/two.txt"), objectstorage.CopyOptions{})
	require.NoError(t, err)
	page, err := store.List(ctx, objectstorage.Own(""), objectstorage.ListOptions{})
	require.NoError(t, err)
	require.Len(t, page.Objects, 2)
	require.Zero(t, page.Skipped, "the staged upload was removed")
	require.NoError(t, store.Delete(ctx, objectstorage.Own("notes/one.txt")))
	// A staged part: written under the ingest session, read under the read
	// session, removed under the maintenance session, and no object.
	staged, err := store.PutStaged(ctx, "installation-a", "upload1part0001", strings.NewReader("a part"), objectstorage.PutOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len("a part")), staged)
	part, size, err := store.GetStaged(ctx, "installation-a", "upload1part0001")
	require.NoError(t, err)
	content, err = io.ReadAll(part)
	require.NoError(t, err)
	_ = part.Close()
	require.Equal(t, "a part", string(content))
	require.Equal(t, int64(len("a part")), size)
	_, _, err = sibling.GetStaged(ctx, "installation-b", "upload1part0001")
	require.ErrorIs(t, err, objectstorage.ErrNotFound, "a staged part is found in its own scope only")
	require.NoError(t, store.DeleteStaged(ctx, "installation-a", "upload1part0001"))
	_, _, err = store.GetStaged(ctx, "installation-a", "upload1part0001")
	require.ErrorIs(t, err, objectstorage.ErrNotFound)
	for _, exchange := range stack.wire.since(mark) {
		require.NotEqual(t, stack.rootKey, exchange.accessKey, "the Store reached MinIO with the gateway's own credential: %+v", exchange)
	}

	// The sibling installation's Store does not find the first one's content.
	_, _, err = sibling.Get(ctx, written.Ref.In("installation-b"))
	require.ErrorIs(t, err, objectstorage.ErrNotFound)

	// A forged key: the first installation's content named under the
	// sibling's scope, by a caller that skipped the Store.
	codec, _ := objectkey.New(scopedBase)
	key, err := codec.Key(objectkey.Key{Partition: tenant, Scope: "installation-a", Slot: objectkey.Content(), Name: written.Ref.Name})
	require.NoError(t, err)
	stack.refusedByMinIO(t, "a forged key under the sibling's scope", readMode, func() error {
		_, err := drain(stack.client.Get(ctx, &storagev0.GetRequest{Key: key, Scope: in(tenant, "installation-b", readMode)}))
		return err
	})
	// And with no scope the same client is refused before any store is asked.
	_, err = drain(stack.client.Get(ctx, &storagev0.GetRequest{Key: key}))
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	reason, _ := reasonOf(err)
	require.Equal(t, "SCOPE_REQUIRED", reason)
	_, err = stack.client.List(ctx, &storagev0.ListRequest{})
	reason, _ = reasonOf(err)
	require.Equal(t, "SCOPE_REQUIRED", reason)
}

func fetch(t *testing.T, method, target string, headers map[string]string, body io.Reader) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, target, body)
	require.NoError(t, err)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	answer, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(answer)
}

func postForm(t *testing.T, target string, fields map[string]string, contentType string, file []byte) (int, string) {
	t.Helper()
	var form bytes.Buffer
	writer := multipart.NewWriter(&form)
	for name, value := range fields {
		require.NoError(t, writer.WriteField(name, value))
	}
	if contentType != "" {
		require.NoError(t, writer.WriteField("Content-Type", contentType))
	}
	part, err := writer.CreateFormFile("file", "upload")
	require.NoError(t, err)
	_, _ = part.Write(file)
	require.NoError(t, writer.Close())
	return fetch(t, http.MethodPost, target, map[string]string{"Content-Type": writer.FormDataContentType()}, &form)
}

// The direct mode: a URL signed by a scope's session reaches one object with
// one method for a short time, binds the upload's type and size where asked,
// and lists nothing. MinIO does the refusing: the URLs are fetched over plain
// HTTP with no credential.
func TestMinIOScopedPresignReachesOneObjectWithinItsBounds(t *testing.T) {
	stack := newScopedStack(t, server.ScopedAllow, "", true)
	l := writeLayout(t, stack)
	ctx := context.Background()
	c := stack.client
	get, put, post := storagev0.PresignMethod_PRESIGN_METHOD_GET, storagev0.PresignMethod_PRESIGN_METHOD_PUT, storagev0.PresignMethod_PRESIGN_METHOD_POST
	readScope, ingestScope := in(l.tenant, l.installation, readMode), in(l.tenant, l.installation, ingestMode)

	signed, err := c.Presign(ctx, &storagev0.PresignRequest{Key: l.own, Method: get, ExpirySeconds: 60, Scope: readScope})
	require.NoError(t, err)
	require.Contains(t, signed.GetUrl(), "X-Amz-Security-Token", "the URL is signed by the session, not by the gateway's own key")
	require.NotContains(t, signed.GetUrl(), "Credential="+stack.rootKey)
	code, body := fetch(t, http.MethodGet, signed.GetUrl(), nil, nil)
	require.Equal(t, http.StatusOK, code, body)
	require.Equal(t, l.bodies[l.own], body)
	// One method.
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		code, _ = fetch(t, method, signed.GetUrl(), nil, strings.NewReader("x"))
		require.Equal(t, http.StatusForbidden, code, "%s on a GET URL", method)
	}
	// A POST to an object's URL is not a form upload; MinIO answers 400.
	code, _ = fetch(t, http.MethodPost, signed.GetUrl(), nil, strings.NewReader("x"))
	require.GreaterOrEqual(t, code, 400, "POST on a GET URL")
	l.unchanged(t, stack)
	// One key: the same query on any other path is refused, the sibling's
	// content and the bucket's listing included.
	parsed, err := url.Parse(signed.GetUrl())
	require.NoError(t, err)
	for what, path := range map[string]string{
		"another object of the scope": "/" + stack.bucket + "/" + stack.prefix + l.second,
		"a sibling's content":         "/" + stack.bucket + "/" + stack.prefix + l.inSibling,
		"the bucket (a listing)":      "/" + stack.bucket,
		"the bucket, with a slash":    "/" + stack.bucket + "/",
	} {
		moved := *parsed
		moved.Path, moved.RawPath = path, ""
		code, _ = fetch(t, http.MethodGet, moved.String(), nil, nil)
		require.Equal(t, http.StatusForbidden, code, "the URL moved to %s", what)
	}
	listing := *parsed
	listing.Path, listing.RawPath = "/"+stack.bucket, ""
	listing.RawQuery += "&list-type=2&prefix=" + url.QueryEscape(stack.prefix+l.scopePrefix)
	code, _ = fetch(t, http.MethodGet, listing.String(), nil, nil)
	require.Equal(t, http.StatusForbidden, code, "the URL with a listing query")
	// A short time.
	brief, err := c.Presign(ctx, &storagev0.PresignRequest{Key: l.own, Method: get, ExpirySeconds: 1, Scope: readScope})
	require.NoError(t, err)
	time.Sleep(2500 * time.Millisecond)
	code, _ = fetch(t, http.MethodGet, brief.GetUrl(), nil, nil)
	require.Equal(t, http.StatusForbidden, code, "a one-second URL after 2.5 seconds")

	// A typed PUT writes its key with its type and nothing else.
	upload := l.scopePrefix + "service.documents/_staging/" + strings.Repeat("a", 32)
	typed, err := c.Presign(ctx, &storagev0.PresignRequest{Key: upload, Method: put, ExpirySeconds: 60, ContentType: "application/pdf", Scope: ingestScope})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"Content-Type": "application/pdf"}, typed.GetHeaders())
	code, body = fetch(t, http.MethodPut, typed.GetUrl(), map[string]string{"Content-Type": "text/html"}, strings.NewReader("<html>"))
	require.GreaterOrEqual(t, code, 400, "another content type: %s", body)
	code, body = fetch(t, http.MethodPut, typed.GetUrl(), nil, strings.NewReader("no type"))
	require.GreaterOrEqual(t, code, 400, "no content type: %s", body)
	_, found := stack.stored(t, upload)
	require.False(t, found)
	code, body = fetch(t, http.MethodPut, typed.GetUrl(), typed.GetHeaders(), strings.NewReader("%PDF"))
	require.Equal(t, http.StatusOK, code, body)
	code, body = fetch(t, http.MethodGet, typed.GetUrl(), typed.GetHeaders(), nil)
	require.GreaterOrEqual(t, code, 400, "a PUT URL does not read")
	require.NotEqual(t, "%PDF", body)

	// A POST binds the size as well: MinIO refuses a body over the maximum,
	// under the minimum, of another type, and any other key.
	form := l.scopePrefix + "service.documents/_staging/" + strings.Repeat("b", 32)
	sized, err := c.Presign(ctx, &storagev0.PresignRequest{Key: form, Method: post, ExpirySeconds: 60,
		ContentType: "application/pdf", ContentLengthMin: 4, ContentLengthMax: 16, Scope: ingestScope})
	require.NoError(t, err)
	require.Equal(t, post, sized.GetMethod())
	require.NotEmpty(t, sized.GetFormFields()["x-amz-security-token"], "the form is signed by the session")
	fields := func(change func(map[string]string)) map[string]string {
		out := map[string]string{}
		for name, value := range sized.GetFormFields() {
			if name != "Content-Type" {
				out[name] = value
			}
		}
		if change != nil {
			change(out)
		}
		return out
	}
	code, body = postForm(t, sized.GetUrl(), fields(nil), "application/pdf", bytes.Repeat([]byte("x"), 17))
	require.GreaterOrEqual(t, code, 400, "a body over the maximum: %s", body)
	code, body = postForm(t, sized.GetUrl(), fields(nil), "application/pdf", []byte("xyz"))
	require.GreaterOrEqual(t, code, 400, "a body under the minimum: %s", body)
	code, body = postForm(t, sized.GetUrl(), fields(nil), "text/html", []byte("<html>"))
	require.GreaterOrEqual(t, code, 400, "another content type: %s", body)
	code, body = postForm(t, sized.GetUrl(), fields(func(f map[string]string) { f["key"] = stack.prefix + l.inSibling }), "application/pdf", []byte("%PDF-1"))
	require.GreaterOrEqual(t, code, 400, "another key: %s", body)
	_, found = stack.stored(t, form)
	require.False(t, found, "a refused form upload wrote its object")
	code, body = postForm(t, sized.GetUrl(), fields(nil), "application/pdf", []byte("%PDF-1"))
	require.Less(t, code, 300, "an upload within every bound: %s", body)
	uploaded, found := stack.stored(t, form)
	require.True(t, found)
	require.Equal(t, "%PDF-1", uploaded)
	l.unchanged(t, stack)

	// The gateway refuses to sign outside the bounds; nothing reaches MinIO.
	for what, request := range map[string]*storagev0.PresignRequest{
		"a sibling's key under its own scope": {Key: l.inSibling, Method: get, ExpirySeconds: 60, Scope: readScope},
		"a legacy key":                        {Key: l.legacy, Method: get, ExpirySeconds: 60, Scope: readScope},
		"a prefix":                            {Key: l.scopePrefix, Method: get, ExpirySeconds: 60, Scope: readScope},
		"a GET by the ingest session":         {Key: l.own, Method: get, ExpirySeconds: 60, Scope: ingestScope},
		"a PUT by the read session":           {Key: upload, Method: put, ExpirySeconds: 60, Scope: readScope},
		"no lifetime":                         {Key: l.own, Method: get, Scope: readScope},
		"a lifetime over five minutes":        {Key: l.own, Method: get, ExpirySeconds: 301, Scope: readScope},
		"a sized PUT":                         {Key: upload, Method: put, ExpirySeconds: 60, ContentLengthMax: 16, Scope: ingestScope},
		"a POST without a size":               {Key: upload, Method: post, ExpirySeconds: 60, Scope: ingestScope},
	} {
		_, err := c.Presign(ctx, request)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%s: %v", what, err)
		reason, _ := reasonOf(err)
		require.Equal(t, "PRESIGN_REFUSED", reason, what)
	}

}
