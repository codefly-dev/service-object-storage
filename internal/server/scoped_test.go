package server_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v1"
	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/backend/mem"
	"github.com/codefly-dev/service-object-storage/internal/events"
	"github.com/codefly-dev/service-object-storage/internal/probetest"
	"github.com/codefly-dev/service-object-storage/internal/server"
	"github.com/codefly-dev/service-object-storage/internal/sessions"
)

// These tests are the gateway's own logic over a fake issuer: which calls are
// refused, and that a scoped call runs on the session's client and never on
// the gateway's. They show no store refusing anything; the MinIO integration
// suite does that.

// stubIssuer issues a credential for any scope its policy can be rendered for.
type stubIssuer struct {
	mu     sync.Mutex
	prefix string
	fail   error
	issued []sessions.Scope
}

func (i *stubIssuer) Kind() string                          { return "stub" }
func (i *stubIssuer) Qualification() sessions.Qualification { return sessions.Qualified }
func (i *stubIssuer) Grant(scope sessions.Scope) ([]byte, error) {
	return sessions.S3SessionPolicy("bucket", i.prefix, scope)
}

func (i *stubIssuer) Issue(_ context.Context, scope sessions.Scope, ttl time.Duration) (sessions.Credential, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.fail != nil {
		return sessions.Credential{}, i.fail
	}
	if _, err := sessions.S3SessionPolicy("bucket", i.prefix, scope); err != nil {
		return sessions.Credential{}, err
	}
	i.issued = append(i.issued, scope)
	return sessions.Credential{AccessKey: "session", SecretKey: "secret", SessionToken: "token", Expires: time.Now().Add(ttl)}, nil
}

// notClosing keeps the shared session store open when a session is dropped.
type notClosing struct{ backend.Backend }

func (notClosing) Close() error { return nil }

type scopedGateway struct {
	client  storagev1.ObjectStorageClient
	own     backend.Backend // what the gateway's own credential reaches
	session backend.Backend // what any session reaches
	issuer  *stubIssuer
}

const grantedBase = "files"

func newScopedGateway(t *testing.T, mode server.ScopedMode, presign bool) scopedGateway {
	t.Helper()
	own, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	session, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	grants, err := sessions.ParseGrants(`[{"base":"files","modes":["read","ingest","maintenance"]},{"base":"readonly","modes":["read"]}]`)
	require.NoError(t, err)
	issuer := &stubIssuer{}
	manager, err := sessions.NewManager(sessions.Options{
		Backend: backend.Config{Kind: "mem", Bucket: "bucket"}, Issuer: issuer, Authorizer: grants,
		Open: func(_ context.Context, cfg backend.Config) (backend.Backend, error) {
			if cfg.SessionToken != "token" {
				return nil, errors.New("opened without the session's credential")
			}
			return notClosing{session}, nil
		},
	})
	require.NoError(t, err)
	hub := events.NewHub()
	t.Cleanup(hub.Close)
	srv := server.New(own, hub, probetest.Monitor(t, own)).WithScoped(server.Scoped{Mode: mode, Manager: manager, Presign: presign})
	return scopedGateway{client: newTestClientForServer(t, srv), own: own, session: session, issuer: issuer}
}

func scope(mode storagev1.ScopeMode) *storagev1.Scope {
	return &storagev1.Scope{Base: grantedBase, Partition: "tenant-a", Scope: "installation-a", Mode: mode}
}

const (
	read        = storagev1.ScopeMode_SCOPE_MODE_READ
	ingest      = storagev1.ScopeMode_SCOPE_MODE_INGEST
	maintenance = storagev1.ScopeMode_SCOPE_MODE_MAINTENANCE
)

var contentKey = "files/tenant-a/installation-a/_sha256/" + strings.Repeat("a", 64)

// everyCall makes each data RPC once with the given scope and returns its
// error by name.
func everyCall(t *testing.T, c storagev1.ObjectStorageClient, with *storagev1.Scope) map[string]error {
	t.Helper()
	ctx := context.Background()
	out := map[string]error{}
	_, out["Stat"] = c.Stat(ctx, &storagev1.StatRequest{Key: contentKey, Scope: with})
	_, _, out["Get"] = getObject(t, c, &storagev1.GetRequest{Key: contentKey, Scope: with})
	many, err := c.GetMany(ctx, &storagev1.GetManyRequest{Items: []*storagev1.GetManyItem{{Key: contentKey}}, Scope: with})
	require.NoError(t, err)
	_, out["GetMany"] = many.Recv()
	_, out["Put"] = putObject(t, c, contentKey, []byte("body"), &storagev1.PutHeader{Scope: with})
	_, out["Delete"] = c.Delete(ctx, &storagev1.DeleteRequest{Key: contentKey, Scope: with})
	_, out["DeleteMany"] = c.DeleteMany(ctx, &storagev1.DeleteManyRequest{Keys: []string{contentKey}, Scope: with})
	_, out["List"] = c.List(ctx, &storagev1.ListRequest{Prefix: "files/", Scope: with})
	_, out["Copy"] = c.Copy(ctx, &storagev1.CopyRequest{SourceKey: contentKey, DestKey: contentKey + "x", Scope: with})
	_, out["Presign"] = c.Presign(ctx, &storagev1.PresignRequest{Key: contentKey, Method: storagev1.PresignMethod_PRESIGN_METHOD_GET, ExpirySeconds: 60, Scope: with})
	return out
}

func requireEmpty(t *testing.T, be backend.Backend) {
	t.Helper()
	listed, err := be.List(context.Background(), backend.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, listed.Objects)
}

// The default: a gateway that was not told to run scoped sessions refuses a
// call that names a scope. It does not run it with its own credential.
func TestAScopedCallIsRefusedWhereScopedSessionsAreOff(t *testing.T) {
	own, err := mem.New(context.Background(), backend.Config{})
	require.NoError(t, err)
	plain := newTestClientFor(t, own)
	off := newScopedGateway(t, server.ScopedOff, true)
	for name, gateway := range map[string]struct {
		client storagev1.ObjectStorageClient
		own    backend.Backend
	}{"never configured": {plain, own}, "configured off": {off.client, off.own}} {
		for rpc, err := range everyCall(t, gateway.client, scope(maintenance)) {
			requireReason(t, err, codes.FailedPrecondition, "SCOPED_SESSIONS_UNAVAILABLE")
			require.NotEmpty(t, rpc)
		}
		requireEmpty(t, gateway.own)
		// And a call without a scope is what it always was.
		_, err := putObject(t, gateway.client, "plain.txt", []byte("x"), nil)
		require.NoError(t, err, name)
		caps, err := gateway.client.Capabilities(context.Background(), &storagev1.CapabilitiesRequest{})
		require.NoError(t, err)
		require.Equal(t, "off", caps.GetScopedSessions())
		require.Empty(t, caps.GetScopedSessionIssuer())
	}
	require.Empty(t, off.issuer.issued)
}

// A scoped call runs on the client that holds the scope's session. Nothing it
// does reaches the client that holds the gateway's own credential.
func TestAScopedCallRunsOnItsSessionNeverOnTheGatewaysCredential(t *testing.T) {
	gateway := newScopedGateway(t, server.ScopedAllow, false)
	ctx := context.Background()
	_, err := putObject(t, gateway.client, contentKey, []byte("scoped"), &storagev1.PutHeader{Scope: scope(ingest)})
	require.NoError(t, err)
	requireEmpty(t, gateway.own)
	_, err = gateway.session.Stat(ctx, contentKey, "")
	require.NoError(t, err)

	header, body, err := getObject(t, gateway.client, &storagev1.GetRequest{Key: contentKey, Scope: scope(read)})
	require.NoError(t, err)
	require.Equal(t, "scoped", string(body))
	require.Equal(t, contentKey, header.GetInfo().GetKey())
	_, err = gateway.client.Stat(ctx, &storagev1.StatRequest{Key: contentKey})
	require.Equal(t, codes.NotFound, status.Code(err), "the gateway's own credential reads its own store, where nothing was written")

	_, err = gateway.client.Copy(ctx, &storagev1.CopyRequest{SourceKey: contentKey, DestKey: contentKey[:len(contentKey)-1] + "b", Scope: scope(maintenance)})
	require.NoError(t, err)
	listed, err := gateway.client.List(ctx, &storagev1.ListRequest{Prefix: "files/tenant-a/installation-a/", Scope: scope(read)})
	require.NoError(t, err)
	require.Len(t, listed.GetObjects(), 2)
	_, err = gateway.client.DeleteMany(ctx, &storagev1.DeleteManyRequest{Keys: []string{contentKey}, Scope: scope(maintenance)})
	require.NoError(t, err)
	_, err = gateway.client.Delete(ctx, &storagev1.DeleteRequest{Key: contentKey[:len(contentKey)-1] + "b", Scope: scope(maintenance)})
	require.NoError(t, err)
	requireEmpty(t, gateway.session)
	requireEmpty(t, gateway.own)

	// One session per mode was issued, each for exactly the scope named.
	modes := map[sessions.Mode]int{}
	for _, issued := range gateway.issuer.issued {
		require.Equal(t, sessions.Scope{Base: "files", Partition: "tenant-a", Scope: "installation-a", Mode: issued.Mode}, issued)
		modes[issued.Mode]++
	}
	require.Equal(t, map[sessions.Mode]int{sessions.Read: 1, sessions.Ingest: 1, sessions.Maintenance: 1}, modes)

	caps, err := gateway.client.Capabilities(ctx, &storagev1.CapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, "allow", caps.GetScopedSessions())
	require.Equal(t, "qualified", caps.GetScopedSessionIssuer())
}

// Where scoped sessions are required, nothing a caller sends runs with the
// gateway's own credential: every call without a scope is refused, Watch and
// Native included, and only Capabilities and Ready still answer.
func TestRequireRefusesEveryCallThatNamesNoScope(t *testing.T) {
	gateway := newScopedGateway(t, server.ScopedRequire, true)
	ctx := context.Background()
	for rpc, err := range everyCall(t, gateway.client, nil) {
		info, _ := requireReason(t, err, codes.PermissionDenied, "SCOPE_REQUIRED")
		require.NotNil(t, info, rpc)
	}
	_, err := gateway.client.Native(ctx, &storagev1.NativeRequest{Verb: "anything"})
	requireReason(t, err, codes.PermissionDenied, "SCOPE_REQUIRED")
	watch, err := gateway.client.Watch(ctx, &storagev1.WatchRequest{})
	require.NoError(t, err)
	_, err = watch.Recv()
	requireReason(t, err, codes.PermissionDenied, "SCOPE_REQUIRED")
	requireEmpty(t, gateway.own)
	requireEmpty(t, gateway.session)
	require.Empty(t, gateway.issuer.issued)

	caps, err := gateway.client.Capabilities(ctx, &storagev1.CapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, "require", caps.GetScopedSessions())
	_, err = gateway.client.Ready(ctx, &storagev1.ReadyRequest{})
	require.NoError(t, err)

	// With a scope the same calls are served.
	_, err = putObject(t, gateway.client, contentKey, []byte("scoped"), &storagev1.PutHeader{Scope: scope(ingest)})
	require.NoError(t, err)
}

// Every way of not having a session is a refusal that names its reason, and
// none of them leaves anything in either store.
func TestAScopeThatCannotHaveItsSessionIsRefused(t *testing.T) {
	gateway := newScopedGateway(t, server.ScopedAllow, true)
	ctx := context.Background()
	stat := func(with *storagev1.Scope) error {
		_, err := gateway.client.Stat(ctx, &storagev1.StatRequest{Key: contentKey, Scope: with})
		return err
	}
	put := func(with *storagev1.Scope) error {
		_, err := putObject(t, gateway.client, contentKey, []byte("never written"), &storagev1.PutHeader{Scope: with})
		return err
	}
	for name, refusal := range map[string]struct {
		with   *storagev1.Scope
		code   codes.Code
		reason string
	}{
		"a base the caller was not granted":       {&storagev1.Scope{Base: "elsewhere", Partition: "t", Scope: "i", Mode: read}, codes.PermissionDenied, "SCOPE_NOT_GRANTED"},
		"a mode the caller was not granted":       {&storagev1.Scope{Base: "readonly", Partition: "t", Scope: "i", Mode: ingest}, codes.PermissionDenied, "SCOPE_NOT_GRANTED"},
		"no mode":                                 {&storagev1.Scope{Base: "files", Partition: "t", Scope: "i"}, codes.InvalidArgument, "SCOPE_INVALID"},
		"a mode this gateway does not know":       {&storagev1.Scope{Base: "files", Partition: "t", Scope: "i", Mode: 99}, codes.InvalidArgument, "SCOPE_INVALID"},
		"no partition":                            {&storagev1.Scope{Base: "files", Scope: "i", Mode: read}, codes.InvalidArgument, "SCOPE_INVALID"},
		"no scope":                                {&storagev1.Scope{Base: "files", Partition: "t", Mode: read}, codes.InvalidArgument, "SCOPE_INVALID"},
		"a base that is a path":                   {&storagev1.Scope{Base: "files/tenant-a", Partition: "t", Scope: "i", Mode: read}, codes.InvalidArgument, "SCOPE_INVALID"},
		"an empty scope message":                  {&storagev1.Scope{}, codes.InvalidArgument, "SCOPE_INVALID"},
		"an identifier over the convention's cap": {&storagev1.Scope{Base: "files", Partition: strings.Repeat("a", 241), Scope: "i", Mode: read}, codes.InvalidArgument, "SCOPE_INVALID"},
	} {
		requireReason(t, stat(refusal.with), refusal.code, refusal.reason)
		requireReason(t, put(refusal.with), refusal.code, refusal.reason)
		require.NotEmpty(t, name)
	}
	require.Empty(t, gateway.issuer.issued)

	// A scope and a binding name two different stores.
	_, err := gateway.client.Stat(ctx, &storagev1.StatRequest{Key: contentKey, Scope: scope(read), Binding: s3Binding("")})
	requireReason(t, err, codes.InvalidArgument, "SCOPE_INVALID")

	// A policy the store's limit does not hold is refused, never shortened.
	gateway.issuer.prefix = strings.Repeat("p", 600) + "/"
	long := &storagev1.Scope{Base: "files", Partition: strings.Repeat("a", 240), Scope: strings.Repeat("b", 240), Mode: read}
	requireReason(t, stat(long), codes.FailedPrecondition, "SCOPE_POLICY_TOO_LARGE")
	long.Mode = ingest
	gateway.issuer.prefix = strings.Repeat("p", 1700) + "/"
	requireReason(t, put(long), codes.FailedPrecondition, "SCOPE_POLICY_TOO_LARGE")
	gateway.issuer.prefix = ""

	// An issuer that does not answer fails the call closed.
	gateway.issuer.fail = errors.New("dial tcp: connection refused")
	requireReason(t, stat(scope(read)), codes.Unavailable, "SCOPE_ISSUER_UNAVAILABLE")
	requireReason(t, put(scope(ingest)), codes.Unavailable, "SCOPE_ISSUER_UNAVAILABLE")
	gateway.issuer.fail = nil

	requireEmpty(t, gateway.own)
	requireEmpty(t, gateway.session)
	require.NoError(t, put(scope(ingest)), "and the next call, once the issuer answers, is served")
}

// A URL signed by a session is the opt-in direct mode, and it is bounded
// here because signing calls no store: one method matching the session's
// mode, one exact object of the scope, an explicit short lifetime.
func TestAScopedPresignIsOptInAndBounded(t *testing.T) {
	ctx := context.Background()
	get, put, post := storagev1.PresignMethod_PRESIGN_METHOD_GET, storagev1.PresignMethod_PRESIGN_METHOD_PUT, storagev1.PresignMethod_PRESIGN_METHOD_POST
	closed := newScopedGateway(t, server.ScopedAllow, false)
	_, err := closed.client.Presign(ctx, &storagev1.PresignRequest{Key: contentKey, Method: get, ExpirySeconds: 60, Scope: scope(read)})
	requireReason(t, err, codes.FailedPrecondition, "SCOPED_SESSIONS_UNAVAILABLE")
	require.Empty(t, closed.issuer.issued)

	gateway := newScopedGateway(t, server.ScopedAllow, true)
	signed, err := gateway.client.Presign(ctx, &storagev1.PresignRequest{Key: contentKey, Method: get, ExpirySeconds: 60, Scope: scope(read)})
	require.NoError(t, err)
	require.Contains(t, signed.GetUrl(), contentKey)
	require.LessOrEqual(t, signed.GetExpiresAtUnixMs(), time.Now().Add(61*time.Second).UnixMilli())

	other := &storagev1.Scope{Base: "files", Partition: "tenant-a", Scope: "installation-b", Mode: read}
	for name, request := range map[string]*storagev1.PresignRequest{
		"a GET signed by the ingest session":      {Key: contentKey, Method: get, ExpirySeconds: 60, Scope: scope(ingest)},
		"a GET signed by the maintenance session": {Key: contentKey, Method: get, ExpirySeconds: 60, Scope: scope(maintenance)},
		"a PUT signed by the read session":        {Key: contentKey, Method: put, ExpirySeconds: 60, Scope: scope(read)},
		"a POST signed by the read session":       {Key: contentKey, Method: post, ExpirySeconds: 60, ContentLengthMax: 10, Scope: scope(read)},
		"no method":                               {Key: contentKey, ExpirySeconds: 60, Scope: scope(read)},
		"a key of a sibling scope":                {Key: strings.Replace(contentKey, "installation-a", "installation-b", 1), Method: get, ExpirySeconds: 60, Scope: scope(read)},
		"its own key under a sibling's scope":     {Key: contentKey, Method: get, ExpirySeconds: 60, Scope: other},
		"a key outside the base":                  {Key: "tenant-a/blobs/" + strings.Repeat("a", 64), Method: get, ExpirySeconds: 60, Scope: scope(read)},
		"the scope's prefix, not an object":       {Key: "files/tenant-a/installation-a/", Method: get, ExpirySeconds: 60, Scope: scope(read)},
		"a key the convention did not write":      {Key: "files/tenant-a/installation-a/../installation-b/x", Method: get, ExpirySeconds: 60, Scope: scope(read)},
		"no lifetime":                             {Key: contentKey, Method: get, Scope: scope(read)},
		"a lifetime over the limit":               {Key: contentKey, Method: get, ExpirySeconds: 301, Scope: scope(read)},
		"a negative lifetime":                     {Key: contentKey, Method: get, ExpirySeconds: -1, Scope: scope(read)},
	} {
		_, err := gateway.client.Presign(ctx, request)
		info, _ := requireReason(t, err, codes.InvalidArgument, "PRESIGN_REFUSED")
		require.NotNil(t, info, name)
	}

	// Bounds are bound or refused. This store binds none, so it signs none.
	for name, request := range map[string]*storagev1.PresignRequest{
		"a typed PUT":           {Key: contentKey, Method: put, ExpirySeconds: 60, ContentType: "application/pdf", Scope: scope(ingest)},
		"a sized POST":          {Key: contentKey, Method: post, ExpirySeconds: 60, ContentLengthMax: 10, Scope: scope(ingest)},
		"a sized POST, unbound": {Key: contentKey, Method: post, ExpirySeconds: 60, ContentLengthMax: 10},
	} {
		_, err := gateway.client.Presign(ctx, request)
		require.Equal(t, codes.Unimplemented, status.Code(err), "%s: %v", name, err)
	}
	for name, request := range map[string]*storagev1.PresignRequest{
		"a sized PUT":                 {Key: contentKey, Method: put, ExpirySeconds: 60, ContentLengthMax: 10, Scope: scope(ingest)},
		"a POST with no size":         {Key: contentKey, Method: post, ExpirySeconds: 60, Scope: scope(ingest)},
		"a POST with min over max":    {Key: contentKey, Method: post, ExpirySeconds: 60, ContentLengthMin: 11, ContentLengthMax: 10, Scope: scope(ingest)},
		"a typed GET":                 {Key: contentKey, Method: get, ExpirySeconds: 60, ContentType: "text/plain", Scope: scope(read)},
		"a typed GET without a scope": {Key: contentKey, Method: get, ExpirySeconds: 60, ContentType: "text/plain"},
	} {
		_, err := gateway.client.Presign(ctx, request)
		info, _ := requireReason(t, err, codes.InvalidArgument, "PRESIGN_REFUSED")
		require.NotNil(t, info, name)
	}
	// An unscoped, unbounded presign is what it was.
	_, err = gateway.client.Presign(ctx, &storagev1.PresignRequest{Key: "any/key", Method: put, ExpirySeconds: 60})
	require.NoError(t, err)
}
