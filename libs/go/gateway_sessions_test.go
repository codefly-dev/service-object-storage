package objectstorage_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v1"
	objectstorage "github.com/codefly-dev/service-object-storage/libs/go"
	"github.com/codefly-dev/service-object-storage/libs/go/internal/gatewaytest"
	"github.com/codefly-dev/service-object-storage/libs/go/miniosession"
)

// scopes records the scope each call named and forwards the call without it,
// to a gateway that runs no scoped sessions: what is under test is what the
// Store asks for, not what a gateway does with it.
type scopes struct {
	storagev1.ObjectStorageClient
	mu   sync.Mutex
	seen []string
}

func (s *scopes) note(rpc string, scope *storagev1.Scope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if scope == nil {
		s.seen = append(s.seen, rpc+" unscoped")
		return
	}
	s.seen = append(s.seen, strings.Join([]string{rpc, scope.GetBase(), scope.GetPartition(), scope.GetScope(),
		strings.ToLower(strings.TrimPrefix(scope.GetMode().String(), "SCOPE_MODE_"))}, " "))
}

func (s *scopes) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := s.seen
	s.seen = nil
	return seen
}

func (s *scopes) Stat(ctx context.Context, in *storagev1.StatRequest, opts ...grpc.CallOption) (*storagev1.GetHeader, error) {
	s.note("Stat", in.GetScope())
	in.Scope = nil
	return s.ObjectStorageClient.Stat(ctx, in, opts...)
}

func (s *scopes) Get(ctx context.Context, in *storagev1.GetRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[storagev1.GetResponse], error) {
	s.note("Get", in.GetScope())
	in.Scope = nil
	return s.ObjectStorageClient.Get(ctx, in, opts...)
}

func (s *scopes) List(ctx context.Context, in *storagev1.ListRequest, opts ...grpc.CallOption) (*storagev1.ListResult, error) {
	s.note("List", in.GetScope())
	in.Scope = nil
	return s.ObjectStorageClient.List(ctx, in, opts...)
}

func (s *scopes) Delete(ctx context.Context, in *storagev1.DeleteRequest, opts ...grpc.CallOption) (*storagev1.DeleteResult, error) {
	s.note("Delete", in.GetScope())
	in.Scope = nil
	return s.ObjectStorageClient.Delete(ctx, in, opts...)
}

func (s *scopes) Copy(ctx context.Context, in *storagev1.CopyRequest, opts ...grpc.CallOption) (*storagev1.CopyResult, error) {
	s.note("Copy", in.GetScope())
	in.Scope = nil
	return s.ObjectStorageClient.Copy(ctx, in, opts...)
}

func (s *scopes) Presign(ctx context.Context, in *storagev1.PresignRequest, opts ...grpc.CallOption) (*storagev1.PresignResult, error) {
	s.note("Presign", in.GetScope())
	in.Scope = nil
	return s.ObjectStorageClient.Presign(ctx, in, opts...)
}

type scopedPut struct {
	grpc.ClientStreamingClient[storagev1.PutRequest, storagev1.PutResult]
	owner *scopes
}

func (p scopedPut) Send(request *storagev1.PutRequest) error {
	if header := request.GetHeader(); header != nil {
		p.owner.note("Put", header.GetScope())
		header.Scope = nil
	}
	return p.ClientStreamingClient.Send(request)
}

func (s *scopes) Put(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[storagev1.PutRequest, storagev1.PutResult], error) {
	stream, err := s.ObjectStorageClient.Put(ctx, opts...)
	return scopedPut{stream, s}, err
}

// Every call of a Store built with gateway sessions names the object's own
// partition and scope under the Store's base, and the one mode its verb
// belongs to: reads read, uploads ingest, and promotion, copy and delete are
// maintenance.
func TestAStoreWithGatewaySessionsNamesItsScopeAndModeOnEveryCall(t *testing.T) {
	ctx := context.Background()
	recorded := &scopes{ObjectStorageClient: gatewaytest.Memory(t)}
	binding := objectstorage.Binding{Partition: "tenant-a", Principal: "service:worker", Reach: []string{"east", "west"},
		Mode: objectstorage.Privileged, Access: objectstorage.ReadWrite}
	store, err := objectstorage.NewStore(recorded, "files", objectstorage.FixedBinder(binding), objectstorage.WithGatewaySessions())
	if err != nil {
		t.Fatal(err)
	}
	expect := func(what string, want ...string) {
		t.Helper()
		got := recorded.take()
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("%s named\n  %s\nwant\n  %s", what, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
		}
	}
	east, west := "files tenant-a east ", "files tenant-a west "

	if _, err := store.Put(ctx, objectstorage.Own("a.txt").In("east"), strings.NewReader("a"), objectstorage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	expect("Put", "Put "+east+"ingest")

	content, err := store.PutContent(ctx, "west", strings.NewReader("content"), objectstorage.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	expect("PutContent", "Put "+west+"ingest", "Copy "+west+"maintenance", "Delete "+west+"maintenance")
	if _, err := store.PutContent(ctx, "west", strings.NewReader("content"), objectstorage.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// Content already there is copied over again, so that it is as young as
	// content just written.
	expect("PutContent of existing content", "Put "+west+"ingest", "Copy "+west+"maintenance", "Delete "+west+"maintenance")

	body, _, err := store.Get(ctx, content.Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(body)
	_ = body.Close()
	if _, err := store.Head(ctx, objectstorage.Own("a.txt").In("east")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(ctx, objectstorage.Own("").In("east"), objectstorage.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListScope(ctx, "west", objectstorage.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	expect("reads", "Get "+west+"read", "Stat "+east+"read", "List "+east+"read", "List "+west+"read")

	if _, err := store.Copy(ctx, objectstorage.Own("a.txt").In("east"), objectstorage.Own("b.txt").In("east"), objectstorage.CopyOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, objectstorage.Own("b.txt").In("east")); err != nil {
		t.Fatal(err)
	}
	expect("copy and delete", "Copy "+east+"maintenance", "Delete "+east+"maintenance")

	// One session is one scope: a copy between two is refused before any call.
	_, err = store.Copy(ctx, objectstorage.Own("a.txt").In("east"), objectstorage.Own("a.txt").In("west"), objectstorage.CopyOptions{})
	if !strings.Contains(errString(err), "inside one scope") {
		t.Fatalf("a copy across scopes: %v", err)
	}
	expect("a copy across scopes")

	if _, err := store.PresignGet(ctx, content.Ref, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PresignPut(ctx, objectstorage.Own("upload").In("east"), time.Minute); err != nil {
		t.Fatal(err)
	}
	expect("presign", "Presign "+west+"read", "Presign "+east+"ingest")

	// Without the option nothing names a scope.
	plain, err := objectstorage.NewStore(recorded, "files", objectstorage.FixedBinder(binding))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Head(ctx, objectstorage.Own("a.txt").In("east")); err != nil {
		t.Fatal(err)
	}
	expect("a plain store", "Stat unscoped")
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// A gateway that runs no scoped sessions refuses such a Store everything: no
// call is quietly served with the gateway's own credential.
func TestAStoreWithGatewaySessionsIsRefusedByAGatewayWithoutThem(t *testing.T) {
	ctx := context.Background()
	binding := objectstorage.Binding{Partition: "tenant-a", Principal: "service:worker", Reach: []string{"east"},
		Mode: objectstorage.Strict, Access: objectstorage.ReadWrite}
	client := gatewaytest.Memory(t)
	store, err := objectstorage.NewStore(client, "files", objectstorage.FixedBinder(binding), objectstorage.WithGatewaySessions())
	if err != nil {
		t.Fatal(err)
	}
	refused := func(what string, err error) {
		t.Helper()
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "scoped sessions") {
			t.Fatalf("%s: %v", what, err)
		}
	}
	_, err = store.Put(ctx, objectstorage.Own("a.txt"), strings.NewReader("a"), objectstorage.PutOptions{})
	refused("Put", err)
	_, err = store.PutContent(ctx, "east", strings.NewReader("a"), objectstorage.PutOptions{})
	refused("PutContent", err)
	_, err = store.Head(ctx, objectstorage.Own("a.txt"))
	refused("Head", err)
	_, _, err = store.Get(ctx, objectstorage.Own("a.txt"))
	refused("Get", err)
	_, err = store.List(ctx, objectstorage.Own(""), objectstorage.ListOptions{})
	refused("List", err)
	refused("Delete", store.Delete(ctx, objectstorage.Own("a.txt")))
	_, err = store.PresignGet(ctx, objectstorage.Own("a.txt"), time.Minute)
	refused("PresignGet", err)
	listed, err := client.List(ctx, &storagev1.ListRequest{})
	if err != nil || len(listed.GetObjects()) != 0 {
		t.Fatalf("a refused Store left objects behind: %v %v", listed.GetObjects(), err)
	}

	// The two ways of confining reads are not combined.
	_, err = objectstorage.NewStore(client, "files", objectstorage.FixedBinder(binding),
		objectstorage.WithGatewaySessions(), objectstorage.WithReaderSessions(miniosession.Issuer{}))
	if err == nil {
		t.Fatal("a store took gateway sessions and its own reader sessions")
	}
}
