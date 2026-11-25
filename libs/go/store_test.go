package objectstorage_test

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	objectstorage "github.com/codefly-dev/service-object-storage/libs/go"
	"github.com/codefly-dev/service-object-storage/libs/go/internal/gatewaytest"
	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
	"github.com/codefly-dev/service-object-storage/libs/go/objectscopetest"
)

// The property the library ships: over random objects and random bindings, a
// Store returns, lists, changes and refuses exactly what the model allows.
// Here over the in-memory backend, so it runs wherever go test does; the
// integration suite runs the same proof on MinIO.
func TestProveHoldsOnAStore(t *testing.T) {
	objectscopetest.Prove(t, objectscopetest.Proof{Client: gatewaytest.Memory(t), Base: "proof", Rounds: 600, Objects: 240})
}

type bindingOf func(context.Context) (objectstorage.Binding, error)

// place is the reference resolution a hand-written store would do.
func place(binding objectstorage.Binding, ref objectstorage.Ref) (string, objectkey.Slot) {
	scope := ref.Scope
	if scope == "" {
		scope = binding.Reach[0]
	}
	switch {
	case ref.Shared:
		return scope, objectkey.Shared()
	case ref.Content:
		return scope, objectkey.Content()
	case ref.Principal == "":
		return scope, objectkey.Principal(binding.Principal)
	default:
		return scope, objectkey.Principal(ref.Principal)
	}
}

// copyUnchecked is a Store whose Copy checks where it writes and not what it
// reads.
type copyUnchecked struct {
	*objectstorage.Store
	bind bindingOf
}

func (d copyUnchecked) Copy(ctx context.Context, from, to objectstorage.Ref, _ objectstorage.CopyOptions) (objectstorage.Info, error) {
	if _, err := d.Store.PresignPut(ctx, to, time.Minute); err != nil {
		return objectstorage.Info{}, err
	}
	binding, _ := d.bind(ctx)
	codec, _ := objectkey.New(d.Store.Base())
	key := func(ref objectstorage.Ref) string {
		scope, slot := place(binding, ref)
		physical, _ := codec.Key(objectkey.Key{Partition: binding.Partition, Scope: scope, Slot: slot, Name: ref.Name})
		return physical
	}
	_, err := d.Store.Client().Copy(ctx, &storagev0.CopyRequest{SourceKey: key(from), DestKey: key(to)})
	if status.Code(err) == codes.NotFound {
		err = errors.Join(objectstorage.ErrNotFound, err)
	}
	return objectstorage.Info{}, err
}

// listRawPrefix is a Store whose List asks for the slot's prefix without the
// separator that ends it, and trusts what comes back.
type listRawPrefix struct {
	*objectstorage.Store
	bind bindingOf
}

func (d listRawPrefix) List(ctx context.Context, in objectstorage.Ref, options objectstorage.ListOptions) (objectstorage.Page, error) {
	if _, err := d.Store.List(ctx, in, objectstorage.ListOptions{Limit: 1}); err != nil || in.Name != "" {
		return d.Store.List(ctx, in, options)
	}
	binding, _ := d.bind(ctx)
	codec, _ := objectkey.New(d.Store.Base())
	scope, slot := place(binding, in)
	prefix, _ := codec.SlotPrefix(binding.Partition, scope, slot)
	listed, err := d.Store.Client().List(ctx, &storagev0.ListRequest{Prefix: strings.TrimSuffix(prefix, "/"), PageToken: options.Token, Limit: int32(options.Limit)})
	if err != nil {
		return objectstorage.Page{}, err
	}
	page := objectstorage.Page{Next: listed.GetNextPageToken()}
	for _, object := range listed.GetObjects() {
		key, err := codec.Parse(object.GetKey())
		if err != nil || key.Staging {
			continue
		}
		page.Objects = append(page.Objects, objectstorage.Info{Size: object.GetSize(), Ref: objectstorage.Ref{
			Scope: key.Scope, Principal: key.Slot.Reference(), Shared: key.Slot.IsShared(), Content: key.Slot.IsContent(), Name: key.Name,
		}})
	}
	return page, nil
}

// joined is the key convention written by hand: the identifiers and the name
// joined by "/", as they come.
type joined struct{ base string }

func (j joined) slot(slot objectkey.Slot) string {
	if reference := slot.Reference(); reference != "" {
		return strings.Replace(reference, ":", ".", 1)
	}
	return slot.String()
}

func (j joined) Key(key objectkey.Key) (string, error) {
	if key.Staging {
		return strings.Join([]string{j.base, key.Partition, key.Scope, j.slot(key.Slot), "_staging", key.Name}, "/"), nil
	}
	return strings.Join([]string{j.base, key.Partition, key.Scope, j.slot(key.Slot), key.Name}, "/"), nil
}

func (j joined) ScopePrefix(partition, scope string) (string, error) {
	return strings.Join([]string{j.base, partition, scope, ""}, "/"), nil
}

func (j joined) DirectoryPrefix(partition, scope string, slot objectkey.Slot, directory string) (string, error) {
	prefix := strings.Join([]string{j.base, partition, scope, j.slot(slot), ""}, "/")
	if directory != "" {
		prefix += directory + "/"
	}
	return prefix, nil
}

func (j joined) Parse(physical string) (objectkey.Key, error) {
	parts := strings.SplitN(physical, "/", 5)
	if len(parts) != 5 || parts[0] != j.base {
		return objectkey.Key{}, objectkey.ErrInvalid
	}
	key := objectkey.Key{Partition: parts[1], Scope: parts[2], Name: parts[4]}
	switch kind, id, found := strings.Cut(parts[3], "."); {
	case parts[3] == "_shared":
		key.Slot = objectkey.Shared()
	case parts[3] == "_sha256":
		key.Slot = objectkey.Content()
	case found:
		key.Slot = objectkey.Principal(kind + ":" + id)
	default:
		return objectkey.Key{}, objectkey.ErrInvalid
	}
	return key, nil
}

// A proof that cannot fail proves nothing. Each case is a store with one
// defect a hand-written wrapper plausibly has, and the proof must notice.
func TestProveCatchesADefectiveStore(t *testing.T) {
	rebound := func(change func(*objectstorage.Binding)) func(objectstorage.Binder) objectstorage.Binder {
		return func(binder objectstorage.Binder) objectstorage.Binder {
			return objectstorage.BinderFunc(func(ctx context.Context) (objectstorage.Binding, error) {
				binding, err := binder.Bind(ctx)
				if err == nil && binding.Partition != "" && binding.Mode != 0 && len(binding.Reach) > 0 {
					change(&binding)
				}
				return binding, err
			})
		}
	}
	for name, defect := range map[string]func(*objectstorage.Store, objectstorage.Binder) objectscopetest.Confined{
		"copy does not check its source": func(store *objectstorage.Store, binder objectstorage.Binder) objectscopetest.Confined {
			return copyUnchecked{Store: store, bind: binder.Bind}
		},
		"list asks for the prefix without its separator": func(store *objectstorage.Store, binder objectstorage.Binder) objectscopetest.Confined {
			return listRawPrefix{Store: store, bind: binder.Bind}
		},
		"keys are the identifiers joined as they come": func(store *objectstorage.Store, _ objectstorage.Binder) objectscopetest.Confined {
			return store.WithLayout(joined{base: store.Base()})
		},
		"the access is never read-only": func(store *objectstorage.Store, binder objectstorage.Binder) objectscopetest.Confined {
			return store.WithBinder(rebound(func(binding *objectstorage.Binding) { binding.Access = objectstorage.ReadWrite })(binder))
		},
		"every binding is privileged": func(store *objectstorage.Store, binder objectstorage.Binder) objectscopetest.Confined {
			return store.WithBinder(rebound(func(binding *objectstorage.Binding) { binding.Mode = objectstorage.Privileged })(binder))
		},
		"the shared slot is always writable": func(store *objectstorage.Store, binder objectstorage.Binder) objectscopetest.Confined {
			return store.WithBinder(rebound(func(binding *objectstorage.Binding) { binding.Shared = objectstorage.ReadWrite })(binder))
		},
		"the reach is every scope": func(store *objectstorage.Store, binder objectstorage.Binder) objectscopetest.Confined {
			return store.WithBinder(rebound(func(binding *objectstorage.Binding) {
				binding.Reach = append(binding.Reach, "inst-1", "inst-10", "_shared", "Inst-1", "inst-1/human.amy")
			})(binder))
		},
	} {
		t.Run(name, func(t *testing.T) {
			client := gatewaytest.Memory(t)
			proof := objectscopetest.Proof{Client: client, Base: "proof", Rounds: 600, Objects: 240, Seed: 20261121}
			proof.Open = func(binder objectstorage.Binder) (objectscopetest.Confined, error) {
				store, err := objectstorage.NewStore(client, proof.Base, binder)
				if err != nil {
					return nil, err
				}
				return defect(store, binder), nil
			}
			recorder := &recorder{TB: t}
			func() {
				defer func() {
					if recovered := recover(); recovered != nil && recovered != recorder {
						panic(recovered)
					}
				}()
				objectscopetest.Prove(recorder, proof)
			}()
			if recorder.failure == "" {
				t.Fatal("the proof passed on a store that does not hold the model")
			}
			t.Log(strings.SplitN(recorder.failure, "\n", 2)[0])
		})
	}
}

// recorder turns the proof's failure into a value, and stops it where a test
// would have stopped.
type recorder struct {
	testing.TB
	failure string
}

func (r *recorder) Fatalf(format string, arguments ...any) {
	r.failure = fmt.Sprintf(format, arguments...)
	panic(r)
}

type boundKey struct{}

func bound(binding objectstorage.Binding) context.Context {
	return context.WithValue(context.Background(), boundKey{}, binding)
}

var fromContext = objectstorage.BinderFunc(func(ctx context.Context) (objectstorage.Binding, error) {
	binding, ok := ctx.Value(boundKey{}).(objectstorage.Binding)
	if !ok {
		return objectstorage.Binding{}, errors.New("no caller")
	}
	return binding, nil
})

func strict(principal string, reach ...string) objectstorage.Binding {
	return objectstorage.Binding{Partition: "org", Principal: principal, Reach: reach, Mode: objectstorage.Strict, Access: objectstorage.ReadWrite}
}

func privileged(reach ...string) objectstorage.Binding {
	return objectstorage.Binding{Partition: "org", Principal: "agent:inst", Reach: reach, Mode: objectstorage.Privileged, Access: objectstorage.ReadWrite}
}

func put(t *testing.T, store *objectstorage.Store, ctx context.Context, ref objectstorage.Ref, body string) {
	t.Helper()
	if _, err := store.Put(ctx, ref, strings.NewReader(body), objectstorage.PutOptions{}); err != nil {
		t.Fatalf("put %s: %v", ref, err)
	}
}

func get(t *testing.T, store *objectstorage.Store, ctx context.Context, ref objectstorage.Ref) string {
	t.Helper()
	body, _, err := store.Get(ctx, ref)
	if err != nil {
		t.Fatalf("get %s: %v", ref, err)
	}
	defer body.Close()
	content, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

// Each refusal has its own error, and each is decided from the reference and
// the Binding alone: none of these calls reaches the gateway, whose client
// here is one that fails the test if it is used.
func TestEveryRefusalIsTypedAndDecidedBeforeTheGateway(t *testing.T) {
	store, err := objectstorage.NewStore(untouchable{t: t}, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	amy, reader := strict("human:amy", "inst"), strict("human:amy", "inst")
	reader.Access = objectstorage.Read
	two := strict("human:amy", "inst", "other")
	undecided := objectstorage.Binding{Partition: "org", Principal: "human:amy", Reach: []string{"inst"}, Access: objectstorage.Read}
	accessless := objectstorage.Binding{Partition: "org", Principal: "human:amy", Reach: []string{"inst"}, Mode: objectstorage.Strict}
	bare := strict("amy", "inst")
	comma := strict("human:amy", "a,b")
	nowhere := strict("human:amy")
	partitionless := strict("human:amy", "inst")
	partitionless.Partition = " "
	reads := func(ref objectstorage.Ref) func(context.Context) error {
		return func(ctx context.Context) error { _, err := store.Head(ctx, ref); return err }
	}
	writes := func(ref objectstorage.Ref) func(context.Context) error {
		return func(ctx context.Context) error { return store.Delete(ctx, ref) }
	}
	for name, test := range map[string]struct {
		ctx  context.Context
		call func(context.Context) error
		want error
	}{
		"no binding":                       {context.Background(), reads(objectstorage.Own("a")), objectstorage.ErrNoBinding},
		"an empty binding":                 {bound(objectstorage.Binding{}), reads(objectstorage.Own("a")), objectstorage.ErrNoBinding},
		"no partition":                     {bound(partitionless), reads(objectstorage.Own("a")), objectstorage.ErrNoPartition},
		"a principal without a kind":       {bound(bare), reads(objectstorage.Own("a")), objectstorage.ErrNoPrincipal},
		"no mode":                          {bound(undecided), reads(objectstorage.Own("a")), objectstorage.ErrUnknownMode},
		"no access":                        {bound(accessless), reads(objectstorage.Own("a")), objectstorage.ErrUnknownAccess},
		"an empty reach":                   {bound(nowhere), reads(objectstorage.Own("a")), objectstorage.ErrNoReach},
		"a reach with a comma":             {bound(comma), reads(objectstorage.Own("a")), objectstorage.ErrInvalidReach},
		"a scope outside the reach":        {bound(amy), reads(objectstorage.Own("a").In("other")), objectstorage.ErrOutsideReach},
		"no scope, with two in reach":      {bound(two), reads(objectstorage.Own("a")), objectstorage.ErrOutsideReach},
		"another principal's, read":        {bound(amy), reads(objectstorage.Of("human:bob", "a")), objectstorage.ErrForeignPrincipal},
		"another principal's, written":     {bound(amy), writes(objectstorage.Of("human:bob", "a")), objectstorage.ErrForeignPrincipal},
		"a write on a read binding":        {bound(reader), writes(objectstorage.Own("a")), objectstorage.ErrReadOnly},
		"a shared write, strict":           {bound(amy), writes(objectstorage.Shared("a")), objectstorage.ErrSharedReadOnly},
		"a content delete, privileged":     {bound(privileged("inst")), writes(objectstorage.Content(strings.Repeat("0", 64))), objectstorage.ErrContentImmutable},
		"a name with a dot-dot segment":    {bound(amy), reads(objectstorage.Own("a/../b")), objectstorage.ErrInvalidName},
		"an empty name":                    {bound(amy), reads(objectstorage.Own("")), objectstorage.ErrInvalidName},
		"a name with an empty segment":     {bound(amy), reads(objectstorage.Own("a//b")), objectstorage.ErrInvalidName},
		"a reference to two slots":         {bound(amy), reads(objectstorage.Ref{Shared: true, Principal: "human:amy", Name: "a"}), objectstorage.ErrInvalidName},
		"a content name that is no digest": {bound(amy), reads(objectstorage.Content("report.pdf")), objectstorage.ErrInvalidName},
		"a strict listing of content": {bound(amy), func(ctx context.Context) error {
			_, err := store.List(ctx, objectstorage.Content(""), objectstorage.ListOptions{})
			return err
		}, objectstorage.ErrNotListable},
		"a strict listing of a scope": {bound(amy), func(ctx context.Context) error {
			_, err := store.ListScope(ctx, "inst", objectstorage.ListOptions{})
			return err
		}, objectstorage.ErrForeignPrincipal},
		"a copy from another principal": {bound(amy), func(ctx context.Context) error {
			_, err := store.Copy(ctx, objectstorage.Of("human:bob", "a"), objectstorage.Own("a"), objectstorage.CopyOptions{})
			return err
		}, objectstorage.ErrForeignPrincipal},
		"a copy into another scope": {bound(amy), func(ctx context.Context) error {
			_, err := store.Copy(ctx, objectstorage.Own("a"), objectstorage.Own("a").In("other"), objectstorage.CopyOptions{})
			return err
		}, objectstorage.ErrOutsideReach},
		"a presigned URL with no lifetime": {bound(amy), func(ctx context.Context) error {
			_, err := store.PresignGet(ctx, objectstorage.Own("a"), 0)
			return err
		}, objectstorage.ErrPresignExpiry},
		"a presigned URL for a day": {bound(amy), func(ctx context.Context) error {
			_, err := store.PresignPut(ctx, objectstorage.Own("a"), 24*time.Hour)
			return err
		}, objectstorage.ErrPresignExpiry},
		"a presigned put of another's": {bound(amy), func(ctx context.Context) error {
			_, err := store.PresignPut(ctx, objectstorage.Of("human:bob", "a"), time.Minute)
			return err
		}, objectstorage.ErrForeignPrincipal},
		"a content write on a read binding": {bound(reader), func(ctx context.Context) error {
			_, err := store.PutContent(ctx, "inst", strings.NewReader("x"), objectstorage.PutOptions{})
			return err
		}, objectstorage.ErrReadOnly},
	} {
		if err := test.call(test.ctx); !errors.Is(err, test.want) {
			t.Errorf("%s: %v, want %v", name, err, test.want)
		}
	}
}

// untouchable is a gateway client no call may reach.
type untouchable struct {
	storagev0.ObjectStorageClient
	t *testing.T
}

func TestAStoreNeedsItsPartsAndABase(t *testing.T) {
	client := gatewaytest.Memory(t)
	for name, build := range map[string]func() (*objectstorage.Store, error){
		"no client": func() (*objectstorage.Store, error) { return objectstorage.NewStore(nil, "files", fromContext) },
		"no binder": func() (*objectstorage.Store, error) { return objectstorage.NewStore(client, "files", nil) },
		"a nil binder func": func() (*objectstorage.Store, error) {
			return objectstorage.NewStore(client, "files", objectstorage.BinderFunc(nil))
		},
		"no base":         func() (*objectstorage.Store, error) { return objectstorage.NewStore(client, "", fromContext) },
		"a nested base":   func() (*objectstorage.Store, error) { return objectstorage.NewStore(client, "a/b", fromContext) },
		"a reserved base": func() (*objectstorage.Store, error) { return objectstorage.NewStore(client, "_shared", fromContext) },
		"a presign limit of a month": func() (*objectstorage.Store, error) {
			return objectstorage.NewStore(client, "files", fromContext, objectstorage.WithPresignLimit(30*24*time.Hour))
		},
	} {
		if store, err := build(); err == nil || store != nil {
			t.Errorf("%s: built a store", name)
		}
	}
}

// No method of a Store, and nothing reachable from a Binding, yields the
// gateway client or the maintenance capability.
func TestAStoreHandsOutNeitherItsClientNorMaintenance(t *testing.T) {
	client := reflect.TypeOf((*storagev0.ObjectStorageClient)(nil)).Elem()
	maintenance := reflect.TypeOf(&objectstorage.Maintenance{})
	for _, holder := range []reflect.Type{reflect.TypeOf(&objectstorage.Store{}), reflect.TypeOf(objectstorage.Binding{}), reflect.TypeOf(objectstorage.Ref{})} {
		for index := range holder.NumMethod() {
			method := holder.Method(index)
			if method.Name == "Client" || method.Name == "WithLayout" || method.Name == "WithBinder" {
				continue // export_test.go: compiled into this test binary only
			}
			for out := range method.Type.NumOut() {
				if result := method.Type.Out(out); result.Implements(client) || result == maintenance {
					t.Errorf("%s.%s returns %s", holder, method.Name, result)
				}
			}
		}
		element := holder
		if element.Kind() == reflect.Pointer {
			element = element.Elem()
		}
		for index := range element.NumField() {
			if field := element.Field(index); field.IsExported() && (field.Type.Kind() == reflect.Interface || field.Type == maintenance) {
				t.Errorf("%s.%s is exported and holds %s", element, field.Name, field.Type)
			}
		}
	}
}

func TestAHandlerAJobAndADeparture(t *testing.T) {
	client := gatewaytest.Memory(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	amy, bob, job := bound(strict("human:amy", "inst")), bound(strict("human:bob", "inst")), bound(privileged("inst"))

	// A handler, strict: its own objects by name, and nothing of Bob's.
	put(t, store, amy, objectstorage.Own("drafts/a.txt"), "amy's draft")
	put(t, store, amy, objectstorage.Own("b.txt"), "amy's other")
	put(t, store, bob, objectstorage.Own("drafts/a.txt"), "bob's draft")
	if got := get(t, store, amy, objectstorage.Own("drafts/a.txt")); got != "amy's draft" {
		t.Fatalf("amy read %q", got)
	}
	if _, _, err := store.Get(amy, objectstorage.Of("human:bob", "drafts/a.txt")); !errors.Is(err, objectstorage.ErrForeignPrincipal) {
		t.Fatalf("amy read bob's draft: %v", err)
	}
	signed, err := store.PresignPut(amy, objectstorage.Own("uploads/photo.jpg"), 5*time.Minute)
	if err != nil || signed.Method != "PUT" || !strings.Contains(signed.URL, "files/org/inst/human.amy/uploads_/photo.jpg") {
		t.Fatalf("presigned put: %+v, %v", signed, err)
	}
	if _, err := store.Put(amy, objectstorage.Own("b.txt"), strings.NewReader("again"), objectstorage.PutOptions{IfAbsent: true}); !errors.Is(err, objectstorage.ErrExists) {
		t.Fatalf("a create over an existing object: %v", err)
	}
	if _, err := store.Head(amy, objectstorage.Own("nothing")); !errors.Is(err, objectstorage.ErrNotFound) {
		t.Fatalf("head of nothing: %v", err)
	}
	page, err := store.List(amy, objectstorage.Own("drafts"), objectstorage.ListOptions{})
	if err != nil || len(page.Objects) != 1 || page.Objects[0].Ref != (objectstorage.Ref{Scope: "inst", Principal: "human:amy", Name: "drafts/a.txt"}) {
		t.Fatalf("amy's drafts: %+v, %v", page, err)
	}

	// Content: the same bytes from two principals are one object, readable by
	// whoever in the scope holds the digest, listable by neither.
	first, err := store.PutContent(amy, "", strings.NewReader("the same bytes"), objectstorage.PutOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.PutContent(bob, "", strings.NewReader("the same bytes"), objectstorage.PutOptions{})
	if err != nil || second.Ref != first.Ref || !first.Ref.Content || len(first.Ref.Name) != 64 {
		t.Fatalf("content: %+v then %+v, %v", first.Ref, second.Ref, err)
	}
	if got := get(t, store, bob, first.Ref); got != "the same bytes" {
		t.Fatalf("bob read the content as %q", got)
	}

	// A job that is the scope's own principal, privileged: the whole scope.
	put(t, store, job, objectstorage.Shared("logo.png"), "the logo")
	scope, err := store.ListScope(job, "inst", objectstorage.ListOptions{})
	if err != nil || len(scope.Objects) != 5 || scope.Skipped != 0 {
		t.Fatalf("the job listed %d objects (%d skipped), %v", len(scope.Objects), scope.Skipped, err)
	}
	if got := get(t, store, amy, objectstorage.Shared("logo.png")); got != "the logo" {
		t.Fatalf("amy read the shared logo as %q", got)
	}
	if _, err := store.Copy(job, objectstorage.Of("human:amy", "b.txt"), objectstorage.Of("human:bob", "from-amy.txt"), objectstorage.CopyOptions{}); err != nil {
		t.Fatalf("the job's copy: %v", err)
	}

	// Maintenance: Amy leaves. Her prefix goes, in every scope; nothing else.
	maintain, err := objectstorage.NewMaintenance(client, "files")
	if err != nil {
		t.Fatal(err)
	}
	exported := map[string]string{}
	if err := maintain.ExportPrincipal(context.Background(), "org", "human:amy", func(info objectstorage.Info, open func(context.Context) (io.ReadCloser, error)) error {
		body, err := open(context.Background())
		if err != nil {
			return err
		}
		defer body.Close()
		content, err := io.ReadAll(body)
		exported[info.Ref.Name] = string(content)
		return err
	}); err != nil || len(exported) != 2 || exported["drafts/a.txt"] != "amy's draft" {
		t.Fatalf("export: %v, %v", exported, err)
	}
	principals, err := maintain.Principals(context.Background(), "org", "inst")
	if err != nil || !reflect.DeepEqual(principals, []string{"human:amy", "human:bob"}) {
		t.Fatalf("principals: %v, %v", principals, err)
	}
	erased, err := maintain.ErasePrincipal(context.Background(), "org", "human:amy")
	if err != nil || erased.Objects != 2 || len(erased.Failed) != 0 {
		t.Fatalf("erase: %+v, %v", erased, err)
	}
	scope, err = store.ListScope(job, "inst", objectstorage.ListOptions{})
	if err != nil || len(scope.Objects) != 4 {
		t.Fatalf("after the erasure the scope holds %d objects, %v", len(scope.Objects), err)
	}
	for _, object := range scope.Objects {
		if object.Ref.Principal == "human:amy" {
			t.Fatalf("%s survived the erasure", object.Ref)
		}
	}

	// Content nothing references any more is swept; the grace protects what
	// was only just written.
	swept, err := maintain.SweepContent(context.Background(), "org", "inst", time.Hour, func(string) (bool, error) { return false, nil })
	if err != nil || swept.Objects != 0 {
		t.Fatalf("a sweep inside the grace removed %d, %v", swept.Objects, err)
	}
	swept, err = maintain.SweepContent(context.Background(), "org", "inst", -time.Second, func(digest string) (bool, error) { return digest != first.Ref.Name, nil })
	if err != nil || swept.Objects != 1 {
		t.Fatalf("the sweep removed %d, %v", swept.Objects, err)
	}
	if _, err := store.Head(bob, first.Ref); !errors.Is(err, objectstorage.ErrNotFound) {
		t.Fatalf("swept content: %v", err)
	}
}

func rawPut(t *testing.T, client storagev0.ObjectStorageClient, key string) {
	t.Helper()
	stream, err := client.Put(context.Background())
	if err == nil {
		err = stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Header{Header: &storagev0.PutHeader{Key: key, TotalSize: 1}}})
	}
	if err == nil {
		err = stream.Send(&storagev0.PutRequest{Kind: &storagev0.PutRequest_Data{Data: []byte("x")}})
	}
	if err == nil {
		_, err = stream.CloseAndRecv()
	}
	if err != nil {
		t.Fatalf("raw put %s: %v", key, err)
	}
}

// failures collects what RequireLayout would have failed a test with.
type failures struct{ messages []string }

func (f *failures) Helper() {}
func (f *failures) Fatalf(format string, arguments ...any) {
	f.messages = append(f.messages, fmt.Sprintf(format, arguments...))
}

// The layout gate: green on what the convention wrote, and red, naming the
// key, on each thing that should not be there.
func TestRequireLayout(t *testing.T) {
	client := gatewaytest.Memory(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	put(t, store, bound(strict("human:amy", "inst")), objectstorage.Own("a"), "x")
	put(t, store, bound(privileged("inst")), objectstorage.Shared("b"), "x")
	known := objectstorage.Known{
		Partition: func(partition string) bool { return partition == "org" },
		Scope:     func(_, scope string) bool { return scope == "inst" },
		Principal: func(_, _, principal string) bool { return principal == "human:amy" },
	}
	objectscopetest.RequireLayout(t, context.Background(), client, known, "files")

	for name, damage := range map[string]struct{ key, want string }{
		"a key in the old layout":           {"org/blobs/0000", "no declared base"},
		"a top-level object":                {"README", "outside every base"},
		"another store's base":              {"other/org/inst/_shared/a", "no declared base"},
		"a hand-built key under the base":   {"files/org/inst/amy/a", "neither reserved nor kind.id"},
		"a key that is not canonical":       {"files/org/inst/human.amy/a_2eb", "canonical"},
		"a partition nobody knows":          {"files/elsewhere/inst/_shared/a", "unknown partition"},
		"a scope nobody knows":              {"files/org/gone/_shared/a", "unknown scope"},
		"a principal nobody knows":          {"files/org/inst/human.zed/a", "unknown principal"},
		"an object where a slot should be":  {"files/org/inst/loose", "no partition, scope, slot and name"},
		"an encoded separator in a name":    {"files/org/inst/human.amy/a_2fb", "encoded separator"},
		"a directory without its mark":      {"files/org/inst/human.amy/dir/a", "without its mark"},
		"a dot-dot segment under the base":  {"files/org/../inst/human.amy/a", "not an identifier"},
		"an unknown reserved slot":          {"files/org/inst/_private/a", "neither reserved nor kind.id"},
		"an object directly under the base": {"files/loose", "no partition, scope, slot and name"},
	} {
		t.Run(name, func(t *testing.T) {
			rawPut(t, client, damage.key)
			defer func() {
				if _, err := client.Delete(context.Background(), &storagev0.DeleteRequest{Key: damage.key}); err != nil {
					t.Fatal(err)
				}
			}()
			var failed failures
			objectscopetest.RequireLayout(&failed, context.Background(), client, known, "files")
			if len(failed.messages) != 1 || !strings.Contains(failed.messages[0], damage.want) || !strings.Contains(failed.messages[0], strings.SplitN(damage.key, "/", 2)[0]) {
				t.Fatalf("the gate said %q, want one finding with %q", failed.messages, damage.want)
			}
		})
	}
	objectscopetest.RequireLayout(t, context.Background(), client, known, "files")
}

func TestStagingIsInvisibleAndSwept(t *testing.T) {
	client := gatewaytest.Memory(t)
	store, err := objectstorage.NewStore(client, "files", fromContext)
	if err != nil {
		t.Fatal(err)
	}
	amy := bound(strict("human:amy", "inst"))
	put(t, store, amy, objectstorage.Own("a"), "x")
	// What a writer that died between its upload and its copy leaves behind.
	rawPut(t, client, "files/org/inst/human.amy/_staging/0123abcd")
	page, err := store.List(amy, objectstorage.Own(""), objectstorage.ListOptions{})
	if err != nil || len(page.Objects) != 1 || page.Skipped != 1 {
		t.Fatalf("a listing beside an upload in flight: %d objects, %d skipped, %v", len(page.Objects), page.Skipped, err)
	}
	maintain, _ := objectstorage.NewMaintenance(client, "files")
	if swept, err := maintain.SweepStaging(context.Background(), time.Hour); err != nil || swept.Objects != 0 {
		t.Fatalf("a sweep of young uploads removed %d, %v", swept.Objects, err)
	}
	if swept, err := maintain.SweepStaging(context.Background(), -time.Second); err != nil || swept.Objects != 1 {
		t.Fatalf("the sweep removed %d, %v", swept.Objects, err)
	}
	if got := get(t, store, amy, objectstorage.Own("a")); got != "x" {
		t.Fatalf("the sweep touched an object: %q", got)
	}
}

func TestSessionPolicy(t *testing.T) {
	binding := strict("human:amy", "inst")
	policy, err := objectstorage.S3SessionPolicy("bucket", "cell/", "files", binding)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/cell/files/org/inst/human.amy/*","arn:aws:s3:::bucket/cell/files/org/inst/_shared/*","arn:aws:s3:::bucket/cell/files/org/inst/_sha256/*"]},` +
		`{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::bucket"],"Condition":{"StringLike":{"s3:prefix":["cell/files/org/inst/human.amy/*","cell/files/org/inst/_shared/*"]}}},` +
		`{"Effect":"Allow","Action":["s3:PutObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts"],"Resource":["arn:aws:s3:::bucket/cell/files/org/inst/human.amy/*","arn:aws:s3:::bucket/cell/files/org/inst/_sha256/*"]},` +
		`{"Effect":"Allow","Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::bucket/cell/files/org/inst/human.amy/*"]}]}`
	if string(policy) != want {
		t.Errorf("the strict policy changed:\n got  %s\n want %s", policy, want)
	}
	// Nothing an identifier holds becomes a wildcard or a variable.
	hostile := strict("human:*", "${aws:username}", "a?b")
	hostile.Partition = "*"
	policy, err = objectstorage.S3SessionPolicy("bucket", "", "files", hostile)
	if err != nil {
		t.Fatal(err)
	}
	if text := strings.ReplaceAll(strings.ReplaceAll(string(policy), `/*"`, `"`), "arn:aws:s3:::", ""); strings.ContainsAny(text, "*?$") {
		t.Errorf("an identifier reached the policy as a pattern: %s", policy)
	}
	for _, configured := range [][2]string{{"", ""}, {"buck*et", ""}, {"bucket", "cell"}, {"bucket", "${x}/"}} {
		if _, err := objectstorage.S3SessionPolicy(configured[0], configured[1], "files", binding); err == nil {
			t.Errorf("bucket %q, prefix %q: rendered a policy", configured[0], configured[1])
		}
	}
	// A session policy is 2048 bytes. A wide reach does not fit, and says so.
	var wide []string
	for index := range 12 {
		wide = append(wide, fmt.Sprintf("0190f6a2-aaaa-7bbb-8ccc-0123456789%02d", index))
	}
	if _, err := objectstorage.S3SessionPolicy("bucket", "", "files", strict("human:0190f6a2-dddd-7eee-8fff-0123456789ab", wide...)); !errors.Is(err, objectstorage.ErrPolicyTooLarge) {
		t.Errorf("a reach of twelve scopes: %v, want ErrPolicyTooLarge", err)
	}
	if _, err := objectstorage.S3SessionPolicy("bucket", "", "files", objectstorage.Binding{}); !errors.Is(err, objectstorage.ErrNoBinding) {
		t.Errorf("no binding: %v", err)
	}
}

type uploadContextClient struct {
	storagev0.ObjectStorageClient
	upload context.Context
}

func (c *uploadContextClient) Put(ctx context.Context, opts ...grpc.CallOption) (storagev0.ObjectStorage_PutClient, error) {
	c.upload = ctx
	return c.ObjectStorageClient.Put(ctx, opts...)
}

func TestSourceFailureCancelsUpload(t *testing.T) {
	client := &uploadContextClient{ObjectStorageClient: gatewaytest.Memory(t)}
	store, err := objectstorage.NewStore(client, "files", objectstorage.FixedBinder(strict("human:amy", "inst")))
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("input interrupted")
	_, err = store.Put(context.Background(), objectstorage.Own("file"), io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(failure)), objectstorage.PutOptions{})
	if !errors.Is(err, failure) {
		t.Fatalf("source error: %v", err)
	}
	if client.upload.Err() != context.Canceled {
		t.Fatal("unfinished upload context remained live")
	}
}
