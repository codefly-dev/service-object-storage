// Package objectstorage is the request-time boundary of a confined object
// store: a Store binds every call to one Binding and addresses objects by
// logical reference, never by key. It is the object counterpart of the
// row-confined Postgres Store and uses its words: partition, scope, reach,
// principal, mode, actor.
//
// The package does not know what any of those words means to an application.
// It builds keys from them (package objectkey) and refuses, before anything
// reaches the gateway, a reference the Binding does not cover.
//
// What that guarantees: a call made through a Store cannot name, list, copy
// from or presign an object outside its Binding. What it does not: the Store
// is the only thing between its caller and the whole base, so whoever holds
// the gateway client itself is not confined by it. The Store never returns
// that client.
package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

var (
	// ErrUnauthenticated: nothing verified says who is calling.
	ErrUnauthenticated = errors.New("object store: unauthenticated")
	// ErrUnauthorized: the caller is known and this is not theirs to do.
	ErrUnauthorized = errors.New("object store: unauthorized")

	// ErrNoBinding: nothing verified is bound to the context.
	ErrNoBinding = fmt.Errorf("%w: no binding on this context", ErrUnauthenticated)
	// ErrNoPartition: the binding names no partition.
	ErrNoPartition = fmt.Errorf("%w: the binding has an empty partition", ErrUnauthenticated)
	// ErrNoPrincipal: the binding names no principal, or not as "kind:id".
	ErrNoPrincipal = fmt.Errorf("%w: the binding's principal is not a kind:id reference", ErrUnauthenticated)
	// ErrUnknownMode: the zero Mode is unknown on purpose. A Binding nobody
	// decided opens nothing.
	ErrUnknownMode = fmt.Errorf("%w: the binding has an unknown mode", ErrUnauthorized)
	// ErrUnknownAccess: the zero Access is unknown for the same reason.
	ErrUnknownAccess = fmt.Errorf("%w: the binding has an unknown access", ErrUnauthorized)
	// ErrNoReach: the binding reaches no scope. It reaches nothing, never
	// everything, and the gateway is not called to find that out.
	ErrNoReach = fmt.Errorf("%w: the binding reaches no scope", ErrUnauthorized)
	// ErrInvalidReach: a scope in the reach is empty, padded or holds a comma.
	ErrInvalidReach = fmt.Errorf("%w: a scope in the binding's reach is empty or contains a comma", ErrUnauthorized)
	// ErrOutsideReach: the reference names a scope the binding does not reach,
	// or names none while the binding reaches several.
	ErrOutsideReach = fmt.Errorf("%w: the scope is outside the binding's reach", ErrUnauthorized)
	// ErrForeignPrincipal: a strict binding named another principal's objects.
	ErrForeignPrincipal = fmt.Errorf("%w: the objects are another principal's", ErrUnauthorized)
	// ErrReadOnly: the binding may not write.
	ErrReadOnly = fmt.Errorf("%w: this binding may not write", ErrUnauthorized)
	// ErrSharedReadOnly: the binding may write, and not to the shared slot.
	ErrSharedReadOnly = fmt.Errorf("%w: this binding may not write shared objects", ErrUnauthorized)
	// ErrContentImmutable: content is written by PutContent and removed by
	// Maintenance, and by nothing else.
	ErrContentImmutable = fmt.Errorf("%w: content is written once, under its digest", ErrUnauthorized)
	// ErrNotListable: the binding may read the slot's objects by name and may
	// not enumerate them.
	ErrNotListable = fmt.Errorf("%w: this binding may not list the slot", ErrUnauthorized)

	// ErrInvalidName: a reference, a name or an identifier the key convention
	// cannot carry. It is objectkey.ErrInvalid.
	ErrInvalidName = objectkey.ErrInvalid
	// ErrNotFound: there is no such object.
	ErrNotFound = errors.New("object store: no such object")
	// ErrExists: a write that asked to create found the object there.
	ErrExists = errors.New("object store: the object exists")
	// ErrPresignExpiry: a presigned URL was asked for without a lifetime, or
	// with one over the Store's limit.
	ErrPresignExpiry = errors.New("object store: a presigned URL needs a lifetime within the store's limit")
)

// Mode selects what a binding reaches inside the scopes in reach. There is no
// third value: work across partitions is Maintenance, built from the client at
// the composition root, and a Binding cannot spell it.
type Mode uint8

const (
	// Strict reaches the principal's own objects, reads the scope's shared
	// ones, and reads content by digest.
	Strict Mode = iota + 1
	// Privileged reaches every principal's objects in the scopes in reach.
	Privileged
)

func (mode Mode) String() string {
	switch mode {
	case Strict:
		return "strict"
	case Privileged:
		return "privileged"
	default:
		return "unknown"
	}
}

// Access is whether a binding may change anything.
type Access uint8

const (
	// Read may read and list. It never mutates.
	Read Access = iota + 1
	// ReadWrite may also write and delete.
	ReadWrite
)

func (access Access) String() string {
	switch access {
	case Read:
		return "read"
	case ReadWrite:
		return "read-write"
	default:
		return "unknown"
	}
}

// Binding is everything one call is confined to. It is decided before it
// reaches this package, by whatever adapts the application's verified caller:
// nothing here chooses a mode, and no caller of a Store is handed one.
//
// Its first six fields are, name for name, the Binding of the row-confined
// Postgres Store, so that one adapter derives both. It is declared here and
// not imported because neither library may depend on the other; the type
// belongs in a package both can import.
type Binding struct {
	// Partition is the wall no call crosses. Required.
	Partition string
	// Principal is whose objects, as "kind:id". Required.
	Principal string
	// Reach is the scopes the call may touch. An empty reach is ErrNoReach.
	Reach []string
	// Mode is required; the zero value is ErrUnknownMode.
	Mode Mode
	// Access is required; the zero value is ErrUnknownAccess.
	Access Access
	// Actor is who is executing, for audit. Nothing here decides on it.
	Actor string
	// Shared is what the binding may do in a scope's shared slot, and it is
	// the one field with no counterpart in a row: a shared row still has a
	// principal, a shared object has none. Zero means the mode's own answer:
	// Strict reads, Privileged does what Access says. It never exceeds Access.
	Shared Access
}

// Binder resolves the already decided Binding of a call from its context.
type Binder interface {
	Bind(context.Context) (Binding, error)
}

// BinderFunc adapts a function to a Binder.
type BinderFunc func(context.Context) (Binding, error)

func (bind BinderFunc) Bind(ctx context.Context) (Binding, error) { return bind(ctx) }

// FixedBinder binds every context to one Binding. It is for tests and for a
// process that is one caller by construction; a request path derives its
// Binding per call.
func FixedBinder(binding Binding) Binder {
	binding.Reach = append([]string(nil), binding.Reach...)
	return BinderFunc(func(context.Context) (Binding, error) { return binding, nil })
}

// Normalize validates a Binding and returns it trimmed, with its reach
// deduplicated and Shared resolved to what it allows. Everything refused here
// is refused before the gateway is called.
func (binding Binding) Normalize() (Binding, error) {
	binding.Partition = strings.TrimSpace(binding.Partition)
	binding.Principal = strings.TrimSpace(binding.Principal)
	binding.Actor = strings.TrimSpace(binding.Actor)
	if binding.Partition == "" {
		if binding.Principal == "" && binding.Mode == 0 && binding.Access == 0 && len(binding.Reach) == 0 {
			return Binding{}, ErrNoBinding
		}
		return Binding{}, ErrNoPartition
	}
	if _, _, err := objectkey.SplitPrincipal(binding.Principal); err != nil {
		return Binding{}, errors.Join(ErrNoPrincipal, err)
	}
	if binding.Mode != Strict && binding.Mode != Privileged {
		return Binding{}, ErrUnknownMode
	}
	if binding.Access != Read && binding.Access != ReadWrite {
		return Binding{}, ErrUnknownAccess
	}
	if binding.Shared > ReadWrite {
		return Binding{}, ErrUnknownAccess
	}
	reach := make([]string, 0, len(binding.Reach))
	for _, scope := range binding.Reach {
		// The same values the row store refuses, so one Binding is valid in
		// both or in neither.
		if scope == "" || scope != strings.TrimSpace(scope) || strings.Contains(scope, ",") {
			return Binding{}, ErrInvalidReach
		}
		duplicate := false
		for _, kept := range reach {
			duplicate = duplicate || kept == scope
		}
		if !duplicate {
			reach = append(reach, scope)
		}
	}
	if len(reach) == 0 {
		return Binding{}, ErrNoReach
	}
	binding.Reach = reach
	switch {
	case binding.Shared == 0 && binding.Mode == Strict:
		binding.Shared = Read
	case binding.Shared == 0:
		binding.Shared = binding.Access
	}
	if binding.Access == Read {
		binding.Shared = Read
	}
	return binding, nil
}

// Ref is the logical address of an object: which scope, whose, and its name.
// A caller never passes or receives a key.
type Ref struct {
	// Scope is a scope in reach. Empty means the binding's only scope, and is
	// ErrOutsideReach when the binding reaches several.
	Scope string
	// Principal is whose objects, as "kind:id". Empty means the binding's own.
	Principal string
	// Shared addresses the scope's shared slot, and Content its content slot.
	// Neither has a principal.
	Shared  bool
	Content bool
	// Name is the logical name: segments joined by "/". For content it is the
	// digest, 64 lowercase hex digits.
	Name string
}

// Own addresses one of the binding's own objects.
func Own(name string) Ref { return Ref{Name: name} }

// Shared addresses an object of the scope's shared slot.
func Shared(name string) Ref { return Ref{Shared: true, Name: name} }

// Of addresses an object of a named principal.
func Of(principal, name string) Ref { return Ref{Principal: principal, Name: name} }

// Content addresses content by its digest.
func Content(digest string) Ref { return Ref{Content: true, Name: digest} }

// In returns the reference in a named scope.
func (ref Ref) In(scope string) Ref {
	ref.Scope = scope
	return ref
}

func (ref Ref) String() string {
	slot := ref.Principal
	switch {
	case ref.Shared:
		slot = "shared"
	case ref.Content:
		slot = "content"
	case slot == "":
		slot = "own"
	}
	return fmt.Sprintf("%s[%s]/%s", ref.Scope, slot, ref.Name)
}

// place resolves a reference against a normalized binding: the scope it
// names, which must be in reach, and the slot.
func (binding Binding) place(ref Ref) (scope string, slot objectkey.Slot, err error) {
	scope = ref.Scope
	if scope == "" {
		if len(binding.Reach) != 1 {
			return "", objectkey.Slot{}, ErrOutsideReach
		}
		scope = binding.Reach[0]
	}
	reached := false
	for _, candidate := range binding.Reach {
		reached = reached || candidate == scope
	}
	if !reached {
		return "", objectkey.Slot{}, ErrOutsideReach
	}
	switch {
	case ref.Shared && ref.Content, (ref.Shared || ref.Content) && ref.Principal != "":
		return "", objectkey.Slot{}, fmt.Errorf("%w: a reference names one slot", ErrInvalidName)
	case ref.Shared:
		return scope, objectkey.Shared(), nil
	case ref.Content:
		return scope, objectkey.Content(), nil
	case ref.Principal == "":
		return scope, objectkey.Principal(binding.Principal), nil
	default:
		if _, _, err := objectkey.SplitPrincipal(ref.Principal); err != nil {
			return "", objectkey.Slot{}, err
		}
		return scope, objectkey.Principal(ref.Principal), nil
	}
}

// mayRead is the read half of the model, for a slot of a scope in reach.
func (binding Binding) mayRead(slot objectkey.Slot) error {
	if binding.Mode == Privileged || slot.IsShared() || slot.IsContent() || slot.Reference() == binding.Principal {
		return nil
	}
	return ErrForeignPrincipal
}

// mayList is narrower than mayRead in one place: a strict binding reads
// content it can name and may not enumerate what it cannot.
func (binding Binding) mayList(slot objectkey.Slot) error {
	if slot.IsContent() && binding.Mode != Privileged {
		return ErrNotListable
	}
	return binding.mayRead(slot)
}

// mayWrite is the write half of the model.
func (binding Binding) mayWrite(slot objectkey.Slot) error {
	switch {
	case binding.Access != ReadWrite:
		return ErrReadOnly
	case slot.IsContent():
		return ErrContentImmutable
	case slot.IsShared():
		if binding.Shared != ReadWrite {
			return ErrSharedReadOnly
		}
		return nil
	case binding.Mode == Privileged || slot.Reference() == binding.Principal:
		return nil
	default:
		return ErrForeignPrincipal
	}
}
