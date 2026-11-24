// Package sessions is how the gateway runs a call on its own bucket under a
// credential that reaches one scope and nothing else.
//
// The gateway holds the bucket-wide credential. A caller that names a Scope
// does not get the call made with it: the gateway asks the store's own issuer
// for a short-lived session whose grant is the scope's key prefix and one
// mode, and makes the call with that session. A key outside the scope is then
// refused by the store, not by a check in this process, so a bug or a forged
// key above the gateway cannot reach outside the scope although the process
// that serves it could.
//
// The unit of a session is (caller, base, partition, scope, mode): partition
// and scope are the two identifiers of the key convention a storage policy can
// tell apart by prefix. Anything finer (whose object, which record) is not a
// storage decision and is not made here.
//
// Only the MinIO issuer is qualified. The S3, GCS and Azure mechanisms are
// rendered and tested as documents and issue nothing: see Qualification.
package sessions

import (
	"errors"
	"fmt"

	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

var (
	// ErrInvalidScope: the scope is not one the key convention can write, or
	// names no mode.
	ErrInvalidScope = errors.New("scoped session: invalid scope")
	// ErrNotGranted: the authenticated caller was not granted this base, mode
	// or partition.
	ErrNotGranted = errors.New("scoped session: the scope is not granted to the caller")
	// ErrPolicyTooLarge: the scope's grant does not fit the store's limit on
	// one session. It is refused; a wider grant is never issued in its place.
	ErrPolicyTooLarge = errors.New("scoped session: the scope does not fit one session policy")
	// ErrUnqualified: the store's issuer was never proven against the store.
	ErrUnqualified = errors.New("scoped session: the store has no qualified session issuer")
	// ErrIssuerUnavailable: the issuer did not answer with a session. The call
	// fails; it is never made with another credential.
	ErrIssuerUnavailable = errors.New("scoped session: the issuer did not issue a session")
)

// Mode is which of a scope's three identities a session is. They are disjoint
// on purpose: a read session writes nothing, an ingest session reads and
// deletes nothing.
type Mode uint8

const (
	// Read reads and lists.
	Read Mode = iota + 1
	// Ingest writes, multipart included.
	Ingest
	// Maintenance copies and deletes, and reads and lists to do so.
	Maintenance
)

func (mode Mode) String() string {
	switch mode {
	case Read:
		return "read"
	case Ingest:
		return "ingest"
	case Maintenance:
		return "maintenance"
	default:
		return "unknown"
	}
}

// ParseMode reads a mode as configuration writes it.
func ParseMode(name string) (Mode, error) {
	for _, mode := range []Mode{Read, Ingest, Maintenance} {
		if mode.String() == name {
			return mode, nil
		}
	}
	return 0, fmt.Errorf("%w: unknown mode %q (want read, ingest or maintenance)", ErrInvalidScope, name)
}

// Scope is what one session is confined to.
type Scope struct {
	// Base, Partition and Scope are the identifiers of the key convention.
	Base, Partition, Scope string
	Mode                   Mode
}

// Prefix is the scope's key prefix under the key convention, ending in "/":
// every object of the scope is under it and no other scope's is. It is the
// one mapping from a scope to keys, shared by every provider's rendering.
func (scope Scope) Prefix() (string, error) {
	if scope.Mode != Read && scope.Mode != Ingest && scope.Mode != Maintenance {
		return "", fmt.Errorf("%w: a scope names its mode", ErrInvalidScope)
	}
	codec, err := objectkey.New(scope.Base)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidScope, err)
	}
	prefix, err := codec.ScopePrefix(scope.Partition, scope.Scope)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidScope, err)
	}
	return prefix, nil
}

// Contains reports whether key, relative to the gateway's prefix, is an
// object of the scope as the key convention writes one: under the scope's
// prefix and canonical. It is used where no store call would decide (signing
// a URL), never in place of the store's own refusal.
func (scope Scope) Contains(key string) bool {
	codec, err := objectkey.New(scope.Base)
	if err != nil {
		return false
	}
	parsed, err := codec.Parse(key)
	return err == nil && parsed.Partition == scope.Partition && parsed.Scope == scope.Scope
}
