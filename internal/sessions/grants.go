package sessions

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

// Authorizer decides whether the caller of this call may name this scope,
// and says who the caller is. The name it returns keys the session cache, so
// two callers never share a session.
//
// It is the seam where a scope is bound to a verified identity. The one
// implementation here, Grants, binds it to what the gateway authenticates
// today: a single caller, the holder of the gateway's token (or, deployed,
// whoever the network admits). It cannot tell one partition of that caller
// from another; a verifier of a signed per-request identity would, and is not
// built.
type Authorizer interface {
	Authorize(ctx context.Context, scope Scope) (caller string, err error)
}

// Grant is what the gateway's authenticated caller may ask sessions for: one
// base, the modes it may use there, and optionally the only partitions it may
// name.
type Grant struct {
	Base  string   `json:"base"`
	Modes []string `json:"modes"`
	// Partitions, when not empty, are the only partitions the caller may name
	// in this base. Empty means any: the gateway then confines each call to
	// the partition it declares and cannot verify the declaration.
	Partitions []string `json:"partitions,omitempty"`

	modes []Mode
}

// Grants is the Authorizer of a gateway with one authenticated caller.
type Grants []Grant

// gatewayCaller names the one caller a token-authenticated gateway has.
const gatewayCaller = "gateway-caller"

// ParseGrants reads SOS_SCOPED_GRANTS: a JSON array of grants. Unknown
// fields, an unknown mode, a base the key convention cannot write and a base
// granted twice are refused: a grant nobody can read exactly is not applied.
func ParseGrants(raw string) (Grants, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var grants Grants
	if err := decoder.Decode(&grants); err != nil {
		return nil, fmt.Errorf("scoped grants: %w", err)
	}
	if decoder.More() {
		return nil, fmt.Errorf("scoped grants: one JSON array, and nothing after it")
	}
	seen := map[string]bool{}
	for index := range grants {
		grant := &grants[index]
		if _, err := objectkey.New(grant.Base); err != nil {
			return nil, fmt.Errorf("scoped grants: base %q: %w", grant.Base, err)
		}
		if seen[grant.Base] {
			return nil, fmt.Errorf("scoped grants: base %q is granted twice", grant.Base)
		}
		seen[grant.Base] = true
		if len(grant.Modes) == 0 {
			return nil, fmt.Errorf("scoped grants: base %q grants no mode", grant.Base)
		}
		for _, name := range grant.Modes {
			mode, err := ParseMode(name)
			if err != nil {
				return nil, fmt.Errorf("scoped grants: base %q: %w", grant.Base, err)
			}
			grant.modes = append(grant.modes, mode)
		}
		for _, partition := range grant.Partitions {
			if partition == "" {
				return nil, fmt.Errorf("scoped grants: base %q names an empty partition", grant.Base)
			}
		}
	}
	return grants, nil
}

// Authorize admits a scope whose base, mode and partition were granted.
func (grants Grants) Authorize(_ context.Context, scope Scope) (string, error) {
	for _, grant := range grants {
		if grant.Base != scope.Base {
			continue
		}
		if !slices.Contains(grant.modes, scope.Mode) {
			return "", fmt.Errorf("%w: mode %s in base %q", ErrNotGranted, scope.Mode, scope.Base)
		}
		if len(grant.Partitions) > 0 && !slices.Contains(grant.Partitions, scope.Partition) {
			return "", fmt.Errorf("%w: the partition in base %q", ErrNotGranted, scope.Base)
		}
		return gatewayCaller, nil
	}
	return "", fmt.Errorf("%w: base %q", ErrNotGranted, scope.Base)
}
