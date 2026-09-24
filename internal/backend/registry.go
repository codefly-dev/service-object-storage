package backend

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Constructor opens a backend from a resolved Config.
type Constructor func(ctx context.Context, cfg Config) (Backend, error)

var (
	mu       sync.RWMutex
	registry = map[string]Constructor{}
)

// Register wires a backend kind to its constructor. Backend packages call this
// from an init() so importing the package makes the kind available.
func Register(kind string, c Constructor) {
	mu.Lock()
	defer mu.Unlock()
	registry[kind] = c
}

// Open constructs the backend named by cfg.Kind. A configured cfg.Prefix is
// applied here, once, for every kind: the backend implementation sees full
// object keys and the caller sees keys relative to the prefix.
func Open(ctx context.Context, cfg Config) (Backend, error) {
	mu.RLock()
	c, ok := registry[cfg.Kind]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown backend kind %q (registered: %v)", cfg.Kind, Registered())
	}
	prefix, err := NormalizePrefix(cfg.Prefix)
	if err != nil {
		return nil, err
	}
	if prefix != "" && cfg.ProbeKey != "" {
		// The stat probe heads a real object key; keep it inside the prefix so
		// a probe never needs a grant outside the consumer's own key space.
		cfg.ProbeKey = prefix + cfg.ProbeKey
	}
	cfg.Prefix = prefix
	inner, err := c(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		return inner, nil
	}
	return &prefixed{inner: inner, prefix: prefix}, nil
}

// Registered lists the available backend kinds, sorted.
func Registered() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
