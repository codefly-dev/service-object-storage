package sessions

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/codefly-dev/service-object-storage/internal/backend"
)

// The bounds of a session's lifetime and of the cache that holds sessions.
const (
	// MinTTL is the shortest session S3 and MinIO issue.
	MinTTL = 15 * time.Minute
	// MaxTTL is the longest this gateway asks for: the ceiling of a chained
	// role on S3, and an hour is already long for a credential nothing revokes.
	MaxTTL = time.Hour
	// DefaultTTL is MinTTL: the shortest a store accepts.
	DefaultTTL = MinTTL
	// DefaultCacheSize and MaxCacheSize bound how many sessions are held.
	DefaultCacheSize = 1024
	MaxCacheSize     = 65536
	// reuseMargin is how long before its expiry a session stops being handed
	// to new calls, so a call that starts has that long to finish.
	reuseMargin = time.Minute
	// issueTimeout bounds one exchange with the issuer.
	issueTimeout = 10 * time.Second
)

// Options configure a Manager.
type Options struct {
	// Backend is the gateway's own store: sessions are issued from its
	// credential and reach the same endpoint, bucket and prefix.
	Backend    backend.Config
	Issuer     Issuer
	Authorizer Authorizer
	// TTL is the lifetime asked of the issuer; zero means DefaultTTL.
	TTL time.Duration
	// CacheSize is the most sessions held; zero means DefaultCacheSize.
	CacheSize int
	// Now and Open are for tests; nil means time.Now and backend.Open.
	Now  func() time.Time
	Open func(context.Context, backend.Config) (backend.Backend, error)
}

// Manager issues, caches and hands out the sessions calls run under.
type Manager struct {
	options Options

	mu      sync.Mutex
	entries map[cacheKey]*entry
}

// cacheKey is everything a session is issued for, and who asked. It is a
// struct of whole values: no two (caller, scope) pairs can be spelled as the
// same key, whatever characters an identifier holds.
type cacheKey struct {
	caller string
	scope  Scope
}

type entry struct {
	done    chan struct{}
	err     error
	backend backend.Backend
	expires time.Time
	// refs counts calls in flight; an evicted entry closes when it reaches 0.
	refs    int
	evicted bool
}

// NewManager validates options. An unqualified issuer is refused here, at
// start, so a gateway never serves scoped calls it cannot confine.
func NewManager(options Options) (*Manager, error) {
	if options.Issuer == nil || options.Authorizer == nil {
		return nil, errors.New("scoped sessions need an issuer and an authorizer")
	}
	if options.Issuer.Qualification() != Qualified {
		return nil, fmt.Errorf("%w: the %s issuer is %s", ErrUnqualified, options.Issuer.Kind(), options.Issuer.Qualification())
	}
	if options.TTL == 0 {
		options.TTL = DefaultTTL
	}
	if options.TTL < MinTTL || options.TTL > MaxTTL {
		return nil, fmt.Errorf("a scoped session lives between %s and %s, not %s", MinTTL, MaxTTL, options.TTL)
	}
	if options.CacheSize == 0 {
		options.CacheSize = DefaultCacheSize
	}
	if options.CacheSize < 1 || options.CacheSize > MaxCacheSize {
		return nil, fmt.Errorf("the scoped session cache holds between 1 and %d sessions, not %d", MaxCacheSize, options.CacheSize)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Open == nil {
		options.Open = backend.Open
	}
	return &Manager{options: options, entries: map[cacheKey]*entry{}}, nil
}

// Issuer is the issuer sessions come from.
func (m *Manager) Issuer() Issuer { return m.options.Issuer }

// Session is a store client holding one scope's session, and nothing else.
type Session struct {
	backend.Backend
	// Expires is when the store stops honouring the session.
	Expires time.Time
	// Release ends this call's use of it.
	Release func()
}

// Session returns the backend a call on scope runs against: one opened with
// a session confined to the scope, issued for this caller, reused until
// shortly before it expires. Every failure is a refusal of the call. There is
// no other credential to fall back to in here.
func (m *Manager) Session(ctx context.Context, scope Scope) (*Session, error) {
	if _, err := scope.Prefix(); err != nil {
		return nil, err
	}
	caller, err := m.options.Authorizer.Authorize(ctx, scope)
	if err != nil {
		return nil, err
	}
	if caller == "" {
		return nil, fmt.Errorf("%w: the caller has no identity", ErrNotGranted)
	}
	key := cacheKey{caller: caller, scope: scope}
	for {
		m.mu.Lock()
		held := m.entries[key]
		if held != nil && held.finished() && !m.usable(held) {
			m.drop(key, held)
			held = nil
		}
		issuedHere := held == nil
		if held == nil {
			held = &entry{done: make(chan struct{})}
			m.makeRoom()
			m.entries[key] = held
			m.mu.Unlock()
			m.issue(ctx, key, held)
		} else {
			m.mu.Unlock()
		}
		select {
		case <-held.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if held.err != nil {
			return nil, held.err
		}
		m.mu.Lock()
		if held.evicted || !m.usable(held) {
			if issuedHere && !m.usable(held) {
				// The issuer answered with a session already inside its last
				// minute. Asking again would answer the same; refuse.
				m.drop(key, held)
				m.mu.Unlock()
				return nil, fmt.Errorf("%w: the session issued expires within %s", ErrIssuerUnavailable, reuseMargin)
			}
			m.mu.Unlock()
			continue
		}
		held.refs++
		m.mu.Unlock()
		return &Session{Backend: held.backend, Expires: held.expires, Release: sync.OnceFunc(func() { m.release(held) })}, nil
	}
}

func (e *entry) finished() bool {
	select {
	case <-e.done:
		return true
	default:
		return false
	}
}

// usable: issued, and far enough from its expiry to start a call on.
func (m *Manager) usable(e *entry) bool {
	return e.err == nil && m.options.Now().Before(e.expires.Add(-reuseMargin))
}

// issue fills an entry. It runs detached from the asking call's cancellation:
// other calls may be waiting on the same session.
func (m *Manager) issue(ctx context.Context, key cacheKey, held *entry) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), issueTimeout)
	defer cancel()
	defer close(held.done)
	fail := func(err error) {
		held.err = err
		m.mu.Lock()
		if m.entries[key] == held {
			delete(m.entries, key)
		}
		m.mu.Unlock()
	}
	credential, err := m.options.Issuer.Issue(ctx, key.scope, m.options.TTL)
	if err != nil {
		if !errors.Is(err, ErrPolicyTooLarge) && !errors.Is(err, ErrInvalidScope) && !errors.Is(err, ErrUnqualified) && !errors.Is(err, ErrIssuerUnavailable) {
			err = fmt.Errorf("%w: %w", ErrIssuerUnavailable, err)
		}
		fail(err)
		return
	}
	cfg := m.options.Backend
	cfg.AccessKey, cfg.SecretKey, cfg.SessionToken = credential.AccessKey, credential.SecretKey, credential.SessionToken
	// A session is used and never probed: it may not list the bucket's root.
	cfg.ProbeStrategy, cfg.ProbeKey = "", ""
	opened, err := m.options.Open(ctx, cfg)
	if err != nil {
		fail(fmt.Errorf("%w: %w", ErrIssuerUnavailable, err))
		return
	}
	held.backend, held.expires = opened, credential.Expires
}

// makeRoom keeps the cache within its bound: what can no longer be used goes
// first, then the session closest to its expiry. Called with mu held.
func (m *Manager) makeRoom() {
	for key, held := range m.entries {
		if held.finished() && !m.usable(held) {
			m.drop(key, held)
		}
	}
	for len(m.entries) >= m.options.CacheSize {
		var oldest cacheKey
		var victim *entry
		for key, held := range m.entries {
			if !held.finished() {
				continue
			}
			if victim == nil || held.expires.Before(victim.expires) {
				oldest, victim = key, held
			}
		}
		if victim == nil {
			// Everything held is still being issued; the bound is exceeded by
			// what is in flight and no further.
			return
		}
		m.drop(oldest, victim)
	}
}

// drop removes an entry and closes its backend once no call uses it. Called
// with mu held.
func (m *Manager) drop(key cacheKey, held *entry) {
	if m.entries[key] == held {
		delete(m.entries, key)
	}
	held.evicted = true
	if held.refs == 0 && held.backend != nil {
		_ = held.backend.Close()
	}
}

func (m *Manager) release(held *entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	held.refs--
	if held.evicted && held.refs == 0 && held.backend != nil {
		_ = held.backend.Close()
	}
}

// Held is how many sessions the cache holds, for tests and operators.
func (m *Manager) Held() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}
