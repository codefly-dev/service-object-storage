// Package egress guards the dials a bound call makes. A binding's endpoint is
// supplied by the caller on behalf of a tenant, so it is an egress the gateway
// must not let reach its own network: every connection is judged against the
// concrete address being dialed, after DNS resolution, on the first request
// and on every redirect — so neither a rebinding name nor a redirect slips
// past the check.
//
// The classifier is the one the host's object-store connector carried
// (module-saas-starter accounts/pkg/auth.IsPublicDestination at 877e8a28),
// moved here with its vectors so the host's copy can go. It reads the IANA
// special-purpose registries rather than composing Go's net.IP predicates,
// which class several non-public ranges (documentation, benchmarking,
// reserved, NAT64, 6to4, Teredo) as public.
package egress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codefly-dev/core/network/urlguard"
)

// ErrNonPublicDestination is returned when a bound call would reach an address
// that is not on the public internet. It names no address, so a caller cannot
// use the refusal to map the cell's network.
var ErrNonPublicDestination = errors.New("destination is not a public address")

// specialUseIPv4 is the IANA IPv4 Special-Purpose Address Registry, minus the
// ranges netip.Addr already answers for (loopback, link-local, multicast).
var specialUseIPv4 = []string{
	"0.0.0.0/8",          // "this network"
	"10.0.0.0/8",         // private
	"100.64.0.0/10",      // shared address space (carrier NAT)
	"172.16.0.0/12",      // private
	"192.0.0.0/24",       // IETF protocol assignments
	"192.0.2.0/24",       // documentation (TEST-NET-1)
	"192.31.196.0/24",    // AS112
	"192.52.193.0/24",    // AMT
	"192.88.99.0/24",     // 6to4 relay anycast, deprecated
	"192.168.0.0/16",     // private
	"192.175.48.0/24",    // AS112 direct delegation
	"198.18.0.0/15",      // benchmarking
	"198.51.100.0/24",    // documentation (TEST-NET-2)
	"203.0.113.0/24",     // documentation (TEST-NET-3)
	"240.0.0.0/4",        // reserved
	"255.255.255.255/32", // limited broadcast
}

// specialUseIPv6 is the IANA IPv6 Special-Purpose Address Registry. The
// transition prefixes that embed an IPv4 address (NAT64, 6to4, Teredo) are
// refused outright rather than decoded: the first hop is a translator or relay
// on the cell's own network, whatever address it carries.
var specialUseIPv6 = []string{
	"::/128",         // unspecified
	"::1/128",        // loopback
	"64:ff9b::/96",   // NAT64
	"64:ff9b:1::/48", // local-use NAT64
	"100::/64",       // discard-only
	"100:0:0:1::/64", // dummy prefix
	"2001::/23",      // IETF protocol assignments, Teredo included
	"2001:db8::/32",  // documentation
	"2002::/16",      // 6to4
	"3fff::/20",      // documentation (RFC 9637)
	"5f00::/16",      // segment routing
	"fc00::/7",       // unique local
	"fe80::/10",      // link-local unicast
}

// globalUnicastIPv6 is the only IPv6 block IANA allocates for global unicast;
// an IPv6 address outside it is refused before the registries are read.
var globalUnicastIPv6 = netip.MustParsePrefix("2000::/3")

var specialUsePrefixes = func() []netip.Prefix {
	var out []netip.Prefix
	for _, raw := range append(append([]string{}, specialUseIPv4...), specialUseIPv6...) {
		out = append(out, netip.MustParsePrefix(raw))
	}
	return out
}()

// IsPublic reports whether address is a unicast address on the public
// internet. A zoned (scoped) address is refused: a scope is meaningless for a
// public destination, and prefix matching ignores zoned addresses.
func IsPublic(address netip.Addr) bool {
	if !address.IsValid() || address.Zone() != "" {
		return false
	}
	address = address.Unmap()
	if address.Is6() && !globalUnicastIPv6.Contains(address) {
		return false
	}
	if address.IsLoopback() || address.IsMulticast() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() ||
		address.IsInterfaceLocalMulticast() || address.IsUnspecified() {
		return false
	}
	for _, prefix := range specialUsePrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

// guardDial is the Control hook of every bound dial. It runs with the concrete
// "ip:port" about to be connected to; an address it cannot parse is refused.
func guardDial(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return ErrNonPublicDestination
	}
	parsed, err := netip.ParseAddr(host)
	if err != nil || !IsPublic(parsed) {
		return ErrNonPublicDestination
	}
	return nil
}

// lookupNetIP is the resolver Qualify uses; a variable only so a test can
// make a name resolve where no real resolver would.
var lookupNetIP = net.DefaultResolver.LookupNetIP

// dialGuard is guardDial in production; it is a variable only so a test can
// observe the verdict on every dial.
var dialGuard = guardDial

// Composed is the set of endpoints the platform resolved for the gateway's
// composed dependencies — the services it declares as service-dependencies —
// as the gateway reaches them. It is the one exception to the public-only rule,
// and it is exact: a call is admitted to a non-public address only while it is
// talking to one of these authorities, by name and port, and only at the
// addresses that name resolved to. A redirect or a token request to any other
// name — even one that resolves to the same address — is judged by the
// public-only rule.
//
// Its entries are names, never addresses: codefly's mappings name a
// dependency (host.docker.internal, a service DNS name), so an address literal
// is not a composed endpoint and is refused here. And whatever a composed name
// resolves to, link-local (the cloud metadata service), multicast and
// unspecified addresses are never admitted: no composition places a
// dependency there.
type Composed map[string]bool

// ParseComposed reads a comma-separated list of host:port endpoints.
func ParseComposed(list string) (Composed, error) {
	composed := Composed{}
	for _, entry := range strings.Split(list, ",") {
		if entry = strings.TrimSpace(entry); entry == "" {
			continue
		}
		origin, err := urlguard.NormalizeOrigin("http://" + entry)
		if err != nil || strings.Count(entry, ":") == 0 {
			return nil, fmt.Errorf("composed endpoint %q is not a host:port", entry)
		}
		if _, literal := netip.ParseAddr(origin.Host); literal == nil {
			return nil, fmt.Errorf("composed endpoint %q is an address; a composed endpoint is a name", entry)
		}
		composed[origin.HostPort()] = true
	}
	return composed, nil
}

// Admission is what Qualify decided for one endpoint: for a composed
// endpoint, its authority and the exact addresses it may reach; otherwise
// nothing beyond the public-only rule.
type Admission struct {
	authority string
	addresses []netip.AddrPort
}

// Guard is one bound call's HTTP client, and its record of whether the guard
// refused a dial. The record lets a caller classify the refusal however the
// SDK in between reported it — some format the dial error into text and drop
// its identity.
type Guard struct {
	Client  *http.Client
	refused atomic.Bool

	mu   sync.Mutex
	sent []string
}

// Refused reports whether any dial this guard judged was refused.
func (g *Guard) Refused() bool { return g.refused.Load() }

// Sent is every credential this call put on the wire — the Authorization
// values, a bearer minted from a key included, which no field of the binding
// holds. A caller scrubs them from anything it emits.
func (g *Guard) Sent() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.sent...)
}

func (g *Guard) record(authorization string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, value := range []string{authorization, strings.TrimPrefix(authorization, "Bearer ")} {
		if value != "" && !slices.Contains(g.sent, value) {
			g.sent = append(g.sent, value)
		}
	}
}

// maxRedactedErrorBody bounds the error body the guard reads to redact.
const maxRedactedErrorBody = 1 << 20

// redacting is the guard's transport: it records the credentials a request
// carries, and removes them from what the response brings back — every header,
// and the body of an error — before the SDK above parses or logs it. A store
// may echo what it was sent, and an SDK's debug logging writes response
// headers and error bodies to the gateway's own stderr. A successful body is
// the caller's data and passes untouched.
type redacting struct {
	guard *Guard
	base  http.RoundTripper
}

func (t *redacting) RoundTrip(req *http.Request) (*http.Response, error) {
	if authorization := req.Header.Get("Authorization"); authorization != "" {
		t.guard.record(authorization)
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp == nil {
		return resp, err
	}
	sent := t.guard.Sent()
	if len(sent) == 0 {
		return resp, nil
	}
	for name, values := range resp.Header {
		for i, value := range values {
			resp.Header[name][i] = Redact(value, sent)
		}
	}
	if resp.StatusCode >= 400 && resp.Body != nil {
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, maxRedactedErrorBody))
		_ = resp.Body.Close()
		if rerr != nil {
			return nil, rerr
		}
		redacted := Redact(string(body), sent)
		resp.Body, resp.ContentLength = io.NopCloser(strings.NewReader(redacted)), int64(len(redacted))
		resp.Header.Del("Content-Length")
	}
	return resp, nil
}

// Redact replaces every occurrence of each secret in text.
func Redact(text string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[redacted]")
		}
	}
	return text
}

// NewGuard returns the guarded client for one bound call. Every dial is judged
// against the concrete address being connected to. The composed exception
// applies only to a dial for the admitted authority — the host:port the
// transport was asked to reach — and only at its admitted addresses; any other
// dial must reach a public address. The client never uses a proxy: a proxy
// would make the guard judge the proxy's address instead of the destination's.
// The caller closes idle connections when the call returns.
func NewGuard(admission Admission) *Guard {
	g := &Guard{}
	dial := func(ctx context.Context, network, authority string) (net.Conn, error) {
		exception := admission.authority != "" && canonicalAuthority(authority) == admission.authority
		dialer := &net.Dialer{
			Timeout: 10 * time.Second,
			Control: func(network, address string, c syscall.RawConn) error {
				if exception && admission.admits(address) {
					return nil
				}
				if err := dialGuard(network, address, c); err != nil {
					g.refused.Store(true)
					return err
				}
				return nil
			},
		}
		return dialer.DialContext(ctx, network, authority)
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dial,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	g.Client = &http.Client{Transport: &redacting{guard: g, base: transport}}
	return g
}

func (a Admission) admits(address string) bool {
	target, err := netip.ParseAddrPort(address)
	if err != nil {
		return false
	}
	target = netip.AddrPortFrom(target.Addr().Unmap(), target.Port())
	for _, allowed := range a.addresses {
		if target == allowed {
			return true
		}
	}
	return false
}

// canonicalAuthority is host:port in the form Composed keys use, or "" when
// it is not one.
func canonicalAuthority(authority string) string {
	origin, err := urlguard.NormalizeOrigin("http://" + authority)
	if err != nil {
		return ""
	}
	return origin.HostPort()
}

// Qualify judges a caller-supplied endpoint before the first dial. It must be
// a bare http(s) origin (core's urlguard.NormalizeOrigin refuses userinfo, a
// path, a query and IDNA-ambiguous hosts). A composed endpoint is admitted at
// the addresses it resolves to, none of which may be link-local, multicast or
// unspecified. Any other endpoint must resolve only to public addresses, and
// is admitted by the public-only rule alone. The dial guard still runs
// afterwards, because a name may resolve differently on the dial than it did
// here; this check exists so a refusal is immediate and names the endpoint,
// instead of surfacing as an SDK retrying a refused dial.
func Qualify(ctx context.Context, endpoint string, composed Composed) (Admission, error) {
	origin, err := urlguard.NormalizeOrigin(endpoint)
	if err != nil {
		return Admission{}, fmt.Errorf("binding endpoint: %w", err)
	}
	var addresses []netip.Addr
	if literal, perr := netip.ParseAddr(origin.Host); perr == nil {
		addresses = []netip.Addr{literal}
	} else if addresses, err = lookupNetIP(ctx, "ip", origin.Host); err != nil {
		return Admission{}, fmt.Errorf("binding endpoint %q does not resolve: %w", origin.Host, err)
	}
	if composed[origin.HostPort()] {
		admission := Admission{authority: origin.HostPort()}
		for _, address := range addresses {
			address = address.Unmap()
			if address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsMulticast() ||
				address.IsInterfaceLocalMulticast() || address.IsUnspecified() {
				return Admission{}, fmt.Errorf("binding endpoint %q: %w", origin.Host, ErrNonPublicDestination)
			}
			admission.addresses = append(admission.addresses, netip.AddrPortFrom(address, uint16(origin.Port)))
		}
		return admission, nil
	}
	for _, address := range addresses {
		if !IsPublic(address) {
			return Admission{}, fmt.Errorf("binding endpoint %q: %w", origin.Host, ErrNonPublicDestination)
		}
	}
	return Admission{}, nil
}
