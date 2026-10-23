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
	"net"
	"net/http"
	"net/netip"
	"strings"
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

// dialGuard is guardDial in production; it is a variable only so a test can
// observe the verdict on every dial.
var dialGuard = guardDial

// Composed is the set of endpoints the platform resolved for the gateway's
// composed dependencies — the services it declares as service-dependencies —
// as the gateway reaches them. It is the one exception to the public-only rule,
// and it is exact: an endpoint is admitted only when it is one of these, by
// name and port, and then only the addresses that name resolves to, on that
// port. It cannot widen to a range, a host on another port, or a redirect.
// Nothing in it is chosen by a caller or an operator: the agent writes it from
// the dependency network mappings codefly hands its Runtime.
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
		composed[origin.HostPort()] = true
	}
	return composed, nil
}

// Client returns an HTTP client for one bound call. Its every dial is guarded:
// an address in admitted (what Qualify returned for a composed endpoint) is
// dialed, anything else must be public. It never uses a proxy: a proxy would
// make the guard judge the proxy's address instead of the destination's. The
// caller closes idle connections when the call returns, so nothing outlives it.
func Client(admitted []netip.AddrPort) *http.Client {
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			if target, err := netip.ParseAddrPort(address); err == nil {
				target = netip.AddrPortFrom(target.Addr().Unmap(), target.Port())
				for _, allowed := range admitted {
					if target == allowed {
						return nil
					}
				}
			}
			return dialGuard(network, address, c)
		},
	}
	return &http.Client{Transport: &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}}
}

// Qualify judges a caller-supplied endpoint before the first dial. It must be
// a bare http(s) origin (core's urlguard.NormalizeOrigin refuses userinfo, a
// path, a query and IDNA-ambiguous hosts). When it is a composed endpoint,
// Qualify returns the exact addresses it resolves to, on its port, for Client
// to admit. Otherwise every address it resolves to must be public, and nothing
// is admitted beyond the public-only rule. The dial guard still runs
// afterwards, because a name may resolve differently on the dial than it did
// here; this check exists so a refusal is immediate and names the endpoint,
// instead of surfacing as an SDK retrying a refused dial.
func Qualify(ctx context.Context, endpoint string, composed Composed) ([]netip.AddrPort, error) {
	origin, err := urlguard.NormalizeOrigin(endpoint)
	if err != nil {
		return nil, fmt.Errorf("binding endpoint: %w", err)
	}
	var addresses []netip.Addr
	if literal, perr := netip.ParseAddr(origin.Host); perr == nil {
		addresses = []netip.Addr{literal}
	} else if addresses, err = net.DefaultResolver.LookupNetIP(ctx, "ip", origin.Host); err != nil {
		return nil, fmt.Errorf("binding endpoint %q does not resolve: %w", origin.Host, err)
	}
	if composed[origin.HostPort()] {
		admitted := make([]netip.AddrPort, 0, len(addresses))
		for _, address := range addresses {
			admitted = append(admitted, netip.AddrPortFrom(address.Unmap(), uint16(origin.Port)))
		}
		return admitted, nil
	}
	for _, address := range addresses {
		if !IsPublic(address) {
			return nil, fmt.Errorf("binding endpoint %q: %w", origin.Host, ErrNonPublicDestination)
		}
	}
	return nil, nil
}
