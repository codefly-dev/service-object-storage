package egress

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"syscall"
	"testing"
)

// The vectors the host's guard was held to, one per registry entry kind.
var refused = []string{
	"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "100.64.0.1",
	"169.254.169.254", "0.0.0.0", "192.0.2.1", "198.18.0.1", "203.0.113.9",
	"240.0.0.1", "255.255.255.255", "224.0.0.1",
	"::1", "::", "fe80::1", "fc00::1", "64:ff9b::a00:1", "2002:a00:1::",
	"2001::1", "2001:db8::1", "::ffff:10.0.0.1", "::a00:1", "fec0::1",
}

var admitted = []string{"8.8.8.8", "1.1.1.1", "65.49.37.6", "2001:4860:4860::8888", "::ffff:8.8.8.8"}

func TestIsPublic(t *testing.T) {
	for _, raw := range refused {
		if IsPublic(netip.MustParseAddr(raw)) {
			t.Errorf("IsPublic(%s) = true, want refused", raw)
		}
	}
	for _, raw := range admitted {
		if !IsPublic(netip.MustParseAddr(raw)) {
			t.Errorf("IsPublic(%s) = false, want admitted", raw)
		}
	}
	if IsPublic(netip.MustParseAddr("fe80::1%eth0")) {
		t.Error("a zoned address was admitted")
	}
}

func TestGuardDialRefusesUnparseable(t *testing.T) {
	for _, address := range []string{"", "localhost:80", "8.8.8.8"} {
		if err := guardDial("tcp", address, nil); !errors.Is(err, ErrNonPublicDestination) {
			t.Errorf("guardDial(%q) = %v, want refused", address, err)
		}
	}
}

// Client's every dial runs the guard on the concrete address, and a refused
// dial never reaches the listener.
func TestClientGuardsEveryDial(t *testing.T) {
	var reached atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer srv.Close()

	var judged atomic.Int64
	saved := dialGuard
	dialGuard = func(network, address string, c syscall.RawConn) error {
		judged.Add(1)
		return guardDial(network, address, c)
	}
	t.Cleanup(func() { dialGuard = saved })

	_, err := Client().Get(srv.URL)
	if !errors.Is(err, ErrNonPublicDestination) {
		t.Fatalf("dial to %s = %v, want ErrNonPublicDestination", srv.URL, err)
	}
	if judged.Load() == 0 || reached.Load() != 0 {
		t.Fatalf("guard judged %d dials, listener reached %d times", judged.Load(), reached.Load())
	}
}

// A redirect is a new dial and is judged like the first: an admitted origin
// that redirects to a non-public one is refused on the redirect.
func TestClientGuardsRedirects(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer target.Close()
	var redirected atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer origin.Close()
	admit := netip.MustParseAddrPort(origin.Listener.Addr().String()).String()

	saved := dialGuard
	dialGuard = func(network, address string, c syscall.RawConn) error {
		if address == admit {
			return nil
		}
		return guardDial(network, address, c)
	}
	t.Cleanup(func() { dialGuard = saved })

	if _, err := Client().Get(origin.URL); !errors.Is(err, ErrNonPublicDestination) {
		t.Fatalf("redirect to %s = %v, want ErrNonPublicDestination", target.URL, err)
	}
	if redirected.Load() != 1 {
		t.Fatalf("the admitted origin answered %d times, want 1", redirected.Load())
	}
}

func TestQualify(t *testing.T) {
	ctx := context.Background()
	for _, endpoint := range []string{"http://localhost:9000", "http://127.0.0.1:9000", "https://[::1]", "http://10.0.0.5"} {
		if err := Qualify(ctx, endpoint); !errors.Is(err, ErrNonPublicDestination) {
			t.Errorf("Qualify(%q) = %v, want ErrNonPublicDestination", endpoint, err)
		}
	}
	for _, endpoint := range []string{"ftp://example.com", "https://user:pw@example.com", "https://example.com/bucket", "example.com"} {
		if err := Qualify(ctx, endpoint); err == nil || errors.Is(err, ErrNonPublicDestination) {
			t.Errorf("Qualify(%q) = %v, want a malformed-endpoint refusal", endpoint, err)
		}
	}
	if err := Qualify(ctx, "https://8.8.8.8"); err != nil {
		t.Errorf("Qualify(public literal) = %v", err)
	}
}
