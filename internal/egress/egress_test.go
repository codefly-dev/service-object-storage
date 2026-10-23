package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
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

	_, err := Client(nil).Get(srv.URL)
	if !errors.Is(err, ErrNonPublicDestination) {
		t.Fatalf("dial to %s = %v, want ErrNonPublicDestination", srv.URL, err)
	}
	if judged.Load() == 0 || reached.Load() != 0 {
		t.Fatalf("guard judged %d dials, listener reached %d times", judged.Load(), reached.Load())
	}
}

// A redirect is a new dial and is judged like the first: a composed origin
// that redirects to another loopback port is refused on the redirect, because
// admission is the composed endpoint's exact address and port, nothing wider.
func TestClientGuardsRedirectsFromAComposedEndpoint(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer target.Close()
	var redirected atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer origin.Close()

	admitted, err := Qualify(context.Background(), origin.URL, composedOf(t, origin.URL))
	if err != nil {
		t.Fatalf("Qualify(composed %s) = %v", origin.URL, err)
	}
	if _, err := Client(admitted).Get(origin.URL); !errors.Is(err, ErrNonPublicDestination) {
		t.Fatalf("redirect to %s = %v, want ErrNonPublicDestination", target.URL, err)
	}
	if redirected.Load() != 1 {
		t.Fatalf("the composed origin answered %d times, want 1", redirected.Load())
	}
}

func composedOf(t *testing.T, rawURL string) Composed {
	t.Helper()
	composed, err := ParseComposed(strings.TrimPrefix(rawURL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	return composed
}

// The composed endpoint is admitted, by name as well as by address; an
// arbitrary loopback or private endpoint is still refused beside it, and a
// public endpoint is admitted as before.
func TestComposedEndpointIsTheOnlyNonPublicAdmission(t *testing.T) {
	ctx := context.Background()
	var reached atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer srv.Close()
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	composed := composedOf(t, srv.URL)

	for _, endpoint := range []string{srv.URL, fmt.Sprintf("http://localhost:%d", port)} {
		if strings.Contains(endpoint, "localhost") {
			composed = composedOf(t, endpoint)
		}
		admitted, err := Qualify(ctx, endpoint, composed)
		if err != nil || len(admitted) == 0 {
			t.Fatalf("Qualify(composed %s) = %v, %v; want admitted", endpoint, admitted, err)
		}
		resp, err := Client(admitted).Get(endpoint)
		if err != nil {
			t.Fatalf("composed %s: %v", endpoint, err)
		}
		resp.Body.Close()
	}
	if reached.Load() != 2 {
		t.Fatalf("composed endpoint reached %d times, want 2", reached.Load())
	}

	composed = composedOf(t, srv.URL)
	for _, endpoint := range []string{
		fmt.Sprintf("http://127.0.0.1:%d", port+1), // same host, another port
		fmt.Sprintf("http://localhost:%d", port),   // same address, by a name that is not the composed one
		"http://10.0.0.5:9000", "http://169.254.169.254",
	} {
		if _, err := Qualify(ctx, endpoint, composed); !errors.Is(err, ErrNonPublicDestination) {
			t.Errorf("Qualify(%s) beside a composed endpoint = %v, want refused", endpoint, err)
		}
	}
	if admitted, err := Qualify(ctx, "https://8.8.8.8", composed); err != nil || admitted != nil {
		t.Errorf("Qualify(public) = %v, %v; want admitted by the public rule alone", admitted, err)
	}
}

func TestParseComposedRefusesAnythingButHostPort(t *testing.T) {
	for _, entry := range []string{"minio", "http://minio:9000", "user@minio:9000", "minio:9000/path"} {
		if _, err := ParseComposed(entry); err == nil {
			t.Errorf("ParseComposed(%q) accepted", entry)
		}
	}
	composed, err := ParseComposed(" host.docker.internal:56826 ,[::1]:9000,")
	if err != nil || !composed["host.docker.internal:56826"] || !composed["[::1]:9000"] || len(composed) != 2 {
		t.Fatalf("ParseComposed = %v, %v", composed, err)
	}
}

func TestQualify(t *testing.T) {
	ctx := context.Background()
	for _, endpoint := range []string{"http://localhost:9000", "http://127.0.0.1:9000", "https://[::1]", "http://10.0.0.5"} {
		if _, err := Qualify(ctx, endpoint, nil); !errors.Is(err, ErrNonPublicDestination) {
			t.Errorf("Qualify(%q) = %v, want ErrNonPublicDestination", endpoint, err)
		}
	}
	for _, endpoint := range []string{"ftp://example.com", "https://user:pw@example.com", "https://example.com/bucket", "example.com"} {
		if _, err := Qualify(ctx, endpoint, nil); err == nil || errors.Is(err, ErrNonPublicDestination) {
			t.Errorf("Qualify(%q) = %v, want a malformed-endpoint refusal", endpoint, err)
		}
	}
	if _, err := Qualify(ctx, "https://8.8.8.8", nil); err != nil {
		t.Errorf("Qualify(public literal) = %v", err)
	}
}
