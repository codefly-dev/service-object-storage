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

// The host classifier's full vector set (module-saas-starter
// accounts/pkg/auth/egresstest at 877e8a28), carried over whole: the
// classifier moved here, and so does every case it was held to — range edges,
// AS112/AMT, the dummy/ORCHID/segment-routing ranges, zoned and mapped forms.
var (
	refusedVectors = []string{
		"100.64.0.1", "100.100.100.200", "100.127.255.255", "64:ff9b::a9fe:a9fe", "64:ff9b::a00:1",
		"64:ff9b::808:808", "64:ff9b:1::a9fe:a9fe", "2002:a9fe:a9fe::1", "2002:808:808::1",
		"2001:0:4136:e378:8000:63bf:3fff:fdd2", "198.18.0.1", "198.19.255.255", "192.0.0.170",
		"240.0.0.1", "255.255.255.254", "127.0.0.1", "127.255.255.254", "::1", "10.1.2.3", "172.16.0.1",
		"172.31.255.255", "192.168.0.1", "169.254.169.254", "fe80::1", "0.0.0.0", "::", "224.0.0.1",
		"ff02::1", "0.1.2.3", "239.1.1.1", "255.255.255.255", "192.0.2.1", "198.51.100.1", "203.0.113.1",
		"192.31.196.1", "192.52.193.1", "192.88.99.1", "192.175.48.1", "100::1", "100:0:0:1::1",
		"2001:100::1", "2001:1::1", "2001:2::1", "2001:20::1", "2001:db8::1", "3fff::1", "5f00::1",
		"fc00::1", "fd00::1", "ff0e::1", "ff01::1", "::a9fe:a9fe", "::a00:1", "fec0::1", "4001::1",
		"8000::1", "::ffff:127.0.0.1", "::ffff:10.0.0.1", "::ffff:169.254.169.254",
		"::ffff:100.100.100.200", "::ffff:198.18.0.1", "fe80::1%eth0", "fd00::1%lo0",
		"2606:4700::1111%eth0", "::ffff:8.8.8.8%eth0",
	}
	admittedVectors = []string{
		"8.8.8.8", "1.1.1.1", "100.63.255.255", "100.128.0.0", "198.17.255.255", "198.20.0.0",
		"192.0.1.1", "223.255.255.254", "::ffff:8.8.8.8", "2606:4700:4700::1111", "2001:200::1",
		"2001:4860:4860::8888", "2003::1", "2a00:1450:4001::1",
	}
	// unclassifiableDialAddresses are what a dialer could hand the guard
	// that is not an ip:port; each is refused.
	unclassifiableDialAddresses = []string{
		"not-an-ip:80", "example.com:443", "", "1.1.1.1", "[2606:4700::1111]", "01.1.1.1:80",
	}
)

// TestReviewAllOriginalHostVectors is review r10's reproduction, kept: every
// original vector, judged both by IsPublic and by the dial guard.
func TestReviewAllOriginalHostVectors(t *testing.T) {
	for _, v := range refusedVectors {
		if IsPublic(netip.MustParseAddr(v)) {
			t.Errorf("IsPublic(%s) = true, want refused", v)
		}
		if guardDial("tcp", net.JoinHostPort(v, "443"), nil) == nil {
			t.Errorf("dial to %s admitted", v)
		}
	}
	for _, v := range admittedVectors {
		if !IsPublic(netip.MustParseAddr(v)) {
			t.Errorf("IsPublic(%s) = false, want admitted", v)
		}
	}
	for _, v := range unclassifiableDialAddresses {
		if guardDial("tcp", v, nil) == nil {
			t.Errorf("unclassifiable %q admitted", v)
		}
	}
	if len(refusedVectors) != 65 || len(admittedVectors) != 14 || len(unclassifiableDialAddresses) != 6 {
		t.Fatalf("vector set shrank: %d refused, %d admitted, %d unclassifiable",
			len(refusedVectors), len(admittedVectors), len(unclassifiableDialAddresses))
	}
}

// localName is the composed name for a loopback test listener: composed
// endpoints are names, never addresses.
func localName(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	return fmt.Sprintf("localhost:%d", srv.Listener.Addr().(*net.TCPAddr).Port)
}

func composedOf(t *testing.T, entries ...string) Composed {
	t.Helper()
	composed, err := ParseComposed(strings.Join(entries, ","))
	if err != nil {
		t.Fatal(err)
	}
	return composed
}

// The guard's every dial judges the concrete address, and a refused dial
// never reaches the listener — and is recorded, whatever error the caller
// sees.
func TestGuardRefusesEveryNonPublicDialAndRecordsIt(t *testing.T) {
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

	guard := NewGuard(Admission{}, 0)
	if _, err := guard.Client.Get(srv.URL); !errors.Is(err, ErrNonPublicDestination) {
		t.Fatalf("dial to %s = %v, want ErrNonPublicDestination", srv.URL, err)
	}
	if judged.Load() == 0 || reached.Load() != 0 || !guard.Refused() {
		t.Fatalf("judged %d dials, listener reached %d times, refused recorded %v", judged.Load(), reached.Load(), guard.Refused())
	}
}

// A redirect is a new dial and is judged like the first: a composed origin
// that redirects to another port is refused on the redirect.
func TestComposedRedirectToAnotherPortIsRefused(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer target.Close()
	var redirected atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		http.Redirect(w, r, "http://"+localName(t, target), http.StatusFound)
	}))
	defer origin.Close()
	name := localName(t, origin)

	admission, err := Qualify(context.Background(), "http://"+name, composedOf(t, name))
	if err != nil {
		t.Fatalf("Qualify(composed %s) = %v", name, err)
	}
	if _, err := NewGuard(admission, 0).Client.Get("http://" + name); !errors.Is(err, ErrNonPublicDestination) {
		t.Fatalf("redirect to %s = %v, want ErrNonPublicDestination", target.URL, err)
	}
	if redirected.Load() != 1 {
		t.Fatalf("the composed origin answered %d times, want 1", redirected.Load())
	}
}

// TestReviewRedirectAlias is review r2's reproduction, kept: the exception is
// the composed authority, not its address. A composed origin redirecting to
// another name for the very same address and port is refused without the
// second authority being reached.
func TestReviewRedirectAlias(t *testing.T) {
	var hosts []string
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hosts = append(hosts, r.Host)
		if r.URL.Path == "/" {
			http.Redirect(w, r, srv.URL+"/alias", http.StatusFound) // 127.0.0.1:<same port>
			return
		}
		fmt.Fprint(w, "alias reached")
	}))
	defer srv.Close()
	name := localName(t, srv)

	admission, err := Qualify(context.Background(), "http://"+name, composedOf(t, name))
	if err != nil {
		t.Fatal(err)
	}
	guard := NewGuard(admission, 0)
	defer guard.Client.CloseIdleConnections()
	if _, err := guard.Client.Get("http://" + name); !errors.Is(err, ErrNonPublicDestination) {
		t.Fatalf("redirect to the alias = %v, want ErrNonPublicDestination", err)
	}
	if len(hosts) != 1 || !strings.HasPrefix(hosts[0], "localhost:") || !guard.Refused() {
		t.Fatalf("authorities reached = %v, refused recorded %v; want only the composed one", hosts, guard.Refused())
	}
}

// The composed endpoint is admitted, by its name; the same listener by any
// other name or port, and every other non-public address, is still refused;
// a public endpoint is admitted by the public rule alone.
func TestComposedEndpointIsTheOnlyNonPublicAdmission(t *testing.T) {
	ctx := context.Background()
	var reached atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer srv.Close()
	name := localName(t, srv)
	port := srv.Listener.Addr().(*net.TCPAddr).Port
	composed := composedOf(t, name)

	admission, err := Qualify(ctx, "http://"+name, composed)
	if err != nil {
		t.Fatalf("Qualify(composed %s) = %v", name, err)
	}
	resp, err := NewGuard(admission, 0).Client.Get("http://" + name)
	if err != nil {
		t.Fatalf("composed %s: %v", name, err)
	}
	resp.Body.Close()
	if reached.Load() != 1 {
		t.Fatalf("composed endpoint reached %d times, want 1", reached.Load())
	}

	for _, endpoint := range []string{
		srv.URL, // the same listener, by address
		fmt.Sprintf("http://localhost:%d", port+1), // the composed name, another port
		"http://10.0.0.5:9000", "http://169.254.169.254",
	} {
		if _, err := Qualify(ctx, endpoint, composed); !errors.Is(err, ErrNonPublicDestination) {
			t.Errorf("Qualify(%s) beside a composed endpoint = %v, want refused", endpoint, err)
		}
	}
	if admission, err := Qualify(ctx, "https://8.8.8.8", composed); err != nil || admission.authority != "" {
		t.Errorf("Qualify(public) = %+v, %v; want admitted by the public rule alone", admission, err)
	}
}

// TestReviewEnvironmentAdmission is review r3's reproduction, kept: whoever
// sets SOS_COMPOSED_ENDPOINTS, it cannot admit the metadata address (an
// address is not a composed endpoint), nor a name that resolves to a
// link-local address.
func TestReviewEnvironmentAdmission(t *testing.T) {
	for _, entry := range []string{"169.254.169.254:80", "10.0.0.5:9000", "[fe80::1]:80", "[::1]:9000"} {
		if _, err := ParseComposed(entry); err == nil {
			t.Errorf("ParseComposed(%q) accepted an address", entry)
		}
	}
	saved := lookupNetIP
	lookupNetIP = func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
	}
	t.Cleanup(func() { lookupNetIP = saved })
	if _, err := Qualify(context.Background(), "http://metadata.internal:80", composedOf(t, "metadata.internal:80")); !errors.Is(err, ErrNonPublicDestination) {
		t.Fatalf("a composed name resolving to the metadata address = %v, want refused", err)
	}
}

func TestParseComposedRefusesAnythingButHostPort(t *testing.T) {
	for _, entry := range []string{"minio", "http://minio:9000", "user@minio:9000", "minio:9000/path"} {
		if _, err := ParseComposed(entry); err == nil {
			t.Errorf("ParseComposed(%q) accepted", entry)
		}
	}
	composed, err := ParseComposed(" host.docker.internal:56826 ,localhost:9000,")
	if err != nil || !composed["host.docker.internal:56826"] || !composed["localhost:9000"] || len(composed) != 2 {
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
