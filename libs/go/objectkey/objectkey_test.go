package objectkey_test

import (
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/codefly-dev/service-object-storage/libs/go/objectkey"
)

// The golden vectors. Keys are persistent: a change that makes any line of
// this table fail changes where every existing object is looked for. It is
// not a test to update; it is the contract.
var golden = []struct {
	base string
	key  objectkey.Key
	want string
}{
	{"files", objectkey.Key{Partition: "org-1", Scope: "inst-1", Slot: objectkey.Principal("human:01HZX3"), Name: "report.pdf"},
		"files/org-1/inst-1/human.01HZX3/report.pdf"},
	{"files", objectkey.Key{Partition: "org-1", Scope: "inst-1", Slot: objectkey.Shared(), Name: "logo.png"},
		"files/org-1/inst-1/_shared/logo.png"},
	{"files", objectkey.Key{Partition: "org-1", Scope: "inst-1", Slot: objectkey.Content(), Name: strings.Repeat("ab", 32)},
		"files/org-1/inst-1/_sha256/" + strings.Repeat("ab", 32)},
	{"files", objectkey.Key{Partition: "org-1", Scope: "inst-1", Slot: objectkey.Principal("agent:a1"), Name: "u1", Staging: true},
		"files/org-1/inst-1/agent.a1/_staging/u1"},
	// A UUID and a nested name pass through unchanged.
	{"docs-content", objectkey.Key{Partition: "0b9c2a4e-7d1f-4c3a-9e2b-5f6a7b8c9d0e", Scope: "0190f6a2-aaaa-7bbb-8ccc-0123456789ab", Slot: objectkey.Principal("human:0190f6a2-dddd-7eee-8fff-0123456789ab"), Name: "2026/11/invoice-42.pdf"},
		"docs-content/0b9c2a4e-7d1f-4c3a-9e2b-5f6a7b8c9d0e/0190f6a2-aaaa-7bbb-8ccc-0123456789ab/human.0190f6a2-dddd-7eee-8fff-0123456789ab/2026_/11_/invoice-42.pdf"},
	// "_" is the escape, so it is always escaped: nothing an identifier or a
	// name holds can spell a reserved segment.
	{"files", objectkey.Key{Partition: "_shared", Scope: "_sha256", Slot: objectkey.Principal("human:_shared"), Name: "_shared/_staging"},
		"files/_5fshared/_5fsha256/human._5fshared/_5fshared_/_5fstaging"},
	// A separator inside an identifier is a byte like any other.
	{"files", objectkey.Key{Partition: "a/b", Scope: "inst-1/human.zed", Slot: objectkey.Principal("human:x/../y"), Name: "n"},
		"files/a_2fb/inst-1_2fhuman.zed/human.x_2f.._2fy/n"},
	// A name that is also the directory of another does not share a prefix
	// with it: the directory carries a mark the object does not.
	{"files", objectkey.Key{Partition: "p", Scope: "s", Slot: objectkey.Shared(), Name: "a"}, "files/p/s/_shared/a"},
	{"files", objectkey.Key{Partition: "p", Scope: "s", Slot: objectkey.Shared(), Name: "a/b"}, "files/p/s/_shared/a_/b"},
	{"files", objectkey.Key{Partition: "p", Scope: "s", Slot: objectkey.Shared(), Name: "a/b/c"}, "files/p/s/_shared/a_/b_/c"},
	// A leading and a trailing dot are escaped; an interior one is not.
	{"files", objectkey.Key{Partition: ".p", Scope: "s.", Slot: objectkey.Principal("human:.a.b."), Name: ".hidden/a.tar.gz/..."},
		"files/_2ep/s_2e/human._2ea.b_2e/_2ehidden_/a.tar.gz_/_2e._2e"},
	// The principal separator, a space, a percent, a tilde, and the escape
	// sequence written out by hand.
	{"files", objectkey.Key{Partition: "p:q", Scope: "s q", Slot: objectkey.Principal("human:a:b"), Name: "100%~_2f"},
		"files/p_3aq/s_20q/human.a_3ab/100_25_7e_5f2f"},
	// Case is kept. Bytes outside ASCII are escaped one by one, so a key is
	// ASCII and no store can normalize it.
	{"files", objectkey.Key{Partition: "Org", Scope: "org", Slot: objectkey.Principal("human:É"), Name: "e\u0301"},
		"files/Org/org/human._c3_89/e_cc_81"},
}

func TestGoldenVectors(t *testing.T) {
	for _, vector := range golden {
		codec, err := objectkey.New(vector.base)
		if err != nil {
			t.Fatal(err)
		}
		got, err := codec.Key(vector.key)
		if err != nil {
			t.Fatalf("%+v: %v", vector.key, err)
		}
		if got != vector.want {
			t.Errorf("the encoding changed:\n got  %s\n want %s", got, vector.want)
		}
		back, err := codec.Parse(vector.want)
		if err != nil {
			t.Fatalf("parse %s: %v", vector.want, err)
		}
		if back != vector.key {
			t.Errorf("parse %s:\n got  %+v\n want %+v", vector.want, back, vector.key)
		}
		for index := 0; index < len(got); index++ {
			if character := got[index]; !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._/", rune(character)) {
				t.Errorf("%s carries %q, which is outside the alphabet", got, character)
			}
		}
	}
}

func TestRefusedIdentifiersAndNames(t *testing.T) {
	codec, _ := objectkey.New("files")
	good := objectkey.Key{Partition: "p", Scope: "s", Slot: objectkey.Principal("human:a"), Name: "n"}
	long := strings.Repeat("a", objectkey.MaxSegmentBytes+1)
	for name, damage := range map[string]func(*objectkey.Key){
		"empty partition":          func(k *objectkey.Key) { k.Partition = "" },
		"dot partition":            func(k *objectkey.Key) { k.Partition = "." },
		"dot-dot partition":        func(k *objectkey.Key) { k.Partition = ".." },
		"empty scope":              func(k *objectkey.Key) { k.Scope = "" },
		"dot-dot scope":            func(k *objectkey.Key) { k.Scope = ".." },
		"over-long scope":          func(k *objectkey.Key) { k.Scope = long },
		"scope with a newline":     func(k *objectkey.Key) { k.Scope = "a\nb" },
		"scope with a NUL":         func(k *objectkey.Key) { k.Scope = "a\x00b" },
		"scope that is not UTF-8":  func(k *objectkey.Key) { k.Scope = "a\xffb" },
		"no slot":                  func(k *objectkey.Key) { k.Slot = objectkey.Slot{} },
		"principal without a kind": func(k *objectkey.Key) { k.Slot = objectkey.Principal("01HZX3") },
		"principal with empty id":  func(k *objectkey.Key) { k.Slot = objectkey.Principal("human:") },
		"principal kind in caps":   func(k *objectkey.Key) { k.Slot = objectkey.Principal("Human:a") },
		"principal kind with dot":  func(k *objectkey.Key) { k.Slot = objectkey.Principal("hu.man:a") },
		"principal id dot-dot":     func(k *objectkey.Key) { k.Slot = objectkey.Principal("human:..") },
		"over-long principal":      func(k *objectkey.Key) { k.Slot = objectkey.Principal("human:" + long) },
		"empty name":               func(k *objectkey.Key) { k.Name = "" },
		"name with a leading /":    func(k *objectkey.Key) { k.Name = "/a" },
		"name with a trailing /":   func(k *objectkey.Key) { k.Name = "a/" },
		"name with an empty part":  func(k *objectkey.Key) { k.Name = "a//b" },
		"name with a dot part":     func(k *objectkey.Key) { k.Name = "a/./b" },
		"name with a dot-dot part": func(k *objectkey.Key) { k.Name = "a/../b" },
		"name that is dot-dot":     func(k *objectkey.Key) { k.Name = ".." },
		"name with a tab":          func(k *objectkey.Key) { k.Name = "a\tb" },
		"name that is not UTF-8":   func(k *objectkey.Key) { k.Name = "\xc3\x28" },
		"over-long name segment":   func(k *objectkey.Key) { k.Name = "a/" + long },
		"over-long key":            func(k *objectkey.Key) { k.Name = strings.Repeat(strings.Repeat("a", 200)+"/", 5) + "z" },
		"content that is no hash":  func(k *objectkey.Key) { k.Slot, k.Name = objectkey.Content(), "report.pdf" },
		"content hash in caps":     func(k *objectkey.Key) { k.Slot, k.Name = objectkey.Content(), strings.Repeat("AB", 32) },
		"staging in shared":        func(k *objectkey.Key) { k.Slot, k.Staging = objectkey.Shared(), true },
		"staging with a path":      func(k *objectkey.Key) { k.Staging, k.Name = true, "a/b" },
	} {
		key := good
		damage(&key)
		if physical, err := codec.Key(key); !errors.Is(err, objectkey.ErrInvalid) {
			t.Errorf("%s: got %q, %v; want ErrInvalid", name, physical, err)
		}
	}
	if _, err := codec.Key(good); err != nil {
		t.Fatal(err)
	}
}

func TestRefusedBases(t *testing.T) {
	for _, base := range []string{"", "a/b", "A", "-a", "a_b", "a.b", "a b", "_shared", strings.Repeat("a", objectkey.MaxBaseBytes+1), "é"} {
		if _, err := objectkey.New(base); !errors.Is(err, objectkey.ErrInvalid) {
			t.Errorf("base %q: %v, want ErrInvalid", base, err)
		}
	}
	for _, base := range []string{"a", "files", "docs-content", "0a", strings.Repeat("a", objectkey.MaxBaseBytes)} {
		if _, err := objectkey.New(base); err != nil {
			t.Errorf("base %q: %v", base, err)
		}
	}
	if _, err := (objectkey.Codec{}).Key(objectkey.Key{Partition: "p", Scope: "s", Slot: objectkey.Shared(), Name: "n"}); !errors.Is(err, objectkey.ErrInvalid) {
		t.Errorf("the zero codec wrote a key: %v", err)
	}
}

// A key this codec did not write has no reading, however close it is.
func TestParseRefusesWhatTheCodecDidNotWrite(t *testing.T) {
	codec, _ := objectkey.New("files")
	for name, physical := range map[string]string{
		"another base":                   "other/p/s/human.a/n",
		"a base that only begins alike":  "files-2/p/s/human.a/n",
		"no name":                        "files/p/s/human.a",
		"no slot":                        "files/p/s",
		"empty name":                     "files/p/s/human.a/",
		"empty segment":                  "files/p//human.a/n",
		"dot-dot segment":                "files/p/../human.a/n",
		"dot name":                       "files/p/s/human.a/._/n",
		"slot without a kind":            "files/p/s/a/n",
		"slot with an empty id":          "files/p/s/human./n",
		"slot with a capital kind":       "files/p/s/Human.a/n",
		"unknown reserved slot":          "files/p/s/_private/n",
		"reserved partition":             "files/_shared/s/human.a/n",
		"raw underscore":                 "files/p/s/human.a/a_b",
		"escape of a plain byte":         "files/p/s/human.a/_61",
		"escape in upper hex":            "files/p/s/human.a/_2F",
		"escape cut short":               "files/p/s/human.a/a_2",
		"escaped interior dot":           "files/p/s/human.a/a_2eb",
		"raw leading dot":                "files/p/s/human.a/.a",
		"raw trailing dot":               "files/p/s/human.a/a.",
		"escaped control character":      "files/p/s/human.a/a_0ab",
		"escaped invalid UTF-8":          "files/p/s/human.a/_ff",
		"a byte outside the alphabet":    "files/p/s/human.a/a~b",
		"an encoded separator in a name": "files/p/s/human.a/a_2fb",
		"a percent escape":               "files/p/s/human.a/a%2fb",
		"a space":                        "files/p/s/human.a/a b",
		"content with a path":            "files/p/s/_sha256/" + strings.Repeat("ab", 32) + "/x",
		"content that is no hash":        "files/p/s/_sha256/report.pdf",
		"staging in shared":              "files/p/s/_shared/_staging/u1",
		"staging with a path":            "files/p/s/human.a/_staging/u1/x",
		"a directory without its mark":   "files/p/s/human.a/a/b",
		"a marked last segment":          "files/p/s/human.a/a_/b_",
		"a mark alone":                   "files/p/s/human.a/_/b",
		"over-long key":                  "files/p/s/human.a/" + strings.Repeat("a_/", 400) + "z",
	} {
		if key, err := codec.Parse(physical); !errors.Is(err, objectkey.ErrInvalid) {
			t.Errorf("%s: %q parsed as %+v, %v; want ErrInvalid", name, physical, key, err)
		}
	}
}

func TestPrefixesEndWithASeparatorAndReadBack(t *testing.T) {
	codec, _ := objectkey.New("files")
	slot := objectkey.Principal("human:a")
	base := codec.BasePrefix()
	partition, _ := codec.PartitionPrefix("p")
	scope, _ := codec.ScopePrefix("p", "s")
	own, _ := codec.SlotPrefix("p", "s", slot)
	directory, _ := codec.DirectoryPrefix("p", "s", slot, "a/b.c")
	staging, _ := codec.StagingPrefix("p", "s", slot)
	for want, got := range map[string]string{
		"files/":                      base,
		"files/p/":                    partition,
		"files/p/s/":                  scope,
		"files/p/s/human.a/":          own,
		"files/p/s/human.a/a_/b.c_/":  directory,
		"files/p/s/human.a/_staging/": staging,
	} {
		if got != want {
			t.Errorf("prefix %q, want %q", got, want)
		}
	}
	gotPartition, gotScope, gotSlot, depth, err := codec.ParsePrefix(own)
	if err != nil || gotPartition != "p" || gotScope != "s" || gotSlot != slot || depth != 3 {
		t.Errorf("ParsePrefix(%q) = %q %q %v %d %v", own, gotPartition, gotScope, gotSlot, depth, err)
	}
	if _, _, _, depth, err := codec.ParsePrefix(partition); err != nil || depth != 1 {
		t.Errorf("ParsePrefix(%q): depth %d, %v", partition, depth, err)
	}
	for _, prefix := range []string{"files/p", "other/p/", "files/p/s/human.a/x/", "files/_shared/"} {
		if _, _, _, _, err := codec.ParsePrefix(prefix); !errors.Is(err, objectkey.ErrInvalid) {
			t.Errorf("ParsePrefix(%q): %v, want ErrInvalid", prefix, err)
		}
	}
	if _, err := codec.StagingPrefix("p", "s", objectkey.Shared()); !errors.Is(err, objectkey.ErrInvalid) {
		t.Errorf("the shared slot has a staging prefix: %v", err)
	}
	if _, err := codec.DirectoryPrefix("p", "s", slot, "a/../b"); !errors.Is(err, objectkey.ErrInvalid) {
		t.Errorf("a directory with a dot-dot segment: %v", err)
	}
}

// hostile is what identifiers and names are drawn from in the property tests:
// every byte the convention gives a meaning to, and what sits next to them.
var hostile = []string{
	"/", "_", ".", "..", ":", "~", "%", " ", "-", "a", "b", "A", "0", "2f", "5f", "_2f", "_5f", "%2f",
	"_shared", "_sha256", "_staging", "human", "human.a", "é", "e\u0301", "日本", "\u202e", "\x00", "\n", "\xff",
}

func randomText(random *rand.Rand, parts int) string {
	var out strings.Builder
	for range 1 + random.IntN(parts) {
		out.WriteString(hostile[random.IntN(len(hostile))])
	}
	return out.String()
}

func randomKey(random *rand.Rand) objectkey.Key {
	key := objectkey.Key{Partition: randomText(random, 3), Scope: randomText(random, 3), Name: randomText(random, 6)}
	switch random.IntN(5) {
	case 0:
		key.Slot = objectkey.Shared()
	case 1:
		key.Slot, key.Name = objectkey.Content(), strings.Repeat(string("0123456789abcdef"[random.IntN(16)]), 64)
	default:
		key.Slot = objectkey.Principal([]string{"human", "agent", "service"}[random.IntN(3)] + ":" + randomText(random, 3))
	}
	return key
}

// Injectivity, as a property: every key that encodes reads back as itself,
// and two different logical addresses never share a key. It also holds across
// case: nothing here relies on a store that folds it.
func TestEncodingIsInjectiveAndRoundTrips(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	codec, _ := objectkey.New("files")
	seen := map[string]objectkey.Key{}
	accepted := 0
	for range 200_000 {
		key := randomKey(random)
		physical, err := codec.Key(key)
		if err != nil {
			if !errors.Is(err, objectkey.ErrInvalid) {
				t.Fatalf("%+v: %v", key, err)
			}
			continue
		}
		accepted++
		if previous, found := seen[physical]; found && previous != key {
			t.Fatalf("two addresses share the key %q:\n %+v\n %+v", physical, previous, key)
		}
		seen[physical] = key
		back, err := codec.Parse(physical)
		if err != nil {
			t.Fatalf("%+v wrote %q, which does not parse: %v", key, physical, err)
		}
		if back != key {
			t.Fatalf("%q reads back as %+v, was written from %+v", physical, back, key)
		}
		if strings.Contains(physical, "//") || strings.Contains(physical, "/./") || strings.Contains(physical, "/../") || len(physical) > objectkey.MaxKeyBytes {
			t.Fatalf("%+v wrote the unusable key %q", key, physical)
		}
	}
	if accepted < 20_000 || len(seen) < 10_000 {
		t.Fatalf("the property ran on too little: %d accepted, %d distinct", accepted, len(seen))
	}
}

// A key in one slot is never under the prefix of another slot, scope or
// partition, whatever the identifiers: the property every confined listing
// and every prefix-wide erasure rests on.
func TestNoKeyFallsUnderAnotherAddressesPrefix(t *testing.T) {
	random := rand.New(rand.NewPCG(3, 4))
	codec, _ := objectkey.New("files")
	checked := 0
	for range 100_000 {
		key, other := randomKey(random), randomKey(random)
		physical, err := codec.Key(key)
		if err != nil {
			continue
		}
		prefix, err := codec.SlotPrefix(other.Partition, other.Scope, other.Slot)
		if err != nil {
			continue
		}
		same := key.Partition == other.Partition && key.Scope == other.Scope && key.Slot == other.Slot
		if strings.HasPrefix(physical, prefix) != same {
			t.Fatalf("key %q of %+v and prefix %q of %+v: under=%v, same slot=%v", physical, key, prefix, other, !same, same)
		}
		checked++
	}
	if checked < 10_000 {
		t.Fatalf("the property ran on %d pairs", checked)
	}
}

// No key is a prefix-with-separator of another: nothing is both an object and
// a directory, whatever the names.
func TestNoKeyIsTheDirectoryOfAnother(t *testing.T) {
	random := rand.New(rand.NewPCG(5, 6))
	codec, _ := objectkey.New("files")
	pool := []string{"a", "b", "a/b", "a/b/c", "a/a", "b/a", "a_", "a_/b", "_", "a/_/b", "a/b/c/d"}
	held := map[string]bool{}
	for range 4000 {
		key := objectkey.Key{Partition: "p", Scope: "s", Slot: objectkey.Shared(), Name: pool[random.IntN(len(pool))]}
		if random.IntN(3) == 0 {
			key.Name = randomText(random, 6)
		}
		if physical, err := codec.Key(key); err == nil {
			held[physical] = true
		}
	}
	for physical := range held {
		for index := range len(physical) {
			if physical[index] == '/' && held[physical[:index]] {
				t.Fatalf("%q is an object and the directory of %q", physical[:index], physical)
			}
		}
	}
	if len(held) < 500 {
		t.Fatalf("the property ran on %d keys", len(held))
	}
}

func FuzzSegmentRoundTrips(f *testing.F) {
	for _, seed := range hostile {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		segment, err := objectkey.Segment(value)
		if err != nil {
			return
		}
		if segment == "" || segment == "." || segment == ".." || strings.ContainsAny(segment, "/") || strings.HasPrefix(segment, "_s") || strings.HasSuffix(segment, "_") {
			t.Fatalf("%q encoded as the unusable segment %q", value, segment)
		}
		back, err := objectkey.Unsegment(segment)
		if err != nil || back != value {
			t.Fatalf("%q encoded as %q, which reads back as %q, %v", value, segment, back, err)
		}
	})
}

// Parse is the inverse of Key and of nothing else: whatever it accepts, Key
// writes back byte for byte.
func FuzzParseAcceptsOnlyCanonicalKeys(f *testing.F) {
	for _, vector := range golden {
		f.Add(vector.want)
	}
	f.Add("files/p/s/human.a/_61")
	f.Add("files/p/s/_shared/../x")
	f.Fuzz(func(t *testing.T, physical string) {
		codec, _ := objectkey.New("files")
		key, err := codec.Parse(physical)
		if err != nil {
			return
		}
		again, err := codec.Key(key)
		if err != nil || again != physical {
			t.Fatalf("%q parsed as %+v, which writes %q, %v", physical, key, again, err)
		}
		if !utf8.ValidString(key.Partition + key.Scope + key.Name) {
			t.Fatalf("%q parsed into text that is not UTF-8", physical)
		}
	})
}
