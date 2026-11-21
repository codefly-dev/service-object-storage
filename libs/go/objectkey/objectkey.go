// Package objectkey is the key convention of a confined object store: how a
// partition, a scope, a principal and a logical name become one object key,
// and how a key is read back.
//
//	<base>/<partition>/<scope>/<slot>/<name…>
//
//	name = <segment>                     a name of one segment
//	     | <segment>_/<segment>_/…/<segment>   every segment but the last is marked
//
//	slot = <kind>.<id>     one principal's objects
//	     | _shared         the scope's own objects, no principal
//	     | _sha256         content named by its digest, no principal
//
// Keys are persistent, so this encoding is a contract: it never changes. The
// golden vectors in the tests are the pin. A different convention is a
// different base.
//
// Every segment after the base is written by one function, Segment, over the
// alphabet A-Z a-z 0-9 - . and the escape "_hh" (two lowercase hex digits per
// byte). It is injective because Unsegment inverts it, and Parse accepts a key
// only when re-encoding what it decoded gives the same bytes back, so a key
// has exactly one reading. No identifier can produce a "/", an empty segment,
// "." or "..", or a reserved segment: "_" is always escaped, and a reserved
// segment is "_" followed by a letter that is not a hex digit.
//
// The alphabet is also what every signature and policy language leaves alone:
// no character of a key is percent-encoded in a signed URL, and none is a
// wildcard or a variable in an access policy.
package objectkey

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxKeyBytes bounds a whole key. Stores accept 1024 bytes; the rest is
	// left to the prefix the gateway itself puts in front.
	MaxKeyBytes = 900
	// MaxSegmentBytes bounds one encoded segment, under the 254 and 255 byte
	// segment limits of the stores that have one.
	MaxSegmentBytes = 240
	// MaxBaseBytes bounds a base.
	MaxBaseBytes = 63

	sharedSegment  = "_shared"
	contentSegment = "_sha256"
	stagingSegment = "_staging"

	maxKindBytes = 32
	digestBytes  = 64
)

// ErrInvalid is every refusal of this package: an identifier, a name, a base
// or a key that the convention cannot carry, or that it did not write.
var ErrInvalid = errors.New("invalid object key")

func invalid(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, arguments...))
}

// Slot is the fourth segment of a key: whose objects these are.
type Slot struct {
	kind      slotKind
	principal string
}

type slotKind uint8

const (
	slotPrincipal slotKind = iota + 1
	slotShared
	slotContent
)

// Principal is the slot of one principal, named "kind:id".
func Principal(reference string) Slot { return Slot{kind: slotPrincipal, principal: reference} }

// Shared is the slot of a scope's own objects.
func Shared() Slot { return Slot{kind: slotShared} }

// Content is the slot of objects named by the SHA-256 of their bytes.
func Content() Slot { return Slot{kind: slotContent} }

// IsShared reports the shared slot.
func (slot Slot) IsShared() bool { return slot.kind == slotShared }

// IsContent reports the content slot.
func (slot Slot) IsContent() bool { return slot.kind == slotContent }

// Reference is the principal of a principal slot, as "kind:id", and empty for
// the two slots that have none.
func (slot Slot) Reference() string {
	if slot.kind != slotPrincipal {
		return ""
	}
	return slot.principal
}

func (slot Slot) String() string {
	switch slot.kind {
	case slotPrincipal:
		return slot.principal
	case slotShared:
		return sharedSegment
	case slotContent:
		return contentSegment
	default:
		return "(no slot)"
	}
}

// Key is one object's logical address inside a base.
type Key struct {
	Partition string
	Scope     string
	Slot      Slot
	// Name is the logical name: one or more segments joined by "/". Under the
	// content slot it is 64 lowercase hex digits.
	Name string
	// Staging marks an upload in flight under a principal's slot. Name is then
	// one segment of lowercase letters and digits.
	Staging bool
}

// Codec writes and reads the keys of one base.
type Codec struct {
	base string
}

// New returns the codec of a base: one segment of lowercase letters, digits
// and hyphens, beginning with a letter or a digit. One segment, so that no
// base is ever a prefix of another.
func New(base string) (Codec, error) {
	if base == "" || len(base) > MaxBaseBytes {
		return Codec{}, invalid("a base is 1 to %d bytes, got %d", MaxBaseBytes, len(base))
	}
	for index := 0; index < len(base); index++ {
		character := base[index]
		lower := character >= 'a' && character <= 'z'
		digit := character >= '0' && character <= '9'
		if !lower && !digit && (character != '-' || index == 0) {
			return Codec{}, invalid("base %q: want lowercase letters, digits and hyphens, beginning with a letter or digit", base)
		}
	}
	return Codec{base: base}, nil
}

// Base is the base this codec writes under.
func (codec Codec) Base() string { return codec.base }

// Segment encodes one identifier or one name segment.
func Segment(value string) (string, error) {
	if err := validate(value); err != nil {
		return "", err
	}
	encoded := encode(value)
	if len(encoded) > MaxSegmentBytes {
		return "", invalid("a segment of %d encoded bytes is over the limit of %d", len(encoded), MaxSegmentBytes)
	}
	return encoded, nil
}

// Unsegment decodes a segment Segment wrote, and refuses any other.
func Unsegment(segment string) (string, error) {
	if segment == "" {
		return "", invalid("an empty segment")
	}
	if len(segment) > MaxSegmentBytes {
		return "", invalid("a segment of %d bytes is over the limit of %d", len(segment), MaxSegmentBytes)
	}
	decoded := make([]byte, 0, len(segment))
	for index := 0; index < len(segment); index++ {
		character := segment[index]
		switch {
		case character == '_':
			if index+2 >= len(segment) {
				return "", invalid("segment %q ends inside an escape", segment)
			}
			high, low := unhex(segment[index+1]), unhex(segment[index+2])
			if high < 0 || low < 0 {
				return "", invalid("segment %q carries an escape that is not two lowercase hex digits", segment)
			}
			decoded = append(decoded, byte(high<<4|low))
			index += 2
		case plain(character) || character == '.':
			decoded = append(decoded, character)
		default:
			return "", invalid("segment %q carries a byte outside the alphabet", segment)
		}
	}
	value := string(decoded)
	if err := validate(value); err != nil {
		return "", err
	}
	// One reading per key: what was decoded must encode to exactly this.
	if encode(value) != segment {
		return "", invalid("segment %q is not in canonical form", segment)
	}
	return value, nil
}

// validate is what an identifier or a name segment must be before it is
// encoded. The encoding would carry anything; these are refused because they
// are never meant.
func validate(value string) error {
	switch {
	case value == "":
		return invalid("an empty identifier or name segment")
	case value == "." || value == "..":
		return invalid("%q is not an identifier or a name segment", value)
	case !utf8.ValidString(value):
		return invalid("an identifier or name segment that is not UTF-8")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return invalid("an identifier or name segment with a control character")
		}
	}
	return nil
}

func plain(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' || character == '-'
}

const hexDigits = "0123456789abcdef"

func unhex(character byte) int {
	switch {
	case character >= '0' && character <= '9':
		return int(character - '0')
	case character >= 'a' && character <= 'f':
		return int(character-'a') + 10
	default:
		return -1
	}
}

// encode is the whole encoding. A byte of the plain alphabet is itself; a "."
// is itself except first or last in the segment; every other byte is "_hh".
func encode(value string) string {
	var out strings.Builder
	out.Grow(len(value))
	for index := 0; index < len(value); index++ {
		character := value[index]
		if plain(character) || character == '.' && index > 0 && index < len(value)-1 {
			out.WriteByte(character)
			continue
		}
		out.WriteByte('_')
		out.WriteByte(hexDigits[character>>4])
		out.WriteByte(hexDigits[character&0x0f])
	}
	return out.String()
}

// SplitPrincipal reads a "kind:id" reference: a kind of 1 to 32 lowercase
// letters, and an id.
func SplitPrincipal(reference string) (kind, id string, err error) {
	kind, id, found := strings.Cut(reference, ":")
	if !found || kind == "" || len(kind) > maxKindBytes {
		return "", "", invalid("principal %q is not a kind:id reference", reference)
	}
	for index := 0; index < len(kind); index++ {
		if kind[index] < 'a' || kind[index] > 'z' {
			return "", "", invalid("principal %q: a kind is lowercase letters", reference)
		}
	}
	if err := validate(id); err != nil {
		return "", "", err
	}
	return kind, id, nil
}

func (slot Slot) segment() (string, error) {
	switch slot.kind {
	case slotShared:
		return sharedSegment, nil
	case slotContent:
		return contentSegment, nil
	case slotPrincipal:
		kind, id, err := SplitPrincipal(slot.principal)
		if err != nil {
			return "", err
		}
		encoded := kind + "." + encode(id)
		if len(encoded) > MaxSegmentBytes {
			return "", invalid("a principal segment of %d encoded bytes is over the limit of %d", len(encoded), MaxSegmentBytes)
		}
		return encoded, nil
	default:
		return "", invalid("a key without a slot")
	}
}

func parseSlot(segment string) (Slot, error) {
	switch segment {
	case sharedSegment:
		return Shared(), nil
	case contentSegment:
		return Content(), nil
	}
	kind, encoded, found := strings.Cut(segment, ".")
	if !found {
		return Slot{}, invalid("slot %q is neither reserved nor kind.id", segment)
	}
	id, err := Unsegment(encoded)
	if err != nil {
		return Slot{}, err
	}
	slot := Principal(kind + ":" + id)
	if canonical, err := slot.segment(); err != nil || canonical != segment {
		return Slot{}, invalid("slot %q is not in canonical form", segment)
	}
	return slot, nil
}

func isDigest(name string) bool {
	if len(name) != digestBytes {
		return false
	}
	for index := 0; index < len(name); index++ {
		if unhex(name[index]) < 0 {
			return false
		}
	}
	return true
}

func isStagingID(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for index := 0; index < len(name); index++ {
		if !(name[index] >= 'a' && name[index] <= 'z') && !(name[index] >= '0' && name[index] <= '9') {
			return false
		}
	}
	return true
}

// directoryMark ends every name segment but the last. A name is then never
// both an object and the directory of another: "a" is the key ".../a" and
// "a/b" is ".../a_/b". Stores disagree about a key that is also a prefix (one
// serves both and lists only the first, another refuses the second), and a
// listing that hides objects is an erasure that misses them. The mark cannot
// be confused with an escape, which is "_" and two hex digits.
const directoryMark = "_"

// encodeName encodes a logical name segment by segment. With directory set,
// the name is a directory and its last segment is marked too.
func encodeName(name string, directory bool) (string, error) {
	if name == "" {
		return "", invalid("an empty name")
	}
	segments := strings.Split(name, "/")
	for index, segment := range segments {
		encoded, err := Segment(segment)
		if err != nil {
			return "", err
		}
		if directory || index < len(segments)-1 {
			encoded += directoryMark
		}
		segments[index] = encoded
	}
	return strings.Join(segments, "/"), nil
}

func (codec Codec) locate(partition, scope string) (string, error) {
	if codec.base == "" {
		return "", invalid("a codec without a base")
	}
	encodedPartition, err := Segment(partition)
	if err != nil {
		return "", fmt.Errorf("partition: %w", err)
	}
	encodedScope, err := Segment(scope)
	if err != nil {
		return "", fmt.Errorf("scope: %w", err)
	}
	return codec.base + "/" + encodedPartition + "/" + encodedScope + "/", nil
}

// Key writes the object key of a logical address.
func (codec Codec) Key(key Key) (string, error) {
	prefix, err := codec.SlotPrefix(key.Partition, key.Scope, key.Slot)
	if err != nil {
		return "", err
	}
	var name string
	switch {
	case key.Staging:
		if key.Slot.kind != slotPrincipal || !isStagingID(key.Name) {
			return "", invalid("a staging key is a principal's, named by lowercase letters and digits")
		}
		name = stagingSegment + "/" + key.Name
	case key.Slot.kind == slotContent:
		if !isDigest(key.Name) {
			return "", invalid("a content name is %d lowercase hex digits", digestBytes)
		}
		name = key.Name
	default:
		if name, err = encodeName(key.Name, false); err != nil {
			return "", err
		}
	}
	physical := prefix + name
	if len(physical) > MaxKeyBytes {
		return "", invalid("a key of %d bytes is over the limit of %d", len(physical), MaxKeyBytes)
	}
	return physical, nil
}

// BasePrefix is the prefix of everything in the base. Like every prefix this
// package returns, it ends with "/": a prefix that does not would also match
// its longer siblings.
func (codec Codec) BasePrefix() string { return codec.base + "/" }

// PartitionPrefix is the prefix of everything in a partition.
func (codec Codec) PartitionPrefix(partition string) (string, error) {
	if codec.base == "" {
		return "", invalid("a codec without a base")
	}
	encoded, err := Segment(partition)
	if err != nil {
		return "", fmt.Errorf("partition: %w", err)
	}
	return codec.base + "/" + encoded + "/", nil
}

// ScopePrefix is the prefix of everything in a scope.
func (codec Codec) ScopePrefix(partition, scope string) (string, error) {
	return codec.locate(partition, scope)
}

// SlotPrefix is the prefix of everything in a slot.
func (codec Codec) SlotPrefix(partition, scope string, slot Slot) (string, error) {
	prefix, err := codec.locate(partition, scope)
	if err != nil {
		return "", err
	}
	segment, err := slot.segment()
	if err != nil {
		return "", err
	}
	return prefix + segment + "/", nil
}

// DirectoryPrefix is the prefix of the names under a directory of a slot: dir
// is whole name segments, and empty means the slot itself.
func (codec Codec) DirectoryPrefix(partition, scope string, slot Slot, directory string) (string, error) {
	prefix, err := codec.SlotPrefix(partition, scope, slot)
	if err != nil {
		return "", err
	}
	if directory == "" {
		return prefix, nil
	}
	if slot.kind == slotContent {
		return "", invalid("the content slot has no directories")
	}
	encoded, err := encodeName(directory, true)
	if err != nil {
		return "", err
	}
	prefix += encoded + "/"
	if len(prefix) > MaxKeyBytes {
		return "", invalid("a prefix of %d bytes is over the limit of %d", len(prefix), MaxKeyBytes)
	}
	return prefix, nil
}

// StagingPrefix is the prefix of a principal's uploads in flight.
func (codec Codec) StagingPrefix(partition, scope string, slot Slot) (string, error) {
	if slot.kind != slotPrincipal {
		return "", invalid("only a principal has staging keys")
	}
	prefix, err := codec.SlotPrefix(partition, scope, slot)
	if err != nil {
		return "", err
	}
	return prefix + stagingSegment + "/", nil
}

// Parse reads back a key this codec wrote. Any other key is ErrInvalid: one
// of another base, one this convention could not have produced, and one that
// is not in canonical form.
func (codec Codec) Parse(physical string) (Key, error) {
	if codec.base == "" {
		return Key{}, invalid("a codec without a base")
	}
	if len(physical) > MaxKeyBytes {
		return Key{}, invalid("a key of %d bytes is over the limit of %d", len(physical), MaxKeyBytes)
	}
	rest, found := strings.CutPrefix(physical, codec.base+"/")
	if !found {
		return Key{}, invalid("key %q is not under base %q", physical, codec.base)
	}
	segments := strings.Split(rest, "/")
	if len(segments) < 4 {
		return Key{}, invalid("key %q has no partition, scope, slot and name", physical)
	}
	var key Key
	var err error
	if key.Partition, err = Unsegment(segments[0]); err != nil {
		return Key{}, fmt.Errorf("partition: %w", err)
	}
	if key.Scope, err = Unsegment(segments[1]); err != nil {
		return Key{}, fmt.Errorf("scope: %w", err)
	}
	if key.Slot, err = parseSlot(segments[2]); err != nil {
		return Key{}, err
	}
	names := segments[3:]
	switch {
	case key.Slot.kind == slotContent:
		if len(names) != 1 || !isDigest(names[0]) {
			return Key{}, invalid("key %q: a content name is %d lowercase hex digits", physical, digestBytes)
		}
		key.Name = names[0]
	case names[0] == stagingSegment:
		if key.Slot.kind != slotPrincipal || len(names) != 2 || !isStagingID(names[1]) {
			return Key{}, invalid("key %q is not a staging key", physical)
		}
		key.Staging, key.Name = true, names[1]
	default:
		for index, segment := range names {
			if index < len(names)-1 {
				marked, found := strings.CutSuffix(segment, directoryMark)
				if !found {
					return Key{}, invalid("key %q: a directory segment without its mark", physical)
				}
				segment = marked
			}
			if names[index], err = Unsegment(segment); err != nil {
				return Key{}, err
			}
			// A name is its segments joined by "/", so a segment that holds
			// one would read back as two.
			if strings.Contains(names[index], "/") {
				return Key{}, invalid("key %q: a name segment holds an encoded separator", physical)
			}
		}
		key.Name = strings.Join(names, "/")
	}
	return key, nil
}

// ParsePrefix reads the identifiers of a prefix one of the Prefix methods
// returned, or that a delimited listing reported: depth 1 is a partition, 2 a
// scope, 3 a slot.
func (codec Codec) ParsePrefix(prefix string) (partition, scope string, slot Slot, depth int, err error) {
	rest, found := strings.CutPrefix(prefix, codec.base+"/")
	if !found || codec.base == "" || !strings.HasSuffix(rest, "/") {
		return "", "", Slot{}, 0, invalid("prefix %q is not a prefix under base %q", prefix, codec.base)
	}
	segments := strings.Split(strings.TrimSuffix(rest, "/"), "/")
	if len(segments) > 3 {
		return "", "", Slot{}, 0, invalid("prefix %q is deeper than a slot", prefix)
	}
	if partition, err = Unsegment(segments[0]); err != nil {
		return "", "", Slot{}, 0, fmt.Errorf("partition: %w", err)
	}
	if len(segments) > 1 {
		if scope, err = Unsegment(segments[1]); err != nil {
			return "", "", Slot{}, 0, fmt.Errorf("scope: %w", err)
		}
	}
	if len(segments) > 2 {
		if slot, err = parseSlot(segments[2]); err != nil {
			return "", "", Slot{}, 0, err
		}
	}
	return partition, scope, slot, len(segments), nil
}
