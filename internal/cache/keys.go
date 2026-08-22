package cache

import "fmt"

// metaKey identifies an object's metadata pointer. A version-pinned key
// ("live" otherwise) means a specific immutable version; the "live" pointer is
// the only thing invalidated on write.
func metaKey(ns, backend, key, versionID string) string {
	v := versionID
	if v == "" {
		v = "live"
	}
	return fmt.Sprintf("%s:meta:%s:%s:%s", ns, backend, key, v)
}

// bytesKey identifies object bytes by their validator (ETag). This is the
// immutable-block trick: bytes keyed by validator never need invalidation — a
// new write produces a new validator and thus a new key; the old entry ages
// out. Never key bytes on the object key alone.
func bytesKey(ns, backend, etag string) string {
	return fmt.Sprintf("%s:obj:%s:%s", ns, backend, etag)
}
