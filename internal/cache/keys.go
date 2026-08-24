package cache

import "fmt"

// metaKey identifies an object's metadata pointer. A version-pinned key
// ("live" otherwise) means a specific immutable version; the "live" pointer is
// the only thing invalidated on write. The bucket is part of the key so two
// gateways bound to different buckets never collide on a shared Redis tier.
func metaKey(ns, backend, bucket, key, versionID string) string {
	v := versionID
	if v == "" {
		v = "live"
	}
	return fmt.Sprintf("%s:meta:%s:%s:%s:%s", ns, backend, bucket, key, v)
}

// bytesKey identifies object bytes by their validator (ETag). This is the
// immutable-block trick: bytes keyed by validator never need invalidation — a
// new write produces a new validator and thus a new key; the old entry ages
// out. Never key bytes on the object key alone. The bucket is part of the key
// because ETags are opaque (not content hashes), so identical ETags in
// different buckets sharing a Redis tier would otherwise cross-serve bytes.
func bytesKey(ns, backend, bucket, etag string) string {
	return fmt.Sprintf("%s:obj:%s:%s:%s", ns, backend, bucket, etag)
}
