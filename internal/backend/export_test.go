package backend

// WithPrefix exposes the prefix wrapper to the external test package, so a
// test can put two prefixes over ONE underlying store and observe isolation.
func WithPrefix(inner Backend, prefix string) Backend {
	return &prefixed{inner: inner, prefix: prefix}
}
