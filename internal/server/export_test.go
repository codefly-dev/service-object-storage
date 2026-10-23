package server

import "time"

// SetBoundOperationTimeout shortens the bound operation deadline for a test
// and returns the restore.
func SetBoundOperationTimeout(d time.Duration) func() {
	saved := boundOperationTimeout
	boundOperationTimeout = d
	return func() { boundOperationTimeout = saved }
}
