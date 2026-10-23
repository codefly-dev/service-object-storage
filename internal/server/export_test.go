package server

import "time"

// SetBoundOperationTimeout shortens the bound operation deadline for a test
// and returns the restore.
func SetBoundOperationTimeout(d time.Duration) func() {
	saved := boundOperationTimeout
	boundOperationTimeout = d
	return func() { boundOperationTimeout = saved }
}

// SetBoundBatchTimeout shortens the GetMany call deadline for a test and
// returns the restore.
func SetBoundBatchTimeout(d time.Duration) func() {
	saved := boundBatchTimeout
	boundBatchTimeout = d
	return func() { boundBatchTimeout = saved }
}
