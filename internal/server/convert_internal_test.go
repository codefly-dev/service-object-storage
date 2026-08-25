package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	storagev0 "github.com/codefly-dev/service-object-storage/gen/codefly/storage/v0"
	"github.com/codefly-dev/service-object-storage/internal/events"
)

// TestWriteOpToProto pins the mapping and, critically, that an op this binary
// does not recognize (e.g. one added by a newer replica on the shared tier)
// maps to UNSPECIFIED rather than being silently mislabeled as a PUT.
func TestWriteOpToProto(t *testing.T) {
	require.Equal(t, storagev0.WriteOp_WRITE_OP_PUT, writeOpToProto(events.OpPut))
	require.Equal(t, storagev0.WriteOp_WRITE_OP_DELETE, writeOpToProto(events.OpDelete))
	require.Equal(t, storagev0.WriteOp_WRITE_OP_UNSPECIFIED, writeOpToProto(events.Op(99)))
}
