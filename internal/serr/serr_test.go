package serr_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/serr"
)

func TestCodeOfAndIs(t *testing.T) {
	err := serr.New(serr.NotFound, "stat", "missing")
	require.Equal(t, serr.NotFound, serr.CodeOf(err))
	require.True(t, serr.Is(err, serr.NotFound))
	require.False(t, serr.Is(err, serr.Internal))
}

func TestWrapUnwrap(t *testing.T) {
	cause := errors.New("boom")
	err := serr.Wrap(serr.Throttled, "put", cause)
	require.Equal(t, serr.Throttled, serr.CodeOf(err))
	require.ErrorIs(t, err, cause)
	require.Contains(t, err.Error(), "boom")
}

func TestCodeOfPlainError(t *testing.T) {
	require.Equal(t, serr.Internal, serr.CodeOf(fmt.Errorf("plain")))
}

func TestCodeString(t *testing.T) {
	require.Equal(t, "PreconditionFailed", serr.PreconditionFailed.String())
	require.Equal(t, "Internal", serr.Internal.String())
}

// TestUnreachableClassifiesNoAnswer pins what a probe failure normalizes to
// when the service never answered. A context deadline satisfies net.Error only
// incidentally and context.Canceled does not satisfy it at all, so a cancelled
// probe used to fall through to Internal — telling an operator the store
// misbehaved when the gateway had simply stopped waiting.
func TestUnreachableClassifiesNoAnswer(t *testing.T) {
	require.True(t, serr.Unreachable(context.DeadlineExceeded))
	require.True(t, serr.Unreachable(context.Canceled))
	require.True(t, serr.Unreachable(fmt.Errorf("probe: %w", context.Canceled)))

	// A refusal the service produced is an answer, and must stay one.
	require.False(t, serr.Unreachable(errors.New("AccessDenied")))
	require.False(t, serr.Unreachable(serr.New(serr.PermissionDenied, "probe", "denied")))
}
