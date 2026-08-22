package serr_test

import (
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
