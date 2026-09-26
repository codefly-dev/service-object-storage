package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublishPrefixScoped(t *testing.T) {
	h := NewHub()
	defer h.Close()

	all := h.Subscribe("")
	scoped := h.Subscribe("tenant=a/")
	defer all.Close()
	defer scoped.Close()

	h.Publish(Event{Key: "tenant=a/x", Op: OpPut, ETag: `"1"`})
	h.Publish(Event{Key: "tenant=b/y", Op: OpDelete})

	// The unscoped subscriber sees both writes in order.
	e := <-all.Events()
	require.Equal(t, "tenant=a/x", e.Key)
	require.Equal(t, OpPut, e.Op)
	require.Equal(t, `"1"`, e.ETag)
	e = <-all.Events()
	require.Equal(t, "tenant=b/y", e.Key)
	require.Equal(t, OpDelete, e.Op)

	// The prefix-scoped subscriber sees only its tenant.
	e = <-scoped.Events()
	require.Equal(t, "tenant=a/x", e.Key)
	select {
	case leaked := <-scoped.Events():
		t.Fatalf("scoped subscriber leaked out-of-prefix event: %q", leaked.Key)
	default:
	}
}

func TestPublishStampsTime(t *testing.T) {
	h := NewHub()
	defer h.Close()
	fixed := time.UnixMilli(1_700_000_000_000)
	h.nowFn = func() time.Time { return fixed }

	sub := h.Subscribe("")
	defer sub.Close()

	h.Publish(Event{Key: "k", Op: OpPut})
	require.Equal(t, fixed, (<-sub.Events()).Time)
}

func TestSlowSubscriberDropped(t *testing.T) {
	h := NewHub()
	defer h.Close()

	sub := h.Subscribe("")
	defer sub.Close()

	// Overflow the buffer without draining: the subscriber is dropped and its
	// channel closed so the RPC ends with a lag signal.
	for i := 0; i < subBuffer+5; i++ {
		h.Publish(Event{Key: "k", Op: OpPut})
	}

	got := 0
	for range sub.Events() {
		got++
	}
	require.Equal(t, subBuffer, got, "buffered events drain, then the channel closes")
}

func TestCloseDropsSubscribers(t *testing.T) {
	h := NewHub()
	sub := h.Subscribe("")

	h.Close()
	_, ok := <-sub.Events()
	require.False(t, ok, "Close closes subscriber channels")

	sub.Close() // must be safe after the Hub already closed it
	h.Close()   // idempotent
}
