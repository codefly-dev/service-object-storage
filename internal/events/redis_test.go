package events_test

import (
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/events"
)

// TestPublishDoesNotBlockOnRedis proves the cross-replica hop is off the write
// path: with a Redis that accepts connections but never answers, Publish must
// still return immediately and deliver locally. The pre-async code blocked here
// for the go-redis read timeout (seconds) on every write.
func TestPublishDoesNotBlockOnRedis(t *testing.T) {
	// A listening socket that is never Accept()ed: the kernel completes the TCP
	// handshake, so go-redis connects, but every reply read hangs.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	rdb := redis.NewClient(&redis.Options{Addr: ln.Addr().String()})
	t.Cleanup(func() { _ = rdb.Close() })

	h := events.NewHub("mem", "mem://local", rdb)
	t.Cleanup(h.Close)

	sub := h.Subscribe("")
	defer sub.Close()

	done := make(chan struct{})
	go func() {
		h.Publish(events.Event{Key: "k", Op: events.OpPut})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on an unresponsive Redis")
	}

	// Local delivery is synchronous and unaffected by the stalled remote path.
	select {
	case ev := <-sub.Events():
		require.Equal(t, "k", ev.Key)
	case <-time.After(time.Second):
		t.Fatal("local subscriber did not receive the event")
	}
}

// TestCrossReplicaDelivery proves the Redis bridge: an event published on one
// replica reaches a Watch subscriber connected to another replica bound to the
// same backend location.
func TestCrossReplicaDelivery(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	newClient := func() redis.UniversalClient {
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	replicaA := events.NewHub("mem", "mem://local", newClient())
	replicaB := events.NewHub("mem", "mem://local", newClient())
	t.Cleanup(replicaA.Close)
	t.Cleanup(replicaB.Close)

	sub := replicaB.Subscribe("")
	defer sub.Close()

	// The remote subscription comes up asynchronously, so republish until B's
	// watcher observes the first event, then assert on its contents.
	deadline := time.After(2 * time.Second)
	for {
		replicaA.Publish(events.Event{Key: "k", Op: events.OpPut, ETag: `"e"`})
		select {
		case ev := <-sub.Events():
			require.Equal(t, "k", ev.Key)
			require.Equal(t, events.OpPut, ev.Op)
			require.Equal(t, `"e"`, ev.ETag)
			return
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("cross-replica event never delivered")
		}
	}
}
