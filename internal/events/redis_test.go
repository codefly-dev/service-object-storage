package events_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/events"
)

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
