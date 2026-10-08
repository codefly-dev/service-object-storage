package feed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
)

// scriptedFeeder plays one channel per subscription, so a test can script what
// a re-subscription finds. A subscription past the last script entry blocks
// until ctx ends, which is what a healthy idle stream does.
type scriptedFeeder struct {
	streams [][]backend.ChangeEvent
	calls   chan string
}

func (f *scriptedFeeder) Changes(ctx context.Context, prefix string) <-chan backend.ChangeEvent {
	out := make(chan backend.ChangeEvent)
	var script []backend.ChangeEvent
	if len(f.streams) > 0 {
		script, f.streams = f.streams[0], f.streams[1:]
	} else {
		script = nil
	}
	f.calls <- prefix
	go func() {
		defer close(out)
		for _, ev := range script {
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
		if script == nil {
			<-ctx.Done()
		}
	}()
	return out
}

func put(key, etag string) backend.ChangeEvent {
	return backend.ChangeEvent{Change: backend.Change{Key: key, Op: backend.ChangePut, ETag: etag}}
}

// receive waits for one event, failing rather than hanging if the feed never
// publishes it.
func receive(t *testing.T, sub *events.Subscription) events.Event {
	t.Helper()
	select {
	case ev, ok := <-sub.Events():
		require.True(t, ok, "subscription was dropped")
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no event published")
		return events.Event{}
	}
}

func TestRunRepublishesStoreChangesOntoTheHub(t *testing.T) {
	hub := events.NewHub()
	defer hub.Close()
	sub := hub.Subscribe("")
	defer sub.Close()

	src := &scriptedFeeder{calls: make(chan string, 4), streams: [][]backend.ChangeEvent{{
		put("docs/one.txt", "etag-1"),
		{Change: backend.Change{Key: "docs/two.txt", Op: backend.ChangeDelete, VersionID: "v2"}},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, src, hub)

	require.Equal(t, "", <-src.calls, "the gateway watches the whole store, not a prefix")

	first := receive(t, sub)
	require.Equal(t, "docs/one.txt", first.Key)
	require.Equal(t, events.OpPut, first.Op)
	require.Equal(t, "etag-1", first.ETag)
	// The store reports no time for this change, so the hub stamps one: an
	// event with a zero time would read as the epoch to every consumer.
	require.False(t, first.Time.IsZero())

	second := receive(t, sub)
	require.Equal(t, "docs/two.txt", second.Key)
	require.Equal(t, events.OpDelete, second.Op)
	require.Equal(t, "v2", second.VersionID)
}

// TestRunPreservesTheStoreTime keeps the store's own timestamp rather than the
// moment the gateway happened to read it: a consumer ordering against its own
// records needs when the write landed, not when this replica noticed.
func TestRunPreservesTheStoreTime(t *testing.T) {
	hub := events.NewHub()
	defer hub.Close()
	sub := hub.Subscribe("")
	defer sub.Close()

	at := time.Date(2026, 9, 26, 13, 55, 58, 0, time.UTC)
	src := &scriptedFeeder{calls: make(chan string, 4), streams: [][]backend.ChangeEvent{{
		{Change: backend.Change{Key: "k", Op: backend.ChangePut, Time: at}},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, src, hub)
	<-src.calls

	require.Equal(t, at, receive(t, sub).Time)
}

// TestRunReportsAnErrorWithoutEndingTheStream: the store's client surfaces a
// transient failure it then retries internally, so an error entry must not cost
// the subscriber the changes that follow it on the same stream.
func TestRunReportsAnErrorWithoutEndingTheStream(t *testing.T) {
	hub := events.NewHub()
	defer hub.Close()
	sub := hub.Subscribe("")
	defer sub.Close()

	src := &scriptedFeeder{calls: make(chan string, 4), streams: [][]backend.ChangeEvent{{
		{Err: errors.New("notification stream hiccup")},
		put("after/the/error.txt", "etag-2"),
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, src, hub)
	<-src.calls

	require.Equal(t, "after/the/error.txt", receive(t, sub).Key)
}

// TestRunResubscribesAfterTheStreamEnds is the property that makes the feed
// usable: a store that drops the stream must not leave Watch silently
// per-replica for the rest of the process's life.
func TestRunResubscribesAfterTheStreamEnds(t *testing.T) {
	hub := events.NewHub()
	defer hub.Close()
	sub := hub.Subscribe("")
	defer sub.Close()

	src := &scriptedFeeder{calls: make(chan string, 4), streams: [][]backend.ChangeEvent{
		{put("before/the/drop.txt", "etag-1")},
		{put("after/the/drop.txt", "etag-2")},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, src, hub)

	<-src.calls
	require.Equal(t, "before/the/drop.txt", receive(t, sub).Key)

	select {
	case <-src.calls:
	case <-time.After(5 * time.Second):
		t.Fatal("the feed never re-subscribed after the stream ended")
	}
	require.Equal(t, "after/the/drop.txt", receive(t, sub).Key)
}

// TestRunStopsWithItsContext: the gateway cancels the feed on shutdown, and a
// Run that outlived it would keep publishing onto a closed hub.
func TestRunStopsWithItsContext(t *testing.T) {
	hub := events.NewHub()
	defer hub.Close()

	src := &scriptedFeeder{calls: make(chan string, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, src, hub); close(done) }()
	<-src.calls

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run outlived its context")
	}
}
