// Package feed republishes the backing store's own write notifications onto the
// gateway's Watch hub. The hub otherwise carries only the writes this replica
// served, so a consumer that needs every write to the bucket — a cache
// invalidating on an out-of-band write, an index reconciling after a presigned
// PUT — has nothing to subscribe to. The store observes the bucket rather than
// one gateway process, so consuming its notifications is what makes a Watch
// stream cross-replica, and the consumer keeps one gRPC API and links no cloud
// SDK.
//
// The feed is an ADDITIONAL publisher, not a replacement: a write this replica
// serves is published when it completes and again when the store reports it.
// Watch is at-least-once by contract, and a duplicate costs a consumer an
// idempotent repeat, where suppressing the local publication would cost it
// every event for as long as the store's stream is down.
package feed

import (
	"context"
	"log"
	"time"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/events"
)

// retryMin and retryMax bound the wait before re-subscribing after the store's
// stream ends. The store's own client retries a transient read internally, so
// reaching here means a failure it could not absorb — an endpoint that refused
// the connection, or credentials the store rejected — and those are worth
// backing off from rather than reconnecting in a tight loop.
const (
	retryMin = time.Second
	retryMax = 30 * time.Second
)

// Run consumes src and publishes every change onto hub until ctx is done,
// re-subscribing with backoff whenever the store's stream ends.
//
// Writes made while no stream was up are lost and are not reported as lost:
// Watch is a change hint a consumer reconciles against authoritative state, and
// a gap signal would have to come from the store, which offers none.
func Run(ctx context.Context, src backend.ChangeFeeder, hub *events.Hub) {
	backoff := retryMin
	for ctx.Err() == nil {
		if drain(ctx, src.Changes(ctx, ""), hub) {
			backoff = retryMin
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("store change feed: stream ended, re-subscribing in %s", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, retryMax)
	}
}

// drain publishes every change on in until it closes, reporting whether any
// change arrived: a stream that delivered before it failed is healthy enough to
// re-subscribe to promptly.
func drain(ctx context.Context, in <-chan backend.ChangeEvent, hub *events.Hub) bool {
	delivered := false
	for {
		select {
		case <-ctx.Done():
			return delivered
		case ev, ok := <-in:
			if !ok {
				return delivered
			}
			if ev.Err != nil {
				log.Printf("store change feed: %v", ev.Err)
				continue
			}
			hub.Publish(toEvent(ev.Change))
			delivered = true
		}
	}
}

func toEvent(c backend.Change) events.Event {
	op := events.OpPut
	if c.Op == backend.ChangeDelete {
		op = events.OpDelete
	}
	return events.Event{Key: c.Key, Op: op, ETag: c.ETag, VersionID: c.VersionID, Time: c.Time}
}
