// Package events is the gateway's client-facing write-event fan-out. Every
// mutating RPC that completes through the gateway publishes a WriteEvent to the
// Hub, which delivers it to matching Watch subscribers — the journal seed a
// consumer reconciles its own index against.
//
// Delivery is per replica: the Hub is in-process, so a Watch subscriber sees
// the writes served by the replica it is connected to, and only those. A
// consumer that needs every write across replicas uses the backing store's own
// event notifications (S3/MinIO bucket notifications, GCS Pub/Sub, Azure Event
// Grid), which observe the bucket rather than one gateway process. Delivery is
// a change hint, not a log: consumers reconcile out of band (see the
// WriteEvent proto contract).
//
// A slow Watch subscriber is dropped rather than allowed to stall the fan-out:
// its channel is closed once its buffer overflows, ending the RPC with a lag
// signal so the client reconciles out of band before re-watching.
package events

import (
	"strings"
	"sync"
	"time"
)

// Op is the mutating operation a write event reports.
type Op int8

const (
	OpPut Op = iota
	OpDelete
)

// Event is a single mutation observed at the gateway. ETag/VersionID are set on
// a put when the backend returns them; a delete carries only the key (and
// VersionID when a specific version was removed).
type Event struct {
	Key       string
	Op        Op
	ETag      string
	VersionID string
	Time      time.Time
}

// subBuffer bounds how many undelivered events a subscriber may queue before it
// is dropped as too slow.
const subBuffer = 256

// Hub fans write events out to the Watch subscribers of this replica.
type Hub struct {
	nowFn func() time.Time

	mu     sync.Mutex
	subs   map[uint64]*Subscription
	nextID uint64
	closed bool
}

// NewHub builds an empty Hub.
func NewHub() *Hub {
	return &Hub{
		nowFn: time.Now,
		subs:  make(map[uint64]*Subscription),
	}
}

// Subscription is a prefix-scoped view of the event stream. The server ranges
// over Events until it closes (the RPC ends) or the subscriber is dropped.
type Subscription struct {
	hub    *Hub
	id     uint64
	prefix string
	ch     chan Event
}

// Events yields matching events. A closed channel means the subscriber was
// dropped for falling behind.
func (s *Subscription) Events() <-chan Event { return s.ch }

// Close removes the subscription. It is safe to call after the Hub has already
// dropped or closed it.
func (s *Subscription) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	if _, ok := s.hub.subs[s.id]; ok {
		delete(s.hub.subs, s.id)
		close(s.ch)
	}
}

// Subscribe registers a subscriber for keys under prefix (empty = all keys).
func (h *Hub) Subscribe(prefix string) *Subscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.nextID
	h.nextID++
	s := &Subscription{hub: h, id: id, prefix: prefix, ch: make(chan Event, subBuffer)}
	h.subs[id] = s
	return s
}

// Publish delivers e to matching subscribers, synchronously and in order. Time
// is stamped here if unset.
func (h *Hub) Publish(e Event) {
	if e.Time.IsZero() {
		e.Time = h.nowFn()
	}
	h.dispatch(e)
}

// dispatch delivers e to every matching subscriber, dropping any whose buffer is
// full. Sends are non-blocking, so holding the lock cannot deadlock.
func (h *Hub) dispatch(e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, s := range h.subs {
		if !strings.HasPrefix(e.Key, s.prefix) {
			continue
		}
		select {
		case s.ch <- e:
		default:
			delete(h.subs, id)
			close(s.ch)
		}
	}
}

// Close drops every subscription.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, s := range h.subs {
		delete(h.subs, id)
		close(s.ch)
	}
}
