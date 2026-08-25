// Package events is the gateway's client-facing write-event fan-out. Every
// mutating RPC that completes through the gateway publishes a WriteEvent to the
// Hub, which delivers it to matching Watch subscribers — the journal seed a
// consumer reconciles its own index against.
//
// The Hub mirrors the cache's cross-replica design: with a shared Redis tier it
// republishes each event on a pub/sub channel and re-dispatches events from
// other replicas, so a Watch client connected to one replica observes writes
// served by any replica bound to the same backend location. Events are scoped
// by backend name+identity (a replica bound elsewhere sharing the Redis
// keyspace must not leak its writes here) and tagged with a per-process origin
// so a replica never re-dispatches its own echo.
//
// A slow Watch subscriber is dropped rather than allowed to stall the fan-out:
// its channel is closed once its buffer overflows, ending the RPC with a lag
// signal so the client reconciles out of band before re-watching.
package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
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

// eventChannel carries client-facing write events across replicas. It is kept
// separate from the cache's internal "sos:invalidate" channel so eviction and
// observation stay independent concerns.
const eventChannel = "sos:events"

// subBuffer bounds how many undelivered events a subscriber may queue before it
// is dropped as too slow.
const subBuffer = 256

// Hub fans write events out to local Watch subscribers and, when a shared tier
// is configured, across replicas.
type Hub struct {
	name     string
	identity string
	origin   string
	rdb      redis.UniversalClient // nil = single-replica, in-process only
	nowFn    func() time.Time

	mu     sync.Mutex
	subs   map[uint64]*Subscription
	nextID uint64
	stop   chan struct{}
	closed bool
}

// NewHub builds a Hub scoped to one backend location (name+identity). rdb may be
// nil for a single-replica gateway; when set, the Hub bridges events across
// every replica sharing that Redis tier and backend location.
func NewHub(name, identity string, rdb redis.UniversalClient) *Hub {
	h := &Hub{
		name:     name,
		identity: identity,
		origin:   newOrigin(),
		rdb:      rdb,
		nowFn:    time.Now,
		subs:     make(map[uint64]*Subscription),
		stop:     make(chan struct{}),
	}
	if rdb != nil {
		go h.subscribeRemote()
	}
	return h
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

// Publish delivers e to matching local subscribers and, when a shared tier is
// configured, mirrors it to other replicas. Time is stamped here if unset.
func (h *Hub) Publish(e Event) {
	if e.Time.IsZero() {
		e.Time = h.nowFn()
	}
	h.dispatch(e)
	if h.rdb == nil {
		return
	}
	w := wireEvent{
		Origin:    h.origin,
		Name:      h.name,
		Identity:  h.identity,
		Key:       e.Key,
		Op:        e.Op,
		ETag:      e.ETag,
		VersionID: e.VersionID,
		TimeMS:    e.Time.UnixMilli(),
	}
	if raw, err := json.Marshal(w); err == nil {
		_ = h.rdb.Publish(context.Background(), eventChannel, raw).Err()
	}
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

// Close stops the remote subscriber and drops every subscription.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	close(h.stop)
	for id, s := range h.subs {
		delete(h.subs, id)
		close(s.ch)
	}
}

// wireEvent is the cross-replica encoding of an Event, tagged with the emitting
// replica's origin and backend location for receiver-side filtering.
type wireEvent struct {
	Origin    string `json:"o"`
	Name      string `json:"n"`
	Identity  string `json:"i"`
	Key       string `json:"k"`
	Op        Op     `json:"op"`
	ETag      string `json:"e,omitempty"`
	VersionID string `json:"v,omitempty"`
	TimeMS    int64  `json:"t"`
}

func (h *Hub) subscribeRemote() {
	sub := h.rdb.Subscribe(context.Background(), eventChannel)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-h.stop:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			h.handleRemote(msg.Payload)
		}
	}
}

// handleRemote re-dispatches an event from another replica, ignoring this
// replica's own echo and any event bound to a different backend location.
func (h *Hub) handleRemote(payload string) {
	var w wireEvent
	if json.Unmarshal([]byte(payload), &w) != nil {
		return
	}
	if w.Origin == h.origin || w.Name != h.name || w.Identity != h.identity {
		return
	}
	h.dispatch(Event{
		Key:       w.Key,
		Op:        w.Op,
		ETag:      w.ETag,
		VersionID: w.VersionID,
		Time:      time.UnixMilli(w.TimeMS),
	})
}

func newOrigin() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
