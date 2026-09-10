// Package health keeps a gRPC health service in step with the backing store, so
// an orchestrator's startup check reflects real access to the configured bucket
// rather than a listening socket.
package health

import (
	"context"
	"log"
	"sync"
	"time"

	grpchealth "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// Verdict is the outcome of the most recent probe. It is what the Ready RPC
// reports, so a caller sees the same view the orchestrator acts on.
type Verdict struct {
	// Ready is whether the last probe reached the store.
	Ready bool
	// Code is the normalized failure class when Ready is false, empty otherwise.
	Code string
	// Detail is the backend error message when Ready is false.
	Detail string
	// CheckedAt is when that probe completed. It is the zero time before the
	// first probe finishes, which callers report as "not yet probed" rather
	// than inventing a timestamp.
	CheckedAt time.Time
}

// Monitor probes a backend on an interval and publishes the outcome as the
// serving status of one named health service, retaining the last verdict so
// callers can read readiness without probing the store themselves.
//
// It never touches the overall ("") service, which stays SERVING for the
// lifetime of the process. That service carries BOTH liveness and readiness,
// deliberately: readiness that follows the backend probe is a global kill
// switch, not per-replica load shedding. Every replica shares one bucket and
// one credential set, so a backend outage, a deleted bucket or a revoked grant
// fails all of them at once and would empty the Service's endpoint list —
// turning a degraded gateway, which can still serve presigned URLs and cached
// reads, into an unreachable one.
//
// The named service is what a startupProbe gates on, so a replica must prove
// access once before it joins rotation and a bad rollout stalls while the
// previous pods keep serving. After that, losing access is reported per request
// as Unavailable rather than by withdrawing the replica.
type Monitor struct {
	be       backend.Backend
	health   *grpchealth.Server
	service  string
	interval time.Duration
	timeout  time.Duration

	mu      sync.RWMutex
	last    healthv1.HealthCheckResponse_ServingStatus
	verdict Verdict
}

// New builds a Monitor publishing the readiness of be as the serving status of
// service. interval and timeout must both be positive; config rejects anything
// else before it reaches here.
func New(be backend.Backend, hs *grpchealth.Server, service string, interval, timeout time.Duration) *Monitor {
	return &Monitor{be: be, health: hs, service: service, interval: interval, timeout: timeout}
}

// Verdict returns the most recent probe outcome.
func (m *Monitor) Verdict() Verdict {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.verdict
}

// Backend names the store being probed.
func (m *Monitor) Backend() string { return m.be.Name() }

// Run probes immediately, then on every tick, until ctx is done.
func (m *Monitor) Run(ctx context.Context) {
	m.check(ctx)

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.check(ctx)
		}
	}
}

func (m *Monitor) check(ctx context.Context) {
	pctx, cancel := context.WithTimeout(ctx, m.timeout)
	err := m.be.Probe(pctx)
	cancel()

	// A probe cut short because the process is shutting down says nothing about
	// the store, so it must not overwrite the verdict on the way out.
	if ctx.Err() != nil {
		return
	}

	status := healthv1.HealthCheckResponse_SERVING
	verdict := Verdict{Ready: true, CheckedAt: time.Now()}
	if err != nil {
		status = healthv1.HealthCheckResponse_NOT_SERVING
		verdict = Verdict{
			Ready:     false,
			Code:      serr.CodeOf(err).String(),
			Detail:    err.Error(),
			CheckedAt: time.Now(),
		}
	}

	m.mu.Lock()
	m.verdict = verdict
	changed := status != m.last
	m.last = status
	m.mu.Unlock()

	if !changed {
		return
	}
	m.health.SetServingStatus(m.service, status)

	if err != nil {
		log.Printf("backend access: NOT_SERVING (%s: %v)", serr.CodeOf(err), err)
		return
	}
	log.Printf("backend access: SERVING (backend %s)", m.be.Name())
}
