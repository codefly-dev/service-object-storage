// Package health keeps a gRPC health service in step with the backing store, so
// an orchestrator's readiness check reflects real access to the configured
// bucket rather than a listening socket.
package health

import (
	"context"
	"log"
	"time"

	grpchealth "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/codefly-dev/service-object-storage/internal/backend"
	"github.com/codefly-dev/service-object-storage/internal/serr"
)

// Monitor probes a backend on an interval and publishes the outcome as the
// serving status of one named health service.
//
// It never touches the overall ("") service, which stays SERVING for the
// lifetime of the process: that is the liveness signal, and a transient cloud
// outage must not get the container killed and restarted. Readiness alone
// follows the probe, so access lost after startup takes the replica out of
// rotation and access regained puts it back.
type Monitor struct {
	be       backend.Backend
	health   *grpchealth.Server
	service  string
	interval time.Duration
	timeout  time.Duration

	last healthv1.HealthCheckResponse_ServingStatus
}

// New builds a Monitor publishing the readiness of be as the serving status of
// service.
func New(be backend.Backend, hs *grpchealth.Server, service string, interval, timeout time.Duration) *Monitor {
	return &Monitor{be: be, health: hs, service: service, interval: interval, timeout: timeout}
}

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

	status := healthv1.HealthCheckResponse_SERVING
	if err != nil {
		status = healthv1.HealthCheckResponse_NOT_SERVING
	}
	if status == m.last {
		return
	}
	m.last = status
	m.health.SetServingStatus(m.service, status)

	if err != nil {
		log.Printf("readiness: NOT_SERVING (%s: %v)", serr.CodeOf(err), err)
		return
	}
	log.Printf("readiness: SERVING (backend %s)", m.be.Name())
}
