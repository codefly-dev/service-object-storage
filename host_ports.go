package main

import (
	"net"
	"strconv"
	"sync"
)

// hostPorts is the agent holding each host port a container of this Init will
// publish, from the moment the port is known until that container starts.
//
// A port that is only written down is free to the kernel. The run holds the
// gateway's port until this service's first Init and lets go of it there; the
// gateway is the last thing Init starts, so for the whole MinIO start — a
// container, a network, a bucket — the port belonged to nobody, and the probe
// that picked MinIO's own port, or any process binding port 0 on this machine,
// could be handed it: "failed to bind host port 0.0.0.0:57062/tcp: address
// already in use". So Init binds the port before anything else and keeps it
// bound until the one step that gives it to Docker.
//
// What is left is that step. Docker binds the port itself and cannot inherit a
// listener, so a port is released just before its container starts and is free
// for the length of that one start — and nothing in this agent asks the kernel
// for a port inside it.
//
// Loopback only, as for the run's own reservations: nothing is served here, so
// no other machine can reach the listener. A port that cannot be bound is
// already held — by this service's kept container, or by a stranger Docker
// will name — and is left as it is.
type hostPorts struct {
	mu   sync.Mutex
	held map[uint16][]net.Listener
}

var loopbackHosts = []string{"127.0.0.1", "::1"}

// claim holds a port the run assigned to this service.
func (p *hostPorts) claim(port uint16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if port == 0 || len(p.held[port]) > 0 {
		return
	}
	for _, host := range loopbackHosts {
		p.listen(host, port)
	}
}

// claimFree asks the kernel for an unused port and keeps it held, so the port
// it returns cannot be one already claimed and cannot be handed out again.
func (p *hostPorts) claimFree() (uint16, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	listener, err := net.Listen("tcp", net.JoinHostPort(loopbackHosts[0], "0"))
	if err != nil {
		return 0, err
	}
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	p.keep(port, listener)
	for _, host := range loopbackHosts[1:] {
		p.listen(host, port)
	}
	return port, nil
}

// holds reports whether the agent is holding a port.
func (p *hostPorts) holds(port uint16) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.held[port]) > 0
}

// handOver lets go of a port for the container that is about to bind it.
func (p *hostPorts) handOver(port uint16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.release(port)
}

// releaseAll lets go of every port still held: Init is over, and a container
// that never started binds nothing.
func (p *hostPorts) releaseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for port := range p.held {
		p.release(port)
	}
}

func (p *hostPorts) listen(host string, port uint16) {
	listener, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(int(port))))
	if err != nil {
		return
	}
	p.keep(port, listener)
}

func (p *hostPorts) keep(port uint16, listener net.Listener) {
	if p.held == nil {
		p.held = map[uint16][]net.Listener{}
	}
	p.held[port] = append(p.held[port], listener)
}

func (p *hostPorts) release(port uint16) {
	for _, listener := range p.held[port] {
		_ = listener.Close()
	}
	delete(p.held, port)
}
