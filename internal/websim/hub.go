package websim

import "sync"

// clientBuffer is the per-client frame queue depth. A browser that falls this
// far behind is treated as dead weight: further frames for it are dropped
// (never blocking the broadcaster), and it will re-sync on reconnect.
const clientBuffer = 256

// Frame is one server-sent event: Event is the SSE "event:" name and Data is
// the JSON-encoded payload.
type Frame struct {
	Event string
	Data  any
}

// Hub fans out frames to all connected SSE clients. All methods are safe for
// concurrent use.
type Hub struct {
	mu      sync.Mutex
	clients map[chan Frame]struct{}
}

// NewHub creates an empty Hub.
func NewHub() *Hub {
	return &Hub{clients: make(map[chan Frame]struct{})}
}

// Add registers a new client and returns its frame channel. The caller reads
// from the channel until it stops, then calls Remove.
func (h *Hub) Add() chan Frame {
	ch := make(chan Frame, clientBuffer)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

// Remove deregisters a client channel and closes it. It is idempotent.
func (h *Hub) Remove(ch chan Frame) {
	h.mu.Lock()
	if _, ok := h.clients[ch]; ok {
		delete(h.clients, ch)
		close(ch)
	}
	h.mu.Unlock()
}

// Broadcast sends f to every connected client. A client whose buffer is full
// silently drops this frame rather than blocking the broadcaster.
func (h *Hub) Broadcast(f Frame) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- f:
		default:
			// Client is backed up; drop the frame for it.
		}
	}
}

// Count returns the number of connected clients.
func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}
