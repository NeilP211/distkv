package websim_test

import (
	"testing"

	"github.com/NeilP211/distkv/internal/websim"
)

func TestHubBroadcastDelivers(t *testing.T) {
	h := websim.NewHub()
	ch := h.Add()
	defer h.Remove(ch)

	if h.Count() != 1 {
		t.Fatalf("Count = %d, want 1", h.Count())
	}
	h.Broadcast(websim.Frame{Event: "state", Data: 42})
	f := <-ch
	if f.Event != "state" || f.Data != 42 {
		t.Fatalf("frame = %+v, want {state 42}", f)
	}
}

func TestHubFullClientDoesNotBlock(t *testing.T) {
	h := websim.NewHub()
	ch := h.Add()
	defer h.Remove(ch)

	// Overfill well past the buffer; Broadcast must never block.
	for i := 0; i < 1000; i++ {
		h.Broadcast(websim.Frame{Event: "rpc", Data: i})
	}
	// The client still has buffered frames available.
	if len(ch) == 0 {
		t.Fatalf("expected buffered frames for the client")
	}
}

func TestHubRemove(t *testing.T) {
	h := websim.NewHub()
	ch := h.Add()
	h.Remove(ch)
	if h.Count() != 0 {
		t.Fatalf("Count after Remove = %d, want 0", h.Count())
	}
	if _, ok := <-ch; ok {
		t.Fatalf("channel should be closed after Remove")
	}
	h.Remove(ch) // idempotent, must not panic
}
