package tui

import (
	"testing"
	"time"

	"github.com/NeilP211/distkv/pkg/client"
)

// TestDashboardNew verifies that the Dashboard can be constructed without
// panicking and that Close is safe to call before Run.  The UI primitives are
// created synchronously in New, so this exercises the layout wiring without
// requiring a real terminal.
func TestDashboardNew(t *testing.T) {
	cl := client.New([]string{"127.0.0.1:9001"})
	defer cl.Close()

	d := New(cl, []string{"127.0.0.1:9001", "127.0.0.1:9002"}, time.Second)
	if d == nil {
		t.Fatal("New returned nil")
	}
	// Close without Run — must not panic or deadlock.
	d.Close()
}
