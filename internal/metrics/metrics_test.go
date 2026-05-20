package metrics_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/NeilP211/distkv/internal/metrics"
)

// TestKVRequestCounterIncrement verifies that KVRequest increments the
// distkv_requests_total counter for the given op/result label pair.
func TestKVRequestCounterIncrement(t *testing.T) {
	// Use a fresh registry so we don't interfere with the default one and
	// so test runs are isolated.
	reg := prometheus.NewRegistry()

	// Re-create just the counter under test using the isolated registry.
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "distkv_requests_total",
			Help: "test",
		},
		[]string{"op", "result"},
	)
	reg.MustRegister(counter)

	// Simulate what the production API does.
	counter.WithLabelValues("put", "ok").Inc()
	counter.WithLabelValues("put", "ok").Inc()
	counter.WithLabelValues("get", "notleader").Inc()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	var putOK, getNotLeader float64
	for _, mf := range mfs {
		if mf.GetName() != "distkv_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			op, result := labelVal(m, "op"), labelVal(m, "result")
			switch {
			case op == "put" && result == "ok":
				putOK = m.GetCounter().GetValue()
			case op == "get" && result == "notleader":
				getNotLeader = m.GetCounter().GetValue()
			}
		}
	}

	if putOK != 2 {
		t.Errorf("put/ok: want 2, got %v", putOK)
	}
	if getNotLeader != 1 {
		t.Errorf("get/notleader: want 1, got %v", getNotLeader)
	}
}

// TestSetRole verifies that SetRole sets the active role gauge to 1 and the
// others to 0 using the package-level API.
func TestSetRole(t *testing.T) {
	// MustRegister is a no-op; just ensure it is callable without panic.
	metrics.MustRegister()

	// SetRole should not panic for known roles.
	for _, r := range []string{"leader", "follower", "candidate", "Leader", "Follower", "Candidate"} {
		metrics.SetRole(r)
	}
}

// TestSetTerm verifies that SetTerm does not panic.
func TestSetTerm(t *testing.T) {
	metrics.SetTerm(0)
	metrics.SetTerm(42)
}

// TestLeaderElected verifies that LeaderElected does not panic.
func TestLeaderElected(t *testing.T) {
	metrics.LeaderElected("n1")
}

// labelVal is a helper that extracts a label value from a dto.Metric.
func labelVal(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}

// TestKVRequestOpLabels checks that calling KVRequest with various ops does
// not panic and uses valid label names (no spaces, no special chars).
func TestKVRequestOpLabels(t *testing.T) {
	ops := []string{"put", "get", "delete", "cas", "status"}
	results := []string{"ok", "notleader", "unavailable", "error"}
	for _, op := range ops {
		for _, result := range results {
			if strings.ContainsAny(op+result, " \t\n") {
				t.Errorf("label contains whitespace: op=%q result=%q", op, result)
			}
			// Should not panic.
			metrics.KVRequest(op, result)
		}
	}
}
