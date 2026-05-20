// Package metrics defines the Prometheus collectors for DistKV and exposes a
// small API that the server and daemon packages call to record observations.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// requestsTotal counts every KV RPC broken down by operation and result.
	requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "distkv_requests_total",
			Help: "Total number of KV requests, partitioned by operation and result.",
		},
		[]string{"op", "result"},
	)

	// leaderElectionsTotal counts how many times this node has become leader.
	leaderElectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "distkv_leader_elections_total",
			Help: "Number of times this node was elected leader.",
		},
		[]string{"node"},
	)

	// replicationLag is the leader's log replication lag: lastIndex minus the
	// minimum matchIndex over all peers.  Updated periodically by the metrics
	// goroutine in main.
	replicationLag = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "distkv_log_replication_lag",
			Help: "Leader replication lag: lastIndex - min(matchIndex over peers).",
		},
	)

	// commitLatency observes the wall-clock time from Propose call-start to
	// apply-complete for each successfully proposed command.
	commitLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "distkv_commit_latency_seconds",
			Help:    "Histogram of Raft commit latency (Propose → apply).",
			Buckets: prometheus.DefBuckets,
		},
	)

	// applyLatency observes the time from "entry committed" to "entry applied to
	// the state machine" inside the apply loop.
	applyLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "distkv_apply_latency_seconds",
			Help:    "Histogram of state-machine apply latency (committed → applied).",
			Buckets: prometheus.DefBuckets,
		},
	)

	// role is a set of gauges keyed by role name; the active role is 1, the
	// others are 0.
	role = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "distkv_role",
			Help: "Current Raft role (1 for active, 0 for others).",
		},
		[]string{"role"},
	)

	// term tracks the current Raft election term.
	term = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "distkv_term",
			Help: "Current Raft election term.",
		},
	)
)

// allRoles lists every role label so SetRole can zero the others atomically.
var allRoles = []string{"follower", "candidate", "leader"}

// MustRegister is a no-op: promauto already registered every collector above
// with the default registry.  It exists as a convenient import side-effect
// trigger; call it from main to ensure the metrics package is initialised.
func MustRegister() {}

// KVRequest records a single KV RPC outcome.
//
//	op     — "put", "get", "delete", "cas", "status"
//	result — "ok", "notleader", "unavailable", "error"
func KVRequest(op, result string) {
	requestsTotal.WithLabelValues(op, result).Inc()
}

// LeaderElected records that this node (node) has just become leader.
func LeaderElected(node string) {
	leaderElectionsTotal.WithLabelValues(node).Inc()
}

// SetReplicationLag sets the current replication lag in log-index units.
func SetReplicationLag(lag float64) {
	replicationLag.Set(lag)
}

// ObserveCommitLatency records a commit latency observation.
func ObserveCommitLatency(d time.Duration) {
	commitLatency.Observe(d.Seconds())
}

// ObserveApplyLatency records an apply-loop latency observation.
func ObserveApplyLatency(d time.Duration) {
	applyLatency.Observe(d.Seconds())
}

// SetRole updates the role gauge: sets the given role to 1 and all others to 0.
// role should be one of "follower", "candidate", "leader" (case-insensitive
// callers should lower-case it; the Raft package uses title-case strings so we
// map here).
func SetRole(r string) {
	// Normalise to lower-case to match label values.
	lr := normRole(r)
	for _, name := range allRoles {
		if name == lr {
			role.WithLabelValues(name).Set(1)
		} else {
			role.WithLabelValues(name).Set(0)
		}
	}
}

// SetTerm updates the term gauge.
func SetTerm(t uint64) {
	term.Set(float64(t))
}

// normRole maps the title-case role strings emitted by raft.Role.String()
// ("Leader", "Follower", "Candidate") to the lower-case label values used in
// the Prometheus metric.
func normRole(r string) string {
	switch r {
	case "Leader", "leader":
		return "leader"
	case "Follower", "follower":
		return "follower"
	case "Candidate", "candidate":
		return "candidate"
	default:
		return r
	}
}
