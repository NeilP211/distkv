// Command distkvd is the DistKV node daemon.  A distkvd process is one Raft
// cluster member: it serves both the internal RaftService (inter-node Raft
// RPCs) and the client-facing KV service on a single gRPC listener, drives a
// Raft node, applies committed entries to a key-value state machine, and runs
// until interrupted.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/NeilP211/distkv/api"
	"github.com/NeilP211/distkv/internal/metrics"
	"github.com/NeilP211/distkv/internal/raft"
	"github.com/NeilP211/distkv/internal/raftstore"
	"github.com/NeilP211/distkv/internal/server"
	"github.com/NeilP211/distkv/internal/transport"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("distkvd: %v", err)
	}
}

func run() error {
	var (
		idFlag        = flag.String("id", "", "this node's id (required)")
		listen        = flag.String("listen", "", "host:port for this node's gRPC server (required)")
		peersFlag     = flag.String("peers", "", "comma-separated id=host:port for ALL members, including self (required)")
		dataDir       = flag.String("data-dir", "", "directory for this node's bbolt storage (required)")
		metricsListen = flag.String("metrics-listen", "127.0.0.1:9101", "host:port for the Prometheus /metrics HTTP server")
	)
	flag.Parse()

	if *idFlag == "" || *listen == "" || *peersFlag == "" || *dataDir == "" {
		flag.Usage()
		return fmt.Errorf("--id, --listen, --peers and --data-dir are all required")
	}
	id := raft.NodeID(*idFlag)

	// Parse the peer map: id=host:port entries for every cluster member.
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		return err
	}
	if _, ok := peers[id]; !ok {
		return fmt.Errorf("--peers must include this node's own id %q", id)
	}
	memberIDs := make([]raft.NodeID, 0, len(peers))
	for pid := range peers {
		memberIDs = append(memberIDs, pid)
	}

	// Storage: a bbolt database under the data directory.
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	storage, err := raftstore.Open(filepath.Join(*dataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() { _ = storage.Close() }()

	// Transport: gRPC to every peer.
	tr := transport.NewGRPCTransport(id, peers)
	defer func() { _ = tr.Close() }()

	// Raft node driver.
	rn, err := server.NewRaftNode(server.RaftNodeConfig{
		Raft: raft.Config{
			ID:                 id,
			Peers:              memberIDs,
			Storage:            storage,
			Transport:          tr,
			ElectionTimeoutMin: 10,
			ElectionTimeoutMax: 20,
			HeartbeatInterval:  3,
		},
		TickInterval: 50 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("create raft node: %w", err)
	}

	// gRPC server hosting both services on one listener.
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listen, err)
	}
	gs := grpc.NewServer()
	api.RegisterRaftServiceServer(gs, transport.NewRaftServer(rn.Step))
	api.RegisterKVServer(gs, server.NewKVService(rn, peers))

	// Ensure the metrics package collectors are registered (promauto does this
	// on init, but calling MustRegister makes the dependency explicit).
	metrics.MustRegister()

	// Start the Prometheus /metrics HTTP server on a dedicated port.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{Addr: *metricsListen, Handler: metricsMux}
	go func() {
		log.Printf("distkvd: metrics server listening on %s", *metricsListen)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("distkvd: metrics server error: %v", err)
		}
	}()

	rn.Start()
	log.Printf("distkvd: node %s listening on %s, peers=%v", id, *listen, peers)

	// Serve in a background goroutine; report a fatal serve error.
	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(ln) }()

	// Log role changes and update Prometheus metrics at a low cadence.
	roleStop := make(chan struct{})
	go watchRole(rn, roleStop)

	// Wait for SIGINT/SIGTERM or a serve failure.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("distkvd: received %s, shutting down", sig)
	case err := <-serveErr:
		close(roleStop)
		rn.Stop()
		return fmt.Errorf("gRPC server: %w", err)
	}

	// Graceful shutdown.
	close(roleStop)
	gs.GracefulStop()
	rn.Stop()
	_ = metricsServer.Close()
	log.Printf("distkvd: node %s stopped", id)
	return nil
}

// parsePeers parses a comma-separated list of id=host:port entries.
func parsePeers(s string) (map[raft.NodeID]string, error) {
	peers := make(map[raft.NodeID]string)
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		k, v, ok := strings.Cut(entry, "=")
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("invalid peer entry %q, want id=host:port", entry)
		}
		peers[raft.NodeID(k)] = v
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers parsed from %q", s)
	}
	return peers, nil
}

// watchRole logs role changes and keeps Prometheus metrics in sync with the
// node's current Raft state.  It runs until stop is closed.
func watchRole(rn *server.RaftNode, stop <-chan struct{}) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	lastRole := ""
	lastTerm := uint64(0)
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			st := rn.Status()

			// Update Prometheus role/term gauges on every tick.
			metrics.SetRole(st.Role)
			metrics.SetTerm(st.Term)

			// Replication lag: Status does not expose matchIndex, so we
			// leave lag at 0.  A future accessor on RaftNode could populate
			// this without modifying the Raft core.
			metrics.SetReplicationLag(0)

			// Log role/term changes and count leader elections.
			if st.Role != lastRole {
				log.Printf("distkvd: node %s role=%s term=%d leader=%s",
					st.ID, st.Role, st.Term, st.Leader)
				if st.Role == "Leader" {
					metrics.LeaderElected(string(st.ID))
				}
				lastRole = st.Role
			}
			if st.Term != lastTerm {
				lastTerm = st.Term
			}
		}
	}
}
