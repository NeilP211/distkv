// Command distkv-bench is a throughput/latency benchmark for a live DistKV cluster.
//
// Usage:
//
//	distkv-bench [flags]
//	distkv-bench --failover --pids n1=<pid>,n2=<pid>,n3=<pid>
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NeilP211/distkv/internal/bench"
	"github.com/NeilP211/distkv/pkg/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "distkv-bench:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("distkv-bench", flag.ContinueOnError)

	endpoints := fs.String("endpoints", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003",
		"comma-separated cluster endpoints")
	concurrency := fs.Int("concurrency", 32, "number of concurrent client goroutines")
	duration := fs.Duration("duration", 10*time.Second, "benchmark measurement duration")
	warmup := fs.Duration("warmup", 2*time.Second, "warmup duration (discarded from results)")
	keySpace := fs.Int("key-space", 1000, "number of distinct keys")
	valueSize := fs.Int("value-size", 128, "value size in bytes")
	workloadStr := fs.String("workload", "mixed", "workload: write-only | read-only | mixed")
	seed := fs.Int64("seed", 1, "random seed for key/value generation")
	failover := fs.Bool("failover", false, "run failover sub-benchmark instead")
	pids := fs.String("pids", "", "comma-separated id=pid pairs for failover (e.g. n1=12345,n2=12346,n3=12347)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	eps := splitCSV(*endpoints)
	if len(eps) == 0 {
		return fmt.Errorf("no endpoints configured")
	}

	if *failover {
		return runFailover(eps, *pids)
	}

	wl, err := bench.ParseWorkload(*workloadStr)
	if err != nil {
		return err
	}

	cfg := bench.Config{
		Endpoints:      eps,
		Concurrency:    *concurrency,
		Duration:       *duration,
		WarmupDuration: *warmup,
		KeySpace:       *keySpace,
		ValueSize:      *valueSize,
		Workload:       wl,
		Seed:           *seed,
	}

	fmt.Printf("distkv-bench: workload=%s concurrency=%d duration=%s warmup=%s key-space=%d value-size=%d\n",
		wl, *concurrency, *duration, *warmup, *keySpace, *valueSize)
	fmt.Println("running warmup...")

	result, err := bench.Run(cfg)
	if err != nil {
		return err
	}

	printResult(result)
	return nil
}

func printResult(r bench.Result) {
	fmt.Println()
	fmt.Println("┌─────────────────────────────────────────────────────────────────┐")
	fmt.Printf("│  Workload  : %-52s│\n", r.Workload)
	fmt.Printf("│  Duration  : %-52s│\n", r.Duration)
	fmt.Printf("│  Total ops : %-52d│\n", r.TotalOps)
	fmt.Printf("│  Errors    : %-52d│\n", r.Errors)
	fmt.Println("├─────────────────────────────────────────────────────────────────┤")
	fmt.Printf("│  Throughput: %-48.0f ops/sec│\n", r.Throughput)
	fmt.Println("├─────────────────────────────────────────────────────────────────┤")
	fmt.Printf("│  P50 latency: %-.2f ms%*s│\n", msec(r.P50), padding(msec(r.P50), 45), "")
	fmt.Printf("│  P95 latency: %-.2f ms%*s│\n", msec(r.P95), padding(msec(r.P95), 45), "")
	fmt.Printf("│  P99 latency: %-.2f ms%*s│\n", msec(r.P99), padding(msec(r.P99), 45), "")
	fmt.Println("└─────────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// Also print a compact CSV-friendly line for easy copying.
	fmt.Printf("RESULT: workload=%s throughput=%.0f p50=%.2fms p95=%.2fms p99=%.2fms errors=%d\n",
		r.Workload, r.Throughput, msec(r.P50), msec(r.P95), msec(r.P99), r.Errors)
}

func msec(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// padding computes how many spaces to append so the line is exactly 65 chars wide.
func padding(val float64, width int) int {
	s := fmt.Sprintf("%.2f ms", val)
	pad := width - len(s)
	if pad < 0 {
		return 0
	}
	return pad
}

func runFailover(eps []string, pidsStr string) error {
	if pidsStr == "" {
		return fmt.Errorf("--pids is required for --failover (e.g. --pids n1=12345,n2=12346,n3=12347)")
	}

	pidMap, err := parsePIDs(pidsStr)
	if err != nil {
		return err
	}

	// Find the current leader.
	cl := client.New(eps,
		client.WithMaxAttempts(5),
	)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	st, err := cl.Status(ctx)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot get cluster status: %w", err)
	}
	if st.LeaderID == "" {
		return fmt.Errorf("no leader elected yet")
	}

	leaderID := st.LeaderID
	leaderPID, ok := pidMap[leaderID]
	if !ok {
		return fmt.Errorf("no pid for leader %q in --pids (got: %v)", leaderID, pidsStr)
	}

	fmt.Printf("distkv-bench: failover test — killing leader %s (pid %d)\n", leaderID, leaderPID)

	fcfg := bench.FailoverConfig{
		Endpoints: eps,
		LeaderPID: leaderPID,
		LeaderID:  leaderID,
	}
	d, err := bench.FailoverTime(fcfg)
	if err != nil {
		return err
	}

	fmt.Printf("\nFailover time: %.0f ms (kill→first-successful-Put on new leader)\n", float64(d)/float64(time.Millisecond))
	fmt.Printf("RESULT: failover=%.0fms\n", float64(d)/float64(time.Millisecond))
	return nil
}

// parsePIDs parses a comma-separated "id=pid,..." string.
func parsePIDs(s string) (map[string]int, error) {
	out := make(map[string]int)
	for _, part := range splitCSV(s) {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad pid spec %q (want id=pid)", part)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil || pid <= 0 {
			return nil, fmt.Errorf("bad pid value in %q: %w", part, err)
		}
		out[strings.TrimSpace(kv[0])] = pid
	}
	return out, nil
}

// splitCSV splits a comma-separated string, trimming whitespace.
func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
