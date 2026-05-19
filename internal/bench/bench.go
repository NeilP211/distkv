package bench

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NeilP211/distkv/pkg/client"
)

// WorkloadKind identifies what mix of operations the benchmark runs.
type WorkloadKind int

const (
	WriteOnly WorkloadKind = iota
	ReadOnly
	Mixed
)

// ParseWorkload converts a string like "write-only" to a WorkloadKind.
func ParseWorkload(s string) (WorkloadKind, error) {
	switch strings.ToLower(s) {
	case "write-only", "writeonly":
		return WriteOnly, nil
	case "read-only", "readonly":
		return ReadOnly, nil
	case "mixed":
		return Mixed, nil
	default:
		return 0, fmt.Errorf("unknown workload %q (want write-only|read-only|mixed)", s)
	}
}

func (w WorkloadKind) String() string {
	switch w {
	case WriteOnly:
		return "write-only"
	case ReadOnly:
		return "read-only"
	case Mixed:
		return "mixed"
	default:
		return "unknown"
	}
}

// Config parameterises a benchmark run.
type Config struct {
	Endpoints      []string
	Concurrency    int
	Duration       time.Duration
	WarmupDuration time.Duration
	KeySpace       int
	ValueSize      int
	Workload       WorkloadKind
	Seed           int64
}

// Result holds the outcome of a benchmark run.
type Result struct {
	Workload   WorkloadKind
	Throughput float64 // ops/sec, over the measured window only
	P50        time.Duration
	P95        time.Duration
	P99        time.Duration
	Errors     int64
	TotalOps   int64
	Duration   time.Duration
}

// opKind selects which operation to issue based on workload and an [0,100) roll.
func opKind(w WorkloadKind, roll int) string {
	switch w {
	case WriteOnly:
		return "put"
	case ReadOnly:
		return "get"
	case Mixed:
		// 70% read, 25% write, 5% CAS
		if roll < 70 {
			return "get"
		}
		if roll < 95 {
			return "put"
		}
		return "cas"
	}
	return "put"
}

// makeValue returns a deterministic value of length n seeded from seed.
func makeValue(n int, seed int64) string {
	const alpha = "abcdefghijklmnopqrstuvwxyz0123456789"
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec
	b := make([]byte, n)
	for i := range b {
		b[i] = alpha[rng.Intn(len(alpha))]
	}
	return string(b)
}

// Run executes the benchmark according to cfg and returns measured results.
// It opens a pkg/client.Client, runs cfg.Concurrency closed-loop goroutines
// for WarmupDuration + Duration, discards warmup samples, then computes stats.
func Run(cfg Config) (Result, error) {
	if len(cfg.Endpoints) == 0 {
		return Result{}, fmt.Errorf("bench: no endpoints")
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.KeySpace < 1 {
		cfg.KeySpace = 1
	}

	cl := client.New(cfg.Endpoints,
		client.WithMaxAttempts(10),
		client.WithBackoff(10*time.Millisecond, 200*time.Millisecond),
	)
	defer cl.Close()

	// Build the fixed value payload.
	value := makeValue(cfg.ValueSize, cfg.Seed)

	// Shared histogram and counters — only populated after warmup.
	var hist Histogram
	var errCount atomic.Int64
	var opsCount atomic.Int64

	// measuring becomes true after the warmup expires.
	var measuring atomic.Bool

	totalDur := cfg.WarmupDuration + cfg.Duration
	ctx, cancel := context.WithTimeout(context.Background(), totalDur+5*time.Second)
	defer cancel()

	// Timer to flip measuring on after warmup.
	var wg sync.WaitGroup
	done := make(chan struct{})

	// Start a goroutine that flips measuring after WarmupDuration, then closes
	// done after the full Duration.
	wg.Add(1)
	go func() {
		defer wg.Done()
		warmupTimer := time.NewTimer(cfg.WarmupDuration)
		defer warmupTimer.Stop()
		select {
		case <-warmupTimer.C:
			measuring.Store(true)
		case <-ctx.Done():
			return
		}
		measureTimer := time.NewTimer(cfg.Duration)
		defer measureTimer.Stop()
		select {
		case <-measureTimer.C:
		case <-ctx.Done():
		}
		close(done)
	}()

	// Worker goroutines.
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(workerID))) //nolint:gosec
			for {
				select {
				case <-done:
					return
				default:
				}

				keyIdx := rng.Intn(cfg.KeySpace)
				key := fmt.Sprintf("k%07d", keyIdx)
				roll := rng.Intn(100)
				op := opKind(cfg.Workload, roll)

				start := time.Now()
				var err error
				switch op {
				case "put":
					err = cl.Put(ctx, key, value)
				case "get":
					_, _, err = cl.Get(ctx, key)
				case "cas":
					// CAS: swap from whatever is there to the bench value.
					// We use a blind CAS with empty expect so it always attempts
					// and the latency is representative.
					_, err = cl.CAS(ctx, key, "", value)
				}
				elapsed := time.Since(start)

				// Context cancelled or done channel closed — don't record.
				if ctx.Err() != nil {
					return
				}

				if measuring.Load() {
					if err != nil {
						errCount.Add(1)
					} else {
						hist.Record(elapsed)
						opsCount.Add(1)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	ops := opsCount.Load()
	errs := errCount.Load()

	res := Result{
		Workload:   cfg.Workload,
		TotalOps:   ops + errs,
		Errors:     errs,
		Duration:   cfg.Duration,
		Throughput: float64(ops) / cfg.Duration.Seconds(),
		P50:        hist.Percentile(0.50),
		P95:        hist.Percentile(0.95),
		P99:        hist.Percentile(0.99),
	}
	return res, nil
}

// FailoverConfig configures the failover latency measurement.
type FailoverConfig struct {
	Endpoints []string
	LeaderPID int    // OS PID of the leader process to kill
	LeaderID  string // node id of the leader (for logging)
}

// FailoverTime measures the time from killing the leader to the first
// successful Put on a new leader. It returns the measured duration.
//
// The caller is responsible for identifying the leader PID (e.g. by reading
// data/<id>.pid after distkvctl status reports the leader id).
func FailoverTime(cfg FailoverConfig) (time.Duration, error) {
	if len(cfg.Endpoints) == 0 {
		return 0, fmt.Errorf("bench: no endpoints")
	}
	if cfg.LeaderPID <= 0 {
		return 0, fmt.Errorf("bench: invalid leader PID %d", cfg.LeaderPID)
	}

	cl := client.New(cfg.Endpoints,
		client.WithMaxAttempts(3),
		client.WithBackoff(50*time.Millisecond, 500*time.Millisecond),
	)
	defer cl.Close()

	// Confirm cluster is up before killing.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := cl.Put(ctx, "failover-probe", "before")
	cancel()
	if err != nil {
		return 0, fmt.Errorf("bench: baseline put failed: %w", err)
	}

	// Kill the leader.
	killStart := time.Now()
	if err := killPID(cfg.LeaderPID); err != nil {
		return 0, fmt.Errorf("bench: kill pid %d: %w", cfg.LeaderPID, err)
	}

	// Poll until a Put succeeds on the new leader.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := cl.Put(ctx, "failover-probe", "after")
		cancel()
		if err == nil {
			return time.Since(killStart), nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("bench: cluster did not elect a new leader within 30s")
}
