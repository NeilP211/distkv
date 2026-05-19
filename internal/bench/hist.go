// Package bench provides latency histogram and benchmark utilities for DistKV.
package bench

import (
	"math"
	"sync/atomic"
	"time"
)

const (
	histBuckets = 200
	histMinUs   = 10         // 10 µs minimum
	histMaxUs   = 30_000_000 // 30 s maximum (µs)
)

// Histogram is a thread-safe, log-scale latency histogram.
// It uses 200 exponentially-spaced buckets from 10µs to 30s.
// All bucket operations use atomic increments for lock-free recording.
type Histogram struct {
	counts   [histBuckets]atomic.Int64
	overflow atomic.Int64 // samples above histMaxUs
	total    atomic.Int64 // total sample count
}

// logBase is precomputed: log(histMaxUs/histMinUs) / (histBuckets-1)
var logBase = math.Log(float64(histMaxUs)/float64(histMinUs)) / float64(histBuckets-1)

// bucketFor returns the bucket index for a duration in microseconds.
func bucketFor(us float64) int {
	if us <= float64(histMinUs) {
		return 0
	}
	if us >= float64(histMaxUs) {
		return histBuckets - 1
	}
	idx := int(math.Log(us/float64(histMinUs)) / logBase)
	if idx < 0 {
		return 0
	}
	if idx >= histBuckets {
		return histBuckets - 1
	}
	return idx
}

// Record records one latency sample into the histogram.
func (h *Histogram) Record(d time.Duration) {
	us := float64(d) / float64(time.Microsecond)
	if us > float64(histMaxUs) {
		h.overflow.Add(1)
	} else {
		idx := bucketFor(us)
		h.counts[idx].Add(1)
	}
	h.total.Add(1)
}

// Percentile returns the latency at the given percentile p in [0, 1].
// Returns 0 if no samples have been recorded.
func (h *Histogram) Percentile(p float64) time.Duration {
	total := h.total.Load()
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(p * float64(total)))
	if target < 1 {
		target = 1
	}
	var cumulative int64
	for i := 0; i < histBuckets; i++ {
		cumulative += h.counts[i].Load()
		if cumulative >= target {
			// Return the upper bound of this bucket in µs.
			upperUs := float64(histMinUs) * math.Exp(float64(i+1)*logBase)
			return time.Duration(upperUs * float64(time.Microsecond))
		}
	}
	// Must be in overflow.
	return time.Duration(histMaxUs) * time.Microsecond
}

// Count returns the total number of samples recorded.
func (h *Histogram) Count() int64 { return h.total.Load() }
