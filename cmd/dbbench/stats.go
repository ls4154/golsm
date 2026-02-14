package main

import (
	"fmt"
	"math"
	"time"
)

type latencyHistogram struct {
	bounds []time.Duration
	counts []uint64
	total  uint64
}

func newLatencyHistogram() latencyHistogram {
	bounds := []time.Duration{
		5 * time.Microsecond,
		10 * time.Microsecond,
		20 * time.Microsecond,
		50 * time.Microsecond,
		100 * time.Microsecond,
		200 * time.Microsecond,
		500 * time.Microsecond,
		1 * time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}
	return latencyHistogram{
		bounds: bounds,
		counts: make([]uint64, len(bounds)+1),
	}
}

func (h *latencyHistogram) Observe(d time.Duration) {
	h.total++
	for i := range h.bounds {
		if d <= h.bounds[i] {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.counts)-1]++
}

func (h *latencyHistogram) Merge(other latencyHistogram) {
	if len(h.counts) != len(other.counts) {
		return
	}
	h.total += other.total
	for i := range h.counts {
		h.counts[i] += other.counts[i]
	}
}

func (h latencyHistogram) percentile(p float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	target := uint64(math.Ceil((p / 100.0) * float64(h.total)))
	if target == 0 {
		target = 1
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= target {
			if i < len(h.bounds) {
				return h.bounds[i]
			}
			return h.bounds[len(h.bounds)-1]
		}
	}
	return h.bounds[len(h.bounds)-1]
}

func (h latencyHistogram) P50() time.Duration { return h.percentile(50) }
func (h latencyHistogram) P95() time.Duration { return h.percentile(95) }
func (h latencyHistogram) P99() time.Duration { return h.percentile(99) }

type runResult struct {
	requested int64
	ops       int64
	errors    int64
	misses    int64
	bytes     int64
	opsPerSec float64
	mbPerSec  float64
	avgMicros float64
	hist      latencyHistogram
	message   string
}

func finalizeResult(requested, ops, errors, misses, bytes int64, wallElapsed, threadElapsed time.Duration, hist latencyHistogram) runResult {
	sec := wallElapsed.Seconds()
	if sec <= 0 {
		sec = 1e-9
	}
	return runResult{
		requested: requested,
		ops:       ops,
		errors:    errors,
		misses:    misses,
		bytes:     bytes,
		opsPerSec: float64(ops) / sec,
		mbPerSec:  (float64(bytes) / (1024 * 1024)) / sec,
		avgMicros: float64(threadElapsed.Microseconds()) / float64(max64(ops, 1)),
		hist:      hist,
	}
}

func formatDurationMicros(d time.Duration) string {
	return fmt.Sprintf("%.1f", float64(d.Microseconds()))
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
