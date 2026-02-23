package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ls4154/golsm/db"
)

func runBenchmark(ldb db.DB, cfg config, spec benchSpec) (runResult, error) {
	switch spec.name {
	case "fillseq":
		return runFillSeq(ldb, cfg), nil
	case "fillrandom":
		return runFillRandom(ldb, cfg), nil
	case "overwrite":
		return runOverwrite(ldb, cfg), nil
	case "readrandom":
		return runReadRandom(ldb, cfg), nil
	case "readseq":
		return runReadSeq(ldb, cfg), nil
	case "readreverse":
		return runReadReverse(ldb, cfg), nil
	default:
		return runResult{}, fmt.Errorf("unknown benchmark %q", spec.name)
	}
}

type workerResult struct {
	ops     int64
	errors  int64
	misses  int64
	bytes   int64
	hist    latencyHistogram
	elapsed time.Duration
}

func runFillSeq(ldb db.DB, cfg config) runResult {
	start := time.Now()
	gens := make([]*randomValueGenerator, cfg.threads)
	merged := runWorkers(cfg.num, cfg.threads, cfg.seed,
		func(workerID int, i int, wr *workerResult, _ *rand.Rand) bool {
			gen := gens[workerID]
			if gen == nil {
				gen = newRandomValueGenerator(cfg.compressionRatio, 301)
				gens[workerID] = gen
			}
			key := makeKey(i)
			value := gen.Generate(cfg.valueSize)

			t0 := time.Now()
			err := ldb.Put(key, value, &db.WriteOptions{Sync: cfg.sync})
			wr.hist.Observe(time.Since(t0))
			if err != nil {
				wr.errors++
				return true
			}
			wr.bytes += int64(len(key) + len(value))
			return true
		},
		nil,
	)

	requested := int64(cfg.num) * int64(cfg.threads)
	return finalizeResult(requested, merged.ops, merged.errors, 0, merged.bytes, time.Since(start), merged.elapsed, merged.hist)
}

func runFillRandom(ldb db.DB, cfg config) runResult {
	start := time.Now()
	gens := make([]*randomValueGenerator, cfg.threads)
	merged := runWorkers(cfg.num, cfg.threads, cfg.seed,
		func(workerID int, _ int, wr *workerResult, rng *rand.Rand) bool {
			gen := gens[workerID]
			if gen == nil {
				gen = newRandomValueGenerator(cfg.compressionRatio, 301)
				gens[workerID] = gen
			}
			keyID := rng.Intn(cfg.num)
			key := makeKey(keyID)
			value := gen.Generate(cfg.valueSize)

			t0 := time.Now()
			err := ldb.Put(key, value, &db.WriteOptions{Sync: cfg.sync})
			wr.hist.Observe(time.Since(t0))
			if err != nil {
				wr.errors++
				return true
			}
			wr.bytes += int64(len(key) + len(value))
			return true
		},
		nil,
	)

	requested := int64(cfg.num) * int64(cfg.threads)
	return finalizeResult(requested, merged.ops, merged.errors, 0, merged.bytes, time.Since(start), merged.elapsed, merged.hist)
}

func runOverwrite(ldb db.DB, cfg config) runResult {
	return runFillRandom(ldb, cfg)
}

func runReadRandom(ldb db.DB, cfg config) runResult {
	readsPerThread := readsPerThread(cfg)
	start := time.Now()
	merged := runWorkers(readsPerThread, cfg.threads, cfg.seed,
		func(_ int, _ int, wr *workerResult, rng *rand.Rand) bool {
			keyID := rng.Intn(cfg.num)
			key := makeKey(keyID)

			t0 := time.Now()
			val, err := ldb.Get(key, nil)
			wr.hist.Observe(time.Since(t0))
			if err != nil {
				if errors.Is(err, db.ErrNotFound) {
					wr.misses++
					return true
				}
				wr.errors++
				return true
			}
			wr.bytes += int64(len(key) + len(val))
			return true
		},
		nil,
	)

	requested := int64(readsPerThread) * int64(cfg.threads)
	r := finalizeResult(requested, merged.ops, merged.errors, merged.misses, merged.bytes, time.Since(start), merged.elapsed, merged.hist)
	r.message = fmt.Sprintf("(%d of %d found)", r.ops-r.misses, r.ops)
	return r
}

func runReadSeq(ldb db.DB, cfg config) runResult {
	readsPerThread := readsPerThread(cfg)
	start := time.Now()
	iters := make([]db.Iterator, cfg.threads)

	merged := runWorkers(readsPerThread, cfg.threads, cfg.seed,
		func(workerID int, _ int, wr *workerResult, _ *rand.Rand) bool {
			iter := iters[workerID]
			if iter == nil {
				var err error
				iter, err = ldb.NewIterator(nil)
				if err != nil {
					wr.errors++
					return false
				}
				iter.SeekToFirst()
				iters[workerID] = iter
			}

			if !iter.Valid() {
				return false
			}

			t0 := time.Now()
			key := iter.Key()
			value := iter.Value()
			wr.bytes += int64(len(key) + len(value))
			iter.Next()
			wr.hist.Observe(time.Since(t0))
			return true
		},
		func(workerID int, wr *workerResult) {
			iter := iters[workerID]
			if iter == nil {
				return
			}
			if err := iter.Error(); err != nil {
				wr.errors++
			}
			if err := iter.Close(); err != nil {
				wr.errors++
			}
		},
	)

	requested := int64(readsPerThread) * int64(cfg.threads)
	misses := requested - merged.ops
	if misses < 0 {
		misses = 0
	}
	return finalizeResult(requested, merged.ops, merged.errors, misses, merged.bytes, time.Since(start), merged.elapsed, merged.hist)
}

func runReadReverse(ldb db.DB, cfg config) runResult {
	readsPerThread := readsPerThread(cfg)
	start := time.Now()
	iters := make([]db.Iterator, cfg.threads)

	merged := runWorkers(readsPerThread, cfg.threads, cfg.seed,
		func(workerID int, _ int, wr *workerResult, _ *rand.Rand) bool {
			iter := iters[workerID]
			if iter == nil {
				var err error
				iter, err = ldb.NewIterator(nil)
				if err != nil {
					wr.errors++
					return false
				}
				iter.SeekToLast()
				iters[workerID] = iter
			}

			if !iter.Valid() {
				return false
			}

			t0 := time.Now()
			key := iter.Key()
			value := iter.Value()
			wr.bytes += int64(len(key) + len(value))
			iter.Prev()
			wr.hist.Observe(time.Since(t0))
			return true
		},
		func(workerID int, wr *workerResult) {
			iter := iters[workerID]
			if iter == nil {
				return
			}
			if err := iter.Error(); err != nil {
				wr.errors++
			}
			if err := iter.Close(); err != nil {
				wr.errors++
			}
		},
	)

	requested := int64(readsPerThread) * int64(cfg.threads)
	misses := requested - merged.ops
	if misses < 0 {
		misses = 0
	}
	return finalizeResult(requested, merged.ops, merged.errors, misses, merged.bytes, time.Since(start), merged.elapsed, merged.hist)
}

func readsPerThread(cfg config) int {
	if cfg.reads >= 0 {
		return cfg.reads
	}
	return cfg.num
}

func runWorkers(
	opsPerThread int,
	threads int,
	seed int64,
	fn func(workerID int, i int, wr *workerResult, rng *rand.Rand) bool,
	done func(workerID int, wr *workerResult),
) workerResult {
	var wg sync.WaitGroup
	out := make(chan workerResult, threads)

	for wid := 0; wid < threads; wid++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			wr := workerResult{hist: newLatencyHistogram()}
			rng := rand.New(rand.NewSource(seed + int64(workerID)))
			begin := time.Now()

			for i := 0; i < opsPerThread; i++ {
				if !fn(workerID, i, &wr, rng) {
					break
				}
				wr.ops++
			}

			if done != nil {
				done(workerID, &wr)
			}

			wr.elapsed = time.Since(begin)
			out <- wr
		}(wid)
	}

	wg.Wait()
	close(out)

	merged := workerResult{hist: newLatencyHistogram()}
	for wr := range out {
		merged.ops += wr.ops
		merged.errors += wr.errors
		merged.misses += wr.misses
		merged.bytes += wr.bytes
		merged.elapsed += wr.elapsed
		merged.hist.Merge(wr.hist)
	}
	return merged
}
