package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ls4154/golsm"
	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type config struct {
	dbPath         string
	duration       time.Duration
	reopenInterval time.Duration
	reportInterval time.Duration
	verifyInterval time.Duration
	writerDelay    time.Duration

	writers       int
	readers       int
	iterators     int
	verifySamples int
	iteratorSteps int

	keySpace      int
	valueSize     int
	valueJitter   int
	deletePercent int
	seekPercent   int

	maxDiskMB          int64
	seed               int64
	syncWrites         bool
	reuseDB            bool
	finalCheck         bool
	finalMaxMismatches int
	finalProgressEvery int
	partitionedWriters bool
	writeBuffer        int
	maxFileSize        uint64
	compression        db.CompressionType
	bloomBits          int
	compressionRaw     string
}

type stats struct {
	writes     atomic.Uint64
	puts       atomic.Uint64
	deletes    atomic.Uint64
	reads      atomic.Uint64
	gets       atomic.Uint64
	seeks      atomic.Uint64
	iterProbes atomic.Uint64
	iterSteps  atomic.Uint64
	verifyRuns atomic.Uint64
	errors     atomic.Uint64
	mismatches atomic.Uint64
}

type runError struct {
	mu  sync.Mutex
	err error
}

func (r *runError) set(err error, cancel context.CancelFunc) {
	if err == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	r.err = err
	cancel()
}

func (r *runError) get() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

type oracleState struct {
	mu sync.RWMutex
	m  map[int][]byte
}

func newOracleState(capacity int) *oracleState {
	return &oracleState{m: make(map[int][]byte, capacity)}
}

func (o *oracleState) Set(idx int, value []byte) {
	o.mu.Lock()
	o.m[idx] = cloneBytes(value)
	o.mu.Unlock()
}

func (o *oracleState) Delete(idx int) {
	o.mu.Lock()
	delete(o.m, idx)
	o.mu.Unlock()
}

func (o *oracleState) Get(idx int) ([]byte, bool) {
	o.mu.RLock()
	v, ok := o.m[idx]
	o.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return cloneBytes(v), true
}

func (o *oracleState) Len() int {
	o.mu.RLock()
	n := len(o.m)
	o.mu.RUnlock()
	return n
}

func main() {
	cfg := parseFlags()

	if err := validateConfig(cfg); err != nil {
		fatalf("%v", err)
	}

	if !cfg.reuseDB {
		if err := os.RemoveAll(cfg.dbPath); err != nil {
			fatalf("remove db path: %v", err)
		}
	}
	if err := os.MkdirAll(cfg.dbPath, 0o755); err != nil {
		fatalf("mkdir db path: %v", err)
	}

	keys := buildKeys(cfg.keySpace)
	oracle := newOracleState(cfg.keySpace)

	if cfg.reuseDB {
		ldb, err := openDB(cfg)
		if err != nil {
			fatalf("open db for oracle bootstrap: %v", err)
		}
		if err := bootstrapOracle(ldb, keys, oracle); err != nil {
			_ = ldb.Close()
			fatalf("bootstrap oracle: %v", err)
		}
		if err := ldb.Close(); err != nil {
			fatalf("close db after bootstrap: %v", err)
		}
	}

	fmt.Printf("stressrw: db=%s duration=%s reopen_interval=%s writers=%d readers=%d iterators=%d keyspace=%d value=%d+%d delete=%d%% seek=%d%% writer_delay=%s compression=%s bloom_bits=%d max_disk_mb=%d partitioned_writers=%t final_max_mismatches=%d\n",
		cfg.dbPath,
		cfg.duration,
		cfg.reopenInterval,
		cfg.writers,
		cfg.readers,
		cfg.iterators,
		cfg.keySpace,
		cfg.valueSize,
		cfg.valueJitter,
		cfg.deletePercent,
		cfg.seekPercent,
		cfg.writerDelay,
		compressionName(cfg.compression),
		cfg.bloomBits,
		cfg.maxDiskMB,
		cfg.partitionedWriters,
		cfg.finalMaxMismatches,
	)

	start := time.Now()
	remaining := cfg.duration
	epoch := 1
	for remaining > 0 {
		epochDur := remaining
		if cfg.reopenInterval > 0 && epochDur > cfg.reopenInterval {
			epochDur = cfg.reopenInterval
		}

		ldb, err := openDB(cfg)
		if err != nil {
			fatalf("open db (epoch=%d): %v", epoch, err)
		}

		fmt.Printf("stressrw: epoch=%d start duration=%s\n", epoch, epochDur)
		err = runEpoch(ldb, cfg, keys, oracle, epochDur, epoch)
		closeErr := ldb.Close()
		if err != nil {
			fatalf("epoch=%d failed: %v", epoch, err)
		}
		if closeErr != nil {
			fatalf("close db (epoch=%d): %v", epoch, closeErr)
		}

		remaining -= epochDur
		epoch++
	}

	fmt.Printf("stressrw: workload done elapsed=%s oracle_keys=%d\n", time.Since(start).Round(time.Millisecond), oracle.Len())

	if err := finalScanCheck(cfg); err != nil {
		fatalf("final scan check: %v", err)
	}
	if cfg.finalCheck {
		if err := finalOracleCheck(cfg, keys, oracle); err != nil {
			fatalf("final oracle check: %v", err)
		}
	}

	sz, err := dirSize(cfg.dbPath)
	if err != nil {
		fatalf("final size check: %v", err)
	}
	fmt.Printf("stressrw: PASS elapsed=%s final_size=%.1fMB\n", time.Since(start).Round(time.Millisecond), float64(sz)/(1024.0*1024.0))
}

func runEpoch(ldb db.DB, cfg config, keys [][]byte, oracle *oracleState, epochDur time.Duration, epoch int) error {
	ctx, cancel := context.WithTimeout(context.Background(), epochDur)
	defer cancel()

	rs := &runError{}
	st := &stats{}
	epochStart := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < cfg.writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			writerWorker(ctx, cancel, rs, st, ldb, cfg, keys, oracle, id)
		}(i)
	}
	for i := 0; i < cfg.readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			readerWorker(ctx, cancel, rs, st, ldb, cfg, keys, id)
		}(i)
	}
	for i := 0; i < cfg.iterators; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			iteratorWorker(ctx, cancel, rs, st, ldb, cfg, keys, id)
		}(i)
	}
	if cfg.verifyInterval > 0 && cfg.verifySamples > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshotVerifier(ctx, cancel, rs, st, ldb, cfg, keys)
		}()
	}
	if cfg.reportInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reporter(ctx, cancel, rs, st, cfg, epoch, epochStart)
		}()
	}

	<-ctx.Done()
	wg.Wait()

	if err := rs.get(); err != nil {
		return err
	}

	elapsed := time.Since(epochStart).Seconds()
	writes := st.writes.Load()
	reads := st.reads.Load()
	total := writes + reads
	rate := 0.0
	if elapsed > 0 {
		rate = float64(total) / elapsed
	}
	fmt.Printf("stressrw: epoch=%d done ops=%d (w=%d r=%d) rate=%.0f/s put=%d del=%d get=%d seek=%d iter=%d verify=%d errors=%d mismatches=%d\n",
		epoch,
		total,
		writes,
		reads,
		rate,
		st.puts.Load(),
		st.deletes.Load(),
		st.gets.Load(),
		st.seeks.Load(),
		st.iterProbes.Load(),
		st.verifyRuns.Load(),
		st.errors.Load(),
		st.mismatches.Load(),
	)

	return nil
}

func writerWorker(ctx context.Context, cancel context.CancelFunc, rs *runError, st *stats, ldb db.DB, cfg config, keys [][]byte, oracle *oracleState, id int) {
	r := rand.New(rand.NewSource(cfg.seed + 1000 + int64(id)*7919))
	wo := &db.WriteOptions{Sync: cfg.syncWrites}
	start, end := 0, cfg.keySpace
	if cfg.partitionedWriters {
		start, end = writerKeyRange(cfg.keySpace, cfg.writers, id)
		if end <= start {
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if cfg.writerDelay > 0 {
			time.Sleep(cfg.writerDelay)
		}

		idx := start + r.Intn(end-start)
		key := keys[idx]

		if r.Intn(100) < cfg.deletePercent {
			if err := ldb.Delete(key, wo); err != nil {
				st.errors.Add(1)
				rs.set(fmt.Errorf("writer delete: %w", err), cancel)
				return
			}
			oracle.Delete(idx)
			st.deletes.Add(1)
		} else {
			value := randomValue(r, cfg.valueSize, cfg.valueJitter)
			if err := ldb.Put(key, value, wo); err != nil {
				st.errors.Add(1)
				rs.set(fmt.Errorf("writer put: %w", err), cancel)
				return
			}
			oracle.Set(idx, value)
			st.puts.Add(1)
		}

		st.writes.Add(1)
	}
}

func readerWorker(ctx context.Context, cancel context.CancelFunc, rs *runError, st *stats, ldb db.DB, cfg config, keys [][]byte, id int) {
	r := rand.New(rand.NewSource(cfg.seed + 2000 + int64(id)*104729))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		idx := r.Intn(cfg.keySpace)
		key := keys[idx]

		if r.Intn(100) < cfg.seekPercent {
			if err := doSeekProbe(ldb, key); err != nil {
				st.errors.Add(1)
				if errors.Is(err, errInvariant) {
					st.mismatches.Add(1)
				}
				rs.set(fmt.Errorf("reader seek probe: %w", err), cancel)
				return
			}
			st.seeks.Add(1)
		} else {
			if _, err := ldb.Get(key, nil); err != nil && !errors.Is(err, db.ErrNotFound) {
				st.errors.Add(1)
				rs.set(fmt.Errorf("reader get: %w", err), cancel)
				return
			}
			st.gets.Add(1)
		}

		st.reads.Add(1)
	}
}

func iteratorWorker(ctx context.Context, cancel context.CancelFunc, rs *runError, st *stats, ldb db.DB, cfg config, keys [][]byte, id int) {
	r := rand.New(rand.NewSource(cfg.seed + 3000 + int64(id)*1301081))
	steps := cfg.iteratorSteps
	if steps <= 0 {
		steps = 8
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		it, err := ldb.NewIterator(nil)
		if err != nil {
			st.errors.Add(1)
			rs.set(fmt.Errorf("iterator open: %w", err), cancel)
			return
		}

		it.Seek(keys[r.Intn(cfg.keySpace)])
		var prev []byte
		for i := 0; i < steps && it.Valid(); i++ {
			cur := it.Key()
			if prev != nil && bytes.Compare(prev, cur) >= 0 {
				_ = it.Close()
				st.errors.Add(1)
				st.mismatches.Add(1)
				rs.set(fmt.Errorf("iterator monotonicity: %w", errInvariant), cancel)
				return
			}
			prev = cloneBytes(cur)
			it.Next()
			st.iterSteps.Add(1)
		}

		if err := it.Error(); err != nil {
			_ = it.Close()
			st.errors.Add(1)
			rs.set(fmt.Errorf("iterator error: %w", err), cancel)
			return
		}
		if err := it.Close(); err != nil {
			st.errors.Add(1)
			rs.set(fmt.Errorf("iterator close: %w", err), cancel)
			return
		}

		st.iterProbes.Add(1)
	}
}

func snapshotVerifier(ctx context.Context, cancel context.CancelFunc, rs *runError, st *stats, ldb db.DB, cfg config, keys [][]byte) {
	ticker := time.NewTicker(cfg.verifyInterval)
	defer ticker.Stop()

	r := rand.New(rand.NewSource(cfg.seed + 4000))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		snap := ldb.GetSnapshot()
		ro := &db.ReadOptions{Snapshot: snap}

		for i := 0; i < cfg.verifySamples; i++ {
			idx := r.Intn(cfg.keySpace)
			key := keys[idx]

			got, gerr := ldb.Get(key, ro)
			if gerr != nil && !errors.Is(gerr, db.ErrNotFound) {
				snap.Release()
				st.errors.Add(1)
				rs.set(fmt.Errorf("verify get: %w", gerr), cancel)
				return
			}

			it, err := ldb.NewIterator(ro)
			if err != nil {
				snap.Release()
				st.errors.Add(1)
				rs.set(fmt.Errorf("verify iterator open: %w", err), cancel)
				return
			}
			it.Seek(key)

			if gerr == nil {
				if !it.Valid() || !bytes.Equal(it.Key(), key) || !bytes.Equal(it.Value(), got) {
					_ = it.Close()
					snap.Release()
					st.errors.Add(1)
					st.mismatches.Add(1)
					rs.set(fmt.Errorf("verify mismatch get-vs-seek: %w", errInvariant), cancel)
					return
				}
			} else {
				if it.Valid() && bytes.Equal(it.Key(), key) {
					_ = it.Close()
					snap.Release()
					st.errors.Add(1)
					st.mismatches.Add(1)
					rs.set(fmt.Errorf("verify mismatch expected notfound but seek found key: %w", errInvariant), cancel)
					return
				}
			}

			if it.Valid() {
				cur := cloneBytes(it.Key())
				it.Prev()
				if it.Valid() && bytes.Compare(it.Key(), cur) >= 0 {
					_ = it.Close()
					snap.Release()
					st.errors.Add(1)
					st.mismatches.Add(1)
					rs.set(fmt.Errorf("verify mismatch prev-order: %w", errInvariant), cancel)
					return
				}
			}

			if err := it.Error(); err != nil {
				_ = it.Close()
				snap.Release()
				st.errors.Add(1)
				rs.set(fmt.Errorf("verify iterator error: %w", err), cancel)
				return
			}
			if err := it.Close(); err != nil {
				snap.Release()
				st.errors.Add(1)
				rs.set(fmt.Errorf("verify iterator close: %w", err), cancel)
				return
			}
		}

		snap.Release()
		st.verifyRuns.Add(1)
	}
}

func reporter(ctx context.Context, cancel context.CancelFunc, rs *runError, st *stats, cfg config, epoch int, epochStart time.Time) {
	ticker := time.NewTicker(cfg.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(epochStart).Seconds()
			writes := st.writes.Load()
			reads := st.reads.Load()
			total := writes + reads
			rate := 0.0
			if elapsed > 0 {
				rate = float64(total) / elapsed
			}

			sz, err := dirSize(cfg.dbPath)
			if err != nil {
				st.errors.Add(1)
				rs.set(fmt.Errorf("report dir size: %w", err), cancel)
				return
			}

			fmt.Printf("stressrw: epoch=%d elapsed=%s ops=%d rate=%.0f/s w=%d r=%d put=%d del=%d get=%d seek=%d iter=%d verify=%d err=%d mismatch=%d size=%.1fMB\n",
				epoch,
				time.Since(epochStart).Round(time.Second),
				total,
				rate,
				writes,
				reads,
				st.puts.Load(),
				st.deletes.Load(),
				st.gets.Load(),
				st.seeks.Load(),
				st.iterProbes.Load(),
				st.verifyRuns.Load(),
				st.errors.Load(),
				st.mismatches.Load(),
				float64(sz)/(1024.0*1024.0),
			)

			if cfg.maxDiskMB > 0 && sz > cfg.maxDiskMB*1024*1024 {
				st.errors.Add(1)
				rs.set(fmt.Errorf("db size exceeds limit: size=%dMB limit=%dMB", sz/(1024*1024), cfg.maxDiskMB), cancel)
				return
			}
		}
	}
}

func doSeekProbe(ldb db.DB, target []byte) error {
	it, err := ldb.NewIterator(nil)
	if err != nil {
		return err
	}
	defer it.Close()

	it.Seek(target)
	if it.Valid() && bytes.Compare(it.Key(), target) < 0 {
		return fmt.Errorf("seek returned key smaller than target: %w", errInvariant)
	}

	if it.Valid() {
		cur := cloneBytes(it.Key())
		it.Prev()
		if it.Valid() && bytes.Compare(it.Key(), cur) >= 0 {
			return fmt.Errorf("prev did not move backward: %w", errInvariant)
		}
	}

	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

func bootstrapOracle(ldb db.DB, keys [][]byte, oracle *oracleState) error {
	for idx, key := range keys {
		v, err := ldb.Get(key, nil)
		if err == nil {
			oracle.Set(idx, v)
			continue
		}
		if errors.Is(err, db.ErrNotFound) {
			continue
		}
		return err
	}
	return nil
}

func finalScanCheck(cfg config) error {
	ldb, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer ldb.Close()

	it, err := ldb.NewIterator(nil)
	if err != nil {
		return err
	}
	defer it.Close()

	var prev []byte
	count := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		cur := it.Key()
		if prev != nil && bytes.Compare(prev, cur) >= 0 {
			return fmt.Errorf("final scan order violation at #%d", count)
		}
		prev = cloneBytes(cur)
		count++
	}
	if err := it.Error(); err != nil {
		return err
	}

	fmt.Printf("stressrw: final scan entries=%d\n", count)
	return nil
}

func finalOracleCheck(cfg config, keys [][]byte, oracle *oracleState) error {
	if cfg.reuseDB {
		fmt.Println("stressrw: final oracle check skipped (reuse_db=true)")
		return nil
	}

	ldb, err := openDB(cfg)
	if err != nil {
		return err
	}
	defer ldb.Close()

	start := time.Now()
	mismatches := 0
	for idx, key := range keys {
		if cfg.finalProgressEvery > 0 && (idx+1)%cfg.finalProgressEvery == 0 {
			fmt.Printf("stressrw: final oracle progress checked=%d/%d mismatches=%d elapsed=%s\n",
				idx+1, len(keys), mismatches, time.Since(start).Round(time.Second))
		}

		expected, ok := oracle.Get(idx)
		got, gerr := ldb.Get(key, nil)
		if ok {
			if gerr != nil {
				mismatches++
				if mismatches <= 5 {
					fmt.Printf("stressrw: mismatch key=%q expected=present get_err=%v\n", key, gerr)
				}
				if cfg.finalMaxMismatches > 0 && mismatches >= cfg.finalMaxMismatches {
					return fmt.Errorf("oracle mismatches=%d (checked=%d/%d)", mismatches, idx+1, len(keys))
				}
				continue
			}
			if !bytes.Equal(expected, got) {
				mismatches++
				if mismatches <= 5 {
					fmt.Printf("stressrw: mismatch key=%q expected_len=%d got_len=%d\n", key, len(expected), len(got))
				}
				if cfg.finalMaxMismatches > 0 && mismatches >= cfg.finalMaxMismatches {
					return fmt.Errorf("oracle mismatches=%d (checked=%d/%d)", mismatches, idx+1, len(keys))
				}
			}
			continue
		}

		if gerr == nil {
			mismatches++
			if mismatches <= 5 {
				fmt.Printf("stressrw: mismatch key=%q expected=notfound got=present\n", key)
			}
			if cfg.finalMaxMismatches > 0 && mismatches >= cfg.finalMaxMismatches {
				return fmt.Errorf("oracle mismatches=%d (checked=%d/%d)", mismatches, idx+1, len(keys))
			}
			continue
		}
		if !errors.Is(gerr, db.ErrNotFound) {
			mismatches++
			if mismatches <= 5 {
				fmt.Printf("stressrw: mismatch key=%q unexpected_err=%v\n", key, gerr)
			}
			if cfg.finalMaxMismatches > 0 && mismatches >= cfg.finalMaxMismatches {
				return fmt.Errorf("oracle mismatches=%d (checked=%d/%d)", mismatches, idx+1, len(keys))
			}
		}
	}

	if mismatches > 0 {
		return fmt.Errorf("oracle mismatches=%d", mismatches)
	}
	fmt.Printf("stressrw: final oracle check ok (keys=%d)\n", len(keys))
	return nil
}

func openDB(cfg config) (db.DB, error) {
	opt := db.DefaultOptions()
	opt.CreateIfMissing = true
	opt.ErrorIfExists = false
	opt.Comparator = util.BytewiseComparator
	opt.WriteBufferSize = cfg.writeBuffer
	if cfg.maxFileSize > 0 {
		opt.MaxFileSize = cfg.maxFileSize
	}
	opt.Compression = cfg.compression
	if cfg.bloomBits > 0 {
		opt.FilterPolicy = util.NewBloomFilterPolicy(cfg.bloomBits)
	}

	return golsm.Open(opt, cfg.dbPath)
}

func buildKeys(keySpace int) [][]byte {
	keys := make([][]byte, keySpace)
	for i := 0; i < keySpace; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
	}
	return keys
}

func randomValue(r *rand.Rand, base, jitter int) []byte {
	n := base
	if jitter > 0 {
		n += r.Intn(jitter + 1)
	}
	if n <= 0 {
		return nil
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	return b
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

func parseFlags() config {
	cfg := config{}
	def := db.DefaultOptions()

	flag.StringVar(&cfg.dbPath, "db", "/tmp/golsm-stress", "database path")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Minute, "total stress duration")
	flag.DurationVar(&cfg.reopenInterval, "reopen-interval", 0, "close/open interval during run (0 disables)")
	flag.DurationVar(&cfg.reportInterval, "report-interval", 5*time.Second, "progress report interval (0 disables)")
	flag.DurationVar(&cfg.verifyInterval, "verify-interval", 7*time.Second, "snapshot consistency verify interval (0 disables)")
	flag.DurationVar(&cfg.writerDelay, "writer-delay", 500*time.Microsecond, "delay per writer op to limit write pressure")

	flag.IntVar(&cfg.writers, "writers", 4, "number of concurrent writer goroutines")
	flag.IntVar(&cfg.readers, "readers", 12, "number of concurrent get/seek reader goroutines")
	flag.IntVar(&cfg.iterators, "iterators", 4, "number of concurrent iterator probe goroutines")
	flag.IntVar(&cfg.verifySamples, "verify-samples", 32, "number of keys checked per verify run")
	flag.IntVar(&cfg.iteratorSteps, "iterator-steps", 16, "steps per iterator probe")

	flag.IntVar(&cfg.keySpace, "keyspace", 50000, "fixed keyspace size (caps live dataset)")
	flag.IntVar(&cfg.valueSize, "value-size", 64, "base value size")
	flag.IntVar(&cfg.valueJitter, "value-jitter", 32, "random additional value bytes")
	flag.IntVar(&cfg.deletePercent, "delete-percent", 20, "delete ratio [0..100]")
	flag.IntVar(&cfg.seekPercent, "seek-percent", 35, "seek ratio among read ops [0..100]")

	flag.Int64Var(&cfg.maxDiskMB, "max-disk-mb", 2048, "stop run if DB directory exceeds this size in MB (0 disables)")
	flag.Int64Var(&cfg.seed, "seed", 20260222, "random seed")
	flag.BoolVar(&cfg.syncWrites, "sync", false, "use sync writes")
	flag.BoolVar(&cfg.reuseDB, "reuse-db", false, "reuse existing DB instead of deleting at start")
	flag.BoolVar(&cfg.finalCheck, "final-check", true, "run final keyspace oracle check")
	flag.IntVar(&cfg.finalMaxMismatches, "final-max-mismatches", 1, "fail fast after this many final-check mismatches (0 disables)")
	flag.IntVar(&cfg.finalProgressEvery, "final-progress-every", 1000000, "progress print interval (keys) during final oracle check (0 disables)")
	flag.BoolVar(&cfg.partitionedWriters, "partitioned-writers", true, "assign disjoint key ranges per writer for deterministic oracle tracking")
	flag.IntVar(&cfg.writeBuffer, "write-buffer-size", def.WriteBufferSize, "db write buffer size")
	flag.Uint64Var(&cfg.maxFileSize, "max-file-size", def.MaxFileSize, "max sstable file size")
	flag.IntVar(&cfg.bloomBits, "bloom-bits", 10, "bloom bits per key (0 disables)")
	flag.StringVar(&cfg.compressionRaw, "compression", "no", "compression: no|snappy")
	flag.Parse()

	cfg.compression = parseCompression(cfg.compressionRaw)
	return cfg
}

func validateConfig(cfg config) error {
	if cfg.duration <= 0 {
		return fmt.Errorf("duration must be > 0")
	}
	if cfg.reopenInterval < 0 {
		return fmt.Errorf("reopen-interval must be >= 0")
	}
	if cfg.reportInterval < 0 {
		return fmt.Errorf("report-interval must be >= 0")
	}
	if cfg.verifyInterval < 0 {
		return fmt.Errorf("verify-interval must be >= 0")
	}
	if cfg.writers < 0 || cfg.readers < 0 || cfg.iterators < 0 {
		return fmt.Errorf("workers must be >= 0")
	}
	if cfg.writers+cfg.readers+cfg.iterators == 0 {
		return fmt.Errorf("at least one worker required")
	}
	if cfg.keySpace <= 0 {
		return fmt.Errorf("keyspace must be > 0")
	}
	if cfg.valueSize < 0 || cfg.valueJitter < 0 {
		return fmt.Errorf("value-size/value-jitter must be >= 0")
	}
	if cfg.deletePercent < 0 || cfg.deletePercent > 100 {
		return fmt.Errorf("delete-percent must be in [0,100]")
	}
	if cfg.seekPercent < 0 || cfg.seekPercent > 100 {
		return fmt.Errorf("seek-percent must be in [0,100]")
	}
	if cfg.writeBuffer <= 0 {
		return fmt.Errorf("write-buffer-size must be > 0")
	}
	if cfg.maxFileSize < 1024 {
		return fmt.Errorf("max-file-size must be >= 1024")
	}
	if cfg.bloomBits < 0 {
		return fmt.Errorf("bloom-bits must be >= 0")
	}
	if cfg.maxDiskMB < 0 {
		return fmt.Errorf("max-disk-mb must be >= 0")
	}
	if cfg.finalMaxMismatches < 0 {
		return fmt.Errorf("final-max-mismatches must be >= 0")
	}
	if cfg.finalProgressEvery < 0 {
		return fmt.Errorf("final-progress-every must be >= 0")
	}
	return nil
}

func parseCompression(raw string) db.CompressionType {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "no", "none":
		return db.NoCompression
	case "snappy":
		return db.SnappyCompression
	default:
		fatalf("invalid compression %q (allowed: no|snappy)", raw)
		return db.NoCompression
	}
}

func compressionName(t db.CompressionType) string {
	switch t {
	case db.SnappyCompression:
		return "snappy"
	default:
		return "no"
	}
}

var errInvariant = errors.New("invariant violation")

func writerKeyRange(keySpace, writers, id int) (int, int) {
	if keySpace <= 0 || writers <= 0 || id < 0 || id >= writers {
		return 0, 0
	}
	base := keySpace / writers
	rem := keySpace % writers
	start := id*base + minInt(id, rem)
	end := start + base
	if id < rem {
		end++
	}
	return start, end
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "stressrw: "+format+"\n", args...)
	os.Exit(1)
}
