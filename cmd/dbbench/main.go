package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ls4154/golsm"
	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type config struct {
	dbPath           string
	benchmarks       []string
	num              int
	reads            int
	valueSize        int
	threads          int
	writeBufferSize  int
	blockSize        int
	blockRestart     int
	maxFileSize      uint64
	cacheSize        int
	openFiles        int
	bloomBits        int
	paranoidChecks   bool
	compression      db.CompressionType
	compressionRatio float64
	histogram        bool
	useExistingDB    bool
	sync             bool
	freshDB          bool
	seed             int64
}

func main() {
	cfg := parseFlags()
	printBanner(cfg)
	printHeader(cfg)

	for _, name := range cfg.benchmarks {
		spec, err := benchSpecFor(name)
		if err != nil {
			fatalf("%v", err)
		}

		shouldFresh := cfg.freshDB || spec.freshDBByDefault
		if shouldFresh {
			if cfg.useExistingDB {
				fmt.Printf("%-12s %12s\n", name, "skipped (--use_existing_db=true)")
				continue
			}
			if err := os.RemoveAll(cfg.dbPath); err != nil {
				fatalf("remove db dir: %v", err)
			}
		}

		ldb, err := openDB(cfg)
		if err != nil {
			fatalf("open db for %s: %v", name, err)
		}

		r, runErr := runBenchmark(ldb, cfg, spec)
		closeErr := ldb.Close()
		if runErr != nil {
			fatalf("%s: %v", name, runErr)
		}
		if closeErr != nil {
			fatalf("close db after %s: %v", name, closeErr)
		}
		printResult(cfg, name, r)
	}
}

func openDB(cfg config) (db.DB, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.dbPath), 0o755); err != nil {
		return nil, err
	}
	if cfg.useExistingDB {
		current := filepath.Join(cfg.dbPath, "CURRENT")
		if _, err := os.Stat(current); err != nil {
			return nil, fmt.Errorf("CURRENT not found at %s (use_existing_db=true)", current)
		}
	}

	opt := db.DefaultOptions()
	opt.CreateIfMissing = !cfg.useExistingDB
	opt.ParanoidChecks = cfg.paranoidChecks
	opt.WriteBufferSize = cfg.writeBufferSize
	opt.BlockSize = cfg.blockSize
	opt.BlockRestartInterval = cfg.blockRestart
	opt.Compression = cfg.compression
	if cfg.maxFileSize > 0 {
		opt.MaxFileSize = cfg.maxFileSize
	}
	if cfg.cacheSize >= 0 {
		opt.BlockCacheSize = cfg.cacheSize
	}
	if cfg.openFiles > 0 {
		opt.MaxOpenFiles = cfg.openFiles
	}
	if cfg.bloomBits > 0 {
		opt.FilterPolicy = util.NewBloomFilterPolicy(cfg.bloomBits)
	}

	return golsm.Open(opt, cfg.dbPath)
}

func printBanner(cfg config) {
	reads := cfg.reads
	if reads < 0 {
		reads = cfg.num
	}
	fmt.Printf("dbbench: db=%s num=%d reads=%d value_size=%d threads=%d seed=%d sync=%v compression=%s compression_ratio=%.2f block_size=%d block_restart_interval=%d cache_size=%d open_files=%d bloom_bits=%d paranoid_checks=%v\n",
		cfg.dbPath,
		cfg.num,
		reads,
		cfg.valueSize,
		cfg.threads,
		cfg.seed,
		cfg.sync,
		compressionName(cfg.compression),
		cfg.compressionRatio,
		cfg.blockSize,
		cfg.blockRestart,
		cfg.cacheSize,
		cfg.openFiles,
		cfg.bloomBits,
		cfg.paranoidChecks)
}

func printHeader(cfg config) {
	if cfg.histogram {
		fmt.Printf("%-12s %12s %12s %12s %12s %10s %8s %8s %8s %8s\n",
			"benchmark", "ops", "ops/sec", "MB/sec", "avg(us)", "errors", "miss", "p50", "p95", "p99")
		return
	}
	fmt.Printf("%-12s %12s %12s %12s %12s %10s %8s\n",
		"benchmark", "ops", "ops/sec", "MB/sec", "avg(us)", "errors", "miss")
}

func printResult(cfg config, name string, r runResult) {
	if cfg.histogram {
		fmt.Printf("%-12s %12d %12.0f %12.2f %12.1f %10d %8d %8s %8s %8s\n",
			name,
			r.ops,
			r.opsPerSec,
			r.mbPerSec,
			r.avgMicros,
			r.errors,
			r.misses,
			formatDurationMicros(r.hist.P50()),
			formatDurationMicros(r.hist.P95()),
			formatDurationMicros(r.hist.P99()),
		)
	} else {
		fmt.Printf("%-12s %12d %12.0f %12.2f %12.1f %10d %8d\n",
			name,
			r.ops,
			r.opsPerSec,
			r.mbPerSec,
			r.avgMicros,
			r.errors,
			r.misses,
		)
	}
	if r.message != "" {
		fmt.Printf("%-12s %s\n", "", r.message)
	}
}

func parseFlags() config {
	var benchmarkList string
	var compression string
	cfg := config{}
	def := db.DefaultOptions()

	flag.StringVar(&cfg.dbPath, "db", "/tmp/golsm-bench", "database path")
	flag.StringVar(&benchmarkList, "benchmarks", "fillseq,readrandom", "comma-separated benchmark names")
	flag.IntVar(&cfg.num, "num", 100000, "operations per thread")
	flag.IntVar(&cfg.reads, "reads", -1, "read operations per thread (default: num)")
	flag.IntVar(&cfg.valueSize, "value_size", 100, "value size in bytes")
	flag.IntVar(&cfg.threads, "threads", 1, "number of worker goroutines")
	flag.IntVar(&cfg.writeBufferSize, "write_buffer_size", def.WriteBufferSize, "db write buffer size")
	flag.IntVar(&cfg.blockSize, "block_size", def.BlockSize, "sstable data block size")
	flag.IntVar(&cfg.blockRestart, "block_restart_interval", def.BlockRestartInterval, "restart interval for block key compression")
	flag.Uint64Var(&cfg.maxFileSize, "max_file_size", 0, "max table file size (0: use DB default)")
	flag.IntVar(&cfg.cacheSize, "cache_size", -1, "block cache size in bytes (-1: use DB default)")
	flag.IntVar(&cfg.openFiles, "open_files", 0, "max open files (0: use DB default)")
	flag.IntVar(&cfg.bloomBits, "bloom_bits", 0, "Bloom filter bits per key (0: disable filter)")
	flag.BoolVar(&cfg.paranoidChecks, "paranoid_checks", false, "enable paranoid checks")
	flag.StringVar(&compression, "compression", "no", "table compression: no|snappy")
	flag.Float64Var(&cfg.compressionRatio, "compression_ratio", 0.5, "value compression ratio for generated values")
	flag.BoolVar(&cfg.histogram, "histogram", false, "print latency histogram percentiles")
	flag.BoolVar(&cfg.useExistingDB, "use_existing_db", false, "do not destroy existing DB for fresh benchmarks")
	flag.BoolVar(&cfg.sync, "sync", false, "use sync writes")
	flag.BoolVar(&cfg.freshDB, "fresh_db", false, "force fresh DB for every benchmark")
	flag.Int64Var(&cfg.seed, "seed", 301, "rng seed")
	flag.Parse()

	cfg.benchmarks = parseBenchmarks(benchmarkList)

	if cfg.num <= 0 {
		fatalf("num must be > 0")
	}
	if cfg.reads < -1 {
		fatalf("reads must be >= -1")
	}
	if cfg.valueSize < 0 {
		fatalf("value_size must be >= 0")
	}
	if cfg.threads <= 0 {
		fatalf("threads must be > 0")
	}
	if cfg.writeBufferSize <= 0 {
		fatalf("write_buffer_size must be > 0")
	}
	if cfg.blockSize <= 0 {
		fatalf("block_size must be > 0")
	}
	if cfg.blockRestart <= 0 {
		fatalf("block_restart_interval must be > 0")
	}
	if cfg.maxFileSize > 0 && cfg.maxFileSize < 1024 {
		fatalf("max_file_size must be >= 1024 when specified")
	}
	if cfg.openFiles < 0 {
		fatalf("open_files must be >= 0")
	}
	if cfg.bloomBits < 0 {
		fatalf("bloom_bits must be >= 0")
	}
	if cfg.compressionRatio <= 0 {
		fatalf("compression_ratio must be > 0")
	}
	if len(cfg.benchmarks) == 0 {
		fatalf("benchmarks is empty")
	}

	cfg.compression = parseCompression(compression)
	return cfg
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

func parseBenchmarks(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	return out
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
