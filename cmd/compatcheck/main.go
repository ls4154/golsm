package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ls4154/golsm"
	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type config struct {
	workDir       string
	ops           int
	keySpace      int
	valueSize     int
	seed          int64
	reopenEvery   int
	pointQueries  int
	seekQueries   int
	bloomBits     int
	leveldbTool   string
	fullMatrix    bool
	crashRecovery bool
	crashAfter    int
	keep          bool
}

func main() {
	if handled, err := runInternalMode(os.Args[1:]); handled {
		if err != nil {
			die(err.Error())
		}
		return
	}

	cfg := parseFlags()

	if cfg.ops <= 0 || cfg.keySpace <= 0 || cfg.valueSize < 0 || cfg.pointQueries < 0 || cfg.seekQueries < 0 || cfg.crashAfter < 0 {
		die("invalid numeric flags")
	}

	if err := run(cfg); err != nil {
		die(err.Error())
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.workDir, "workdir", "tmp-compatcheck", "working directory")
	flag.IntVar(&cfg.ops, "ops", 200000, "number of put/delete operations")
	flag.IntVar(&cfg.keySpace, "keyspace", 50000, "key-space size for workload generation")
	flag.IntVar(&cfg.valueSize, "value-size", 200, "base value size in bytes")
	flag.Int64Var(&cfg.seed, "seed", 20260222, "random seed")
	flag.IntVar(&cfg.reopenEvery, "reopen-every", 5000, "reopen interval while writing (0 disables)")
	flag.IntVar(&cfg.pointQueries, "point-queries", 4000, "number of point-get queries")
	flag.IntVar(&cfg.seekQueries, "seek-queries", 4000, "number of seek queries")
	flag.IntVar(&cfg.bloomBits, "bloom-bits", 10, "bloom filter bits per key")
	flag.StringVar(&cfg.leveldbTool, "leveldb-tool", "", "path to prebuilt leveldb_kvtool binary")
	flag.BoolVar(&cfg.fullMatrix, "full-matrix", true, "run extra compression compatibility cases")
	flag.BoolVar(&cfg.crashRecovery, "crash-recovery", true, "run cross-engine crash recovery checks")
	flag.IntVar(&cfg.crashAfter, "crash-after", 0, "intentional crash point in applied ops (0 uses 90% of workload ops)")
	flag.BoolVar(&cfg.keep, "keep", false, "keep generated files")
	flag.Parse()
	return cfg
}

func run(cfg config) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}

	runDir := filepath.Join(root, cfg.workDir, fmt.Sprintf("seed-%d", cfg.seed))
	if err := os.RemoveAll(runDir); err != nil {
		return err
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return err
	}
	if !cfg.keep {
		defer os.RemoveAll(runDir)
	}

	workloadPath := filepath.Join(runDir, "workload.txt")
	queriesPath := filepath.Join(runDir, "queries.txt")
	if err := generateWorkload(workloadPath, cfg); err != nil {
		return fmt.Errorf("generate workload: %w", err)
	}
	if err := generateQueries(queriesPath, cfg); err != nil {
		return fmt.Errorf("generate queries: %w", err)
	}

	toolPath := cfg.leveldbTool
	if toolPath == "" {
		toolPath, err = ensureLevelDBTool(root, runDir)
		if err != nil {
			return fmt.Errorf("build leveldb helper: %w", err)
		}
	}

	crashAfter := resolveCrashAfter(cfg)
	fmt.Printf("compatcheck: seed=%d ops=%d keyspace=%d value=%d reopen_every=%d crash_recovery=%t crash_after=%d\n",
		cfg.seed, cfg.ops, cfg.keySpace, cfg.valueSize, cfg.reopenEvery, cfg.crashRecovery, crashAfter)
	fmt.Printf("compatcheck: workload=%s queries=%s\n", workloadPath, queriesPath)
	fmt.Printf("compatcheck: leveldb_tool=%s\n", toolPath)

	var errs []string
	if err := checkGolsmWrittenDB(
		cfg, toolPath, workloadPath, queriesPath, filepath.Join(runDir, "golsm-write-none"),
		"golsm(no-compression)->leveldb", db.NoCompression,
	); err != nil {
		fmt.Printf("compatcheck: FAIL golsm(no-compression)->leveldb: %v\n", err)
		errs = append(errs, err.Error())
	}
	if err := checkLevelDBWrittenDB(
		cfg, toolPath, workloadPath, queriesPath, filepath.Join(runDir, "leveldb-write-snappy"),
		"leveldb(snappy)->golsm", "snappy",
	); err != nil {
		fmt.Printf("compatcheck: FAIL leveldb(snappy)->golsm: %v\n", err)
		errs = append(errs, err.Error())
	}
	if cfg.fullMatrix {
		if err := checkGolsmWrittenDB(
			cfg, toolPath, workloadPath, queriesPath, filepath.Join(runDir, "golsm-write-snappy"),
			"golsm(snappy)->leveldb", db.SnappyCompression,
		); err != nil {
			fmt.Printf("compatcheck: FAIL golsm(snappy)->leveldb: %v\n", err)
			errs = append(errs, err.Error())
		}
		if err := checkLevelDBWrittenDB(
			cfg, toolPath, workloadPath, queriesPath, filepath.Join(runDir, "leveldb-write-none"),
			"leveldb(no-compression)->golsm", "none",
		); err != nil {
			fmt.Printf("compatcheck: FAIL leveldb(no-compression)->golsm: %v\n", err)
			errs = append(errs, err.Error())
		}
	}
	if cfg.crashRecovery {
		if err := checkGolsmCrashRecovery(
			cfg, toolPath, workloadPath, queriesPath, filepath.Join(runDir, "golsm-crash-write-none"),
			"golsm-crash(no-compression)->leveldb-recover", db.NoCompression, crashAfter,
		); err != nil {
			fmt.Printf("compatcheck: FAIL golsm-crash(no-compression)->leveldb-recover: %v\n", err)
			errs = append(errs, err.Error())
		}
		if err := checkLevelDBCrashRecovery(
			cfg, toolPath, workloadPath, queriesPath, filepath.Join(runDir, "leveldb-crash-write-snappy"),
			"leveldb-crash(snappy)->golsm-recover", "snappy", crashAfter,
		); err != nil {
			fmt.Printf("compatcheck: FAIL leveldb-crash(snappy)->golsm-recover: %v\n", err)
			errs = append(errs, err.Error())
		}
		if cfg.fullMatrix {
			if err := checkGolsmCrashRecovery(
				cfg, toolPath, workloadPath, queriesPath, filepath.Join(runDir, "golsm-crash-write-snappy"),
				"golsm-crash(snappy)->leveldb-recover", db.SnappyCompression, crashAfter,
			); err != nil {
				fmt.Printf("compatcheck: FAIL golsm-crash(snappy)->leveldb-recover: %v\n", err)
				errs = append(errs, err.Error())
			}
			if err := checkLevelDBCrashRecovery(
				cfg, toolPath, workloadPath, queriesPath, filepath.Join(runDir, "leveldb-crash-write-none"),
				"leveldb-crash(no-compression)->golsm-recover", "none", crashAfter,
			); err != nil {
				fmt.Printf("compatcheck: FAIL leveldb-crash(no-compression)->golsm-recover: %v\n", err)
				errs = append(errs, err.Error())
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("compatcheck failed:\n%s", strings.Join(errs, "\n"))
	}

	fmt.Println("compatcheck: PASS (bidirectional transcript match)")
	return nil
}

func resolveCrashAfter(cfg config) int {
	if cfg.crashAfter > 0 {
		return cfg.crashAfter
	}
	if cfg.ops <= 1 {
		return 1
	}
	// Leave enough steady-state writes while still guaranteeing an open WAL at crash time.
	v := cfg.ops - maxInt(1, cfg.ops/10)
	if v < 1 {
		return 1
	}
	return v
}

func checkGolsmWrittenDB(cfg config, toolPath, workloadPath, queriesPath, caseDir, label string, golsmCompression db.CompressionType) error {
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		return err
	}
	dbPath := filepath.Join(caseDir, "db")
	if err := applyWorkloadGo(dbPath, workloadPath, cfg, golsmCompression); err != nil {
		return fmt.Errorf("[%s] golsm apply failed: %w", label, err)
	}

	goTranscript := filepath.Join(caseDir, "golsm.transcript")
	cppTranscript := filepath.Join(caseDir, "leveldb.transcript")
	if err := writeTranscriptGo(dbPath, queriesPath, goTranscript, cfg); err != nil {
		return fmt.Errorf("[%s] golsm transcript failed: %w", label, err)
	}
	if err := runLevelDBTranscript(toolPath, dbPath, queriesPath, cppTranscript, cfg); err != nil {
		return fmt.Errorf("[%s] leveldb transcript failed: %w", label, err)
	}

	return compareTranscripts(label, goTranscript, cppTranscript)
}

func checkLevelDBWrittenDB(cfg config, toolPath, workloadPath, queriesPath, caseDir, label, leveldbCompression string) error {
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		return err
	}
	dbPath := filepath.Join(caseDir, "db")
	if err := runLevelDBApply(toolPath, dbPath, workloadPath, cfg, leveldbCompression); err != nil {
		return fmt.Errorf("[%s] leveldb apply failed: %w", label, err)
	}

	goTranscript := filepath.Join(caseDir, "golsm.transcript")
	cppTranscript := filepath.Join(caseDir, "leveldb.transcript")
	if err := runLevelDBTranscript(toolPath, dbPath, queriesPath, cppTranscript, cfg); err != nil {
		return fmt.Errorf("[%s] leveldb transcript failed: %w", label, err)
	}
	if err := writeTranscriptGo(dbPath, queriesPath, goTranscript, cfg); err != nil {
		return fmt.Errorf("[%s] golsm transcript failed: %w", label, err)
	}

	return compareTranscripts(label, goTranscript, cppTranscript)
}

func checkGolsmCrashRecovery(cfg config, toolPath, workloadPath, queriesPath, caseDir, label string, golsmCompression db.CompressionType, crashAfter int) error {
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		return err
	}
	dbPath := filepath.Join(caseDir, "db")
	if err := runGolsmCrashWriter(dbPath, workloadPath, cfg, golsmCompression, crashAfter); err != nil {
		return fmt.Errorf("[%s] golsm crash-writer failed: %w", label, err)
	}
	logPath, logSize, err := largestLogFile(dbPath)
	if err != nil {
		return fmt.Errorf("[%s] inspect wal before recovery: %w", label, err)
	}
	if logSize <= 0 {
		return fmt.Errorf("[%s] wal is empty before cross-engine recovery: %s", label, logPath)
	}
	fmt.Printf("compatcheck: %s pre-recovery wal=%s size=%d\n", label, logPath, logSize)

	goTranscript := filepath.Join(caseDir, "golsm.transcript")
	cppTranscript := filepath.Join(caseDir, "leveldb.transcript")
	// Cross-engine replay check: LevelDB opens first and replays golsm WAL.
	if err := runLevelDBTranscript(toolPath, dbPath, queriesPath, cppTranscript, cfg); err != nil {
		return fmt.Errorf("[%s] leveldb recovery+transcript failed: %w", label, err)
	}
	if err := writeTranscriptGo(dbPath, queriesPath, goTranscript, cfg); err != nil {
		return fmt.Errorf("[%s] golsm transcript failed: %w", label, err)
	}
	return compareTranscripts(label, goTranscript, cppTranscript)
}

func checkLevelDBCrashRecovery(cfg config, toolPath, workloadPath, queriesPath, caseDir, label, leveldbCompression string, crashAfter int) error {
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		return err
	}
	dbPath := filepath.Join(caseDir, "db")
	if err := runLevelDBApplyCrash(toolPath, dbPath, workloadPath, cfg, leveldbCompression, crashAfter); err != nil {
		return fmt.Errorf("[%s] leveldb crash-writer failed: %w", label, err)
	}
	logPath, logSize, err := largestLogFile(dbPath)
	if err != nil {
		return fmt.Errorf("[%s] inspect wal before recovery: %w", label, err)
	}
	if logSize <= 0 {
		return fmt.Errorf("[%s] wal is empty before cross-engine recovery: %s", label, logPath)
	}
	fmt.Printf("compatcheck: %s pre-recovery wal=%s size=%d\n", label, logPath, logSize)

	goTranscript := filepath.Join(caseDir, "golsm.transcript")
	cppTranscript := filepath.Join(caseDir, "leveldb.transcript")
	// Cross-engine replay check: golsm opens first and replays leveldb WAL.
	if err := writeTranscriptGo(dbPath, queriesPath, goTranscript, cfg); err != nil {
		return fmt.Errorf("[%s] golsm recovery+transcript failed: %w", label, err)
	}
	if err := runLevelDBTranscript(toolPath, dbPath, queriesPath, cppTranscript, cfg); err != nil {
		return fmt.Errorf("[%s] leveldb transcript failed: %w", label, err)
	}
	return compareTranscripts(label, goTranscript, cppTranscript)
}

func compareTranscripts(label, aPath, bPath string) error {
	aHash, aSize, err := hashFileSHA256(aPath)
	if err != nil {
		return err
	}
	bHash, bSize, err := hashFileSHA256(bPath)
	if err != nil {
		return err
	}
	fmt.Printf("compatcheck: %s sha256 golsm=%s leveldb=%s\n", label, aHash, bHash)

	if aHash != bHash {
		return fmt.Errorf("%s transcript mismatch (size golsm=%d leveldb=%d)\n  golsm: %s\n  leveldb: %s",
			label, aSize, bSize, aPath, bPath)
	}
	return nil
}

func ensureLevelDBTool(root, runDir string) (string, error) {
	leveldbSrc := filepath.Join(root, "leveldb")
	if _, err := os.Stat(filepath.Join(leveldbSrc, "CMakeLists.txt")); err != nil {
		return "", fmt.Errorf("leveldb source not found at %s", leveldbSrc)
	}

	cppSrc := filepath.Join(root, "cmd", "compatcheck", "cpp")
	buildDir := filepath.Join(runDir, "cpp-build")
	if err := os.MkdirAll(buildDir, 0o755); err != nil {
		return "", err
	}

	if err := runCmd(root, "cmake",
		"-S", cppSrc,
		"-B", buildDir,
		"-DLEVELDB_SRC_DIR="+leveldbSrc,
		"-DCMAKE_BUILD_TYPE=Release",
	); err != nil {
		return "", err
	}
	if err := runCmd(root, "cmake", "--build", buildDir, "--target", "leveldb_kvtool", "-j"); err != nil {
		return "", err
	}

	tool := filepath.Join(buildDir, "leveldb_kvtool")
	if _, err := os.Stat(tool); err != nil {
		return "", err
	}
	return tool, nil
}

func runLevelDBApply(toolPath, dbPath, workloadPath string, cfg config, compression string) error {
	args := []string{
		"apply",
		"--db", dbPath,
		"--workload", workloadPath,
		"--reopen-every", fmt.Sprintf("%d", cfg.reopenEvery),
		"--bloom-bits", fmt.Sprintf("%d", cfg.bloomBits),
		"--compression", compression,
	}
	return runCmd("", toolPath, args...)
}

func runLevelDBApplyCrash(toolPath, dbPath, workloadPath string, cfg config, compression string, crashAfter int) error {
	args := []string{
		"apply-crash",
		"--db", dbPath,
		"--workload", workloadPath,
		"--crash-after", fmt.Sprintf("%d", crashAfter),
		"--bloom-bits", fmt.Sprintf("%d", cfg.bloomBits),
		"--compression", compression,
	}
	return runCmdExpectExit("", toolPath, 97, args...)
}

func runLevelDBTranscript(toolPath, dbPath, queriesPath, outPath string, cfg config) error {
	args := []string{
		"transcript",
		"--db", dbPath,
		"--queries", queriesPath,
		"--out", outPath,
		"--bloom-bits", fmt.Sprintf("%d", cfg.bloomBits),
	}
	var lastErr error
	for i := 0; i < 3; i++ {
		if err := runCmd("", toolPath, args...); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func applyWorkloadGo(dbPath, workloadPath string, cfg config, compression db.CompressionType) error {
	openDB := func(createIfMissing bool) (db.DB, error) {
		opt := db.DefaultOptions()
		opt.CreateIfMissing = createIfMissing
		opt.ErrorIfExists = false
		opt.Comparator = util.BytewiseComparator
		opt.FilterPolicy = util.NewBloomFilterPolicy(cfg.bloomBits)
		opt.Compression = compression
		opt.WriteBufferSize = 1 << 20
		opt.MaxFileSize = 1 << 20
		return golsm.Open(opt, dbPath)
	}

	ldb, err := openDB(true)
	if err != nil {
		return err
	}

	f, err := os.Open(workloadPath)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	count := 0

	for sc.Scan() {
		typ, key, value, ok, err := parseLine(sc.Text())
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		switch typ {
		case 'P':
			if err := ldb.Put(key, value, nil); err != nil {
				return err
			}
		case 'D':
			if err := ldb.Delete(key, nil); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid workload op: %c", typ)
		}

		count++
		if cfg.reopenEvery > 0 && count%cfg.reopenEvery == 0 {
			if err := ldb.Close(); err != nil {
				return err
			}
			ldb, err = openDB(false)
			if err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		_ = ldb.Close()
		return err
	}

	return ldb.Close()
}

func runGolsmCrashWriter(dbPath, workloadPath string, cfg config, compression db.CompressionType, crashAfter int) error {
	exePath, err := os.Executable()
	if err != nil {
		return err
	}
	compressionText := "none"
	if compression == db.SnappyCompression {
		compressionText = "snappy"
	}
	args := []string{
		"internal-golsm-crash-writer",
		"--db", dbPath,
		"--workload", workloadPath,
		"--crash-after", fmt.Sprintf("%d", crashAfter),
		"--bloom-bits", fmt.Sprintf("%d", cfg.bloomBits),
		"--compression", compressionText,
	}
	return runCmdExpectExit("", exePath, 97, args...)
}

func writeTranscriptGo(dbPath, queriesPath, outPath string, cfg config) error {
	opt := db.DefaultOptions()
	opt.CreateIfMissing = false
	opt.ErrorIfExists = false
	opt.Comparator = util.BytewiseComparator
	opt.FilterPolicy = util.NewBloomFilterPolicy(cfg.bloomBits)

	ldb, err := golsm.Open(opt, dbPath)
	if err != nil {
		return err
	}
	defer ldb.Close()

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	tw := transcriptWriter{w: out}

	it, err := ldb.NewIterator(nil)
	if err != nil {
		return err
	}
	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		if err := tw.writeKV('F', it.Key(), it.Value()); err != nil {
			_ = it.Close()
			return err
		}
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		return err
	}
	if err := it.Close(); err != nil {
		return err
	}

	it, err = ldb.NewIterator(nil)
	if err != nil {
		return err
	}
	it.SeekToLast()
	for ; it.Valid(); it.Prev() {
		if err := tw.writeKV('R', it.Key(), it.Value()); err != nil {
			_ = it.Close()
			return err
		}
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		return err
	}
	if err := it.Close(); err != nil {
		return err
	}

	seekIt, err := ldb.NewIterator(nil)
	if err != nil {
		return err
	}
	defer seekIt.Close()

	qf, err := os.Open(queriesPath)
	if err != nil {
		return err
	}
	defer qf.Close()

	sc := bufio.NewScanner(qf)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		typ, key, _, ok, err := parseLine(sc.Text())
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		switch typ {
		case 'G':
			val, err := ldb.Get(key, nil)
			if err == nil {
				if err := tw.writePoint(key, true, val); err != nil {
					return err
				}
			} else if errors.Is(err, db.ErrNotFound) {
				if err := tw.writePoint(key, false, nil); err != nil {
					return err
				}
			} else {
				return err
			}
		case 'S':
			seekIt.Seek(key)
			if seekIt.Valid() {
				if err := tw.writeSeek('S', key, true, seekIt.Key(), seekIt.Value()); err != nil {
					return err
				}
				seekIt.Prev()
				if seekIt.Valid() {
					if err := tw.writeSeekNoTarget('P', 1, seekIt.Key(), seekIt.Value()); err != nil {
						return err
					}
				} else {
					if err := tw.writeSeekNoTarget('P', 0, nil, nil); err != nil {
						return err
					}
				}
			} else {
				if err := tw.writeSeek('S', key, false, nil, nil); err != nil {
					return err
				}
				if err := tw.writeSeekNoTarget('P', 2, nil, nil); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("invalid query op: %c", typ)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := seekIt.Error(); err != nil {
		return err
	}

	return nil
}

type transcriptWriter struct {
	w io.Writer
}

func (tw *transcriptWriter) writeKV(tag byte, key, value []byte) error {
	if _, err := tw.w.Write([]byte{tag}); err != nil {
		return err
	}
	if err := writeBytes(tw.w, key); err != nil {
		return err
	}
	return writeBytes(tw.w, value)
}

func (tw *transcriptWriter) writePoint(queryKey []byte, found bool, value []byte) error {
	if _, err := tw.w.Write([]byte{'G'}); err != nil {
		return err
	}
	if err := writeBytes(tw.w, queryKey); err != nil {
		return err
	}
	status := byte(0)
	if found {
		status = 1
	}
	if _, err := tw.w.Write([]byte{status}); err != nil {
		return err
	}
	if found {
		return writeBytes(tw.w, value)
	}
	return nil
}

func (tw *transcriptWriter) writeSeek(tag byte, target []byte, found bool, key, value []byte) error {
	if _, err := tw.w.Write([]byte{tag}); err != nil {
		return err
	}
	if err := writeBytes(tw.w, target); err != nil {
		return err
	}
	status := byte(0)
	if found {
		status = 1
	}
	if _, err := tw.w.Write([]byte{status}); err != nil {
		return err
	}
	if found {
		if err := writeBytes(tw.w, key); err != nil {
			return err
		}
		return writeBytes(tw.w, value)
	}
	return nil
}

func (tw *transcriptWriter) writeSeekNoTarget(tag byte, status byte, key, value []byte) error {
	if _, err := tw.w.Write([]byte{tag, status}); err != nil {
		return err
	}
	if status == 1 {
		if err := writeBytes(tw.w, key); err != nil {
			return err
		}
		return writeBytes(tw.w, value)
	}
	return nil
}

func writeBytes(w io.Writer, b []byte) error {
	var nbuf [8]byte
	binary.LittleEndian.PutUint64(nbuf[:], uint64(len(b)))
	if _, err := w.Write(nbuf[:]); err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	_, err := w.Write(b)
	return err
}

func hashFileSHA256(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func generateWorkload(path string, cfg config) error {
	r := rand.New(rand.NewSource(cfg.seed))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	for i := 0; i < cfg.ops; i++ {
		put := r.Intn(100) < 85
		key := randomKey(r, cfg.keySpace)
		if put {
			vlen := cfg.valueSize
			if vlen > 0 {
				vlen += r.Intn(maxInt(1, cfg.valueSize/4+1))
			}
			value := randomBytes(r, vlen)
			if _, err := fmt.Fprintf(w, "P %s %s\n", encodeToken(key), encodeToken(value)); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(w, "D %s\n", encodeToken(key)); err != nil {
				return err
			}
		}
	}

	// inject deterministic edge cases
	edges := [][]byte{
		{},
		[]byte("a"),
		[]byte("\x00"),
		bytes.Repeat([]byte("k"), 512),
	}
	for i, k := range edges {
		v := []byte(fmt.Sprintf("edge-value-%d", i))
		if _, err := fmt.Fprintf(w, "P %s %s\n", encodeToken(k), encodeToken(v)); err != nil {
			return err
		}
	}
	return nil
}

func generateQueries(path string, cfg config) error {
	r := rand.New(rand.NewSource(cfg.seed + 1))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	for i := 0; i < cfg.pointQueries; i++ {
		k := randomQueryKey(r, cfg.keySpace)
		if _, err := fmt.Fprintf(w, "G %s\n", encodeToken(k)); err != nil {
			return err
		}
	}
	for i := 0; i < cfg.seekQueries; i++ {
		k := randomSeekTarget(r, cfg.keySpace)
		if _, err := fmt.Fprintf(w, "S %s\n", encodeToken(k)); err != nil {
			return err
		}
	}

	return nil
}

func randomKey(r *rand.Rand, keySpace int) []byte {
	switch r.Intn(100) {
	case 0:
		return nil
	case 1:
		return []byte{"\x00"[0]}
	case 2:
		return bytes.Repeat([]byte("k"), 256)
	default:
		id := r.Intn(keySpace)
		return []byte(fmt.Sprintf("user-key-%08d", id))
	}
}

func randomQueryKey(r *rand.Rand, keySpace int) []byte {
	if r.Intn(100) < 65 {
		return randomKey(r, keySpace)
	}
	id := r.Intn(keySpace * 2)
	return []byte(fmt.Sprintf("missing-key-%08d", id))
}

func randomSeekTarget(r *rand.Rand, keySpace int) []byte {
	switch r.Intn(8) {
	case 0:
		return nil
	case 1:
		return []byte("user-key-00000000")
	case 2:
		return []byte("user-key-99999999")
	case 3:
		return []byte("zzzzzzzz")
	default:
		id := r.Intn(keySpace * 2)
		return []byte(fmt.Sprintf("user-key-%08d", id))
	}
}

func randomBytes(r *rand.Rand, n int) []byte {
	if n <= 0 {
		return nil
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	return b
}

func encodeToken(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	return hex.EncodeToString(b)
}

func decodeToken(tok string) ([]byte, error) {
	if tok == "-" {
		return nil, nil
	}
	return hex.DecodeString(tok)
}

func parseLine(line string) (typ byte, key, value []byte, ok bool, err error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return 0, nil, nil, false, nil
	}
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return 0, nil, nil, false, nil
	}
	if len(parts[0]) != 1 {
		return 0, nil, nil, false, fmt.Errorf("bad op: %q", line)
	}
	typ = parts[0][0]

	switch typ {
	case 'P':
		if len(parts) != 3 {
			return 0, nil, nil, false, fmt.Errorf("bad put line: %q", line)
		}
		key, err = decodeToken(parts[1])
		if err != nil {
			return 0, nil, nil, false, err
		}
		value, err = decodeToken(parts[2])
		if err != nil {
			return 0, nil, nil, false, err
		}
		return typ, key, value, true, nil
	case 'D', 'G', 'S':
		if len(parts) != 2 {
			return 0, nil, nil, false, fmt.Errorf("bad line: %q", line)
		}
		key, err = decodeToken(parts[1])
		if err != nil {
			return 0, nil, nil, false, err
		}
		return typ, key, nil, true, nil
	default:
		return 0, nil, nil, false, fmt.Errorf("unknown op: %q", line)
	}
}

func runCmd(dir, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func runCmdExpectExit(dir, name string, expectedExit int, args ...string) error {
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err == nil {
		return fmt.Errorf("%s %s: exit code 0 (expected %d)", name, strings.Join(args, " "), expectedExit)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == expectedExit {
		return nil
	}
	return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
}

func largestLogFile(dbPath string) (path string, size int64, err error) {
	ents, err := os.ReadDir(dbPath)
	if err != nil {
		return "", 0, err
	}

	var bestName string
	var bestSize int64 = -1
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		info, infoErr := ent.Info()
		if infoErr != nil {
			return "", 0, infoErr
		}
		if info.Size() > bestSize {
			bestSize = info.Size()
			bestName = name
		}
	}
	if bestName == "" {
		return "", 0, fmt.Errorf("no .log file found in %s", dbPath)
	}
	return filepath.Join(dbPath, bestName), bestSize, nil
}

func runInternalMode(args []string) (handled bool, err error) {
	if len(args) == 0 {
		return false, nil
	}
	if args[0] != "internal-golsm-crash-writer" {
		return false, nil
	}
	return true, runInternalGolsmCrashWriter(args[1:])
}

func runInternalGolsmCrashWriter(args []string) error {
	fs := flag.NewFlagSet("internal-golsm-crash-writer", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var dbPath string
	var workloadPath string
	var bloomBits int
	var compressionText string
	var crashAfter int

	fs.StringVar(&dbPath, "db", "", "db path")
	fs.StringVar(&workloadPath, "workload", "", "workload path")
	fs.IntVar(&bloomBits, "bloom-bits", 10, "bloom bits")
	fs.StringVar(&compressionText, "compression", "none", "none|snappy")
	fs.IntVar(&crashAfter, "crash-after", 0, "crash op count")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if dbPath == "" || workloadPath == "" || crashAfter <= 0 {
		return fmt.Errorf("internal-golsm-crash-writer requires --db, --workload, --crash-after>0")
	}

	var compression db.CompressionType
	switch compressionText {
	case "none":
		compression = db.NoCompression
	case "snappy":
		compression = db.SnappyCompression
	default:
		return fmt.Errorf("unsupported compression: %s", compressionText)
	}

	opt := db.DefaultOptions()
	opt.CreateIfMissing = true
	opt.ErrorIfExists = false
	opt.Comparator = util.BytewiseComparator
	opt.FilterPolicy = util.NewBloomFilterPolicy(bloomBits)
	opt.Compression = compression
	opt.WriteBufferSize = 1 << 20
	opt.MaxFileSize = 1 << 20

	ldb, err := golsm.Open(opt, dbPath)
	if err != nil {
		return err
	}

	f, err := os.Open(workloadPath)
	if err != nil {
		return err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	count := 0

	for sc.Scan() {
		typ, key, value, ok, err := parseLine(sc.Text())
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		switch typ {
		case 'P':
			if err := ldb.Put(key, value, nil); err != nil {
				return err
			}
		case 'D':
			if err := ldb.Delete(key, nil); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid workload op: %c", typ)
		}
		count++
		if count >= crashAfter {
			fmt.Fprintf(os.Stderr, "compatcheck: intentional golsm crash at op=%d\n", count)
			os.Exit(97)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return fmt.Errorf("workload finished before crash-after=%d (applied=%d)", crashAfter, count)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, "compatcheck:", msg)
	os.Exit(1)
}
