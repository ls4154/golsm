package impl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ls4154/golsm/util"
)

type CompactionStats struct {
	elapsed      time.Duration
	bytesRead    uint64
	bytesWritten uint64
}

func (d *dbImpl) addCompactionStats(level Level, elapsed time.Duration, bytesRead, bytesWritten uint64) {
	util.AssertMutexHeld(&d.mu)
	util.Assert(level >= 0 && level < NumLevels)

	d.compactionStats[level].elapsed += elapsed
	d.compactionStats[level].bytesRead += bytesRead
	d.compactionStats[level].bytesWritten += bytesWritten
}

func (d *dbImpl) GetProperty(name string) (string, bool) {
	const ldbPrefix = "leveldb."
	if !strings.HasPrefix(name, ldbPrefix) {
		return "", false
	}
	name = strings.TrimPrefix(name, ldbPrefix)

	d.mu.Lock()
	defer d.mu.Unlock()

	switch {
	case name == "stats":
		return d.levelDBStatsProperty(), true
	case strings.HasPrefix(name, "num-files-at-level"):
		lvStr := strings.TrimPrefix(name, "num-files-at-level")
		lv, ok := parseDecimalLevel(lvStr)
		if !ok || lv >= int(NumLevels) {
			return "", false
		}
		return strconv.Itoa(d.versions.NumLevelFiles(Level(lv))), true
	case name == "sstables":
		return d.versions.current.DebugString(), true
	case name == "approximate-memory-usage":
		return strconv.FormatUint(d.approximateMemoryUsage(), 10), true
	default:
		return "", false
	}
}

func (d *dbImpl) levelDBStatsProperty() string {
	util.AssertMutexHeld(&d.mu)

	var b strings.Builder
	b.WriteString("                               Compactions\n")
	b.WriteString("Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n")
	b.WriteString("--------------------------------------------------\n")

	for level := range NumLevels {
		files := d.versions.NumLevelFiles(Level(level))
		stats := d.compactionStats[level]
		if stats.elapsed == 0 && files == 0 {
			continue
		}

		fmt.Fprintf(&b, "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
			level,
			files,
			float64(d.versions.NumLevelBytes(Level(level)))/1048576,
			stats.elapsed.Seconds(),
			float64(stats.bytesRead)/1048576,
			float64(stats.bytesWritten)/1048576,
		)
	}

	return b.String()
}

func (d *dbImpl) approximateMemoryUsage() uint64 {
	util.AssertMutexHeld(&d.mu)

	var total uint64
	if d.tableCache != nil && d.tableCache.bcache != nil {
		total += uint64(d.tableCache.bcache.TotalCharge())
	}
	if d.mem != nil {
		total += uint64(d.mem.ApproximateMemoryUsage())
	}
	if d.imm != nil {
		total += uint64(d.imm.ApproximateMemoryUsage())
	}
	return total
}

func parseDecimalLevel(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, false
		}
	}

	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return int(v), true
}
