package impl

import (
	"fmt"
	"testing"
	"time"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

func forceFlushAndWait(t *testing.T, d *dbImpl) {
	t.Helper()

	d.mu.Lock()
	err := d.makeRoomForWrite(true)
	d.mu.Unlock()

	require.NoError(t, err)
	d.bgWork.writerWaitForFlushDone()
}

func backgroundError(d *dbImpl) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.GetBackgroundError()
}

func levelFileNumbers(d *dbImpl, level Level) []FileNumber {
	d.mu.Lock()
	defer d.mu.Unlock()

	files := d.versions.current.files[level]
	nums := make([]FileNumber, 0, len(files))
	for _, f := range files {
		nums = append(nums, f.number)
	}
	return nums
}

func hasAnyFileNumber(nums []FileNumber, want []FileNumber) bool {
	set := make(map[FileNumber]struct{}, len(nums))
	for _, num := range nums {
		set[num] = struct{}{}
	}
	for _, num := range want {
		if _, ok := set[num]; ok {
			return true
		}
	}
	return false
}

func TestBackgroundCompactionCoversOverlappingL0(t *testing.T) {
	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 64 << 10
	opt.MaxFileSize = 1 << 20

	ldb, err := Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	dbi := ldb.(*dbImpl)

	const rounds = 6
	const keyCount = 64

	for round := 0; round < rounds; round++ {
		val := []byte(fmt.Sprintf("value-%02d", round))
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("k%03d", i))
			require.NoError(t, ldb.Put(key, val, nil))
		}
		forceFlushAndWait(t, dbi)
	}

	dbi.scheduleCompaction()

	require.Eventually(t, func() bool {
		dbi.mu.Lock()
		defer dbi.mu.Unlock()
		return dbi.versions.NumLevelFiles(1) > 0
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, backgroundError(dbi))

	expected := []byte(fmt.Sprintf("value-%02d", rounds-1))
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		got, getErr := ldb.Get(key, nil)
		require.NoError(t, getErr)
		require.Equal(t, expected, got)
	}
}

func TestBackgroundCompactionDoesNotSetBackgroundErrorAcrossMultipleCompactions(t *testing.T) {
	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 64 << 10
	opt.MaxFileSize = 256 << 10

	ldb, err := Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	dbi := ldb.(*dbImpl)

	const rounds = 8
	const keyCount = 2000
	value := make([]byte, 64)
	for i := range value {
		value[i] = 'a' + byte(i%26)
	}

	writeRound := func(round int) {
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("k%04d", i))
			v := append([]byte(nil), value...)
			v[0] = byte('a' + byte(round%26))
			require.NoError(t, ldb.Put(key, v, nil))
		}
		forceFlushAndWait(t, dbi)
	}

	for round := 0; round < rounds/2; round++ {
		writeRound(round)
	}

	dbi.scheduleCompaction()
	// The first compaction must materialize at least one L1 file without
	// setting bgErr. We snapshot those file numbers so the second phase can
	// prove that later compaction work actually consumed/replaced them.
	require.Eventually(t, func() bool {
		dbi.mu.Lock()
		defer dbi.mu.Unlock()
		return dbi.versions.NumLevelFiles(1) > 0 &&
			dbi.GetBackgroundError() == nil
	}, 10*time.Second, 20*time.Millisecond)

	firstCompactionL1 := levelFileNumbers(dbi, 1)
	require.NotEmpty(t, firstCompactionL1)

	for round := rounds / 2; round < rounds; round++ {
		writeRound(round)
	}

	dbi.scheduleCompaction()
	// The second compaction should finish without bgErr and replace the L1
	// files produced by the first compaction. If any of the original file
	// numbers remain, we have not yet observed the compaction chain that
	// exercises "read previous compaction output as new compaction input".
	require.Eventually(t, func() bool {
		dbi.mu.Lock()
		defer dbi.mu.Unlock()

		if dbi.GetBackgroundError() != nil {
			return true
		}
		if dbi.versions.NumLevelFiles(1) == 0 {
			return false
		}

		currentL1 := make([]FileNumber, 0, len(dbi.versions.current.files[1]))
		for _, f := range dbi.versions.current.files[1] {
			currentL1 = append(currentL1, f.number)
		}
		return !hasAnyFileNumber(currentL1, firstCompactionL1)
	}, 10*time.Second, 20*time.Millisecond)

	require.NoError(t, backgroundError(dbi))
	require.False(t, hasAnyFileNumber(levelFileNumbers(dbi, 1), firstCompactionL1))

	checkKeys := []string{"k0000", "k0100", "k1000", fmt.Sprintf("k%04d", keyCount-1)}
	for _, key := range checkKeys {
		got, getErr := ldb.Get([]byte(key), nil)
		require.NoError(t, getErr)
		require.Len(t, got, len(value))
		require.Equal(t, byte('a'+byte((rounds-1)%26)), got[0])
	}
}
