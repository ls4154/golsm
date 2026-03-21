package impl

import (
	"bytes"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/fs"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func newRecoveryTestDB(t *testing.T, dbname string, userOpt *db.Options) *dbImpl {
	t.Helper()

	opt, err := validateOption(userOpt)
	require.NoError(t, err)

	icmp := &InternalKeyComparator{userCmp: opt.Comparator}
	return &dbImpl{
		dbname:   dbname,
		options:  opt,
		icmp:     icmp,
		ifilter:  newInternalFilterPolicy(opt.FilterPolicy),
		env:      fs.Default(),
		versions: NewVersionSet(dbname, icmp, fs.Default(), nil, opt.ParanoidChecks, opt.MaxManifestFileSize, nopLogger{}, newCompactionPolicy(opt.Compaction, opt.MaxFileSize)),
		logger:   nopLogger{},
	}
}

func writeLogRecords(t *testing.T, dbname string, logNum FileNumber, records [][]byte) {
	t.Helper()

	env := fs.Default()
	f, err := env.NewWritableFile(LogFileName(dbname, logNum))
	require.NoError(t, err)

	w := log.NewWriter(f)
	for _, record := range records {
		require.NoError(t, w.AddRecord(record))
	}
	require.NoError(t, f.Close())
}

func TestRecoverLogFileRejectsShortRecord(t *testing.T) {
	testDir := t.TempDir()
	writeLogRecords(t, testDir, 1, [][]byte{[]byte("short")})

	d := newRecoveryTestDB(t, testDir, db.DefaultOptions())
	edit := &VersionEdit{}
	var maxSeq SequenceNumber

	d.mu.Lock()
	err := d.RecoverLogFile(1, true, edit, &maxSeq)
	d.mu.Unlock()

	require.ErrorIs(t, err, db.ErrCorruption)
	require.Empty(t, edit.newFiles)
	require.Zero(t, maxSeq)
}

func TestRecoverLogFileFlushesMultipleLevel0Tables(t *testing.T) {
	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 64 << 10
	opt.MaxFileSize = 1 << 20

	records := make([][]byte, 0, 3)
	for i := 0; i < 3; i++ {
		batch := NewWriteBatch()
		value := bytes.Repeat([]byte{byte('a' + i)}, 80<<10)
		require.NoError(t, batch.Put([]byte{byte('k' + i)}, value))
		batch.setSequence(SequenceNumber(i + 1))
		records = append(records, append([]byte(nil), batch.contents()...))
	}
	writeLogRecords(t, testDir, 1, records)

	d := newRecoveryTestDB(t, testDir, opt)
	edit := &VersionEdit{}
	var maxSeq SequenceNumber

	d.mu.Lock()
	err := d.RecoverLogFile(1, true, edit, &maxSeq)
	d.mu.Unlock()

	require.NoError(t, err)
	require.Len(t, edit.newFiles, 3)
	require.Equal(t, SequenceNumber(3), maxSeq)
	for _, f := range edit.newFiles {
		require.Equal(t, Level(0), f.level)
		require.NotZero(t, f.size)
	}
}

func TestKeyNotExistsInHigherLevelTracksLevelPointers(t *testing.T) {
	v := &Version{}
	v.files[2] = []*FileMetaData{
		testFile(2, 1, 1, "a", "c"),
		testFile(2, 2, 1, "e", "g"),
	}
	v.files[3] = []*FileMetaData{
		testFile(3, 3, 1, "m", "z"),
	}

	var levelPtrs [NumLevels]int

	require.False(t, keyNotExistsInHigherLevel([]byte("b"), util.BytewiseComparator, v, 2, &levelPtrs))
	require.Equal(t, 0, levelPtrs[2])

	require.True(t, keyNotExistsInHigherLevel([]byte("d"), util.BytewiseComparator, v, 2, &levelPtrs))
	require.Equal(t, 1, levelPtrs[2])

	require.False(t, keyNotExistsInHigherLevel([]byte("f"), util.BytewiseComparator, v, 2, &levelPtrs))
	require.Equal(t, 1, levelPtrs[2])

	require.True(t, keyNotExistsInHigherLevel([]byte("h"), util.BytewiseComparator, v, 2, &levelPtrs))
	require.Equal(t, 2, levelPtrs[2])

	require.False(t, keyNotExistsInHigherLevel([]byte("x"), util.BytewiseComparator, v, 2, &levelPtrs))
	require.Equal(t, 0, levelPtrs[3])
}
