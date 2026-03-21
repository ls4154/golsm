package impl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/fs"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func writeManifestForRecoverTest(t *testing.T, dbname string, manifestNum FileNumber, edits ...VersionEdit) {
	t.Helper()

	env := fs.Default()
	f, err := env.NewWritableFile(DescriptorFileName(dbname, manifestNum))
	require.NoError(t, err)

	w := log.NewWriter(f)
	for _, edit := range edits {
		require.NoError(t, w.AddRecord(edit.Append(nil)))
	}
	require.NoError(t, f.Close())

	current := []byte(filepath.Base(DescriptorFileName(dbname, manifestNum)) + "\n")
	require.NoError(t, os.WriteFile(CurrentFileName(dbname), current, 0o644))
}

func newRecoverTestVersionSet(dbname string) *VersionSet {
	opt := db.DefaultOptions()
	return NewVersionSet(dbname, &InternalKeyComparator{userCmp: util.BytewiseComparator}, fs.Default(), nil, false,
		opt.MaxManifestFileSize, nopLogger{}, newCompactionPolicy(db.DefaultCompactionOptions(opt.WriteBufferSize), opt.MaxFileSize))
}

func TestVersionSetRecoverDefaultsPrevLogNumberToZero(t *testing.T) {
	testDir := t.TempDir()

	edit := VersionEdit{}
	edit.SetComparator(util.BytewiseComparator.Name())
	edit.SetLogNumber(9)
	edit.SetNextFileNumber(7)
	edit.SetLastSequence(11)
	edit.AddFile(2, 5, 123, compactionInternalKey("a", 3), compactionInternalKey("z", 1))

	writeManifestForRecoverTest(t, testDir, 7, edit)

	vs := newRecoverTestVersionSet(testDir)
	err := vs.Recover()
	require.NoError(t, err)

	require.Equal(t, FileNumber(0), vs.prevLogNumber)
	require.Equal(t, FileNumber(9), vs.logNumber)
	require.Equal(t, FileNumber(8), vs.nextFileNumber)
	require.Equal(t, SequenceNumber(11), vs.GetLastSequence())
	require.Len(t, vs.current.files[2], 1)
	require.Equal(t, FileNumber(5), vs.current.files[2][0].number)
}

func TestVersionSetRecoverRequiresLastSequenceNumber(t *testing.T) {
	testDir := t.TempDir()

	edit := VersionEdit{}
	edit.SetComparator(util.BytewiseComparator.Name())
	edit.SetLogNumber(9)
	edit.SetNextFileNumber(7)

	writeManifestForRecoverTest(t, testDir, 7, edit)

	vs := newRecoverTestVersionSet(testDir)
	err := vs.Recover()
	require.ErrorIs(t, err, db.ErrCorruption)
	require.ErrorContains(t, err, "no meta-last-sequence-number")
}
