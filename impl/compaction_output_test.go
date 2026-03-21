package impl

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

type trackingWritableFile struct {
	buf        bytes.Buffer
	syncCount  int
	closeCount int
	syncErr    error
	closeErr   error
}

func (f *trackingWritableFile) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}

func (f *trackingWritableFile) Flush() error {
	return nil
}

func (f *trackingWritableFile) Sync() error {
	f.syncCount++
	return f.syncErr
}

func (f *trackingWritableFile) Close() error {
	f.closeCount++
	return f.closeErr
}

func TestFinishCompactionOutputFileSyncsAndClosesOnSuccess(t *testing.T) {
	outfile := &trackingWritableFile{}
	builder := table.NewTableBuilder(outfile, util.BytewiseComparator, 64, db.NoCompression, 16, nil)
	builder.Add([]byte("k1"), []byte("value-1"))
	builder.Add([]byte("k2"), []byte("value-2"))

	d := &dbImpl{logger: nopLogger{}}
	out := &FileMetaData{number: 8, level: 1}

	err := d.FinishCompactionOutputFile(out, outfile, builder, false)
	require.NoError(t, err)
	require.Equal(t, 1, outfile.syncCount)
	require.Equal(t, 1, outfile.closeCount)
	require.NotZero(t, out.size)
	require.Equal(t, builder.FileSize(), out.size)
}

func TestFinishCompactionOutputFileWrapsSyncError(t *testing.T) {
	outfile := &trackingWritableFile{
		syncErr: errors.New("sync failed"),
	}
	builder := table.NewTableBuilder(outfile, util.BytewiseComparator, 64, db.NoCompression, 16, nil)
	builder.Add([]byte("k"), []byte("value"))

	d := &dbImpl{logger: nopLogger{}}
	out := &FileMetaData{number: 9, level: 1}

	err := d.FinishCompactionOutputFile(out, outfile, builder, false)
	require.ErrorIs(t, err, db.ErrIO)
	require.ErrorContains(t, err, "sync failed")
	require.Equal(t, 1, outfile.syncCount)
	require.Equal(t, 0, outfile.closeCount)
}
