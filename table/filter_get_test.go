package table

import (
	"bytes"
	"io"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

type testWritableFile struct {
	buf bytes.Buffer
}

func (f *testWritableFile) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}

func (f *testWritableFile) Flush() error {
	return nil
}

func (f *testWritableFile) Sync() error {
	return nil
}

func (f *testWritableFile) Close() error {
	return nil
}

type countingRandomAccessFile struct {
	data  []byte
	reads map[int64]int
}

func (f *countingRandomAccessFile) ReadAt(p []byte, off int64) (int, error) {
	f.reads[off]++
	if off < 0 || off+int64(len(p)) > int64(len(f.data)) {
		return 0, io.EOF
	}
	copy(p, f.data[off:])
	return len(p), nil
}

func (f *countingRandomAccessFile) Close() error {
	return nil
}

type denyFilterPolicy struct{}

func (*denyFilterPolicy) Name() string {
	return "test.DenyAll"
}

func (*denyFilterPolicy) AppendFilter(_ [][]byte, dst []byte) []byte {
	return append(dst, 0)
}

func (*denyFilterPolicy) MightContain(_ []byte, _ []byte) bool {
	return false
}

func buildTableDataWithPolicy(t *testing.T, policy db.FilterPolicy) []byte {
	t.Helper()

	f := &testWritableFile{}
	builder := NewTableBuilder(f, util.BytewiseComparator, 64, db.NoCompression, 16, policy)
	for i := 0; i < 32; i++ {
		k, v := getTestKeyValue(i)
		builder.Add(k, []byte(v))
	}
	require.NoError(t, builder.Finish())

	data := f.buf.Bytes()
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func firstDataBlockOffset(t *testing.T, tbl *Table) int64 {
	t.Helper()
	it := tbl.indexBlock.NewBlockIterator(util.BytewiseComparator)
	it.SeekToFirst()
	require.True(t, it.Valid())
	h, _, err := DecodeBlockHandle(it.Value())
	require.NoError(t, err)
	require.NoError(t, it.Close())
	return int64(h.Offset)
}

func TestInternalGetUsesFilterForFastNegative(t *testing.T) {
	policy := &denyFilterPolicy{}
	tableData := buildTableDataWithPolicy(t, policy)

	rafWithFilter := &countingRandomAccessFile{data: tableData, reads: make(map[int64]int)}
	tblWithFilter, err := OpenTable(rafWithFilter, uint64(len(tableData)), util.BytewiseComparator, policy, nil, 1, false)
	require.NoError(t, err)

	dataOffset := firstDataBlockOffset(t, tblWithFilter)
	before := rafWithFilter.reads[dataOffset]

	called := false
	err = tblWithFilter.InternalGet([]byte("absent-key"), func(_, _ []byte) {
		called = true
	}, false)
	require.NoError(t, err)
	require.False(t, called)
	require.Equal(t, before, rafWithFilter.reads[dataOffset])

	rafNoFilter := &countingRandomAccessFile{data: tableData, reads: make(map[int64]int)}
	tblNoFilter, err := OpenTable(rafNoFilter, uint64(len(tableData)), util.BytewiseComparator, nil, nil, 1, false)
	require.NoError(t, err)

	dataOffsetNoFilter := firstDataBlockOffset(t, tblNoFilter)
	beforeNoFilter := rafNoFilter.reads[dataOffsetNoFilter]

	err = tblNoFilter.InternalGet([]byte("absent-key"), func(_, _ []byte) {}, false)
	require.NoError(t, err)
	require.Equal(t, beforeNoFilter+1, rafNoFilter.reads[dataOffsetNoFilter])
}
