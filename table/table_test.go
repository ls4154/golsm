package table

import (
	"fmt"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	env := util.DefaultEnv()
	for _, numEntries := range []int{1, 10, 100, 1000, 10000, 100000} {
		t.Run(fmt.Sprintf("numEntries=%d", numEntries), func(t *testing.T) {
			fname := fmt.Sprintf("%06d.ldb", numEntries)
			writeTable(t, env, fname, numEntries, db.NoCompression)
			readTable(t, env, fname, numEntries)
		})
	}
}

func TestTableSnappy(t *testing.T) {
	env := util.DefaultEnv()
	for _, numEntries := range []int{1, 10, 100, 1000, 10000, 100000} {
		t.Run(fmt.Sprintf("numEntries=%d", numEntries), func(t *testing.T) {
			fname := fmt.Sprintf("%06d.ldb", numEntries)
			writeTable(t, env, fname, numEntries, db.SnappyCompression)
			readTable(t, env, fname, numEntries)
		})
	}
}

func writeTable(t *testing.T, env db.Env, name string, numEntries int, compression db.CompressionType) {
	file, err := env.NewWritableFile(name)
	require.NoError(t, err, "failed to open file")
	defer file.Close()

	builder := NewTableBuilder(file, util.BytewiseComparator, 4096, compression, 16, nil)

	for i := 0; i < numEntries; i++ {
		key, value := getTestKeyValue(i)

		builder.Add(key, []byte(value))
	}

	err = builder.Finish()
	require.NoError(t, err, "failed to finish table")

	builderSize := builder.FileSize()
	fileSize, err := env.GetFileSize(name)
	require.NoError(t, err, "failed to get file size")
	require.Equal(t, builderSize, fileSize)
}

func readTable(t *testing.T, env db.Env, name string, numEntries int) {
	file, err := env.NewRandomAccessFile(name)
	require.NoError(t, err, "failed to open file")
	defer file.Close()

	size, err := env.GetFileSize(name)
	require.NoError(t, err, "failed to get file size")

	tbl, err := OpenTable(file, size, util.BytewiseComparator, nil, nil, 0, false)
	require.NoError(t, err, "failed to open table")

	it := tbl.NewIterator(false)
	defer it.Close()
	it.SeekToFirst()
	for i := 0; i < numEntries; i++ {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, key, it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Next()
	}
	require.False(t, it.Valid())

	mid := numEntries / 2
	it.Seek([]byte(getTestKey(mid)))
	for i := mid; i < numEntries; i++ {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, key, it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Next()
	}
	require.False(t, it.Valid())

	it.Seek([]byte("zzzzz"))
	require.False(t, it.Valid())

	it.Seek([]byte(""))
	for i := 0; i < numEntries; i++ {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, []byte(key), it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Next()
	}
	require.False(t, it.Valid())

	it.SeekToLast()
	for i := numEntries - 1; i >= 0; i-- {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, key, it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Prev()
	}
	require.False(t, it.Valid())

	it.Seek([]byte(getTestKey(mid)))
	for i := mid; i >= 0; i-- {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, key, it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Prev()
	}
	require.False(t, it.Valid())
}

func TestVerifyChecksumMismatch(t *testing.T) {
	// Build table in memory.
	tableData := buildTableDataWithPolicy(t, nil)

	// Find the first data block's handle via a temporary table.
	raf := &countingRandomAccessFile{data: tableData, reads: make(map[int64]int)}
	tbl, err := OpenTable(raf, uint64(len(tableData)), util.BytewiseComparator, nil, nil, 0, false)
	require.NoError(t, err)

	it := tbl.indexBlock.NewBlockIterator(util.BytewiseComparator)
	it.SeekToFirst()
	require.True(t, it.Valid())
	h, _, err := DecodeBlockHandle(it.Value())
	require.NoError(t, err)
	require.NoError(t, it.Close())

	// Corrupt the CRC field in the block trailer (compression_type(1) + crc(4)).
	// Flipping CRC bytes leaves the block data intact so verifyChecksum=false
	// can still read the block successfully.
	corrupted := make([]byte, len(tableData))
	copy(corrupted, tableData)
	corrupted[int(h.Offset)+int(h.Size)+1] ^= 0xFF // first byte of stored CRC

	corruptedRaf := &countingRandomAccessFile{data: corrupted, reads: make(map[int64]int)}
	corruptedTbl, err := OpenTable(corruptedRaf, uint64(len(corrupted)), util.BytewiseComparator, nil, nil, 0, false)
	require.NoError(t, err)

	k, _ := getTestKeyValue(0)

	// verifyChecksum=true: must return ErrCorruption.
	err = corruptedTbl.InternalGet(k, func(_, _ []byte) {}, true)
	require.ErrorIs(t, err, db.ErrCorruption, "expected ErrCorruption, got %v", err)

	// verifyChecksum=false: skips checksum, no error expected.
	err = corruptedTbl.InternalGet(k, func(_, _ []byte) {}, false)
	require.NoError(t, err)
}

func TestTruncatedBlockReturnsCorruption(t *testing.T) {
	tableData := buildTableDataWithPolicy(t, nil)

	// Open table to get first data block handle.
	raf := &countingRandomAccessFile{data: tableData, reads: make(map[int64]int)}
	tbl, err := OpenTable(raf, uint64(len(tableData)), util.BytewiseComparator, nil, nil, 0, false)
	require.NoError(t, err)

	it := tbl.indexBlock.NewBlockIterator(util.BytewiseComparator)
	it.SeekToFirst()
	require.True(t, it.Valid())
	h, _, err := DecodeBlockHandle(it.Value())
	require.NoError(t, err)
	require.NoError(t, it.Close())

	// Provide a file truncated mid-block so ReadBlock gets a short read (io.EOF).
	truncated := tableData[:int(h.Offset)+int(h.Size)/2]
	truncatedRaf := &countingRandomAccessFile{data: truncated, reads: make(map[int64]int)}

	_, err = ReadBlock(truncatedRaf, &h, false)
	require.ErrorIs(t, err, db.ErrCorruption, "expected ErrCorruption for truncated block, got %v", err)
}
