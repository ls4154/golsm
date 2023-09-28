package table

import (
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/env"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	const numEntries = 100000
	fname := "555555.ldb"

	writeTable(t, fname, numEntries)
	readTable(t, fname, numEntries)
}

func writeTable(t *testing.T, name string, numEntries int) {
	file, err := env.DefaultEnv().NewWritableFile(name)
	require.NoError(t, err, "failed to open file")
	defer file.Close()

	builder := NewTableBuilder(file, util.BytewiseComparator, 4096, db.NoCompression, 16)

	for i := 0; i < numEntries; i++ {
		key, value := getTestKeyValue(i)

		builder.Add(key, []byte(value))
	}

	err = builder.Finish()
	require.NoError(t, err, "failed to finish table")
}

func readTable(t *testing.T, name string, numEntries int) {
	file, err := env.DefaultEnv().NewRandomAccessFile(name)
	require.NoError(t, err, "failed to open file")
	defer file.Close()

	size, err := env.DefaultEnv().GetFileSize(name)
	require.NoError(t, err, "failed to get file size")

	tbl, err := OpenTable(file, size, util.BytewiseComparator)
	require.NoError(t, err, "failed to open table")

	it := tbl.NewIterator()
	it.SeekToFirst()
	for i := 0; i < numEntries; i++ {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, key, it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Next()
	}
	require.False(t, it.Valid())

	it.Seek([]byte(getTestKey(100)))
	for i := 100; i < numEntries; i++ {
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

	it.Seek([]byte(getTestKey(100)))
	for i := 100; i >= 0; i-- {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, key, it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Prev()
	}
	require.False(t, it.Valid())
}
