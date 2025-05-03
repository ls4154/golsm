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

	builder := NewTableBuilder(file, util.BytewiseComparator, 4096, compression, 16)

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

	tbl, err := OpenTable(file, size, util.BytewiseComparator, nil, 0)
	require.NoError(t, err, "failed to open table")

	it := tbl.NewIterator()
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
