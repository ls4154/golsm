package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestBlock(t *testing.T) {
	builder := NewBlockBuilder(16)

	const numEntries = 1000

	for i := 0; i < numEntries; i++ {
		key, value := getTestKeyValue(i)
		builder.Add(key, value)
	}

	t.Logf("estimated size: %d", builder.EstimatedSize())

	blockData := builder.Finish()
	builder.Reset()

	t.Logf("acutal size: %d", len(blockData))

	block, err := NewBlock(blockData)
	require.NoError(t, err)

	it := block.NewBlockIterator(util.BytewiseComparator)

	it.SeekToFirst()
	for i := 0; i < numEntries; i++ {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, []byte(key), it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Next()
	}
	require.False(t, it.Valid())

	it.Seek(getTestKey(100))
	for i := 100; i < numEntries; i++ {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, []byte(key), it.Key())
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
	{
		key, value := getTestKeyValue(numEntries - 1)

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
		require.Equal(t, []byte(key), it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Prev()
	}
	require.False(t, it.Valid())

	it.Seek(getTestKey(100))
	for i := 100; i >= 0; i-- {
		key, value := getTestKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, []byte(key), it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Prev()
	}
	require.False(t, it.Valid())

	it.SeekToFirst()
	{
		key, value := getTestKeyValue(0)

		require.True(t, it.Valid())
		require.Equal(t, []byte(key), it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Prev()
	}
	require.False(t, it.Valid())
}

func TestBlockLargeEntries(t *testing.T) {
	builder := NewBlockBuilder(2)

	const numEntries = 20

	for i := 0; i < numEntries; i++ {
		key, value := getLargeKeyValue(i)
		builder.Add(key, value)
	}

	t.Logf("estimated size: %d", builder.EstimatedSize())

	blockData := builder.Finish()
	builder.Reset()

	t.Logf("acutal size: %d", len(blockData))

	block, err := NewBlock(blockData)
	require.NoError(t, err)

	it := block.NewBlockIterator(util.BytewiseComparator)

	it.SeekToFirst()
	for i := 0; i < numEntries; i++ {
		key, value := getLargeKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, key, it.Key())
		require.Equal(t, value, it.Value())

		it.Next()
	}
	require.False(t, it.Valid())

	it.Seek(getTestKeyLarge(10))
	for i := 10; i < numEntries; i++ {
		key, value := getLargeKeyValue(i)

		require.True(t, it.Valid())
		require.Equal(t, key, it.Key())
		require.Equal(t, value, it.Value())

		it.Next()
	}
	require.False(t, it.Valid())
}

func TestDecodeBlockEntryLengthVarint(t *testing.T) {
	var buf []byte
	buf = binary.AppendUvarint(buf, 200)
	buf = binary.AppendUvarint(buf, 300)
	buf = binary.AppendUvarint(buf, 400)

	shared, nonShared, valueLen, read, err := decodeBlockEntryLength(buf)
	require.NoError(t, err)
	require.Equal(t, uint32(200), shared)
	require.Equal(t, uint32(300), nonShared)
	require.Equal(t, uint32(400), valueLen)
	require.Equal(t, len(buf), read)
}

func getTestKey(n int) []byte {
	return []byte(fmt.Sprintf("test-key-%08d", n))
}

func getTestValue(n int) []byte {
	return []byte(fmt.Sprintf("test-value-%08d", n))
}

func getTestKeyValue(n int) ([]byte, []byte) {
	return getTestKey(n), getTestValue(n)
}

var largeKeyPrefix = bytes.Repeat([]byte("large-test-key-"), 20)

func getTestKeyLarge(n int) []byte {
	k := append([]byte(nil), largeKeyPrefix...)
	return append(k, []byte(fmt.Sprintf("%08d", n))...)
}

func getTestValueLarge(n int) []byte {
	return bytes.Repeat([]byte(fmt.Sprintf("%08d", n)), 125)
}

func getLargeKeyValue(n int) ([]byte, []byte) {
	return getTestKeyLarge(n), getTestValueLarge(n)
}

func getTestInternalKey(n int) []byte {
	key := getTestKey(n)
	ikey := []byte(key)
	return binary.LittleEndian.AppendUint64(ikey, (uint64(n+1)<<8 | 1))
}
