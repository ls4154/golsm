package skiplist

import (
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSkipListEmpty(t *testing.T) {
	arena := NewArena(4096)
	list := NewSkipList(Uint64Comparator, arena)

	require.False(t, list.Contains(uint64ToBytes(nil, 777)))

	it := list.Iterator()
	it.SeekToFirst()
	require.False(t, it.Valid())
	it.Seek(uint64ToBytes(nil, 100))
	require.False(t, it.Valid())
	it.SeekToLast()
	require.False(t, it.Valid())
}

func TestSkipListSeq(t *testing.T) {
	const N = 100000

	arena := NewArena(4096)
	list := NewSkipList(Uint64Comparator, arena)

	for i := 0; i < N; i++ {
		num := uint64(i)
		list.Insert(uint64ToBytes(arena, num), nil)
	}

	for i := 0; i < N; i++ {
		require.True(t, list.Contains(uint64ToBytes(nil, uint64(i))))
	}

	it := list.Iterator()
	it.SeekToFirst()
	for i := 0; i < N; i++ {
		num := uint64(i)
		require.True(t, it.Valid())
		require.Equal(t, num, bytesToUint64(it.Key()))
		it.Next()
	}

	it.SeekToLast()
	for i := N - 1; i >= 0; i-- {
		num := uint64(i)
		require.True(t, it.Valid())
		require.Equal(t, num, bytesToUint64(it.Key()))
		it.Prev()
	}
}

func TestSkipListRand(t *testing.T) {
	const N = 100000

	// rand should not generate duplicated keys
	rnd := rand.New(rand.NewSource(99))

	arena := NewArena(4096)
	list := NewSkipList(Uint64Comparator, arena)

	keys := make([]uint64, N)

	for i := 0; i < N; i++ {
		num := uint64(rnd.Int63())
		list.Insert(uint64ToBytes(arena, num), nil)
		keys[i] = num
	}

	for i := 0; i < N; i++ {
		require.True(t, list.Contains(uint64ToBytes(nil, keys[i])))
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	it := list.Iterator()
	it.SeekToFirst()
	for i := 0; i < N; i++ {
		require.True(t, it.Valid())
		require.Equal(t, keys[i], bytesToUint64(it.Key()))
		it.Next()
	}

	it.SeekToLast()
	for i := N - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.Equal(t, keys[i], bytesToUint64(it.Key()))
		it.Prev()
	}
}

func uint64ToBytes(arena *Arena, num uint64) []byte {
	var b []byte
	if arena != nil {
		b = arena.Allocate(8)
	} else {
		b = make([]byte, 8)
	}
	binary.NativeEndian.PutUint64(b, num)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.NativeEndian.Uint64(b)
}
