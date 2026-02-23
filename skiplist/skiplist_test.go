package skiplist

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var Uint64Comparator uint64Comparator

type uint64Comparator struct{}

func (uint64Comparator) Compare(a []byte, b []byte) int {
	numA := binary.NativeEndian.Uint64(a)
	numB := binary.NativeEndian.Uint64(b)

	if numA < numB {
		return -1
	} else if numA == numB {
		return 0
	} else {
		return 1
	}
}

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

func TestSkipListConcurrentReadersSingleWriter(t *testing.T) {
	const (
		totalKeys     = 20000
		readerWorkers = 8
	)

	arena := NewArena(4096)
	list := NewSkipList(Uint64Comparator, arena)

	var published int64 = -1
	done := make(chan struct{})
	errCh := make(chan error, readerWorkers)

	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for i := 0; i < totalKeys; i++ {
			list.Insert(uint64ToBytes(arena, uint64(i)), nil)
			atomic.StoreInt64(&published, int64(i))
		}
		close(done)
	}()

	var readerWG sync.WaitGroup
	for i := 0; i < readerWorkers; i++ {
		readerWG.Add(1)
		go func(seed int64) {
			defer readerWG.Done()
			rnd := rand.New(rand.NewSource(seed))
			for {
				max := atomic.LoadInt64(&published)
				if max >= 0 {
					k := uint64(rnd.Int63n(max + 1))
					key := uint64ToBytes(nil, k)
					if !list.Contains(key) {
						errCh <- fmt.Errorf("missing key in Contains: %d (published=%d)", k, max)
						return
					}
					if _, ok := list.Get(key); !ok {
						errCh <- fmt.Errorf("missing key in Get: %d (published=%d)", k, max)
						return
					}
				}

				select {
				case <-done:
					return
				default:
				}
			}
		}(int64(1000 + i))
	}

	writerWG.Wait()
	readerWG.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	for i := 0; i < totalKeys; i++ {
		require.True(t, list.Contains(uint64ToBytes(nil, uint64(i))))
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
