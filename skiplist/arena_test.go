package skiplist

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArena4k(t *testing.T) {
	arena := NewArena(4096)

	for size := 1; size < 10000; size++ {
		m := arena.Allocate(size)
		require.Len(t, m, size)

		m = arena.AllocateAligned(size)
		require.Len(t, m, size)
		ptr := bytesToPtr(m)
		require.Zero(t, uintptr(ptr)%align != 0)
	}
}

func TestArena1m(t *testing.T) {
	arena := NewArena(1048576)

	for size := 1; size < 10000; size++ {
		m := arena.Allocate(size)
		require.Len(t, m, size)

		m = arena.AllocateAligned(size)
		require.Len(t, m, size)
		ptr := bytesToPtr(m)
		require.Zero(t, uintptr(ptr)%align != 0)
	}
}
