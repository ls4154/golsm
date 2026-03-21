package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardedLRULookupOrLoadEraseAndTotalCharge(t *testing.T) {
	c := NewShardedLRUCache[int](8, 4)

	loadCount := 0
	h, err := c.LookupOrLoad([]byte("a"), func() (int, int, error) {
		loadCount++
		return 42, 2, nil
	})
	require.NoError(t, err)
	require.Equal(t, 42, h.Value())
	c.Release(h)

	require.Equal(t, 1, loadCount)
	require.Equal(t, 2, c.TotalCharge())

	h, err = c.LookupOrLoad([]byte("a"), func() (int, int, error) {
		loadCount++
		return 99, 2, nil
	})
	require.NoError(t, err)
	require.Equal(t, 42, h.Value())
	c.Release(h)

	require.Equal(t, 1, loadCount)

	h = c.Lookup([]byte("a"))
	require.NotNil(t, h)
	require.Equal(t, 42, h.Value())
	c.Release(h)

	c.Erase([]byte("a"))
	require.Nil(t, c.Lookup([]byte("a")))
	require.Equal(t, 0, c.TotalCharge())
}

func TestShardedLRUInsertReleaseAndClose(t *testing.T) {
	c := NewShardedLRUCache[int](8, 4)

	evicted := map[string]int{}
	c.SetOnEvict(func(k []byte, v *int) {
		evicted[string(k)] = *v
	})

	h1 := c.Insert([]byte("a"), 1, 1)
	h2 := c.Insert([]byte("b"), 2, 1)
	c.Release(h1)
	c.Release(h2)

	require.Equal(t, 2, c.TotalCharge())

	h := c.Lookup([]byte("a"))
	require.NotNil(t, h)
	require.Equal(t, 1, h.Value())
	c.Release(h)

	c.Close()

	require.Equal(t, 0, c.TotalCharge())
	require.Equal(t, 1, evicted["a"])
	require.Equal(t, 2, evicted["b"])
}
