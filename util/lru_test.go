package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRUBasic(t *testing.T) {
	c := NewLRUCache[int](2)

	key1 := []byte("a")
	key2 := []byte("b")
	key3 := []byte("c")

	h1 := c.Insert(key1, 1, 1)
	c.Release(h1)

	h1 = c.Lookup(key1)
	require.Equal(t, 1, h1.Value())
	c.Release(h1)

	h2 := c.Insert(key2, 2, 1)
	c.Release(h2)

	h1 = c.Lookup(key1)
	require.Equal(t, 1, h1.Value())
	c.Release(h1)
	h2 = c.Lookup(key2)
	require.Equal(t, 2, h2.Value())
	c.Release(h2)

	h3 := c.Insert(key3, 3, 1)
	c.Release(h3)
	require.Nil(t, c.Lookup(key1))
	h2 = c.Lookup(key2)
	require.Equal(t, 2, h2.Value())
	h3 = c.Lookup(key3)
	require.Equal(t, 3, h3.Value())
}

func TestLRUOnEvict(t *testing.T) {
	c := NewLRUCache[int](2)

	evicted := map[string]int{}
	c.SetOnEvict(func(k []byte, v *int) {
		evicted[string(k)] = *v
	})

	keys := []string{"a", "b", "c", "abc", "def"}
	values := []int{1, 2, 3, 5, 8}

	for i, k := range keys {
		h := c.Insert([]byte(k), values[i], 1)
		c.Release(h)
	}

	c.Insert([]byte("x"), 10, 1)
	c.Insert([]byte("y"), 20, 1)

	for i, k := range keys {
		v, ok := evicted[k]
		require.True(t, ok)
		require.Equal(t, values[i], v)
	}
}

func TestLRURef(t *testing.T) {
	c := NewLRUCache[int](1)

	key1 := []byte("a")
	key2 := []byte("b")

	h1 := c.Insert(key1, 1, 1)
	c.Release(h1)

	h1 = c.Lookup(key1)
	_ = c.Insert(key2, 2, 1)
	require.NotNil(t, c.Lookup(key1))
	require.NotNil(t, c.Lookup(key2))
}

func TestLRUCharge(t *testing.T) {
	c := NewLRUCache[string](10)

	evicted := map[string]string{}
	c.SetOnEvict(func(k []byte, v *string) {
		evicted[string(k)] = *v
	})

	ha := c.Insert([]byte("a"), "x", 4)
	c.Release(ha)
	hb := c.Insert([]byte("b"), "y", 4)
	c.Release(hb)

	require.Empty(t, evicted)
	require.Equal(t, 8, c.usage)

	hc := c.Insert([]byte("c"), "z", 5)
	c.Release(hc)

	require.Contains(t, evicted, "a")
	require.Nil(t, c.Lookup([]byte("a")))
	require.Equal(t, 9, c.usage)

	hb = c.Lookup([]byte("b"))
	require.NotNil(t, hb)
	c.Release(hb)

	hd := c.Insert([]byte("d"), "w", 8)
	c.Release(hd)

	require.Contains(t, evicted, "b")
	require.Contains(t, evicted, "c")
	require.Nil(t, c.Lookup([]byte("b")))
	require.Nil(t, c.Lookup([]byte("c")))
	require.Equal(t, 8, c.usage)

	hd = c.Lookup([]byte("d"))
	require.NotNil(t, hd)
	require.Equal(t, "w", hd.Value())
	c.Release(hd)
}
