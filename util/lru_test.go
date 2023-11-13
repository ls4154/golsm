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

	h1 := c.Insert(key1, 1)
	c.Release(h1)

	h1 = c.Lookup(key1)
	require.Equal(t, 1, h1.Value())
	c.Release(h1)

	h2 := c.Insert(key2, 2)
	c.Release(h2)

	h1 = c.Lookup(key1)
	require.Equal(t, 1, h1.Value())
	c.Release(h1)
	h2 = c.Lookup(key2)
	require.Equal(t, 2, h2.Value())
	c.Release(h2)

	h3 := c.Insert(key3, 3)
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
		h := c.Insert([]byte(k), values[i])
		c.Release(h)
	}

	c.Insert([]byte("x"), 10)
	c.Insert([]byte("y"), 20)

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

	h1 := c.Insert(key1, 1)
	c.Release(h1)

	h1 = c.Lookup(key1)
	_ = c.Insert(key2, 2)
	require.NotNil(t, c.Lookup(key1))
	require.NotNil(t, c.Lookup(key2))
}
