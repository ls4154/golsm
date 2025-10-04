package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloomFilter(t *testing.T) {
	bloom := NewBloomFilterPolicy(10)

	keys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("bye"),
	}

	filter := bloom.AppendFilter(keys, nil)

	for _, k := range keys {
		require.True(t, bloom.MayMatch(k, filter))
	}

	require.False(t, bloom.MayMatch([]byte("x"), filter))
	require.False(t, bloom.MayMatch([]byte("xx"), filter))
	require.False(t, bloom.MayMatch([]byte("foo"), filter))
	require.False(t, bloom.MayMatch([]byte("bar"), filter))
}
