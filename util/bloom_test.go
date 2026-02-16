package util

import (
	"fmt"
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
		require.True(t, bloom.MightContain(k, filter))
	}

	require.False(t, bloom.MightContain([]byte("x"), filter))
	require.False(t, bloom.MightContain([]byte("xx"), filter))
	require.False(t, bloom.MightContain([]byte("foo"), filter))
	require.False(t, bloom.MightContain([]byte("bar"), filter))
}

func benchmarkBloomKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%06d", i))
	}
	return keys
}

func BenchmarkBloomAppendFilter(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			bloom := NewBloomFilterPolicy(10)
			keys := benchmarkBloomKeys(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = bloom.AppendFilter(keys, nil)
			}
		})
	}
}

func BenchmarkBloomMightContain(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		keys := benchmarkBloomKeys(n)
		bloom := NewBloomFilterPolicy(10)
		filter := bloom.AppendFilter(keys, nil)
		missKeys := make([][]byte, n)
		for i := 0; i < n; i++ {
			missKeys[i] = []byte(fmt.Sprintf("miss-%06d", i))
		}

		b.Run(fmt.Sprintf("hit/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = bloom.MightContain(keys[i%n], filter)
			}
		})

		b.Run(fmt.Sprintf("miss/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = bloom.MightContain(missKeys[i%n], filter)
			}
		})
	}
}
