package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnappyCompression(t *testing.T) {
	input := make([]byte, 10000)
	for i := 0; i < 10000; i++ {
		input[i] = byte(i)
	}

	compressed := SnappyCompress(input)
	uncompressed, err := SnappyUncompress(compressed)
	require.NoError(t, err)

	require.Equal(t, input, uncompressed)
}
