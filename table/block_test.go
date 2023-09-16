package table

import (
	"fmt"
	"testing"

	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestBlock(t *testing.T) {
	builder := NewBlockBuilder(16)

	const numEntries = 1000

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("test-key-%03d", i)
		value := fmt.Sprintf("test value %03d", i)

		builder.Add([]byte(key), []byte(value))
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
		key := fmt.Sprintf("test-key-%03d", i)
		value := fmt.Sprintf("test value %03d", i)

		require.True(t, it.Valid())
		require.Equal(t, []byte(key), it.Key())
		require.Equal(t, []byte(value), it.Value())

		it.Next()
	}
}
