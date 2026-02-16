package table

import (
	"testing"

	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestFilterBlockBuilderReader(t *testing.T) {
	policy := util.NewBloomFilterPolicy(10)
	builder := NewFilterBlockBuilder(policy)

	builder.StartBlock(0)
	builder.AddKey([]byte("a"))
	builder.AddKey([]byte("b"))

	// Force one empty filter slot between populated slots.
	builder.StartBlock(4096)
	builder.AddKey([]byte("c"))

	contents := builder.Finish()
	reader := NewFilterBlockReader(policy, contents)

	require.True(t, reader.KeyMightContain(0, []byte("a")))
	require.True(t, reader.KeyMightContain(0, []byte("b")))
	require.True(t, reader.KeyMightContain(4096, []byte("c")))

	// offset 2048 maps to an empty filter entry
	require.False(t, reader.KeyMightContain(2048, []byte("anything")))
}

func TestFilterBlockReaderMalformedTreatedAsMatch(t *testing.T) {
	policy := util.NewBloomFilterPolicy(10)
	reader := NewFilterBlockReader(policy, []byte{1, 2, 3})
	require.True(t, reader.KeyMightContain(0, []byte("k")))
}
