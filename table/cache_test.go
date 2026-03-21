package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockCacheInsertLookupAndTotalCharge(t *testing.T) {
	c := NewBlockCache(32)
	block, err := NewBlock([]byte{0, 0, 0, 0})
	require.NoError(t, err)

	key := BuildBlockCacheKey(7, 11)
	release := c.Insert(key, block)
	release()

	require.Equal(t, block.Size(), c.TotalCharge())

	got, release := c.Lookup(key)
	require.NotNil(t, got)
	require.Equal(t, block, got)
	release()

	require.Equal(t, block.Size(), c.TotalCharge())
}

func TestBlockCacheLookupOrLoadCachesBlock(t *testing.T) {
	c := NewBlockCache(32)
	key := BuildBlockCacheKey(3, 9)

	loadCount := 0
	want, err := NewBlock([]byte{0, 0, 0, 0})
	require.NoError(t, err)

	got, release, err := c.LookupOrLoad(key, func() (*Block, error) {
		loadCount++
		return want, nil
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
	release()

	got, release, err = c.LookupOrLoad(key, func() (*Block, error) {
		loadCount++
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
	release()

	require.Equal(t, 1, loadCount)
	require.Equal(t, want.Size(), c.TotalCharge())
}
