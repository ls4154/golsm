package table

import (
	"encoding/binary"

	"github.com/ls4154/golsm/util"
)

type BlockCache struct {
	lru *util.LRUCache[*Block]
}

func NewBlockCache(capacity int) *BlockCache {
	return &BlockCache{
		lru: util.NewLRUCache[*Block](capacity),
	}
}

type BlockCacheKey [16]byte

func (c *BlockCache) Lookup(key BlockCacheKey) (*Block, func()) {
	h := c.lru.Lookup(key[:])
	if h == nil {
		return nil, nil
	}
	return h.Value(), func() {
		c.lru.Release(h)
	}
}

func (c *BlockCache) Insert(key BlockCacheKey, block *Block) func() {
	h := c.lru.Insert(key[:], block, block.Size())

	return func() {
		c.lru.Release(h)
	}
}

func BuildBlockCacheKey(id, offset uint64) BlockCacheKey {
	cacheKey := BlockCacheKey{}
	binary.LittleEndian.PutUint64(cacheKey[0:], id)
	binary.LittleEndian.PutUint64(cacheKey[8:], offset)
	return cacheKey
}
