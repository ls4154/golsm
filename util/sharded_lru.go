package util

import "math/bits"

type ShardedLRUCache[T any] struct {
	shards []*LRUCache[T]
	mask   uint32
}

func NewShardedLRUCache[T any](capacity, numShards int) *ShardedLRUCache[T] {
	if numShards > capacity {
		numShards = capacity
	}
	if numShards <= 0 {
		numShards = 1
	}
	// Round down to power of 2.
	numShards = 1 << (bits.Len(uint(numShards)) - 1)

	perShard := capacity / numShards
	remainder := capacity % numShards
	shards := make([]*LRUCache[T], numShards)
	for i := range shards {
		cap := perShard
		if i < remainder {
			cap++
		}
		shards[i] = NewLRUCache[T](cap)
	}
	return &ShardedLRUCache[T]{
		shards: shards,
		mask:   uint32(numShards - 1),
	}
}

func (c *ShardedLRUCache[T]) shard(key []byte) *LRUCache[T] {
	return c.shards[LDBHash(key, 0)&c.mask]
}

func (c *ShardedLRUCache[T]) SetOnEvict(fn func([]byte, *T)) {
	for _, s := range c.shards {
		s.SetOnEvict(fn)
	}
}

func (c *ShardedLRUCache[T]) Lookup(key []byte) *LRUHandle[T] {
	return c.shard(key).Lookup(key)
}

func (c *ShardedLRUCache[T]) Insert(key []byte, value T, charge int) *LRUHandle[T] {
	return c.shard(key).Insert(key, value, charge)
}

func (c *ShardedLRUCache[T]) LookupOrLoad(key []byte, load func() (T, int, error)) (*LRUHandle[T], error) {
	return c.shard(key).LookupOrLoad(key, load)
}

func (c *ShardedLRUCache[T]) Release(h *LRUHandle[T]) {
	c.shards[LDBHashString(h.key, 0)&c.mask].Release(h)
}

func (c *ShardedLRUCache[T]) Erase(key []byte) {
	c.shard(key).Erase(key)
}

func (c *ShardedLRUCache[T]) TotalCharge() int {
	sum := 0
	for _, s := range c.shards {
		sum += s.TotalCharge()
	}
	return sum
}

func (c *ShardedLRUCache[T]) Close() {
	for _, s := range c.shards {
		s.Close()
	}
}
