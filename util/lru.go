package util

import "sync"

type LRUCache[T any] struct {
	lru   LRUHandle[T]
	inUse LRUHandle[T]
	table map[string]*LRUHandle[T]
	mu    sync.Mutex
	usage int
	cap   int

	onEvict func(k []byte, v *T)
}

type LRUHandle[T any] struct {
	value T
	next  *LRUHandle[T]
	prev  *LRUHandle[T]
	ref   int
	key   []byte
	cache *LRUCache[T]
}

func (h LRUHandle[T]) Value() T {
	return h.value
}

func NewLRUCache[T any](capacity int) *LRUCache[T] {
	c := &LRUCache[T]{
		table: make(map[string]*LRUHandle[T]),
		cap:   capacity,
	}
	c.lru.next = &c.lru
	c.lru.prev = &c.lru
	c.inUse.next = &c.inUse
	c.inUse.prev = &c.inUse
	return c
}

func (c *LRUCache[T]) SetOnEvict(fn func(k []byte, v *T)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onEvict = fn
}

func (c *LRUCache[T]) Lookup(key []byte) *LRUHandle[T] {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.table[string(key)]; ok {
		lruRemove(v)
		lruAppend(&c.inUse, v)
		v.ref++
		return v
	}
	return nil
}

func (c *LRUCache[T]) Insert(key []byte, value T) *LRUHandle[T] {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyStr := string(key)
	if existing, ok := c.table[keyStr]; ok {
		lruRemove(existing)
		delete(c.table, keyStr)
		c.usage--
	}

	h := &LRUHandle[T]{
		value: value,
		ref:   2,
		key:   key,
		cache: c,
	}
	lruAppend(&c.inUse, h)

	c.table[keyStr] = h
	c.usage++

	for c.usage > c.cap && c.lru.next != &c.lru {
		old := c.lru.next
		lruRemove(old)
		delete(c.table, string(old.key))
		c.usage--
		old.ref--
		if old.ref == 0 && c.onEvict != nil {
			c.onEvict(old.key, &old.value)
		}
	}

	return h
}

func (c *LRUCache[T]) Release(h *LRUHandle[T]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unref(h)
}

func (c *LRUCache[T]) unref(h *LRUHandle[T]) {
	h.ref--
	if h.ref == 1 {
		lruRemove(h)
		lruAppend(&h.cache.lru, h)
	} else if h.ref == 0 {
		if h.cache.onEvict != nil {
			h.cache.onEvict(h.key, &h.value)
		}
	}
}

func lruRemove[T any](e *LRUHandle[T]) {
	e.next.prev = e.prev
	e.prev.next = e.next
}

func lruAppend[T any](l, e *LRUHandle[T]) {
	e.next = l
	e.prev = l.prev
	e.prev.next = e
	e.next.prev = e
}
