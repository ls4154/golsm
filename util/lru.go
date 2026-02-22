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
	value   T
	next    *LRUHandle[T]
	prev    *LRUHandle[T]
	ref     int
	charge  int
	key     string
	inCache bool
}

func (h *LRUHandle[T]) Value() T {
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

func (c *LRUCache[T]) Insert(key []byte, value T, charge int) *LRUHandle[T] {
	c.mu.Lock()
	defer c.mu.Unlock()

	keyStr := string(key)
	if existing, ok := c.table[keyStr]; ok {
		lruRemove(existing)
		delete(c.table, keyStr)
		c.usage -= existing.charge
		existing.inCache = false
		c.unref(existing)
	}

	h := &LRUHandle[T]{
		value:   value,
		ref:     2,
		charge:  charge,
		key:     keyStr,
		inCache: true,
	}
	lruAppend(&c.inUse, h)

	c.table[keyStr] = h
	c.usage += h.charge

	for c.usage > c.cap && c.lru.next != &c.lru {
		old := c.lru.next
		Assert(old.ref == 1)
		lruRemove(old)
		delete(c.table, old.key)
		c.usage -= old.charge
		old.inCache = false
		c.unref(old)
	}

	return h
}

func (c *LRUCache[T]) Release(h *LRUHandle[T]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unref(h)
}

func (c *LRUCache[T]) Erase(key []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	h, ok := c.table[string(key)]
	if !ok {
		return
	}

	lruRemove(h)
	delete(c.table, h.key)
	c.usage -= h.charge
	h.inCache = false
	c.unref(h)
}

func (c *LRUCache[T]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Assert(c.inUse.next == &c.inUse)
	for c.lru.next != &c.lru {
		h := c.lru.next
		Assert(h.inCache)
		Assert(h.ref == 1)
		lruRemove(h)
		delete(c.table, h.key)
		c.usage -= h.charge
		h.inCache = false
		c.unref(h)
	}
}

func (c *LRUCache[T]) unref(h *LRUHandle[T]) {
	h.ref--
	if h.ref == 0 {
		Assert(!h.inCache)
		if c.onEvict != nil {
			c.onEvict([]byte(h.key), &h.value)
		}
	} else if h.inCache && h.ref == 1 {
		lruRemove(h)
		lruAppend(&c.lru, h)
	}
}

func lruRemove[T any](e *LRUHandle[T]) {
	Assert(e.inCache)
	e.next.prev = e.prev
	e.prev.next = e.next
}

func lruAppend[T any](l, e *LRUHandle[T]) {
	e.next = l
	e.prev = l.prev
	e.prev.next = e
	e.next.prev = e
}
