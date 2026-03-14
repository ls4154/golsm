package util

import "sync"

type loadCall[T any] struct {
	done chan struct{}
	err  error
}

type LRUCache[T any] struct {
	lru   LRUHandle[T]
	inUse LRUHandle[T]
	table    map[string]*LRUHandle[T]
	inflight map[string]*loadCall[T]
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
		table:    make(map[string]*LRUHandle[T]),
		inflight: make(map[string]*loadCall[T]),
		cap:      capacity,
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

func (c *LRUCache[T]) lookupLocked(keyStr string) *LRUHandle[T] {
	if h, ok := c.table[keyStr]; ok {
		lruRemove(h)
		lruAppend(&c.inUse, h)
		h.ref++
		return h
	}
	return nil
}

func (c *LRUCache[T]) Lookup(key []byte) *LRUHandle[T] {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lookupLocked(string(key))
}

func (c *LRUCache[T]) insertLocked(key []byte, value T, charge int) *LRUHandle[T] {
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

func (c *LRUCache[T]) Insert(key []byte, value T, charge int) *LRUHandle[T] {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.insertLocked(key, value, charge)
}

// LookupOrLoad returns the cached handle for key, loading it via load on miss.
// Concurrent misses for the same key are deduplicated: only one goroutine calls
// load while the others wait for the result.
func (c *LRUCache[T]) LookupOrLoad(key []byte, load func() (T, int, error)) (*LRUHandle[T], error) {
	keyStr := string(key)

	c.mu.Lock()
	for {
		if h := c.lookupLocked(keyStr); h != nil {
			c.mu.Unlock()
			return h, nil
		}

		if call, ok := c.inflight[keyStr]; ok {
			c.mu.Unlock()
			<-call.done
			if call.err != nil {
				return nil, call.err
			}
			c.mu.Lock()
			continue
		}

		break
	}

	call := &loadCall[T]{done: make(chan struct{})}
	c.inflight[keyStr] = call
	c.mu.Unlock()

	val, charge, err := load()

	c.mu.Lock()
	delete(c.inflight, keyStr)

	var h *LRUHandle[T]
	if err == nil {
		h = c.insertLocked(key, val, charge)
	}

	call.err = err
	close(call.done)
	c.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return h, nil
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

func (c *LRUCache[T]) TotalCharge() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.usage
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
