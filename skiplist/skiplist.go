package skiplist

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

// Concurrency: lock-free multi-reader + externally serialized single-writer.
// Readers can run concurrently with the writer and may miss in-flight inserts.
type SkipList struct {
	cmp          Comparator
	arena        *Arena
	head         *node
	curMaxHeight int32
}

const (
	maxHeight   = 12
	maxNodeSize = int(unsafe.Sizeof(node{}))
	ptrSize     = int(unsafe.Sizeof((*node)(nil)))
)

type node struct {
	keyPtr   *byte
	valuePtr *byte
	keyLen   uint32
	valueLen uint32
	next     [maxHeight]*node
}

func NewSkipList(cmp Comparator, arena *Arena) *SkipList {
	sl := &SkipList{
		cmp:          cmp,
		arena:        arena,
		curMaxHeight: 1,
	}

	n := sl.newNode(nil, nil, maxHeight)
	for i := int32(0); i < maxHeight; i++ {
		n.SetNext(i, nil)
	}
	sl.head = n

	return sl
}

// Insert key into the list.
// The key must be a memory allocated from Arena
// Caller must serialize Insert calls (single writer).
func (s *SkipList) Insert(key, value []byte) {
	var prev [maxHeight]*node
	_ = s.findGreaterOrEqual(key, prev[:])

	height := s.randomHeight()
	curMaxHeight := s.getMaxHeight()
	if height > curMaxHeight {
		for i := curMaxHeight; i < height; i++ {
			prev[i] = s.head
		}
		s.setMaxHeight(height)
	}

	node := s.newNode(key, value, height)
	for i := int32(0); i < height; i++ {
		node.SetNext(i, prev[i].GetNext(i))
		prev[i].SetNext(i, node)
	}
}

// Check if the key exists in the list.
func (s *SkipList) Contains(key []byte) bool {
	node := s.findGreaterOrEqual(key, nil)
	return node != nil && s.cmp.Compare(key, node.Key()) == 0
}

func (s *SkipList) Get(key []byte) ([]byte, bool) {
	node := s.findGreaterOrEqual(key, nil)
	if node != nil && s.cmp.Compare(key, node.Key()) == 0 {
		return node.Value(), true
	}
	return nil, false
}

func (s *SkipList) Iterator() *Iterator {
	return &Iterator{
		list: s,
	}
}

func (s *SkipList) getMaxHeight() int32 {
	return atomic.LoadInt32(&s.curMaxHeight)
}

func (s *SkipList) setMaxHeight(height int32) {
	atomic.StoreInt32(&s.curMaxHeight, height)
}

func (s *SkipList) newNode(key, value []byte, height int32) *node {
	// NOTE: Variable-size tail allocation is memory-efficient, but this []byte->*node
	// cast trips checkptr under -race ("converted pointer straddles multiple allocations").
	// Keep this for now; revisit node allocation layout to make race/checkptr-friendly.
	size := maxNodeSize - int(maxHeight-height)*int(ptrSize)
	node := (*node)(bytesToPtr(s.arena.AllocateAligned(size)))

	if len(key) > 0 {
		node.keyPtr = &key[0]
		node.keyLen = uint32(len(key))
	}
	if len(value) > 0 {
		node.valuePtr = &value[0]
		node.valueLen = uint32(len(value))
	}
	return node
}

func (s *SkipList) findGreaterOrEqual(key []byte, prev []*node) *node {
	cur := s.head
	lv := s.getMaxHeight() - 1
	for {
		next := cur.GetNext(lv)
		if s.keyIsAfterNode(key, next) {
			cur = next
		} else {
			if prev != nil {
				prev[lv] = cur
			}
			if lv == 0 {
				return next
			} else {
				lv--
			}
		}
	}
}

func (s *SkipList) findLessThan(key []byte) *node {
	cur := s.head
	lv := s.getMaxHeight() - 1
	for {
		next := cur.GetNext(lv)
		if s.keyIsAfterNode(key, next) {
			cur = next
		} else {
			if lv == 0 {
				return cur
			} else {
				lv--
			}
		}
	}
}

func (s *SkipList) findLast() *node {
	cur := s.head
	lv := s.getMaxHeight() - 1
	for {
		next := cur.GetNext(lv)
		if next == nil {
			if lv == 0 {
				return cur
			} else {
				lv--
			}
		} else {
			cur = next
		}
	}
}

func (s *SkipList) keyIsAfterNode(key []byte, node *node) bool {
	return node != nil && s.cmp.Compare(node.Key(), key) < 0
}

func (s *SkipList) randomHeight() int32 {
	const branching = 4
	height := int32(1)
	for height < maxHeight && rand.Intn(branching) == 0 {
		height++
	}
	return height
}

func (n *node) GetNext(height int32) *node {
	return (*node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&n.next[height]))))
}

func (n *node) SetNext(height int32, node *node) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&n.next[height])), unsafe.Pointer(node))
}

func (n *node) Key() []byte {
	return unsafe.Slice(n.keyPtr, n.keyLen)
}

func (n *node) Value() []byte {
	return unsafe.Slice(n.valuePtr, n.valueLen)
}

type Iterator struct {
	list *SkipList
	node *node
}

func (it *Iterator) Valid() bool {
	return it.node != nil
}

func (it *Iterator) Next() {
	it.node = it.node.GetNext(0)
}

func (it *Iterator) Prev() {
	it.node = it.list.findLessThan(it.node.Key())
	if it.node == it.list.head {
		it.node = nil
	}
}

func (it *Iterator) SeekToFirst() {
	it.node = it.list.head.GetNext(0)
}

func (it *Iterator) SeekToLast() {
	it.node = it.list.findLast()
	if it.node == it.list.head {
		it.node = nil
	}
}

func (it *Iterator) Seek(key []byte) {
	it.node = it.list.findGreaterOrEqual(key, nil)
}

func (it *Iterator) Key() []byte {
	return it.node.Key()
}

func (it *Iterator) Value() []byte {
	return it.node.Value()
}
