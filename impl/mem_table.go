package impl

import (
	"encoding/binary"

	"github.com/ls4154/golsm/skiplist"
)

type MemTable struct {
	arena *skiplist.Arena
	list  *skiplist.SkipList
	cmp   *InternalKeyComparator
}

func NewMemTable(icmp *InternalKeyComparator) *MemTable {
	arena := skiplist.NewArena(64 * 1024)
	list := skiplist.NewSkipList(icmp, arena)
	return &MemTable{
		arena: arena,
		list:  list,
		cmp:   icmp,
	}
}

func (mt *MemTable) Put(seq uint64, key, value []byte) {
	mt.Add(seq, TypeValue, key, value)
}

func (mt *MemTable) Delete(seq uint64, key []byte) {
	mt.Add(seq, TypeDeletion, key, nil)
}

func (mt *MemTable) Add(seq uint64, valueType ValueType, key, value []byte) {
	// key format: key + tag
	//   tag: (seq << 8) | type
	internalKey := mt.arena.Allocate(len(key) + 8)
	copy(internalKey, key)
	binary.LittleEndian.PutUint64(internalKey[len(key):], PackSequenceAndType(seq, valueType))

	var valueBuf []byte
	if len(value) > 0 {
		valueBuf = mt.arena.Allocate(len(value))
		copy(valueBuf, value)
	}

	mt.list.Insert(internalKey, valueBuf)
}

func (mt *MemTable) Get(lookupKey *LookupKey) (value []byte, deleted, exist bool) {
	iter := mt.list.Iterator()
	iter.Seek(lookupKey.Key())
	if iter.Valid() {
		ikey := iter.Key()
		userKey := ikey[:len(ikey)-8]
		if mt.cmp.userCmp.Compare(userKey, lookupKey.UserKey()) == 0 {
			valueType := ValueType(binary.LittleEndian.Uint64(ikey[len(ikey)-8:]) & 0xff)
			switch valueType {
			case TypeDeletion:
				return nil, true, true
			case TypeValue:
				return iter.Value(), false, true
			default:
				panic("invalid memtable value type")
			}
		}
	}
	return nil, false, false
}

func (mt *MemTable) Iterator() *MemTableIterator {
	return &MemTableIterator{
		listIter: mt.list.Iterator(),
	}
}

func (mt *MemTable) ApproximateMemoryUsage() int {
	return mt.arena.MemoryUsage()
}

var _ internalIterator = &MemTableIterator{}

type MemTableIterator struct {
	listIter *skiplist.Iterator
}

func (it *MemTableIterator) Valid() bool {
	return it.listIter.Valid()
}

func (it *MemTableIterator) Next() {
	it.listIter.Next()
}

func (it *MemTableIterator) Prev() {
	it.listIter.Prev()
}

func (it *MemTableIterator) SeekToFirst() {
	it.listIter.SeekToFirst()
}

func (it *MemTableIterator) SeekToLast() {
	it.listIter.SeekToLast()
}

func (it *MemTableIterator) Seek(key []byte) {
	it.listIter.Seek(key)
}

func (it *MemTableIterator) Key() []byte {
	return it.listIter.Key()
}

func (it *MemTableIterator) Value() []byte {
	return it.listIter.Value()
}

func (it *MemTableIterator) Error() error {
	return nil
}
