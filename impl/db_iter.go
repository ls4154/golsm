package impl

import (
	"fmt"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type iterDirection int8

const (
	directionForward iterDirection = iota
	directionReverse
)

type dbIter struct {
	iter    db.Iterator
	userCmp db.Comparator
	seq     uint64

	direction iterDirection
	valid     bool
	err       error

	savedKey   []byte // current user key for reverse direction, temporary storage
	savedValue []byte

	closed bool
}

func newDBIter(iter db.Iterator, userCmp db.Comparator, seq uint64) *dbIter {
	util.Assert(iter != nil)
	return &dbIter{
		iter:    iter,
		userCmp: userCmp,
		seq:     seq,

		direction: directionForward,
		valid:     false,
		err:       nil,
	}
}

func (it *dbIter) Valid() bool {
	return it.valid
}

func (it *dbIter) saveValue(val []byte) {
	if cap(it.savedValue) > len(val)+1048576 {
		it.savedValue = append([]byte(nil), val...)
	} else {
		it.savedValue = append(it.savedValue[:0], val...)
	}
}

func (it *dbIter) clearSavedValue() {
	if cap(it.savedValue) > 1048576 {
		it.savedValue = []byte{}
	} else {
		it.savedValue = it.savedValue[:0]
	}
}

func (it *dbIter) SeekToFirst() {
	it.direction = directionForward
	it.clearSavedValue()
	it.iter.SeekToFirst()
	if it.iter.Valid() {
		it.findNextUserEntry(false)
	} else {
		it.valid = false
	}
}

func (it *dbIter) SeekToLast() {
	it.direction = directionReverse
	it.clearSavedValue()
	it.iter.SeekToLast()
	it.findPrevUserEntry()
}

func (it *dbIter) Seek(target []byte) {
	it.direction = directionForward
	it.clearSavedValue()
	it.savedKey = it.savedKey[:0]

	var lk LookupKey
	lk.Set(target, it.seq)
	it.iter.Seek(lk.Key())
	if it.iter.Valid() {
		it.findNextUserEntry(false)
	} else {
		it.valid = false
	}
}

func (it *dbIter) Next() {
	util.Assert(it.valid)

	if it.direction == directionReverse {
		it.direction = directionForward

		if !it.iter.Valid() {
			it.iter.SeekToFirst()
		} else {
			it.iter.Next()
		}
	} else {
		userKey := ExtractUserKey(it.iter.Key())
		it.savedKey = append(it.savedKey[:0], userKey...) // skip key
		it.iter.Next()
	}

	if !it.iter.Valid() {
		it.valid = false
		it.savedKey = it.savedKey[:0]
		return
	}

	it.findNextUserEntry(true)
}

func (it *dbIter) Prev() {
	util.Assert(it.valid)

	if it.direction == directionForward {
		util.Assert(it.iter.Valid())

		it.savedKey = append(it.savedKey[:0], ExtractUserKey(it.iter.Key())...)
		for {
			it.iter.Prev()
			if !it.iter.Valid() {
				it.valid = false
				it.savedKey = it.savedKey[:0]
				it.clearSavedValue()
				return
			}

			if it.userCmp.Compare(ExtractUserKey(it.iter.Key()), it.savedKey) < 0 {
				break
			}
		}
		it.direction = directionReverse
	}

	it.findPrevUserEntry()
}

func (it *dbIter) Key() []byte {
	util.Assert(it.valid)
	if it.direction == directionForward {
		return ExtractUserKey(it.iter.Key())
	} else {
		return it.savedKey
	}
}

func (it *dbIter) Value() []byte {
	util.Assert(it.valid)
	if it.direction == directionForward {
		return it.iter.Value()
	} else {
		return it.savedValue
	}
}

func (it *dbIter) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.iter.Error()
}

func (it *dbIter) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true
	var err error
	if it.iter != nil {
		err = it.iter.Close()
		it.iter = nil
	}
	return err
}

func (it *dbIter) findNextUserEntry(skipping bool) {
	util.Assert(it.iter.Valid())
	util.Assert(it.direction == directionForward)

	for it.iter.Valid() {
		ikey, err := ParseInternalKey(it.iter.Key())
		if err != nil {
			it.err = fmt.Errorf("%w: corrupted internal key in dbIter", err)
			it.valid = false
			it.savedKey = it.savedKey[:0]
			it.clearSavedValue()
			return
		}

		if ikey.Sequence <= it.seq {
			switch ikey.Type {
			case TypeDeletion:
				it.savedKey = append(it.savedKey[:0], ikey.UserKey...)
				skipping = true
			case TypeValue:
				// NOTE: skip key is stored in savedKey
				if skipping && it.userCmp.Compare(ikey.UserKey, it.savedKey) <= 0 {
					// skip hidden entry
				} else {
					it.valid = true
					it.savedKey = it.savedKey[:0]
					return
				}
			}
		}

		it.iter.Next()
	}

	it.valid = false
	it.savedKey = it.savedKey[:0]
}

func (it *dbIter) findPrevUserEntry() {
	util.Assert(it.direction == directionReverse)

	lastValueType := TypeDeletion
	for it.iter.Valid() {
		ikey, err := ParseInternalKey(it.iter.Key())
		if err != nil {
			it.err = fmt.Errorf("%w: corrupted internal key in dbIter", err)
			it.valid = false
			it.savedKey = it.savedKey[:0]
			it.clearSavedValue()
			return
		}

		if ikey.Sequence <= it.seq {
			if lastValueType != TypeDeletion && it.userCmp.Compare(ikey.UserKey, it.savedKey) < 0 {
				break
			}
			lastValueType = ikey.Type
			if lastValueType == TypeDeletion {
				it.savedKey = it.savedKey[:0]
				it.clearSavedValue()
			} else {
				userKey := ExtractUserKey(it.iter.Key())
				val := it.iter.Value()
				it.savedKey = append(it.savedKey[:0], userKey...)
				it.saveValue(val)
			}
		}

		it.iter.Prev()
	}

	if lastValueType == TypeDeletion {
		it.valid = false
		it.savedKey = it.savedKey[:0]
		it.clearSavedValue()
		it.direction = directionForward
	} else {
		it.valid = true
	}
}
