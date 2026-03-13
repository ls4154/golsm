package impl

import (
	"fmt"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

const dbIterBufferTrimThreshold = 10 << 20

type dbIter struct {
	iter    db.Iterator
	userCmp db.Comparator
	seq     SequenceNumber

	reverse bool
	valid   bool
	err     error

	savedKey []byte // internal scratch key for skip/reverse traversal
	keyBuf   []byte
	valueBuf []byte

	closed bool
}

func newDBIter(iter db.Iterator, userCmp db.Comparator, seq SequenceNumber) *dbIter {
	util.Assert(iter != nil)
	return &dbIter{
		iter:    iter,
		userCmp: userCmp,
		seq:     seq,

		reverse: false,
		valid:   false,
		err:     nil,
	}
}

func (it *dbIter) Valid() bool {
	return it.valid
}

func (it *dbIter) saveValue(val []byte) {
	if cap(it.valueBuf) > len(val)+dbIterBufferTrimThreshold {
		it.valueBuf = append([]byte(nil), val...)
	} else {
		it.valueBuf = append(it.valueBuf[:0], val...)
	}
}

func (it *dbIter) saveKey(key []byte) {
	it.keyBuf = append(it.keyBuf[:0], key...)
}

func (it *dbIter) setCurrent(key, val []byte) {
	it.saveKey(key)
	it.saveValue(val)
}

func (it *dbIter) clearCurrent() {
	if cap(it.keyBuf) > dbIterBufferTrimThreshold {
		it.keyBuf = []byte{}
	} else {
		it.keyBuf = it.keyBuf[:0]
	}

	if cap(it.valueBuf) > dbIterBufferTrimThreshold {
		it.valueBuf = []byte{}
	} else {
		it.valueBuf = it.valueBuf[:0]
	}
}

func (it *dbIter) SeekToFirst() {
	it.reverse = false
	it.clearCurrent()
	it.savedKey = it.savedKey[:0]
	it.iter.SeekToFirst()
	if it.iter.Valid() {
		it.findNextUserEntry(false)
	} else {
		it.valid = false
	}
}

func (it *dbIter) SeekToLast() {
	it.reverse = true
	it.clearCurrent()
	it.savedKey = it.savedKey[:0]
	it.iter.SeekToLast()
	it.findPrevUserEntry()
}

func (it *dbIter) Seek(target []byte) {
	it.reverse = false
	it.clearCurrent()
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

	if it.reverse {
		it.reverse = false

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
		it.clearCurrent()
		return
	}

	it.findNextUserEntry(true)
}

func (it *dbIter) Prev() {
	util.Assert(it.valid)

	if !it.reverse {
		util.Assert(it.iter.Valid())

		it.savedKey = append(it.savedKey[:0], ExtractUserKey(it.iter.Key())...)
		for {
			it.iter.Prev()
			if !it.iter.Valid() {
				it.valid = false
				it.savedKey = it.savedKey[:0]
				it.clearCurrent()
				return
			}

			if it.userCmp.Compare(ExtractUserKey(it.iter.Key()), it.savedKey) < 0 {
				break
			}
		}
		it.reverse = true
	}

	it.findPrevUserEntry()
}

func (it *dbIter) Key() []byte {
	util.Assert(it.valid)
	return it.keyBuf
}

func (it *dbIter) Value() []byte {
	util.Assert(it.valid)
	return it.valueBuf
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
	util.Assert(!it.reverse)

	for it.iter.Valid() {
		ikey, err := ParseInternalKey(it.iter.Key())
		if err != nil {
			it.err = fmt.Errorf("%w: corrupted internal key in dbIter", err)
			it.valid = false
			it.savedKey = it.savedKey[:0]
			it.clearCurrent()
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
					it.setCurrent(ikey.UserKey, it.iter.Value())
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
	it.clearCurrent()
}

func (it *dbIter) findPrevUserEntry() {
	util.Assert(it.reverse)

	lastValueType := TypeDeletion
	for it.iter.Valid() {
		ikey, err := ParseInternalKey(it.iter.Key())
		if err != nil {
			it.err = fmt.Errorf("%w: corrupted internal key in dbIter", err)
			it.valid = false
			it.savedKey = it.savedKey[:0]
			it.clearCurrent()
			return
		}

		if ikey.Sequence <= it.seq {
			if lastValueType != TypeDeletion && it.userCmp.Compare(ikey.UserKey, it.savedKey) < 0 {
				break
			}
			lastValueType = ikey.Type
			if lastValueType == TypeDeletion {
				it.savedKey = it.savedKey[:0]
				it.clearCurrent()
			} else {
				userKey := ExtractUserKey(it.iter.Key())
				val := it.iter.Value()
				it.savedKey = append(it.savedKey[:0], userKey...)
				it.setCurrent(userKey, val)
			}
		}

		it.iter.Prev()
	}

	if lastValueType == TypeDeletion {
		it.valid = false
		it.savedKey = it.savedKey[:0]
		it.clearCurrent()
		it.reverse = false
	} else {
		it.valid = true
	}
}
