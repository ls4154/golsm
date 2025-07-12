package table

import (
	"bytes"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type BlockFn func([]byte) (db.Iterator, error)

// TwoLevelIterator walks an SSTable via two levels: indexIter scans the index
// block whose values are data block handles, and dataIter scans the current
// data block. Moving across block boundaries advances indexIter and reloads
// dataIter via blockFn.
type TwoLevelIterator struct {
	indexIter       db.Iterator
	dataIter        db.Iterator
	dataBlockHandle []byte

	err error

	blockFn func([]byte) (db.Iterator, error)
}

func NewTwoLevelIterator(indexIter db.Iterator, blockFn BlockFn) *TwoLevelIterator {
	return &TwoLevelIterator{
		indexIter: indexIter,
		blockFn:   blockFn,
	}
}

func (it *TwoLevelIterator) Valid() bool {
	return it.dataIter != nil && it.dataIter.Valid()
}

func (it *TwoLevelIterator) SeekToFirst() {
	it.indexIter.SeekToFirst()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToFirst()
	}
	it.skipEmptyDataBlocksForward()
}

func (it *TwoLevelIterator) SeekToLast() {
	it.indexIter.SeekToLast()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToLast()
	}
	it.skipEmptyDataBlocksBackward()
}

func (it *TwoLevelIterator) Seek(target []byte) {
	it.indexIter.Seek(target)
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.Seek(target)
	}
	it.skipEmptyDataBlocksForward()
}

func (it *TwoLevelIterator) Next() {
	util.Assert(it.Valid())
	it.dataIter.Next()
	it.skipEmptyDataBlocksForward()
}

func (it *TwoLevelIterator) Prev() {
	util.Assert(it.Valid())
	it.dataIter.Prev()
	it.skipEmptyDataBlocksBackward()
}

func (it *TwoLevelIterator) Key() []byte {
	util.Assert(it.Valid())
	return it.dataIter.Key()
}

func (it *TwoLevelIterator) Value() []byte {
	util.Assert(it.Valid())
	return it.dataIter.Value()
}

func (it *TwoLevelIterator) initDataBlock() {
	if !it.indexIter.Valid() {
		if it.dataIter != nil {
			_ = it.dataIter.Close()
		}
		it.dataIter = nil
		return
	}

	handle := it.indexIter.Value()
	if it.dataIter != nil && bytes.Compare(handle, it.dataBlockHandle) == 0 {
		// nothing to do
		return
	}

	it.dataBlockHandle = append(it.dataBlockHandle[:0], handle...)
	if it.dataIter != nil {
		_ = it.dataIter.Close()
	}
	iter, err := it.blockFn(handle)
	if err != nil {
		it.err = err
		it.dataIter = nil
		return
	}
	it.dataIter = iter
}

func (it *TwoLevelIterator) skipEmptyDataBlocksForward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}

		it.indexIter.Next()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToFirst()
		}
	}
}

func (it *TwoLevelIterator) skipEmptyDataBlocksBackward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}

		it.indexIter.Prev()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToLast()
		}
	}
}

func (it *TwoLevelIterator) Error() error {
	if it.dataIter != nil && it.dataIter.Error() != nil {
		return it.dataIter.Error()
	}
	return it.err
}

func (it *TwoLevelIterator) Close() error {
	var err error
	if it.dataIter != nil {
		err = it.dataIter.Close()
		it.dataIter = nil
	}
	if it.indexIter != nil {
		if cerr := it.indexIter.Close(); err == nil {
			err = cerr
		}
		it.indexIter = nil
	}
	return err
}
