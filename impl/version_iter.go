package impl

import (
	"encoding/binary"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type levelFileNumIterator struct {
	icmp     db.Comparator
	files    []*FileMetaData
	idx      int
	valueBuf [16]byte
}

func newLevelFileNumIterator(icmp db.Comparator, files []*FileMetaData) db.Iterator {
	return &levelFileNumIterator{
		icmp:  icmp,
		files: files,
	}
}

func (it *levelFileNumIterator) Close() error {
	return nil
}

func (it *levelFileNumIterator) Error() error {
	return nil
}

func (it *levelFileNumIterator) Key() []byte {
	panic("TODO")
}

func (it *levelFileNumIterator) Next() {
	util.Assert(it.Valid())
	it.idx++
}

func (it *levelFileNumIterator) Prev() {
	util.Assert(it.Valid())
	it.idx--
}

func (it *levelFileNumIterator) Seek(target []byte) {
	it.idx = lowerBoundFiles(it.icmp, it.files, target)
}

func (it *levelFileNumIterator) SeekToFirst() {
	it.idx = 0
}

func (it *levelFileNumIterator) SeekToLast() {
	it.idx = len(it.files) - 1
}

func (it *levelFileNumIterator) Valid() bool {
	return it.idx < len(it.files) && it.idx >= 0
}

func (it *levelFileNumIterator) Value() []byte {
	binary.LittleEndian.PutUint64(it.valueBuf[0:], it.files[it.idx].number)
	binary.LittleEndian.PutUint64(it.valueBuf[8:], it.files[it.idx].size)
	return it.valueBuf[:]
}
