package impl

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

// format:
//   header:
//     sequence number (8B)
//     count of entries (4B)
//   entries (count):
//     type (1B)
//     key length (varint)
//     key (length-prefixed)
//     value length (varint)
//     value (length-prefixed)

const writeBatchHeaderSize = 8 + 4

const (
	maxEncodedLength = 1<<32 - 1
	maxUserKeyLength = maxEncodedLength - 8
	maxValueLength   = maxEncodedLength
)

type WriteBatchImpl struct {
	rep []byte
}

func NewWriteBatch() *WriteBatchImpl {
	return &WriteBatchImpl{
		rep: make([]byte, writeBatchHeaderSize),
	}
}

func WriteBatchFromContents(c []byte) *WriteBatchImpl {
	return &WriteBatchImpl{
		rep: c,
	}
}

func (b *WriteBatchImpl) Clear() {
	b.rep = b.rep[:writeBatchHeaderSize]
	for i := 0; i < writeBatchHeaderSize; i++ {
		b.rep[i] = 0
	}
}

func (b *WriteBatchImpl) sequence() SequenceNumber {
	return SequenceNumber(binary.LittleEndian.Uint64(b.rep[0:8]))
}

func (b *WriteBatchImpl) setSequence(seq SequenceNumber) {
	binary.LittleEndian.PutUint64(b.rep[0:8], uint64(seq))
}

func (b *WriteBatchImpl) count() uint32 {
	return binary.LittleEndian.Uint32(b.rep[8:12])
}

func (b *WriteBatchImpl) setCount(n uint32) {
	binary.LittleEndian.PutUint32(b.rep[8:12], n)
}

func (b *WriteBatchImpl) contents() []byte {
	return b.rep
}

func (b *WriteBatchImpl) Size() int {
	return len(b.rep)
}

func (b *WriteBatchImpl) incrementCount(delta uint32) error {
	cur := b.count()
	if math.MaxUint32-cur < delta {
		return fmt.Errorf("%w: write batch count overflow", db.ErrInvalidArgument)
	}
	b.setCount(cur + delta)
	return nil
}

func (b *WriteBatchImpl) appendContents(src *WriteBatchImpl) error {
	if err := b.incrementCount(src.count()); err != nil {
		return err
	}
	b.rep = append(b.rep, src.rep[writeBatchHeaderSize:]...)
	return nil
}

func (b *WriteBatchImpl) Append(src db.WriteBatch) error {
	if src == nil {
		return fmt.Errorf("%w: source write batch is nil", db.ErrInvalidArgument)
	}
	return b.appendContents(src.(*WriteBatchImpl))
}

func (b *WriteBatchImpl) Put(key, value []byte) error {
	if err := validateWriteLength("key", uint64(len(key)), maxUserKeyLength); err != nil {
		return err
	}
	if err := validateWriteLength("value", uint64(len(value)), maxValueLength); err != nil {
		return err
	}
	if err := b.incrementCount(1); err != nil {
		return err
	}
	b.rep = append(b.rep, byte(TypeValue))
	b.rep = util.AppendLengthPrefixedBytes(b.rep, key)
	b.rep = util.AppendLengthPrefixedBytes(b.rep, value)
	return nil
}

func (b *WriteBatchImpl) Delete(key []byte) error {
	if err := validateWriteLength("key", uint64(len(key)), maxUserKeyLength); err != nil {
		return err
	}
	if err := b.incrementCount(1); err != nil {
		return err
	}
	b.rep = append(b.rep, byte(TypeDeletion))
	b.rep = util.AppendLengthPrefixedBytes(b.rep, key)
	return nil
}

func validateWriteLength(field string, length, maxLen uint64) error {
	if length > maxLen {
		return fmt.Errorf("%w: %s is too large", db.ErrInvalidArgument, field)
	}
	return nil
}

func (b *WriteBatchImpl) Iterate(handler db.WriteBatchHandler) error {
	if handler == nil {
		return fmt.Errorf("%w: write batch handler is nil", db.ErrInvalidArgument)
	}

	cnt := int(b.count())
	rep := b.rep[writeBatchHeaderSize:]
	found := 0
	for len(rep) > 0 {
		tag := ValueType(rep[0])
		rep = rep[1:]
		switch tag {
		case TypeValue:
			key, n := util.GetLengthPrefixedBytes(rep)
			if n <= 0 {
				return fmt.Errorf("%w: bad WriteBatch Put", db.ErrCorruption)
			}
			rep = rep[n:]

			val, n := util.GetLengthPrefixedBytes(rep)
			if n <= 0 {
				return fmt.Errorf("%w: bad WriteBatch Put", db.ErrCorruption)
			}
			rep = rep[n:]

			if err := handler.Put(key, val); err != nil {
				return err
			}
		case TypeDeletion:
			key, n := util.GetLengthPrefixedBytes(rep)
			if n <= 0 {
				return fmt.Errorf("%w: bad WriteBatch Delete", db.ErrCorruption)
			}
			rep = rep[n:]

			if err := handler.Delete(key); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unknown WriteBatch tag", db.ErrCorruption)
		}
		found++
	}

	if cnt != found {
		return fmt.Errorf("%w: wrong WriteBatch count", db.ErrCorruption)
	}

	return nil
}

type memTableBatchInserter struct {
	seq SequenceNumber
	mt  *MemTable
}

func (i *memTableBatchInserter) Put(key, value []byte) error {
	i.mt.Put(i.seq, key, value)
	i.seq++
	return nil
}

func (i *memTableBatchInserter) Delete(key []byte) error {
	i.mt.Delete(i.seq, key)
	i.seq++
	return nil
}

func (b *WriteBatchImpl) InsertIntoMemTable(mt *MemTable) error {
	inserter := memTableBatchInserter{
		seq: b.sequence(),
		mt:  mt,
	}
	return b.Iterate(&inserter)
}
