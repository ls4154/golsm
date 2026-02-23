package impl

import (
	"encoding/binary"
	"fmt"

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

func (b *WriteBatchImpl) clear() {
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

func (b *WriteBatchImpl) setCount(seq uint32) {
	binary.LittleEndian.PutUint32(b.rep[8:12], seq)
}

func (b *WriteBatchImpl) contents() []byte {
	return b.rep
}

func (b *WriteBatchImpl) Size() int {
	return len(b.rep)
}

func (b *WriteBatchImpl) Append(src *WriteBatchImpl) {
	b.setCount(b.count() + src.count())
	b.rep = append(b.rep, src.rep[writeBatchHeaderSize:]...)
}

func (b *WriteBatchImpl) Put(key, value []byte) {
	b.setCount(b.count() + 1)
	b.rep = append(b.rep, byte(TypeValue))
	b.rep = util.AppendLengthPrefixedBytes(b.rep, key)
	b.rep = util.AppendLengthPrefixedBytes(b.rep, value)
}

func (b *WriteBatchImpl) Delete(key []byte) {
	b.setCount(b.count() + 1)
	b.rep = append(b.rep, byte(TypeDeletion))
	b.rep = util.AppendLengthPrefixedBytes(b.rep, key)
}

func (b *WriteBatchImpl) InsertIntoMemTable(mt *MemTable) error {
	seq := b.sequence()
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

			mt.Put(seq, key, val)
		case TypeDeletion:
			key, n := util.GetLengthPrefixedBytes(rep)
			if n <= 0 {
				return fmt.Errorf("%w: bad WriteBatch Put", db.ErrCorruption)
			}
			rep = rep[n:]

			mt.Delete(seq, key)
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
