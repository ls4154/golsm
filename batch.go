package golsm

import (
	"encoding/binary"
	"fmt"

	"github.com/ls4154/golsm/util"
)

// Header: sequence(8B), count(4B)
const writeBatchHeaderSize = 8 + 4

type WriteBatch struct {
	rep []byte
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		rep: make([]byte, writeBatchHeaderSize),
	}
}

func (b *WriteBatch) sequence() uint64 {
	return binary.LittleEndian.Uint64(b.rep[0:8])
}

func (b *WriteBatch) setSequence(seq uint64) {
	binary.LittleEndian.PutUint64(b.rep[0:8], seq)
}

func (b *WriteBatch) count() uint32 {
	return binary.LittleEndian.Uint32(b.rep[8:12])
}

func (b *WriteBatch) setCount(seq uint32) {
	binary.LittleEndian.PutUint32(b.rep[8:12], seq)
}

func (b *WriteBatch) contents() []byte {
	return b.rep
}

func (b *WriteBatch) Put(key, value []byte) {
	b.setCount(b.count() + 1)
	b.rep = append(b.rep, byte(TypeValue))
	b.rep = util.AppendLengthPrefixedBytes(b.rep, key)
	b.rep = util.AppendLengthPrefixedBytes(b.rep, value)
}

func (b *WriteBatch) Delete(key []byte) {
	b.setCount(b.count() + 1)
	b.rep = append(b.rep, byte(TypeDeletion))
	b.rep = util.AppendLengthPrefixedBytes(b.rep, key)
}

func (b *WriteBatch) InsertIntoMemTable(mt *MemTable) error {
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
				return fmt.Errorf("%w: bad WriteBatch Put", ErrCorruption)
			}
			rep = rep[n:]

			val, n := util.GetLengthPrefixedBytes(rep)
			if n <= 0 {
				return fmt.Errorf("%w: bad WriteBatch Put", ErrCorruption)
			}
			rep = rep[n:]

			mt.Put(seq, key, val)
		case TypeDeletion:
			key, n := util.GetLengthPrefixedBytes(rep)
			if n <= 0 {
				return fmt.Errorf("%w: bad WriteBatch Put", ErrCorruption)
			}
			rep = rep[n:]

			mt.Delete(seq, key)
		default:
			return fmt.Errorf("%w: unknown WriteBatch tag", ErrCorruption)
		}
		found++
	}

	if cnt != found {
		return fmt.Errorf("%w: wrong WriteBatch count", ErrCorruption)
	}

	return nil
}
