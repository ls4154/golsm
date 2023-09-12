package golsm

import (
	"encoding/binary"

	"github.com/ls4154/golsm/util"
)

const numLevels = 7

type ValueType byte

const (
	TypeDeletion ValueType = 0
	TypeValue    ValueType = 1

	TypeForSeek = TypeValue
)

type InternalKey []byte

type InternalKeyComparator struct {
	userCmp Comparator
}

func (ic *InternalKeyComparator) Compare(a []byte, b []byte) int {
	r := ic.userCmp.Compare(ExtractUserKey(a), ExtractUserKey(b))
	if r == 0 {
		aNum := binary.LittleEndian.Uint64(a[len(a)-8:])
		bNum := binary.LittleEndian.Uint64(b[len(b)-8:])
		if aNum > bNum {
			r = -1
		} else if aNum < bNum {
			r = 1
		}
	}
	return r
}

func (*InternalKeyComparator) Name() string {
	return "leveldb.InternalKeyComparator"
}

func ExtractUserKey(internalKey []byte) []byte {
	util.Assert(len(internalKey) >= 8)
	return internalKey[:len(internalKey)-8]
}
