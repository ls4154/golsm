package impl

import (
	"encoding/binary"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

const (
	NumLevels           = 7
	L0CompactionTrigger = 4
	L0SlowDownTrigger   = 8
	L0StopWritesTrigger = 12
)

type ValueType byte

const (
	TypeDeletion ValueType = 0
	TypeValue    ValueType = 1

	TypeForSeek = TypeValue
)

const MaxSequenceNumber uint64 = (1 << 56) - 1

func PackSequenceAndType(seq uint64, t ValueType) uint64 {
	return (seq << 8) | uint64(t)
}

type InternalKey []byte

type InternalKeyComparator struct {
	userCmp db.Comparator
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

func (ic *InternalKeyComparator) FindShortestSeparator(start *[]byte, limit []byte) {
	// TODO
	return
}

func (ic *InternalKeyComparator) FindShortSuccessor(key *[]byte) {
	// TODO
	return
}

func ExtractUserKey(internalKey []byte) []byte {
	util.Assert(len(internalKey) >= 8)
	return internalKey[:len(internalKey)-8]
}

type LookupKey struct {
	buf [200]byte // avoid allocation for short keys
	key []byte
}

func (k *LookupKey) Set(userKey []byte, seq uint64) {
	needed := len(userKey) + 8
	var dst []byte
	if needed <= len(k.buf) {
		dst = k.buf[:0]
	} else {
		dst = make([]byte, 0, needed)
	}

	dst = append(dst, userKey...)
	dst = binary.LittleEndian.AppendUint64(dst, PackSequenceAndType(seq, TypeForSeek))

	k.key = dst
}

func (k LookupKey) Key() []byte {
	return k.key
}

func (k LookupKey) UserKey() []byte {
	return k.key[:len(k.key)-8]
}

type ParsedInternalKey struct {
	UserKey  []byte
	Sequence uint64
	Type     ValueType
}

func ParseInternalKey(ikey []byte) (*ParsedInternalKey, error) {
	if len(ikey) < 8 {
		return nil, db.ErrCorruption
	}

	userKey := ExtractUserKey(ikey)
	seqType := binary.LittleEndian.Uint64(ikey[len(userKey):])
	seq := seqType >> 8
	t := ValueType(seqType & 0xff)

	return &ParsedInternalKey{
		UserKey:  userKey,
		Sequence: seq,
		Type:     t,
	}, nil
}
