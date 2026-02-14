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

	// TypeValue is the highest type value, so a seek key lands at the newest version.
	TypeForSeek = TypeValue
)

const MaxSequenceNumber uint64 = (1 << 56) - 1

func PackSequenceAndType(seq uint64, t ValueType) uint64 {
	return (seq << 8) | uint64(t)
}

// InternalKey: [user key] [seq(56b) | type(8b)]
// Sorted by user key ASC, trailer DESC (newer first).
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
	userStart := ExtractUserKey(*start)
	userLimit := ExtractUserKey(limit)

	tmp := append([]byte(nil), userStart...)

	ic.userCmp.FindShortestSeparator(&tmp, userLimit)
	if len(tmp) < len(userStart) && ic.userCmp.Compare(userStart, tmp) < 0 {
		// Build an internal key that is logically larger but physically shorter.
		shortened := make([]byte, 0, len(tmp)+8)
		shortened = append(shortened, tmp...)
		shortened = binary.LittleEndian.AppendUint64(shortened, PackSequenceAndType(MaxSequenceNumber, TypeForSeek))

		util.Assert(ic.Compare(*start, shortened) < 0)
		util.Assert(ic.Compare(shortened, limit) < 0)
		*start = shortened
	}
}

func (ic *InternalKeyComparator) FindShortSuccessor(key *[]byte) {
	userKey := ExtractUserKey(*key)

	tmp := append([]byte(nil), userKey...)

	ic.userCmp.FindShortSuccessor(&tmp)
	if len(tmp) < len(userKey) && ic.userCmp.Compare(userKey, tmp) < 0 {
		// Build the smallest internal key for the shortened user key.
		successor := make([]byte, 0, len(tmp)+8)
		successor = append(successor, tmp...)
		successor = binary.LittleEndian.AppendUint64(successor, PackSequenceAndType(MaxSequenceNumber, TypeForSeek))

		util.Assert(ic.Compare(*key, successor) < 0)
		*key = successor
	}
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
