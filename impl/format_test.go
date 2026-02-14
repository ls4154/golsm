package impl

import (
	"encoding/binary"
	"testing"

	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestInternalKeyComparatorFindShortestSeparator(t *testing.T) {
	icmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}

	start := makeInternalKeyForComparatorTest([]byte("foo"), 100, TypeValue)
	limit := makeInternalKeyForComparatorTest([]byte("hzz"), 90, TypeValue)
	origStart := append([]byte(nil), start...)

	icmp.FindShortestSeparator(&start, limit)

	require.Equal(t, []byte("g"), ExtractUserKey(start))
	require.Equal(t, PackSequenceAndType(MaxSequenceNumber, TypeForSeek), binary.LittleEndian.Uint64(start[len(start)-8:]))
	require.Less(t, icmp.Compare(origStart, start), 0)
	require.Less(t, icmp.Compare(start, limit), 0)
}

func TestInternalKeyComparatorFindShortestSeparatorNoChange(t *testing.T) {
	icmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}

	start := makeInternalKeyForComparatorTest([]byte("foo"), 100, TypeValue)
	// "fop" is the immediate successor of "foo"
	// BytewiseComparator cannot produce a shorter separator
	limit := makeInternalKeyForComparatorTest([]byte("fop"), 90, TypeValue)
	origStart := append([]byte(nil), start...)

	icmp.FindShortestSeparator(&start, limit)

	require.Equal(t, origStart, start)
}

func TestInternalKeyComparatorFindShortSuccessor(t *testing.T) {
	icmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}

	key := makeInternalKeyForComparatorTest([]byte("foo"), 100, TypeValue)
	origKey := append([]byte(nil), key...)

	icmp.FindShortSuccessor(&key)

	require.Equal(t, []byte("g"), ExtractUserKey(key))
	require.Equal(t, PackSequenceAndType(MaxSequenceNumber, TypeForSeek), binary.LittleEndian.Uint64(key[len(key)-8:]))
	require.Less(t, icmp.Compare(origKey, key), 0)
}

func TestInternalKeyComparatorFindShortSuccessorNoChange(t *testing.T) {
	icmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}

	key := makeInternalKeyForComparatorTest([]byte{0xff, 0xff}, 100, TypeValue)
	origKey := append([]byte(nil), key...)

	icmp.FindShortSuccessor(&key)

	require.Equal(t, origKey, key)
}

func makeInternalKeyForComparatorTest(user []byte, seq uint64, t ValueType) []byte {
	out := make([]byte, 0, len(user)+8)
	out = append(out, user...)
	return binary.LittleEndian.AppendUint64(out, PackSequenceAndType(seq, t))
}
