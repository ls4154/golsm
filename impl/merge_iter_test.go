package impl

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

type sliceIter struct {
	keys   [][]byte
	values [][]byte
	idx    int
	cmp    db.Comparator
}

func newSliceIter(pairs [][2][]byte) *sliceIter {
	return newSliceIterWithCmp(pairs, util.BytewiseComparator)
}

func newSliceIterWithCmp(pairs [][2][]byte, cmp db.Comparator) *sliceIter {
	keys := make([][]byte, 0, len(pairs))
	values := make([][]byte, 0, len(pairs))
	for _, p := range pairs {
		keys = append(keys, p[0])
		values = append(values, p[1])
	}
	return &sliceIter{keys: keys, values: values, idx: -1, cmp: cmp}
}

func (it *sliceIter) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *sliceIter) Next() {
	if it.idx < len(it.keys) {
		it.idx++
	}
}

func (it *sliceIter) Prev() {
	if it.idx >= -1 {
		it.idx--
	}
}

func (it *sliceIter) SeekToFirst() {
	if len(it.keys) == 0 {
		it.idx = -1
		return
	}
	it.idx = 0
}

func (it *sliceIter) SeekToLast() {
	if len(it.keys) == 0 {
		it.idx = -1
		return
	}
	it.idx = len(it.keys) - 1
}

func (it *sliceIter) Seek(key []byte) {
	it.idx = -1
	for i, k := range it.keys {
		if it.cmp.Compare(k, key) >= 0 {
			it.idx = i
			return
		}
	}
}

func (it *sliceIter) Key() []byte {
	return it.keys[it.idx]
}

func (it *sliceIter) Value() []byte {
	return it.values[it.idx]
}

func (it *sliceIter) Error() error {
	return nil
}

func (it *sliceIter) Close() error {
	return nil
}

func TestMergingIteratorForward(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	it1 := newSliceIter([][2][]byte{
		{internalKey("a", 1), []byte("1")},
		{internalKey("c", 1), []byte("3")},
		{internalKey("e", 1), []byte("5")},
	})
	it2 := newSliceIter([][2][]byte{
		{internalKey("b", 1), []byte("2")},
		{internalKey("d", 1), []byte("4")},
		{internalKey("f", 1), []byte("6")},
	})

	mit := newMergingIterator(cmp, []db.Iterator{it1, it2})
	defer mit.Close()

	mit.SeekToFirst()
	var keys []string
	for mit.Valid() {
		keys = append(keys, string(ExtractUserKey(mit.Key())))
		mit.Next()
	}
	require.Equal(t, []string{"a", "b", "c", "d", "e", "f"}, keys)
}

func TestMergingIteratorReverse(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	it1 := newSliceIter([][2][]byte{
		{internalKey("a", 1), []byte("1")},
		{internalKey("c", 1), []byte("3")},
		{internalKey("e", 1), []byte("5")},
	})
	it2 := newSliceIter([][2][]byte{
		{internalKey("b", 1), []byte("2")},
		{internalKey("d", 1), []byte("4")},
		{internalKey("f", 1), []byte("6")},
	})

	mit := newMergingIterator(cmp, []db.Iterator{it1, it2})
	defer mit.Close()

	mit.SeekToLast()
	var keys []string
	for mit.Valid() {
		keys = append(keys, string(ExtractUserKey(mit.Key())))
		mit.Prev()
	}
	require.Equal(t, []string{"f", "e", "d", "c", "b", "a"}, keys)
}

func TestMergingIteratorSeek(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	it1 := newSliceIterWithCmp([][2][]byte{
		{internalKey("a", 1), []byte("1")},
		{internalKey("c", 1), []byte("3")},
		{internalKey("e", 1), []byte("5")},
	}, cmp)
	it2 := newSliceIterWithCmp([][2][]byte{
		{internalKey("b", 1), []byte("2")},
		{internalKey("d", 1), []byte("4")},
		{internalKey("f", 1), []byte("6")},
	}, cmp)

	mit := newMergingIterator(cmp, []db.Iterator{it1, it2})
	defer mit.Close()

	mit.Seek(internalKey("c", MaxSequenceNumber))
	require.True(t, mit.Valid())
	require.Equal(t, "c", string(ExtractUserKey(mit.Key())))

	mit.Seek(internalKey("bb", MaxSequenceNumber))
	require.True(t, mit.Valid())
	require.Equal(t, "c", string(ExtractUserKey(mit.Key())))

	mit.Seek(internalKey("g", MaxSequenceNumber))
	require.False(t, mit.Valid())
}

func TestMergingIteratorDirectionSwitchForwardToReverse(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	it1 := newSliceIter([][2][]byte{
		{internalKey("a", 1), []byte("1")},
		{internalKey("c", 1), []byte("3")},
		{internalKey("e", 1), []byte("5")},
	})
	it2 := newSliceIter([][2][]byte{
		{internalKey("b", 1), []byte("2")},
		{internalKey("d", 1), []byte("4")},
		{internalKey("f", 1), []byte("6")},
	})

	mit := newMergingIterator(cmp, []db.Iterator{it1, it2})
	defer mit.Close()

	mit.SeekToFirst()
	require.Equal(t, "a", string(ExtractUserKey(mit.Key())))
	mit.Next()
	require.Equal(t, "b", string(ExtractUserKey(mit.Key())))
	mit.Next()
	require.Equal(t, "c", string(ExtractUserKey(mit.Key())))

	// Switch to reverse
	mit.Prev()
	require.True(t, mit.Valid())
	require.Equal(t, "b", string(ExtractUserKey(mit.Key())))

	mit.Prev()
	require.True(t, mit.Valid())
	require.Equal(t, "a", string(ExtractUserKey(mit.Key())))

	mit.Prev()
	require.False(t, mit.Valid())
}

func TestMergingIteratorDirectionSwitchReverseToForward(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	it1 := newSliceIter([][2][]byte{
		{internalKey("a", 1), []byte("1")},
		{internalKey("c", 1), []byte("3")},
		{internalKey("e", 1), []byte("5")},
	})
	it2 := newSliceIter([][2][]byte{
		{internalKey("b", 1), []byte("2")},
		{internalKey("d", 1), []byte("4")},
		{internalKey("f", 1), []byte("6")},
	})

	mit := newMergingIterator(cmp, []db.Iterator{it1, it2})
	defer mit.Close()

	mit.SeekToLast()
	require.Equal(t, "f", string(ExtractUserKey(mit.Key())))
	mit.Prev()
	require.Equal(t, "e", string(ExtractUserKey(mit.Key())))
	mit.Prev()
	require.Equal(t, "d", string(ExtractUserKey(mit.Key())))

	// Switch to forward
	mit.Next()
	require.True(t, mit.Valid())
	require.Equal(t, "e", string(ExtractUserKey(mit.Key())))

	mit.Next()
	require.True(t, mit.Valid())
	require.Equal(t, "f", string(ExtractUserKey(mit.Key())))

	mit.Next()
	require.False(t, mit.Valid())
}

func TestMergingIteratorDuplicateKeys(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	it1 := newSliceIter([][2][]byte{
		{internalKey("a", 3), []byte("a3")},
		{internalKey("b", 1), []byte("b1")},
	})
	it2 := newSliceIter([][2][]byte{
		{internalKey("a", 2), []byte("a2")},
		{internalKey("b", 4), []byte("b4")},
	})

	mit := newMergingIterator(cmp, []db.Iterator{it1, it2})
	defer mit.Close()

	// Forward: should see higher seq first (internal key ordering)
	mit.SeekToFirst()
	var keys []string
	var seqs []SequenceNumber
	for mit.Valid() {
		parsed, err := ParseInternalKey(mit.Key())
		require.NoError(t, err)
		keys = append(keys, string(parsed.UserKey))
		seqs = append(seqs, parsed.Sequence)
		mit.Next()
	}
	require.Equal(t, []string{"a", "a", "b", "b"}, keys)
	require.Equal(t, []SequenceNumber{3, 2, 4, 1}, seqs)
}

func TestMergingIteratorEmptyChildren(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}

	mit := newMergingIterator(cmp, []db.Iterator{})
	defer mit.Close()

	mit.SeekToFirst()
	require.False(t, mit.Valid())

	mit.SeekToLast()
	require.False(t, mit.Valid())
}

func TestMergingIteratorSingleChild(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	it1 := newSliceIter([][2][]byte{
		{internalKey("a", 1), []byte("1")},
		{internalKey("b", 1), []byte("2")},
	})

	mit := newMergingIterator(cmp, []db.Iterator{it1})
	defer mit.Close()

	mit.SeekToFirst()
	require.True(t, mit.Valid())
	require.Equal(t, "a", string(ExtractUserKey(mit.Key())))
	mit.Next()
	require.Equal(t, "b", string(ExtractUserKey(mit.Key())))
	mit.Next()
	require.False(t, mit.Valid())
}

type errorIter struct {
	sliceIter
	err error
}

func (it *errorIter) Error() error { return it.err }

func TestMergingIteratorErrorPropagation(t *testing.T) {
	cmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	it1 := newSliceIter([][2][]byte{
		{internalKey("a", 1), []byte("1")},
	})
	it2 := &errorIter{
		sliceIter: *newSliceIter([][2][]byte{
			{internalKey("b", 1), []byte("2")},
		}),
		err: fmt.Errorf("test error"),
	}

	mit := newMergingIterator(cmp, []db.Iterator{it1, it2})
	defer mit.Close()

	require.Error(t, mit.Error())
	require.Contains(t, mit.Error().Error(), "test error")
}

func internalKey(user string, seq SequenceNumber) []byte {
	key := []byte(user)
	out := make([]byte, 0, len(key)+8)
	out = append(out, key...)
	out = append(out, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(out[len(key):], PackSequenceAndType(seq, TypeValue))
	return out
}
