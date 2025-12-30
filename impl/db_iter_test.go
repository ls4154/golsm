package impl

import (
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

type corruptInternalIterator struct {
	valid bool
}

func (it *corruptInternalIterator) Valid() bool            { return it.valid }
func (it *corruptInternalIterator) SeekToFirst()           { it.valid = true }
func (it *corruptInternalIterator) SeekToLast()            { it.valid = true }
func (it *corruptInternalIterator) Seek(_ []byte)          { it.valid = true }
func (it *corruptInternalIterator) Next()                  { it.valid = false }
func (it *corruptInternalIterator) Prev()                  { it.valid = false }
func (it *corruptInternalIterator) Key() []byte            { return []byte("bad") } // invalid internal key
func (it *corruptInternalIterator) Value() []byte          { return []byte("v") }
func (it *corruptInternalIterator) Error() error           { return nil }
func (it *corruptInternalIterator) Close() error           { return nil }
func (it *corruptInternalIterator) String() string         { return "corrupt-internal-iterator" }
func (it *corruptInternalIterator) RegisterCleanup(func()) {}

func buildInternalKeys(t *testing.T, entries []struct {
	seq uint64
	typ ValueType
	key string
	val string
},
) db.Iterator {
	t.Helper()
	mt := NewMemTable(&InternalKeyComparator{userCmp: util.BytewiseComparator})
	for _, e := range entries {
		if e.typ == TypeDeletion {
			mt.Delete(e.seq, []byte(e.key))
			continue
		}
		mt.Put(e.seq, []byte(e.key), []byte(e.val))
	}
	return mt.Iterator()
}

func collectIter(it *dbIter) (keys []string, values []string) {
	it.SeekToFirst()
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
		it.Next()
	}
	return keys, values
}

func TestDBIterRespectsSnapshotSeq(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "1"},
		{seq: 2, typ: TypeValue, key: "a", val: "2"},
		{seq: 3, typ: TypeDeletion, key: "b"},
		{seq: 4, typ: TypeValue, key: "b", val: "4"},
		{seq: 5, typ: TypeDeletion, key: "c"},
		{seq: 6, typ: TypeValue, key: "c", val: "6"},
	})

	di := newDBIter(iter, util.BytewiseComparator, 4)
	defer di.Close()

	keys, values := collectIter(di)
	require.Equal(t, []string{"a", "b"}, keys)
	require.Equal(t, []string{"2", "4"}, values)

	di.seq = 5
	keys, values = collectIter(di)
	require.Equal(t, []string{"a", "b"}, keys)
	require.Equal(t, []string{"2", "4"}, values)

	di.seq = 6
	keys, values = collectIter(di)
	require.Equal(t, []string{"a", "b", "c"}, keys)
	require.Equal(t, []string{"2", "4", "6"}, values)
}

func TestDBIterDeletionSkipsKey(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "1"},
		{seq: 2, typ: TypeDeletion, key: "a"},
		{seq: 3, typ: TypeValue, key: "b", val: "2"},
	})

	di := newDBIter(iter, util.BytewiseComparator, 3)
	defer di.Close()

	keys, values := collectIter(di)
	require.Equal(t, []string{"b"}, keys)
	require.Equal(t, []string{"2"}, values)
}

func TestDBIterDirectionSwitchAroundSeek(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 3, typ: TypeValue, key: "a", val: "a3"},
		{seq: 2, typ: TypeDeletion, key: "a"},
		{seq: 4, typ: TypeValue, key: "b", val: "b4"},
		{seq: 1, typ: TypeValue, key: "c", val: "c1"},
	})

	di := newDBIter(iter, util.BytewiseComparator, MaxSequenceNumber)
	defer di.Close()

	di.Seek([]byte("b"))
	require.True(t, di.Valid())
	require.Equal(t, "b", string(di.Key()))

	di.Prev()
	require.True(t, di.Valid())
	require.Equal(t, "a", string(di.Key()))
	require.Equal(t, "a3", string(di.Value()))

	di.Next()
	require.True(t, di.Valid())
	require.Equal(t, "b", string(di.Key()))

	di.Next()
	require.True(t, di.Valid())
	require.Equal(t, "c", string(di.Key()))

	di.Prev()
	require.True(t, di.Valid())
	require.Equal(t, "b", string(di.Key()))
}

func TestDBIterSnapshotBoundary(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "k", val: "v1"},
		{seq: 2, typ: TypeDeletion, key: "k"},
		{seq: 3, typ: TypeValue, key: "k", val: "v3"},
		{seq: 1, typ: TypeValue, key: "m", val: "m1"},
	})

	di := newDBIter(iter, util.BytewiseComparator, 0)
	defer di.Close()

	keys, values := collectIter(di)
	require.Empty(t, keys)
	require.Empty(t, values)

	di.seq = 1
	keys, values = collectIter(di)
	require.Equal(t, []string{"k", "m"}, keys)
	require.Equal(t, []string{"v1", "m1"}, values)

	di.seq = 2
	keys, values = collectIter(di)
	require.Equal(t, []string{"m"}, keys)
	require.Equal(t, []string{"m1"}, values)

	di.seq = 3
	keys, values = collectIter(di)
	require.Equal(t, []string{"k", "m"}, keys)
	require.Equal(t, []string{"v3", "m1"}, values)
}

func TestDBIterSeekBoundary(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "1"},
		{seq: 1, typ: TypeValue, key: "b", val: "2"},
	})

	di := newDBIter(iter, util.BytewiseComparator, MaxSequenceNumber)
	defer di.Close()

	di.Seek([]byte("0"))
	require.True(t, di.Valid())
	require.Equal(t, "a", string(di.Key()))

	di.Seek([]byte("bb"))
	require.False(t, di.Valid())

	di.SeekToLast()
	require.True(t, di.Valid())
	require.Equal(t, "b", string(di.Key()))
	di.Next()
	require.False(t, di.Valid())

	di.SeekToFirst()
	require.True(t, di.Valid())
	require.Equal(t, "a", string(di.Key()))
	di.Prev()
	require.False(t, di.Valid())
}

func TestDBIterMergedSourcesLatestWins(t *testing.T) {
	source1 := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "a1"},
		{seq: 5, typ: TypeValue, key: "b", val: "b5"},
		{seq: 2, typ: TypeValue, key: "d", val: "d2"},
	})
	source2 := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 3, typ: TypeValue, key: "a", val: "a3"},
		{seq: 4, typ: TypeDeletion, key: "b"},
		{seq: 6, typ: TypeValue, key: "c", val: "c6"},
	})

	merged := newMergingIterator(
		&InternalKeyComparator{userCmp: util.BytewiseComparator},
		[]db.Iterator{source1, source2},
	)
	di := newDBIter(merged, util.BytewiseComparator, 6)
	defer di.Close()

	keys, values := collectIter(di)
	require.Equal(t, []string{"a", "b", "c", "d"}, keys)
	require.Equal(t, []string{"a3", "b5", "c6", "d2"}, values)

	di.seq = 4
	keys, values = collectIter(di)
	require.Equal(t, []string{"a", "d"}, keys)
	require.Equal(t, []string{"a3", "d2"}, values)
}

func TestDBIterPrevSkipsOlderVersionOfSameUserKey(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "a1"},
		{seq: 8, typ: TypeValue, key: "b", val: "b8"},
		{seq: 9, typ: TypeDeletion, key: "b"},
		{seq: 10, typ: TypeValue, key: "b", val: "b10"},
		{seq: 1, typ: TypeValue, key: "c", val: "c1"},
	})

	di := newDBIter(iter, util.BytewiseComparator, 10)
	defer di.Close()

	di.Seek([]byte("b"))
	require.True(t, di.Valid())
	require.Equal(t, "b", string(di.Key()))
	require.Equal(t, "b10", string(di.Value()))

	di.Prev()
	require.True(t, di.Valid())
	require.Equal(t, "a", string(di.Key()))
	require.Equal(t, "a1", string(di.Value()))
}

func TestDBIterReverseFullTraversal(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "1"},
		{seq: 2, typ: TypeValue, key: "b", val: "2"},
		{seq: 3, typ: TypeDeletion, key: "c"},
		{seq: 4, typ: TypeValue, key: "d", val: "4"},
	})

	di := newDBIter(iter, util.BytewiseComparator, 4)
	defer di.Close()

	di.SeekToLast()
	var keys, values []string
	for di.Valid() {
		keys = append(keys, string(di.Key()))
		values = append(values, string(di.Value()))
		di.Prev()
	}
	require.NoError(t, di.Error())
	require.Equal(t, []string{"d", "b", "a"}, keys)
	require.Equal(t, []string{"4", "2", "1"}, values)
}

func TestDBIterReverseWithDeletion(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "a1"},
		{seq: 2, typ: TypeValue, key: "b", val: "b2"},
		{seq: 3, typ: TypeDeletion, key: "b"},
		{seq: 4, typ: TypeValue, key: "c", val: "c4"},
	})

	di := newDBIter(iter, util.BytewiseComparator, 4)
	defer di.Close()

	di.SeekToLast()
	require.True(t, di.Valid())
	require.Equal(t, "c", string(di.Key()))

	di.Prev()
	require.True(t, di.Valid())
	require.Equal(t, "a", string(di.Key()))
	require.Equal(t, "a1", string(di.Value()))

	di.Prev()
	require.False(t, di.Valid())
	require.NoError(t, di.Error())
}

func TestDBIterCorruptionInvalidatesIterator(t *testing.T) {
	di := newDBIter(&corruptInternalIterator{}, util.BytewiseComparator, MaxSequenceNumber)
	defer di.Close()

	di.SeekToFirst()
	require.False(t, di.Valid())
	require.Error(t, di.Error())
}

func TestDBIterCorruptionOnSeekToLast(t *testing.T) {
	di := newDBIter(&corruptInternalIterator{}, util.BytewiseComparator, MaxSequenceNumber)
	defer di.Close()

	di.SeekToLast()
	require.False(t, di.Valid())
	require.Error(t, di.Error())
}

func TestDBIterCorruptionOnSeek(t *testing.T) {
	di := newDBIter(&corruptInternalIterator{}, util.BytewiseComparator, MaxSequenceNumber)
	defer di.Close()

	di.Seek([]byte("x"))
	require.False(t, di.Valid())
	require.Error(t, di.Error())
}

func TestDBIterNoErrorOnNormalPath(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "1"},
		{seq: 2, typ: TypeValue, key: "b", val: "2"},
	})

	di := newDBIter(iter, util.BytewiseComparator, 2)
	defer di.Close()

	keys, values := collectIter(di)
	require.NoError(t, di.Error())
	require.Equal(t, []string{"a", "b"}, keys)
	require.Equal(t, []string{"1", "2"}, values)
}

func TestDBIterDoubleClose(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{
		{seq: 1, typ: TypeValue, key: "a", val: "1"},
	})

	di := newDBIter(iter, util.BytewiseComparator, 1)
	require.NoError(t, di.Close())
	require.NoError(t, di.Close())
}

func TestDBIterEmptySource(t *testing.T) {
	iter := buildInternalKeys(t, []struct {
		seq uint64
		typ ValueType
		key string
		val string
	}{})

	di := newDBIter(iter, util.BytewiseComparator, MaxSequenceNumber)
	defer di.Close()

	di.SeekToFirst()
	require.False(t, di.Valid())

	di.SeekToLast()
	require.False(t, di.Valid())

	di.Seek([]byte("a"))
	require.False(t, di.Valid())
	require.NoError(t, di.Error())
}
