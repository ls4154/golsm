package impl

import (
	"errors"
	"math"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

type batchOp struct {
	kind  string
	key   []byte
	value []byte
}

type recordingWriteBatchHandler struct {
	ops []batchOp
}

func (h *recordingWriteBatchHandler) Put(key, value []byte) error {
	h.ops = append(h.ops, batchOp{
		kind:  "put",
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	return nil
}

func (h *recordingWriteBatchHandler) Delete(key []byte) error {
	h.ops = append(h.ops, batchOp{
		kind: "delete",
		key:  append([]byte(nil), key...),
	})
	return nil
}

type stopAfterNHandler struct {
	remaining int
	err       error
}

func (h *stopAfterNHandler) Put(_, _ []byte) error {
	h.remaining--
	if h.remaining == 0 {
		return h.err
	}
	return nil
}

func (h *stopAfterNHandler) Delete(_ []byte) error {
	h.remaining--
	if h.remaining == 0 {
		return h.err
	}
	return nil
}

func newRepeatedSameKeyBatchEndingWithDelete(t *testing.T) *WriteBatchImpl {
	t.Helper()

	batch := NewWriteBatch()
	require.NoError(t, batch.Put([]byte("k"), []byte("v1")))
	require.NoError(t, batch.Delete([]byte("k")))
	require.NoError(t, batch.Put([]byte("k"), []byte("v2")))
	require.NoError(t, batch.Delete([]byte("k")))
	return batch
}

func newRepeatedSameKeyBatchEndingWithPut(t *testing.T) *WriteBatchImpl {
	t.Helper()

	batch := newRepeatedSameKeyBatchEndingWithDelete(t)
	require.NoError(t, batch.Put([]byte("k"), []byte("v3")))
	return batch
}

func requireMemTableEntry(t *testing.T, mt *MemTable, key []byte, seq SequenceNumber, wantValue []byte, wantDeleted, wantExist bool) {
	t.Helper()

	var lookup LookupKey
	lookup.Set(key, seq)
	value, deleted, exist := mt.Get(&lookup)
	require.Equal(t, wantExist, exist)
	if !wantExist {
		require.False(t, deleted)
		require.Nil(t, value)
		return
	}

	require.Equal(t, wantDeleted, deleted)
	if wantDeleted {
		require.Nil(t, value)
		return
	}

	require.Equal(t, wantValue, value)
}

func TestWriteBatchIterateVisitsEntriesInInsertionOrder(t *testing.T) {
	var batch db.WriteBatch = newRepeatedSameKeyBatchEndingWithPut(t)
	handler := &recordingWriteBatchHandler{}

	require.NoError(t, batch.Iterate(handler))
	require.Equal(t, []batchOp{
		{kind: "put", key: []byte("k"), value: []byte("v1")},
		{kind: "delete", key: []byte("k")},
		{kind: "put", key: []byte("k"), value: []byte("v2")},
		{kind: "delete", key: []byte("k")},
		{kind: "put", key: []byte("k"), value: []byte("v3")},
	}, handler.ops)
}

func TestWriteBatchIteratePropagatesHandlerError(t *testing.T) {
	var batch db.WriteBatch = newRepeatedSameKeyBatchEndingWithPut(t)
	wantErr := errors.New("stop iterate")

	err := batch.Iterate(&stopAfterNHandler{remaining: 3, err: wantErr})
	require.ErrorIs(t, err, wantErr)
}

func TestWriteBatchIterateRejectsNilHandler(t *testing.T) {
	var batch db.WriteBatch = newRepeatedSameKeyBatchEndingWithDelete(t)

	err := batch.Iterate(nil)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
}

func TestWriteBatchClearRemovesAllEntries(t *testing.T) {
	var batch db.WriteBatch = newRepeatedSameKeyBatchEndingWithPut(t)
	batch.Clear()

	handler := &recordingWriteBatchHandler{}
	require.NoError(t, batch.Iterate(handler))
	require.Empty(t, handler.ops)

	require.NoError(t, batch.Put([]byte("k"), []byte("fresh")))
	require.NoError(t, batch.Iterate(handler))
	require.Equal(t, []batchOp{
		{kind: "put", key: []byte("k"), value: []byte("fresh")},
	}, handler.ops)
}

func TestWriteBatchAppendCopiesEntriesInInsertionOrder(t *testing.T) {
	var dst db.WriteBatch = NewWriteBatch()
	require.NoError(t, dst.Put([]byte("a"), []byte("1")))

	var src db.WriteBatch = newRepeatedSameKeyBatchEndingWithDelete(t)
	require.NoError(t, dst.Append(src))

	handler := &recordingWriteBatchHandler{}
	require.NoError(t, dst.Iterate(handler))
	require.Equal(t, []batchOp{
		{kind: "put", key: []byte("a"), value: []byte("1")},
		{kind: "put", key: []byte("k"), value: []byte("v1")},
		{kind: "delete", key: []byte("k")},
		{kind: "put", key: []byte("k"), value: []byte("v2")},
		{kind: "delete", key: []byte("k")},
	}, handler.ops)
}

func TestWriteBatchAppendRejectsCountOverflow(t *testing.T) {
	dst := NewWriteBatch()
	dst.setCount(math.MaxUint32)

	src := NewWriteBatch()
	require.NoError(t, src.Put([]byte("k"), []byte("v")))

	err := dst.Append(src)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
}

func TestWriteBatchSameKeyRepeatedOpsEndingWithDeleteAssignsIncreasingSequenceNumbers(t *testing.T) {
	batch := newRepeatedSameKeyBatchEndingWithDelete(t)
	batch.setSequence(7)

	mt := NewMemTable(&InternalKeyComparator{userCmp: util.BytewiseComparator})
	require.NoError(t, batch.InsertIntoMemTable(mt))

	key := []byte("k")
	requireMemTableEntry(t, mt, key, 10, nil, true, true)
	requireMemTableEntry(t, mt, key, 9, []byte("v2"), false, true)
	requireMemTableEntry(t, mt, key, 8, nil, true, true)
	requireMemTableEntry(t, mt, key, 7, []byte("v1"), false, true)
	requireMemTableEntry(t, mt, key, 6, nil, false, false)
}

func TestWriteBatchSameKeyRepeatedOpsEndingWithPutAssignsIncreasingSequenceNumbers(t *testing.T) {
	batch := newRepeatedSameKeyBatchEndingWithPut(t)
	batch.setSequence(7)

	mt := NewMemTable(&InternalKeyComparator{userCmp: util.BytewiseComparator})
	require.NoError(t, batch.InsertIntoMemTable(mt))

	key := []byte("k")
	requireMemTableEntry(t, mt, key, 11, []byte("v3"), false, true)
	requireMemTableEntry(t, mt, key, 10, nil, true, true)
	requireMemTableEntry(t, mt, key, 9, []byte("v2"), false, true)
	requireMemTableEntry(t, mt, key, 8, nil, true, true)
	requireMemTableEntry(t, mt, key, 7, []byte("v1"), false, true)
	requireMemTableEntry(t, mt, key, 6, nil, false, false)
}

func TestWriteBatchSameKeyRepeatedOpsEndingWithDeletePersistsAcrossRecovery(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	batch := newRepeatedSameKeyBatchEndingWithDelete(t)
	require.NoError(t, ldb.Write(batch, nil))

	_, err = ldb.Get([]byte("k"), nil)
	require.ErrorIs(t, err, db.ErrNotFound)

	require.NoError(t, ldb.Close())

	ldb, err = Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	_, err = ldb.Get([]byte("k"), nil)
	require.ErrorIs(t, err, db.ErrNotFound)
}

func TestWriteBatchSameKeyRepeatedOpsEndingWithPutPersistsAcrossRecovery(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	batch := newRepeatedSameKeyBatchEndingWithPut(t)
	require.NoError(t, ldb.Write(batch, nil))

	value, err := ldb.Get([]byte("k"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte("v3"), value)

	require.NoError(t, ldb.Close())

	ldb, err = Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	value, err = ldb.Get([]byte("k"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte("v3"), value)
}

func makeMalformedBatch(t *testing.T, seq SequenceNumber) *WriteBatchImpl {
	t.Helper()

	batch := NewWriteBatch()
	require.NoError(t, batch.Put([]byte("a"), []byte("va")))
	require.NoError(t, batch.Put([]byte("b"), []byte("vb")))
	batch.setSequence(seq)

	raw := append([]byte(nil), batch.contents()...)
	return WriteBatchFromContents(raw[:len(raw)-1])
}

func TestWriteBatchInsertIntoMemTableAppliesPrefixBeforeCorruption(t *testing.T) {
	batch := makeMalformedBatch(t, 7)
	mt := NewMemTable(&InternalKeyComparator{userCmp: util.BytewiseComparator})

	err := batch.InsertIntoMemTable(mt)
	require.ErrorIs(t, err, db.ErrCorruption)

	var lookup LookupKey
	lookup.Set([]byte("a"), MaxSequenceNumber)
	value, deleted, found := mt.Get(&lookup)
	require.True(t, found)
	require.False(t, deleted)
	require.Equal(t, []byte("va"), value)

	lookup.Set([]byte("b"), MaxSequenceNumber)
	value, deleted, found = mt.Get(&lookup)
	require.False(t, found)
	require.False(t, deleted)
	require.Nil(t, value)
}
