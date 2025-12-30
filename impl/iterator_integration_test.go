package impl

import (
	"fmt"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

func TestIteratorAcrossMemAndL0(t *testing.T) {
	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 128 * 1024
	ldb, err := Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	// Force a flush so some data lands in L0.
	require.NoError(t, ldb.Put([]byte("a"), []byte("1"), &db.WriteOptions{}))
	require.NoError(t, ldb.Put([]byte("b"), []byte("2"), &db.WriteOptions{}))
	require.NoError(t, ldb.Put([]byte("c"), []byte("3"), &db.WriteOptions{}))

	dbi := ldb.(*dbImpl)
	dbi.mu.Lock()
	dbi.makeRoomForWrite(true)
	dbi.mu.Unlock()
	dbi.bgWork.writerWaitForFlushDone()

	// Update in memtable.
	require.NoError(t, ldb.Put([]byte("b"), []byte("2b"), &db.WriteOptions{}))
	require.NoError(t, ldb.Delete([]byte("c"), &db.WriteOptions{}))
	require.NoError(t, ldb.Put([]byte("d"), []byte("4"), &db.WriteOptions{}))

	it, err := ldb.NewIterator(nil)
	require.NoError(t, err)
	defer it.Close()

	var keys []string
	var values []string
	it.SeekToFirst()
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
		it.Next()
	}
	require.NoError(t, it.Error())
	require.Equal(t, []string{"a", "b", "d"}, keys)
	require.Equal(t, []string{"1", "2b", "4"}, values)
}

func TestIteratorSnapshotStableDuringFlush(t *testing.T) {
	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 128 * 1024
	ldb, err := Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		require.NoError(t, ldb.Put([]byte(key), []byte("v1"), &db.WriteOptions{}))
	}

	it, err := ldb.NewIterator(nil)
	require.NoError(t, err)
	defer it.Close()

	dbi := ldb.(*dbImpl)
	dbi.mu.Lock()
	dbi.makeRoomForWrite(true)
	dbi.mu.Unlock()
	dbi.bgWork.writerWaitForFlushDone()

	require.NoError(t, ldb.Put([]byte("k05"), []byte("v2"), &db.WriteOptions{}))
	require.NoError(t, ldb.Delete([]byte("k06"), &db.WriteOptions{}))

	it.Seek([]byte("k05"))
	require.True(t, it.Valid())
	require.Equal(t, "k05", string(it.Key()))
	require.Equal(t, "v1", string(it.Value()))

	it.Seek([]byte("k06"))
	require.True(t, it.Valid())
	require.Equal(t, "k06", string(it.Key()))
	require.Equal(t, "v1", string(it.Value()))
	require.NoError(t, it.Error())
}

func TestIteratorReverseTraversal(t *testing.T) {
	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 128 * 1024
	ldb, err := Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	require.NoError(t, ldb.Put([]byte("a"), []byte("1"), &db.WriteOptions{}))
	require.NoError(t, ldb.Put([]byte("b"), []byte("2"), &db.WriteOptions{}))
	require.NoError(t, ldb.Put([]byte("c"), []byte("3"), &db.WriteOptions{}))

	dbi := ldb.(*dbImpl)
	dbi.mu.Lock()
	dbi.makeRoomForWrite(true)
	dbi.mu.Unlock()
	dbi.bgWork.writerWaitForFlushDone()

	require.NoError(t, ldb.Put([]byte("d"), []byte("4"), &db.WriteOptions{}))
	require.NoError(t, ldb.Delete([]byte("b"), &db.WriteOptions{}))

	it, err := ldb.NewIterator(nil)
	require.NoError(t, err)
	defer it.Close()

	var keys []string
	var values []string
	it.SeekToLast()
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
		it.Prev()
	}
	require.NoError(t, it.Error())
	require.Equal(t, []string{"d", "c", "a"}, keys)
	require.Equal(t, []string{"4", "3", "1"}, values)
}

func TestIteratorSnapshotFullTraversal(t *testing.T) {
	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 128 * 1024
	ldb, err := Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		require.NoError(t, ldb.Put([]byte(key), []byte("v1"), &db.WriteOptions{}))
	}

	it, err := ldb.NewIterator(nil)
	require.NoError(t, err)
	defer it.Close()

	dbi := ldb.(*dbImpl)
	dbi.mu.Lock()
	dbi.makeRoomForWrite(true)
	dbi.mu.Unlock()
	dbi.bgWork.writerWaitForFlushDone()

	// Writes after snapshot should not be visible
	require.NoError(t, ldb.Put([]byte("k05"), []byte("v2"), &db.WriteOptions{}))
	require.NoError(t, ldb.Delete([]byte("k06"), &db.WriteOptions{}))
	require.NoError(t, ldb.Put([]byte("k10"), []byte("new"), &db.WriteOptions{}))

	var keys []string
	var values []string
	it.SeekToFirst()
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
		it.Next()
	}
	require.NoError(t, it.Error())

	expectedKeys := make([]string, 10)
	expectedValues := make([]string, 10)
	for i := 0; i < 10; i++ {
		expectedKeys[i] = fmt.Sprintf("k%02d", i)
		expectedValues[i] = "v1"
	}
	require.Equal(t, expectedKeys, keys)
	require.Equal(t, expectedValues, values)
}
