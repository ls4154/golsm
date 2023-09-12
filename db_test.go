package goldb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDBBasic(t *testing.T) {
	testDir := t.TempDir()
	db, err := Open(DefaultOptions(), testDir)
	require.NoError(t, err)

	key1, val1 := []byte("key1"), []byte("some value")
	err = db.Put(key1, val1, WriteOptions{})
	require.NoError(t, err)

	val, err := db.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	key2, val2 := []byte("key2"), []byte("hello world")
	err = db.Put(key2, val2, WriteOptions{})
	require.NoError(t, err)

	val, err = db.Get(key2, nil)
	require.NoError(t, err)
	require.Equal(t, val2, val)

	val1 = []byte("new value")
	err = db.Put(key1, val1, WriteOptions{})
	require.NoError(t, err)

	val, err = db.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	err = db.Delete(key2, WriteOptions{})
	require.NoError(t, err)

	_, err = db.Get(key2, nil)
	require.ErrorIs(t, err, ErrNotFound)

	key3, val3 := []byte(""), []byte("empty key")
	err = db.Put(key3, val3, WriteOptions{})
	require.NoError(t, err)

	val, err = db.Get(key3, nil)
	require.NoError(t, err)
	require.Equal(t, val3, val)

	val, err = db.Get(nil, nil)
	require.NoError(t, err)
	require.Equal(t, val3, val)

	err = db.Delete(key3, WriteOptions{})
	require.NoError(t, err)

	_, err = db.Get(key3, nil)
	require.ErrorIs(t, err, ErrNotFound)

	db.Close()
}

func TestBatch(t *testing.T) {
	testDir := t.TempDir()
	db, err := Open(DefaultOptions(), testDir)
	require.NoError(t, err)

	batch := NewWriteBatch()
	for i := 0; i < 10; i++ {
		batch.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}
	err = db.Write(batch, WriteOptions{})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key, err := db.Get([]byte(fmt.Sprintf("key%d", i)), nil)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("value%d", i), string(key))
	}

	batch = NewWriteBatch()
	for i := 0; i < 10; i++ {
		batch.Delete([]byte(fmt.Sprintf("key%d", i)))
	}
	err = db.Write(batch, WriteOptions{})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("key%d", i)), nil)
		require.ErrorIs(t, err, ErrNotFound)
	}

	db.Close()
}

func TestSnapshot(t *testing.T) {
	testDir := t.TempDir()
	db, err := Open(DefaultOptions(), testDir)
	require.NoError(t, err)

	key1, val1 := []byte("key1"), []byte("v1")
	err = db.Put(key1, val1, WriteOptions{})
	require.NoError(t, err)

	snap := db.GetSnapshot()

	val2 := []byte("v2")
	err = db.Put(key1, val2, WriteOptions{})
	key2 := []byte("key2")
	err = db.Put(key2, nil, WriteOptions{})

	val, err := db.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, val2, val)

	val, err = db.Get(key1, &ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	require.Equal(t, val1, val)

	val, err = db.Get(key2, &ReadOptions{Snapshot: snap})
	require.ErrorIs(t, err, ErrNotFound)

	db.ReleaseSnapshot(snap)

	snap = db.GetSnapshot()
	val, err = db.Get(key1, &ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	require.Equal(t, val2, val)

	db.ReleaseSnapshot(snap)

	db.Close()
}

func TestSnapshotDelete(t *testing.T) {
	testDir := t.TempDir()
	db, err := Open(DefaultOptions(), testDir)
	require.NoError(t, err)

	key, val := []byte("key"), []byte("val")
	err = db.Put(key, val, WriteOptions{})
	require.NoError(t, err)

	snap := db.GetSnapshot()

	err = db.Delete(key, WriteOptions{})
	require.NoError(t, err)

	readVal, err := db.Get(key, &ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	require.Equal(t, val, readVal)

	db.ReleaseSnapshot(snap)

	db.Close()
}

func TestRecover(t *testing.T) {
	testDir := t.TempDir()
	db, err := Open(DefaultOptions(), testDir)
	require.NoError(t, err)

	keys := []string{"foo", "bar", "hello", "empty", ""}
	values := []string{"foovalue", "barvalue", "world", "", "empty"}
	for i := range keys {
		err = db.Put([]byte(keys[i]), []byte(values[i]), WriteOptions{})
		require.NoError(t, err)
	}
	err = db.Close()
	require.NoError(t, err)

	db, err = Open(DefaultOptions(), testDir)
	require.NoError(t, err)

	for i := range keys {
		val, err := db.Get([]byte(keys[i]), nil)
		require.NoError(t, err)
		require.Equal(t, values[i], string(val))
	}

	db.Close()
}
