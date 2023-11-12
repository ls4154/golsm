package impl

import (
	"fmt"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

func TestDBBasic(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	key1, val1 := []byte("key1"), []byte("some value")
	err = ldb.Put(key1, val1, db.WriteOptions{})
	require.NoError(t, err)

	val, err := ldb.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	key2, val2 := []byte("key2"), []byte("hello world")
	err = ldb.Put(key2, val2, db.WriteOptions{})
	require.NoError(t, err)

	val, err = ldb.Get(key2, nil)
	require.NoError(t, err)
	require.Equal(t, val2, val)

	val1 = []byte("new value")
	err = ldb.Put(key1, val1, db.WriteOptions{})
	require.NoError(t, err)

	val, err = ldb.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	err = ldb.Delete(key2, db.WriteOptions{})
	require.NoError(t, err)

	_, err = ldb.Get(key2, nil)
	require.ErrorIs(t, err, db.ErrNotFound)

	key3, val3 := []byte(""), []byte("empty key")
	err = ldb.Put(key3, val3, db.WriteOptions{})
	require.NoError(t, err)

	val, err = ldb.Get(key3, nil)
	require.NoError(t, err)
	require.Equal(t, val3, val)

	val, err = ldb.Get(nil, nil)
	require.NoError(t, err)
	require.Equal(t, val3, val)

	err = ldb.Delete(key3, db.WriteOptions{})
	require.NoError(t, err)

	_, err = ldb.Get(key3, nil)
	require.ErrorIs(t, err, db.ErrNotFound)

	ldb.Close()
}

func TestBatch(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	batch := NewWriteBatch()
	for i := 0; i < 10; i++ {
		batch.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}
	err = ldb.Write(batch, db.WriteOptions{})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key, err := ldb.Get([]byte(fmt.Sprintf("key%d", i)), nil)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("value%d", i), string(key))
	}

	batch = NewWriteBatch()
	for i := 0; i < 10; i++ {
		batch.Delete([]byte(fmt.Sprintf("key%d", i)))
	}
	err = ldb.Write(batch, db.WriteOptions{})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := ldb.Get([]byte(fmt.Sprintf("key%d", i)), nil)
		require.ErrorIs(t, err, db.ErrNotFound)
	}

	ldb.Close()
}

func TestDBOverwrite(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	key := []byte("key1")

	for i := 0; i < 1000; i++ {
		val := []byte(fmt.Sprintf("value%d", i))
		err = ldb.Put(key, val, db.WriteOptions{})
		require.NoError(t, err)

		readVal, err := ldb.Get(key, nil)
		require.NoError(t, err)
		require.Equal(t, val, readVal)
	}

	ldb.Close()
}

func TestSnapshot(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	key1, val1 := []byte("key1"), []byte("v1")
	err = ldb.Put(key1, val1, db.WriteOptions{})
	require.NoError(t, err)

	snap := ldb.GetSnapshot()

	val2 := []byte("v2")
	err = ldb.Put(key1, val2, db.WriteOptions{})
	key2 := []byte("key2")
	err = ldb.Put(key2, nil, db.WriteOptions{})

	val, err := ldb.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, val2, val)

	val, err = ldb.Get(key1, &db.ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	require.Equal(t, val1, val)

	val, err = ldb.Get(key2, &db.ReadOptions{Snapshot: snap})
	require.ErrorIs(t, err, db.ErrNotFound)

	snap.Release()

	snap = ldb.GetSnapshot()
	val, err = ldb.Get(key1, &db.ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	require.Equal(t, val2, val)

	snap.Release()

	ldb.Close()
}

func TestSnapshotDelete(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	key, val := []byte("key"), []byte("val")
	err = ldb.Put(key, val, db.WriteOptions{})
	require.NoError(t, err)

	snap := ldb.GetSnapshot()

	err = ldb.Delete(key, db.WriteOptions{})
	require.NoError(t, err)

	readVal, err := ldb.Get(key, &db.ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	require.Equal(t, val, readVal)

	snap.Release()

	ldb.Close()
}

func TestRecover(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	keys := []string{"foo", "bar", "hello", "empty", ""}
	values := []string{"foovalue", "barvalue", "world", "", "empty"}
	for i := range keys {
		err = ldb.Put([]byte(keys[i]), []byte(values[i]), db.WriteOptions{})
		require.NoError(t, err)
	}
	err = ldb.Close()
	require.NoError(t, err)

	ldb, err = Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	for i := range keys {
		val, err := ldb.Get([]byte(keys[i]), nil)
		require.NoError(t, err)
		require.Equal(t, values[i], string(val))
	}

	ldb.Close()
}

func TestSnapshotWithBatch(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	key1, key2, key3 := []byte("key1"), []byte("key2"), []byte("key3")
	require.NoError(t, ldb.Put(key1, []byte("value1"), db.WriteOptions{}))
	require.NoError(t, ldb.Put(key2, []byte("value2"), db.WriteOptions{}))

	snap := ldb.GetSnapshot()

	batch := NewWriteBatch()
	batch.Put(key1, []byte("value1-updated"))
	batch.Delete(key2)
	batch.Put(key3, []byte("value3"))
	require.NoError(t, ldb.Write(batch, db.WriteOptions{}))

	// latest view
	val, err := ldb.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, "value1-updated", string(val))

	_, err = ldb.Get(key2, nil)
	require.ErrorIs(t, err, db.ErrNotFound)

	val, err = ldb.Get(key3, nil)
	require.NoError(t, err)
	require.Equal(t, "value3", string(val))

	// snapshot view
	val, err = ldb.Get(key1, &db.ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	require.Equal(t, "value1", string(val))

	val, err = ldb.Get(key2, &db.ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	require.Equal(t, "value2", string(val))

	_, err = ldb.Get(key3, &db.ReadOptions{Snapshot: snap})
	require.ErrorIs(t, err, db.ErrNotFound)

	snap.Release()

	ldb.Close()
}

func TestDBFlushAndRecover(t *testing.T) {
	t.Skip("TODO: enable after implementing memtable flush / background compaction")

	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 1024
	ldb, err := Open(opt, testDir)
	require.NoError(t, err)

	const totalKeys = 200
	expected := make(map[string]string, totalKeys)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		val := fmt.Sprintf("val%04d", i)
		require.NoError(t, ldb.Put([]byte(key), []byte(val), db.WriteOptions{}))
		expected[key] = val

		if i%5 == 0 {
			require.NoError(t, ldb.Delete([]byte(key), db.WriteOptions{}))
			delete(expected, key)
		}
	}

	require.NoError(t, ldb.Close())

	opt = db.DefaultOptions()
	opt.WriteBufferSize = 1024
	ldb, err = Open(opt, testDir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, ldb.Close())
	}()

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		val, err := ldb.Get([]byte(key), nil)
		if expectedVal, ok := expected[key]; ok {
			require.NoError(t, err)
			require.Equal(t, expectedVal, string(val))
		} else {
			require.ErrorIs(t, err, db.ErrNotFound)
		}
	}
}
