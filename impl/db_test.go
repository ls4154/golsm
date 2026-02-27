package impl

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

func TestDBBasic(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	key1, val1 := []byte("key1"), []byte("some value")
	err = ldb.Put(key1, val1, nil)
	require.NoError(t, err)

	val, err := ldb.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	key2, val2 := []byte("key2"), []byte("hello world")
	err = ldb.Put(key2, val2, nil)
	require.NoError(t, err)

	val, err = ldb.Get(key2, nil)
	require.NoError(t, err)
	require.Equal(t, val2, val)

	val1 = []byte("new value")
	err = ldb.Put(key1, val1, nil)
	require.NoError(t, err)

	val, err = ldb.Get(key1, nil)
	require.NoError(t, err)
	require.Equal(t, val1, val)

	err = ldb.Delete(key2, nil)
	require.NoError(t, err)

	_, err = ldb.Get(key2, nil)
	require.ErrorIs(t, err, db.ErrNotFound)

	key3, val3 := []byte(""), []byte("empty key")
	err = ldb.Put(key3, val3, nil)
	require.NoError(t, err)

	val, err = ldb.Get(key3, nil)
	require.NoError(t, err)
	require.Equal(t, val3, val)

	val, err = ldb.Get(nil, nil)
	require.NoError(t, err)
	require.Equal(t, val3, val)

	err = ldb.Delete(key3, nil)
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
	err = ldb.Write(batch, nil)
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
	err = ldb.Write(batch, nil)
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
		err = ldb.Put(key, val, nil)
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
	err = ldb.Put(key1, val1, nil)
	require.NoError(t, err)

	snap := ldb.GetSnapshot()

	val2 := []byte("v2")
	err = ldb.Put(key1, val2, nil)
	key2 := []byte("key2")
	err = ldb.Put(key2, nil, nil)

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

func TestDBIteratorBasic(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	require.NoError(t, ldb.Put([]byte("a"), []byte("1"), nil))
	require.NoError(t, ldb.Put([]byte("b"), []byte("@"), nil))
	require.NoError(t, ldb.Put([]byte("c"), []byte("3"), nil))
	require.NoError(t, ldb.Put([]byte("cc"), []byte("33"), nil))

	require.NoError(t, ldb.Put([]byte("b"), []byte("2"), nil))
	require.NoError(t, ldb.Delete([]byte("cc"), nil))

	require.NoError(t, ldb.Put([]byte("d"), []byte("4"), nil))
	require.NoError(t, ldb.Put([]byte("e"), []byte("5"), nil))

	it, err := ldb.NewIterator(nil)
	require.NoError(t, err)
	defer it.Close()

	expectedKeys := []string{"a", "b", "c", "d", "e"}
	expectedVals := []string{"1", "2", "3", "4", "5"}

	it.SeekToFirst()
	i := 0
	for it.Valid() {
		require.Equal(t, expectedKeys[i], string(it.Key()))
		require.Equal(t, expectedVals[i], string(it.Value()))
		it.Next()
		i++
	}
	require.Equal(t, len(expectedKeys), i)

	it.SeekToLast()
	i = len(expectedKeys) - 1
	for it.Valid() {
		require.Equal(t, expectedKeys[i], string(it.Key()))
		require.Equal(t, expectedVals[i], string(it.Value()))
		it.Prev()
		i--
	}
	require.Equal(t, -1, i)

	it.Seek([]byte("c"))
	i = 2
	for it.Valid() {
		require.Equal(t, expectedKeys[i], string(it.Key()))
		require.Equal(t, expectedVals[i], string(it.Value()))
		it.Next()
		i++
	}
	require.Equal(t, len(expectedKeys), i)

	it.Seek([]byte("c"))
	i = 2
	for it.Valid() {
		require.Equal(t, expectedKeys[i], string(it.Key()))
		require.Equal(t, expectedVals[i], string(it.Value()))
		it.Prev()
		i--
	}
	require.Equal(t, -1, i)

	it.Seek([]byte("A"))
	require.True(t, it.Valid())
	require.Equal(t, "a", string(it.Key()))
	require.Equal(t, "1", string(it.Value()))

	it.Seek([]byte("cc"))
	require.True(t, it.Valid())
	require.Equal(t, "d", string(it.Key()))
	require.Equal(t, "4", string(it.Value()))

	it.Seek([]byte("z"))
	require.False(t, it.Valid())

	require.NoError(t, it.Error())
}

func TestDBIteratorSnapshotAtCreation(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	require.NoError(t, ldb.Put([]byte("a"), []byte("1"), nil))
	require.NoError(t, ldb.Put([]byte("b"), []byte("2"), nil))
	require.NoError(t, ldb.Put([]byte("c"), []byte("3"), nil))

	it, err := ldb.NewIterator(nil)
	require.NoError(t, err)

	require.NoError(t, ldb.Put([]byte("b"), []byte("2b"), nil))
	require.NoError(t, ldb.Delete([]byte("c"), nil))
	require.NoError(t, ldb.Put([]byte("d"), []byte("4"), nil))

	it.SeekToFirst()
	var keys []string
	var values []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
		it.Next()
	}
	require.NoError(t, it.Error())
	require.Equal(t, []string{"a", "b", "c"}, keys)
	require.Equal(t, []string{"1", "2", "3"}, values)
	require.NoError(t, it.Close())

	it2, err := ldb.NewIterator(nil)
	require.NoError(t, err)
	defer it2.Close()

	it2.SeekToFirst()
	keys = keys[:0]
	values = values[:0]
	for it2.Valid() {
		keys = append(keys, string(it2.Key()))
		values = append(values, string(it2.Value()))
		it2.Next()
	}
	require.NoError(t, it2.Error())
	require.Equal(t, []string{"a", "b", "d"}, keys)
	require.Equal(t, []string{"1", "2b", "4"}, values)
}

func TestDBIteratorWithReadOptionsSnapshot(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	require.NoError(t, ldb.Put([]byte("a"), []byte("1"), nil))
	require.NoError(t, ldb.Put([]byte("b"), []byte("2"), nil))

	snap := ldb.GetSnapshot()
	defer snap.Release()

	require.NoError(t, ldb.Put([]byte("b"), []byte("2b"), nil))
	require.NoError(t, ldb.Delete([]byte("a"), nil))
	require.NoError(t, ldb.Put([]byte("c"), []byte("3"), nil))

	it, err := ldb.NewIterator(&db.ReadOptions{Snapshot: snap})
	require.NoError(t, err)
	defer it.Close()

	it.SeekToFirst()
	var keys []string
	var values []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
		it.Next()
	}
	require.NoError(t, it.Error())
	require.Equal(t, []string{"a", "b"}, keys)
	require.Equal(t, []string{"1", "2"}, values)
}

func TestSnapshotDelete(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	key, val := []byte("key"), []byte("val")
	err = ldb.Put(key, val, nil)
	require.NoError(t, err)

	snap := ldb.GetSnapshot()

	err = ldb.Delete(key, nil)
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
		err = ldb.Put([]byte(keys[i]), []byte(values[i]), nil)
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
	require.NoError(t, ldb.Put(key1, []byte("value1"), nil))
	require.NoError(t, ldb.Put(key2, []byte("value2"), nil))

	snap := ldb.GetSnapshot()

	batch := NewWriteBatch()
	batch.Put(key1, []byte("value1-updated"))
	batch.Delete(key2)
	batch.Put(key3, []byte("value3"))
	require.NoError(t, ldb.Write(batch, nil))

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

func TestSnapshotConcurrentGetRelease(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	require.NoError(t, ldb.Put([]byte("k"), []byte("v"), nil))

	const goroutines = 8
	const iterations = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				snap := ldb.GetSnapshot()
				snap.Release()
			}
		}()
	}
	wg.Wait()
}

func TestDBFlushAndRecover(t *testing.T) {
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
		require.NoError(t, ldb.Put([]byte(key), []byte(val), nil))
		expected[key] = val

		if i%5 == 0 {
			require.NoError(t, ldb.Delete([]byte(key), nil))
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

func TestOpenFailsWhenCreateIfMissingIsFalse(t *testing.T) {
	testDir := t.TempDir()

	opt := db.DefaultOptions()
	opt.CreateIfMissing = false

	_, err := Open(opt, testDir)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
}

func TestOpenFailsWhenErrorIfExistsIsTrue(t *testing.T) {
	testDir := t.TempDir()

	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	require.NoError(t, ldb.Close())

	opt := db.DefaultOptions()
	opt.ErrorIfExists = true

	_, err = Open(opt, testDir)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
}

func TestWriteDuringCloseDoesNotHang(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)

	const workers = 32
	startCh := make(chan struct{})
	errCh := make(chan error, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			<-startCh
			key := []byte(fmt.Sprintf("k-%d", i))
			errCh <- ldb.Put(key, []byte("v"), nil)
		}(i)
	}

	close(startCh)
	require.NoError(t, ldb.Close())

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent writes did not return after Close")
	}

	close(errCh)
	for err := range errCh {
		if err == nil {
			continue
		}
		require.ErrorContains(t, err, "db closed")
	}

	err = ldb.Put([]byte("after-close"), []byte("v"), nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "db closed")
}
