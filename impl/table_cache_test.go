package impl

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

type memWritableFile struct {
	buf bytes.Buffer
}

func (f *memWritableFile) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}

func (f *memWritableFile) Flush() error {
	return nil
}

func (f *memWritableFile) Sync() error {
	return nil
}

func (f *memWritableFile) Close() error {
	return nil
}

func buildTableBytes(t *testing.T) []byte {
	t.Helper()

	f := &memWritableFile{}
	builder := table.NewTableBuilder(f, util.BytewiseComparator, 4096, db.NoCompression, 16, nil)
	builder.Add([]byte("a"), []byte("1"))
	builder.Add([]byte("b"), []byte("2"))
	require.NoError(t, builder.Finish())

	data := f.buf.Bytes()
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

type memRandomAccessFile struct {
	data   []byte
	closed *int32
}

func (f *memRandomAccessFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off+int64(len(p)) > int64(len(f.data)) {
		return 0, io.EOF
	}
	copy(p, f.data[off:])
	return len(p), nil
}

func (f *memRandomAccessFile) Close() error {
	atomic.AddInt32(f.closed, 1)
	return nil
}

type memEnv struct {
	files  map[string][]byte
	closed map[string]*int32
}

func (e *memEnv) NewSequentialFile(name string) (db.SequentialFile, error) {
	return nil, db.ErrNotSupported
}

func (e *memEnv) NewRandomAccessFile(name string) (db.RandomAccessFile, error) {
	data, ok := e.files[name]
	if !ok {
		return nil, db.ErrNotFound
	}
	counter, ok := e.closed[name]
	if !ok {
		var c int32
		counter = &c
		e.closed[name] = counter
	}
	return &memRandomAccessFile{data: data, closed: counter}, nil
}

func (e *memEnv) NewWritableFile(name string) (db.WritableFile, error) {
	return nil, db.ErrNotSupported
}

func (e *memEnv) NewAppendableFile(name string) (db.WritableFile, error) {
	return nil, db.ErrNotSupported
}

func (e *memEnv) RemoveFile(name string) error {
	return db.ErrNotSupported
}

func (e *memEnv) RenameFile(src, target string) error {
	return db.ErrNotSupported
}

func (e *memEnv) FileExists(name string) bool {
	_, ok := e.files[name]
	return ok
}

func (e *memEnv) GetFileSize(name string) (uint64, error) {
	data, ok := e.files[name]
	if !ok {
		return 0, db.ErrNotFound
	}
	return uint64(len(data)), nil
}

func (e *memEnv) GetChildren(path string) ([]string, error) {
	return nil, db.ErrNotSupported
}

func (e *memEnv) CreateDir(name string) error {
	return db.ErrNotSupported
}

func (e *memEnv) RemoveDir(name string) error {
	return db.ErrNotSupported
}

func (e *memEnv) LockFile(name string) (db.FileLock, error) {
	return nil, db.ErrNotSupported
}

func (e *memEnv) UnlockFile(lock db.FileLock) error {
	return db.ErrNotSupported
}

func TestTableCacheEvictionClosesFile(t *testing.T) {
	data1 := buildTableBytes(t)
	data2 := buildTableBytes(t)

	dbname := "/db"
	f1 := TableFileName(dbname, 1)
	f2 := TableFileName(dbname, 2)

	var closed1 int32
	var closed2 int32
	env := &memEnv{
		files: map[string][]byte{
			f1: data1,
			f2: data2,
		},
		closed: map[string]*int32{
			f1: &closed1,
			f2: &closed2,
		},
	}

	tc := NewTableCache(dbname, env, 1, util.BytewiseComparator, nil, nil, false)

	it1, err := tc.NewIterator(1, uint64(len(data1)), false)
	require.NoError(t, err)
	it1.SeekToFirst()
	require.True(t, it1.Valid())
	require.NoError(t, it1.Close())
	require.Equal(t, int32(0), atomic.LoadInt32(&closed1))

	it2, err := tc.NewIterator(2, uint64(len(data2)), false)
	require.NoError(t, err)
	it2.SeekToFirst()
	require.True(t, it2.Valid())

	require.Equal(t, int32(1), atomic.LoadInt32(&closed1))
	require.NoError(t, it2.Close())
	require.Equal(t, int32(0), atomic.LoadInt32(&closed2))
}

func TestTableCacheInUseNotEvicted(t *testing.T) {
	data1 := buildTableBytes(t)
	data2 := buildTableBytes(t)
	data3 := buildTableBytes(t)

	dbname := "/db"
	f1 := TableFileName(dbname, 1)
	f2 := TableFileName(dbname, 2)
	f3 := TableFileName(dbname, 3)

	var closed1 int32
	var closed2 int32
	var closed3 int32
	env := &memEnv{
		files: map[string][]byte{
			f1: data1,
			f2: data2,
			f3: data3,
		},
		closed: map[string]*int32{
			f1: &closed1,
			f2: &closed2,
			f3: &closed3,
		},
	}

	tc := NewTableCache(dbname, env, 1, util.BytewiseComparator, nil, nil, false)

	it1, err := tc.NewIterator(1, uint64(len(data1)), false)
	require.NoError(t, err)

	it2, err := tc.NewIterator(2, uint64(len(data2)), false)
	require.NoError(t, err)
	t.Cleanup(func() {
		if it2 != nil {
			_ = it2.Close()
		}
	})

	require.Equal(t, int32(0), atomic.LoadInt32(&closed1))
	require.Equal(t, int32(0), atomic.LoadInt32(&closed2))

	require.NoError(t, it1.Close())
	it1 = nil

	it3, err := tc.NewIterator(3, uint64(len(data3)), false)
	require.NoError(t, err)
	t.Cleanup(func() {
		if it3 != nil {
			_ = it3.Close()
		}
	})

	require.Equal(t, int32(1), atomic.LoadInt32(&closed1))
	require.Equal(t, int32(0), atomic.LoadInt32(&closed2))
	require.Equal(t, int32(0), atomic.LoadInt32(&closed3))
}

func TestTableCacheEvict(t *testing.T) {
	data := buildTableBytes(t)

	dbname := "/db"
	f := TableFileName(dbname, 1)

	var closed int32
	env := &memEnv{
		files: map[string][]byte{
			f: data,
		},
		closed: map[string]*int32{
			f: &closed,
		},
	}

	tc := NewTableCache(dbname, env, 1, util.BytewiseComparator, nil, nil, false)

	it, err := tc.NewIterator(1, uint64(len(data)), false)
	require.NoError(t, err)
	require.NoError(t, it.Close())
	require.Equal(t, int32(0), atomic.LoadInt32(&closed))

	tc.Evict(1)
	require.Equal(t, int32(1), atomic.LoadInt32(&closed))
}

func TestTableCacheEvictWithOutstandingIterator(t *testing.T) {
	data := buildTableBytes(t)

	dbname := "/db"
	f := TableFileName(dbname, 1)

	var closed int32
	env := &memEnv{
		files: map[string][]byte{
			f: data,
		},
		closed: map[string]*int32{
			f: &closed,
		},
	}

	tc := NewTableCache(dbname, env, 1, util.BytewiseComparator, nil, nil, false)

	it, err := tc.NewIterator(1, uint64(len(data)), false)
	require.NoError(t, err)

	tc.Evict(1)
	require.Equal(t, int32(0), atomic.LoadInt32(&closed))

	require.NoError(t, it.Close())
	require.Equal(t, int32(1), atomic.LoadInt32(&closed))
}
