package impl

import (
	"errors"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/fs"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

type stubWritableFile struct{}

func (f *stubWritableFile) Write(p []byte) (int, error) { return len(p), nil }
func (f *stubWritableFile) Close() error                { return nil }
func (f *stubWritableFile) Flush() error                { return nil }
func (f *stubWritableFile) Sync() error                 { return nil }

type failingEnv struct {
	newWritableErr     error
	newRandomAccessErr error
	renameErr          error
}

func (e *failingEnv) NewSequentialFile(name string) (fs.SequentialFile, error) {
	return nil, db.ErrNotSupported
}

func (e *failingEnv) NewRandomAccessFile(name string) (fs.RandomAccessFile, error) {
	if e.newRandomAccessErr != nil {
		return nil, e.newRandomAccessErr
	}
	return nil, db.ErrNotSupported
}

func (e *failingEnv) NewWritableFile(name string) (fs.WritableFile, error) {
	if e.newWritableErr != nil {
		return nil, e.newWritableErr
	}
	return &stubWritableFile{}, nil
}

func (e *failingEnv) NewAppendableFile(name string) (fs.WritableFile, error) {
	return nil, db.ErrNotSupported
}

func (e *failingEnv) RemoveFile(name string) error { return nil }

func (e *failingEnv) RenameFile(src, target string) error {
	if e.renameErr != nil {
		return e.renameErr
	}
	return nil
}

func (e *failingEnv) FileExists(name string) bool               { return false }
func (e *failingEnv) GetFileSize(name string) (uint64, error)   { return 0, db.ErrNotSupported }
func (e *failingEnv) GetChildren(path string) ([]string, error) { return nil, db.ErrNotSupported }
func (e *failingEnv) CreateDir(name string) error               { return db.ErrNotSupported }
func (e *failingEnv) RemoveDir(name string) error               { return db.ErrNotSupported }
func (e *failingEnv) LockFile(name string) (fs.FileLock, error) { return nil, db.ErrNotSupported }
func (e *failingEnv) UnlockFile(lock fs.FileLock) error         { return db.ErrNotSupported }

func TestSetCurrentFileWrapsIOError(t *testing.T) {
	env := &failingEnv{
		renameErr: errors.New("rename failed"),
	}

	err := SetCurrentFile(env, "/db", 7)
	require.ErrorIs(t, err, db.ErrIO)
	require.ErrorContains(t, err, "rename failed")
}

func TestTableCacheGetWrapsIOError(t *testing.T) {
	env := &failingEnv{
		newRandomAccessErr: errors.New("open failed"),
	}
	tc := NewTableCache("/db", env, 10, util.BytewiseComparator, nil, nil, false)

	err := tc.Get(1, 0, []byte("key"), func(_, _ []byte) {}, false, false)
	require.ErrorIs(t, err, db.ErrIO)
	require.ErrorContains(t, err, "open failed")
}
