package impl

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/fs"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func TestManifestRotationBySize(t *testing.T) {
	testDir := t.TempDir()

	opt := db.DefaultOptions()
	opt.WriteBufferSize = 4 << 10
	opt.MaxManifestFileSize = 1 << 10

	ldb, err := Open(opt, testDir)
	require.NoError(t, err)

	for i := 0; i < 1500; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := bytes.Repeat([]byte{byte(i)}, 4096)
		require.NoError(t, ldb.Put(key, val, nil))
	}
	require.NoError(t, ldb.Close())

	current, err := os.ReadFile(filepath.Join(testDir, "CURRENT"))
	require.NoError(t, err)

	ftype, fnum, ok := ParseFileName(strings.TrimSpace(string(current)))
	require.True(t, ok)
	require.Equal(t, FileTypeDescriptor, ftype)
	require.Greater(t, fnum, FileNumber(2))

	ldb, err = Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	for _, i := range []int{0, 123, 1499} {
		key := []byte(fmt.Sprintf("key-%06d", i))
		expect := bytes.Repeat([]byte{byte(i)}, 4096)
		got, err := ldb.Get(key, nil)
		require.NoError(t, err)
		require.Equal(t, expect, got)
	}
}

type trackedWritableFile struct {
	closed bool
}

func (f *trackedWritableFile) Write(p []byte) (int, error) {
	if f.closed {
		return 0, errors.New("write on closed file")
	}
	return len(p), nil
}

func (f *trackedWritableFile) Close() error {
	if f.closed {
		return errors.New("close on closed file")
	}
	f.closed = true
	return nil
}

func (f *trackedWritableFile) Flush() error {
	if f.closed {
		return errors.New("flush on closed file")
	}
	return nil
}

func (f *trackedWritableFile) Sync() error {
	if f.closed {
		return errors.New("sync on closed file")
	}
	return nil
}

type manifestRotationFailEnv struct {
	renameErr error
	files     map[string]*trackedWritableFile
	removed   []string
}

func (e *manifestRotationFailEnv) NewSequentialFile(name string) (fs.SequentialFile, error) {
	return nil, db.ErrNotSupported
}

func (e *manifestRotationFailEnv) NewRandomAccessFile(name string) (fs.RandomAccessFile, error) {
	return nil, db.ErrNotSupported
}

func (e *manifestRotationFailEnv) NewWritableFile(name string) (fs.WritableFile, error) {
	if e.files == nil {
		e.files = make(map[string]*trackedWritableFile)
	}
	f := &trackedWritableFile{}
	e.files[name] = f
	return f, nil
}

func (e *manifestRotationFailEnv) NewAppendableFile(name string) (fs.WritableFile, error) {
	return nil, db.ErrNotSupported
}

func (e *manifestRotationFailEnv) RemoveFile(name string) error {
	e.removed = append(e.removed, name)
	return nil
}

func (e *manifestRotationFailEnv) RenameFile(src, target string) error {
	if e.renameErr != nil {
		return e.renameErr
	}
	return nil
}

func (e *manifestRotationFailEnv) FileExists(name string) bool { return false }
func (e *manifestRotationFailEnv) GetFileSize(name string) (uint64, error) {
	return 0, db.ErrNotSupported
}
func (e *manifestRotationFailEnv) GetChildren(path string) ([]string, error) {
	return nil, db.ErrNotSupported
}
func (e *manifestRotationFailEnv) CreateDir(name string) error { return db.ErrNotSupported }
func (e *manifestRotationFailEnv) RemoveDir(name string) error { return db.ErrNotSupported }
func (e *manifestRotationFailEnv) LockFile(name string) (fs.FileLock, error) {
	return nil, db.ErrNotSupported
}
func (e *manifestRotationFailEnv) UnlockFile(lock fs.FileLock) error { return db.ErrNotSupported }

func TestLogAndApplyRotationFailureKeepsOldManifestOpen(t *testing.T) {
	env := &manifestRotationFailEnv{
		renameErr: errors.New("rename failed"),
	}

	vs := NewVersionSet("db", &InternalKeyComparator{userCmp: util.BytewiseComparator}, env, &TableCache{}, false, 1, nil,
		newCompactionPolicy(db.DefaultCompactionOptions(0)))

	oldFile := &trackedWritableFile{}
	vs.manifestFileNumber = 7
	vs.descriptorFile = oldFile
	vs.descriptorLog = log.NewWriter(oldFile)
	require.NoError(t, vs.descriptorLog.AddRecord([]byte("seed")))

	var dbMu sync.Mutex
	dbMu.Lock()
	err := vs.LogAndApply(&VersionEdit{}, &dbMu)
	require.Error(t, err)
	dbMu.Unlock()

	require.False(t, oldFile.closed)
	require.True(t, env.files[DescriptorFileName("db", 2)].closed)
	require.Contains(t, env.removed, DescriptorFileName("db", 2))

	vs.maxManifestFileSize = 1 << 20
	env.renameErr = nil

	dbMu.Lock()
	err = vs.LogAndApply(&VersionEdit{}, &dbMu)
	require.NoError(t, err)
	dbMu.Unlock()
}
