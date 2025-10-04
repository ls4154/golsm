package impl

import (
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

type recordingEnv struct {
	children []string
	removed  []string
}

func (e *recordingEnv) NewSequentialFile(name string) (db.SequentialFile, error) {
	return nil, db.ErrNotSupported
}

func (e *recordingEnv) NewRandomAccessFile(name string) (db.RandomAccessFile, error) {
	return nil, db.ErrNotSupported
}

func (e *recordingEnv) NewWritableFile(name string) (db.WritableFile, error) {
	return nil, db.ErrNotSupported
}

func (e *recordingEnv) NewAppendableFile(name string) (db.WritableFile, error) {
	return nil, db.ErrNotSupported
}

func (e *recordingEnv) RemoveFile(name string) error {
	e.removed = append(e.removed, name)
	return nil
}

func (e *recordingEnv) RenameFile(src, target string) error {
	return db.ErrNotSupported
}

func (e *recordingEnv) FileExists(name string) bool {
	return false
}

func (e *recordingEnv) GetFileSize(name string) (uint64, error) {
	return 0, db.ErrNotSupported
}

func (e *recordingEnv) GetChildren(path string) ([]string, error) {
	return e.children, nil
}

func (e *recordingEnv) CreateDir(name string) error {
	return db.ErrNotSupported
}

func (e *recordingEnv) RemoveDir(name string) error {
	return db.ErrNotSupported
}

func (e *recordingEnv) LockFile(name string) (db.FileLock, error) {
	return nil, db.ErrNotSupported
}

func (e *recordingEnv) UnlockFile(lock db.FileLock) error {
	return db.ErrNotSupported
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

func TestRemoveObsoleteFiles(t *testing.T) {
	env := &recordingEnv{
		children: []string{
			"000001.ldb",
			"000002.ldb",
			"000003.ldb",
			"000004.log",
			"000005.dbtmp",
			"MANIFEST-000001",
			"CURRENT",
		},
	}

	vset := NewVersionSet("db", &InternalKeyComparator{userCmp: util.BytewiseComparator}, env, &TableCache{})
	vset.logNumber = 4
	vset.prevLogNumber = 0
	v := vset.NewVersion()
	v.files[0] = []*FileMetaData{
		{number: 1, size: 100},
	}
	vset.AppendVersion(v)

	d := &dbImpl{
		dbname:         "db",
		env:            env,
		versions:       vset,
		pendingOutputs: map[uint64]struct{}{2: {}, 5: {}},
		logger:         nopLogger{},
	}

	d.mu.Lock()
	d.CleanupObsoleteFiles()
	d.mu.Unlock()

	require.Equal(t, []string{"db/000003.ldb"}, env.removed)
}

func TestRemoveObsoleteFilesMultipleLogs(t *testing.T) {
	env := &recordingEnv{
		children: []string{
			"000001.ldb",
			"000003.log",
			"000004.log",
			"000005.log",
			"CURRENT",
		},
	}

	vset := NewVersionSet("db", &InternalKeyComparator{userCmp: util.BytewiseComparator}, env, &TableCache{})
	vset.logNumber = 5
	vset.prevLogNumber = 4
	v := vset.NewVersion()
	v.files[0] = []*FileMetaData{
		{number: 1, size: 100},
	}
	vset.AppendVersion(v)

	d := &dbImpl{
		dbname:         "db",
		env:            env,
		versions:       vset,
		pendingOutputs: map[uint64]struct{}{},
		logger:         nopLogger{},
	}

	d.mu.Lock()
	d.CleanupObsoleteFiles()
	d.mu.Unlock()

	require.Equal(t, []string{"db/000003.log"}, env.removed)
}
