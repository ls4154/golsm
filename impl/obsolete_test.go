package impl

import (
	"sync/atomic"
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

func TestRemoveObsoleteFilesMultipleLogsAndManifests(t *testing.T) {
	env := &recordingEnv{
		children: []string{
			"000003.log",
			"000004.log",
			"000005.log",
			"MANIFEST-000001",
			"MANIFEST-000006",
			"CURRENT",
		},
	}

	vset := NewVersionSet("db", &InternalKeyComparator{userCmp: util.BytewiseComparator}, env, &TableCache{})
	vset.logNumber = 5
	vset.prevLogNumber = 4
	vset.manifestFileNumber = 6
	v := vset.NewVersion()
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

	require.ElementsMatch(t, []string{"db/000003.log", "db/MANIFEST-000001"}, env.removed)
}

func TestDeleteObsoleteFilesEvictsTableCache(t *testing.T) {
	data := buildTableBytes(t)

	dbname := "/db"
	fname := TableFileName(dbname, 1)
	var closed int32
	env := &memEnv{
		files: map[string][]byte{
			fname: data,
		},
		closed: map[string]*int32{
			fname: &closed,
		},
	}

	tc := NewTableCache(dbname, env, 10, util.BytewiseComparator, nil, nil)
	it, err := tc.NewIterator(1, uint64(len(data)))
	require.NoError(t, err)
	require.NoError(t, it.Close())
	require.Equal(t, int32(0), atomic.LoadInt32(&closed))

	d := &dbImpl{
		dbname:     dbname,
		env:        env,
		tableCache: tc,
	}
	d.DeleteObsoleteFiles([]string{"000001.ldb"})

	require.Equal(t, int32(1), atomic.LoadInt32(&closed))
}
