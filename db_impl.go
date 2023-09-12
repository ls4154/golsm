package goldb

import (
	"io"
	"strings"
	"sync"

	"github.com/ls4154/goldb/env"
	"github.com/ls4154/goldb/log"
)

type dbImpl struct {
	dbname    string
	icmp      *InternalKeyComparator
	versions  *VersionSet
	snapshots *SnapshotList
	mem       *MemTable
	mu        sync.Mutex
	env       env.Env
	log       *log.LogWriter
	logfile   env.WritableFile
}

func open(options *Options, dbname string) (DB, error) {
	icmp := &InternalKeyComparator{
		userCmp: options.Comparator,
	}
	mem := NewMemTable(icmp)
	vset := NewVersionSet(dbname)
	env := env.DefaultEnv()
	snapshots := NewSnapshotList()

	db := &dbImpl{
		dbname:    dbname,
		icmp:      icmp,
		versions:  vset,
		snapshots: snapshots,
		mem:       mem,
		env:       env,
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	env.CreateDir(dbname)

	maxSequence := int64(0)
	logName := LogFileName(db.dbname, 77)
	if env.FileExists(logName) {
		// recover log
		f, err := env.NewReadableFile(logName)
		if err != nil {
			return nil, err
		}
		reader := log.NewLogReader(f)
		for {
			record, ok := reader.ReadRecord()
			if !ok {
				break
			}

			batch := WriteBatch{
				rep: record,
			}
			err := batch.InsertIntoMemTable(mem)
			if err != nil {
				return nil, err
			}
			lastSeq := batch.sequence() + uint64(batch.count()) - 1
			if lastSeq > uint64(maxSequence) {
				maxSequence = int64(lastSeq)
			}
		}
	}
	vset.SetLastSequence(uint64(maxSequence))

	f, err := env.NewAppendableFile(logName)
	if err != nil {
		return nil, err
	}
	db.log = log.NewLogWriter(f)
	db.logfile = f

	return db, nil
}

func (db *dbImpl) Get(key []byte, options *ReadOptions) ([]byte, error) {
	var seq uint64
	if options != nil && options.Snapshot != nil {
		seq = options.Snapshot.seq
	} else {
		db.mu.Lock()
		seq = db.versions.GetLastSequence()
		db.mu.Unlock()
	}

	value, deleted, exist := db.mem.Get(seq, key)
	if exist {
		if deleted {
			return nil, ErrNotFound
		}
		return value, nil
	}

	return nil, ErrNotFound
}

func (db *dbImpl) Put(key []byte, value []byte, options WriteOptions) error {
	batch := NewWriteBatch()
	batch.Put(key, value)
	return db.Write(batch, options)
}

func (db *dbImpl) Delete(key []byte, options WriteOptions) error {
	batch := NewWriteBatch()
	batch.Delete(key)
	return db.Write(batch, options)
}

func (db *dbImpl) Write(batch *WriteBatch, options WriteOptions) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	lastSeq := db.versions.GetLastSequence()
	batch.setSequence(lastSeq + 1)

	err := db.log.AddRecord(batch.contents())
	if err != nil {
		return err
	}

	err = batch.InsertIntoMemTable(db.mem)
	if err != nil {
		return err
	}

	db.versions.SetLastSequence(lastSeq + uint64(batch.count()))

	return nil
}

func (db *dbImpl) NewIterator() (Iterator, error) {
	panic("unimplemented")
}

func (db *dbImpl) GetSnapshot() *Snapshot {
	seq := db.versions.GetLastSequence()
	return db.snapshots.NewSnapshot(seq)
}

func (db *dbImpl) ReleaseSnapshot(snap *Snapshot) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.snapshots.ReleaseSnapshot(snap)
}

func (db *dbImpl) Close() error {
	db.logfile.Sync()
	db.logfile.Close()
	return nil
}

func SetCurrentFile(env env.Env, dbname string, num uint64) error {
	manifest := DescriptorFileName(dbname, num)
	contents := strings.TrimPrefix(manifest, dbname)

	tmp := TempFileName(dbname, num)
	f, err := env.NewWritableFile(tmp)
	if err != nil {
		return err
	}

	_, err = io.WriteString(f, contents+"\n")
	if err != nil {
		env.RemoveFile(tmp)
		return err
	}
	err = f.Sync()
	if err != nil {
		env.RemoveFile(tmp)
		return err
	}
	err = f.Close()
	if err != nil {
		env.RemoveFile(tmp)
		return err
	}

	err = env.RenameFile(tmp, CurrentFileName(dbname))
	if err != nil {
		env.RemoveFile(tmp)
		return err
	}

	return nil
}
