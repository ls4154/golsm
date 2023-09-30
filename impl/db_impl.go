package impl

import (
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/env"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/util"
)

type dbImpl struct {
	dbname    string
	options   db.Options
	icmp      *InternalKeyComparator
	versions  *VersionSet
	snapshots *SnapshotList
	mem       *MemTable
	imm       *MemTable
	mu        sync.Mutex
	env       db.Env
	log       *log.LogWriter
	logfile   db.WritableFile

	wmu sync.Mutex
}

func Open(options *db.Options, dbname string) (db.DB, error) {
	userCmp := options.Comparator
	if userCmp == nil {
		userCmp = util.BytewiseComparator
	}

	icmp := &InternalKeyComparator{
		userCmp: options.Comparator,
	}
	mem := NewMemTable(icmp)
	env := env.DefaultEnv()
	vset := NewVersionSet(dbname, env)
	snapshots := NewSnapshotList()

	db := &dbImpl{
		dbname:    dbname,
		options:   *options,
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
	newLogNumber := db.versions.NewFileNumber()
	logName := LogFileName(db.dbname, newLogNumber)
	if env.FileExists(logName) {
		// recover log
		f, err := env.NewSequentialFile(logName)
		if err != nil {
			return nil, err
		}
		reader := log.NewLogReader(f)
		for {
			record, err := reader.ReadRecord()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				panic("TODO")
			}

			batch := WriteBatchImpl{
				rep: record,
			}
			err = batch.InsertIntoMemTable(mem)
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

func (d *dbImpl) Recover() error {
	d.env.CreateDir(d.dbname)

	// TODO lockfile

	if !d.env.FileExists(CurrentFileName(d.dbname)) {
		// TODO creaste_if_missing option
		err := d.NewDB()
		if err != nil {
			return err
		}
	}
	// TODO error_if_exists option

	// d.versions.Recover
}

func (d *dbImpl) NewDB() error {
}

func (d *dbImpl) Get(key []byte, options *db.ReadOptions) ([]byte, error) {
	var seq uint64
	if options != nil && options.Snapshot != nil {
		seq = options.Snapshot.(*Snapshot).seq
	} else {
		seq = d.versions.GetLastSequence()
	}

	var lookupKey LookupKey
	lookupKey.Set(key, seq)

	value, deleted, exist := d.mem.Get(&lookupKey)
	if exist {
		if deleted {
			return nil, db.ErrNotFound
		}
		return value, nil
	}

	return nil, db.ErrNotFound
}

func (d *dbImpl) Put(key []byte, value []byte, options db.WriteOptions) error {
	batch := NewWriteBatch()
	batch.Put(key, value)
	return d.Write(batch, options)
}

func (d *dbImpl) Delete(key []byte, options db.WriteOptions) error {
	batch := NewWriteBatch()
	batch.Delete(key)
	return d.Write(batch, options)
}

func (d *dbImpl) Write(updates db.WriteBatch, options db.WriteOptions) error {
	d.wmu.Lock()
	defer d.wmu.Unlock()

	lastSeq := d.versions.GetLastSequence()
	batch := updates.(*WriteBatchImpl)
	batch.setSequence(lastSeq + 1)

	err := d.log.AddRecord(batch.contents())
	if err != nil {
		return err
	}

	if options.Sync {
		err := d.logfile.Sync()
		// TODO sync error
		_ = err
	}

	err = batch.InsertIntoMemTable(d.mem)
	if err != nil {
		return err
	}

	d.versions.SetLastSequence(lastSeq + uint64(batch.count()))

	return nil
}

func (d *dbImpl) makeRoomForWrite() error {
	return nil
	panic("TODO")

	// TODO mu
	for {
		if d.mem.ApproximateMemoryUsage() <= d.options.WriteBufferSize {
			// ok
			break
		} else if d.imm != nil {
			// TODO wait
		} else {
			// TODO switch
			logNum := uint64(78) // TODO
			f, err := d.env.NewWritableFile(LogFileName(d.dbname, logNum))
			if err != nil {
				return err
			}

			err = d.logfile.Close()
			if err != nil {
				// TODO bg err
			}

			d.logfile = f
			d.log = log.NewLogWriter(f)

			d.imm = d.mem
			d.mem = NewMemTable(d.icmp)

			// TODO sched compaction
		}
	}
	panic("TODO")
}

func (d *dbImpl) NewIterator() (db.Iterator, error) {
	panic("unimplemented")
}

func (d *dbImpl) GetSnapshot() db.Snapshot {
	seq := d.versions.GetLastSequence()
	return d.snapshots.NewSnapshot(seq)
}

func (d *dbImpl) Close() error {
	d.logfile.Sync()
	d.logfile.Close()
	return nil
}

func SetCurrentFile(env db.Env, dbname string, num uint64) error {
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
