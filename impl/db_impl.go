package impl

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/env"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/util"
)

type dbImpl struct {
	dbname         string
	options        db.Options
	icmp           *InternalKeyComparator
	versions       *VersionSet
	snapshots      *SnapshotList
	pendingOutputs map[uint64]struct{}
	mem            *MemTable
	imm            *MemTable
	mu             sync.Mutex
	env            db.Env
	log            *log.Writer
	logfile        db.WritableFile
	logfileNum     uint64

	wmu sync.Mutex
}

func Open(options *db.Options, dbname string) (db.DB, error) {
	userCmp := options.Comparator
	if userCmp == nil {
		userCmp = util.BytewiseComparator
	}

	icmp := &InternalKeyComparator{
		userCmp: userCmp,
	}
	env := env.DefaultEnv()
	vset := NewVersionSet(dbname, icmp, env)
	snapshots := NewSnapshotList()

	db := &dbImpl{
		dbname:         dbname,
		options:        *options,
		icmp:           icmp,
		versions:       vset,
		snapshots:      snapshots,
		pendingOutputs: make(map[uint64]struct{}),
		mem:            nil,
		env:            env,
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	edit := VersionEdit{}
	err := db.recover(&edit)
	if err != nil {
		return nil, err
	}

	if db.mem == nil {
		newLogNumber := db.versions.NewFileNumber()
		fname := TableFileName(db.dbname, newLogNumber)
		f, err := db.env.NewWritableFile(fname)
		if err != nil {
			return nil, err
		}
		edit.SetLogNumber(newLogNumber)
		db.logfile = f
		db.logfileNum = newLogNumber
		db.log = log.NewWriter(f)
		db.mem = NewMemTable(db.icmp)
	}

	edit.SetPrevLogNumber(0)
	edit.SetLogNumber(db.logfileNum)
	err = db.versions.LogAndApply(&edit, &db.mu)
	if err != nil {
		return nil, err
	}

	// TODO remove obsolete files
	// TODO maybe sched comp

	return db, nil
}

func (d *dbImpl) recover(edit *VersionEdit) error {
	util.AssertMutexHeld(&d.mu)

	d.env.CreateDir(d.dbname)

	// TODO lockfile

	if !d.env.FileExists(CurrentFileName(d.dbname)) {
		// TODO creaste_if_missing option
		err := d.newDB()
		if err != nil {
			return err
		}
	}
	// TODO error_if_exists option

	err := d.versions.Recover()
	if err != nil {
		return err
	}

	maxSequence := uint64(0)

	minLog := d.versions.logNumber
	prevLog := d.versions.prevLogNumber

	filenames, err := d.env.GetChildren(d.dbname)
	if err != nil {
		return err
	}

	expected := d.versions.LiveFiles()
	logs := []uint64{}

	for _, fname := range filenames {
		if ftype, num, ok := ParseFileName(fname); ok {
			delete(expected, num)
			if ftype == FileTypeLog && ((num >= minLog) || (num == prevLog)) {
				logs = append(logs, num)
			}
		}
	}

	if len(expected) > 0 {
		return fmt.Errorf("%w: %d missing files", db.ErrCorruption, len(expected))
	}

	sort.Slice(logs, func(i, j int) bool {
		return logs[i] < logs[j]
	})

	for i, logNum := range logs {
		err := d.RecoverLogFile(logNum, i == len(logs)-1, edit, &maxSequence)
		if err != nil {
			return err
		}

		d.versions.MakeFileNumberUsed(logNum)
	}

	if d.versions.GetLastSequence() < maxSequence {
		d.versions.SetLastSequence(maxSequence)
	}

	return nil
}

func (d *dbImpl) RecoverLogFile(logNum uint64, last bool, edit *VersionEdit, maxSeq *uint64) error {
	util.AssertMutexHeld(&d.mu)

	fname := LogFileName(d.dbname, logNum)
	f, err := d.env.NewSequentialFile(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := log.NewReader(f)
	compactions := 0
	var mem *MemTable
	var recoverErr error
	// TODO ignore corruption option
	for {
		record, err := reader.ReadRecord()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			recoverErr = err
			break
		}

		if len(record) < 12 {
			recoverErr = fmt.Errorf("%w: log record too small", db.ErrCorruption)
			break
		}

		batch := WriteBatchFromContents(record)

		if mem == nil {
			mem = NewMemTable(d.icmp)
		}
		err = batch.InsertIntoMemTable(mem)
		if err != nil {
			recoverErr = err
			break
		}

		lastSeq := batch.sequence() + uint64(batch.count()) - 1
		if lastSeq > *maxSeq {
			*maxSeq = lastSeq
		}

		if mem.ApproximateMemoryUsage() > d.options.WriteBufferSize {
			compactions++
			err := d.WriteLevel0Table(mem, edit)
			mem = nil
			if err != nil {
				recoverErr = err
				break
			}
		}
	}

	if recoverErr != nil {
		return recoverErr
	}

	// TODO reuse log

	if mem != nil {
		err := d.WriteLevel0Table(mem, edit)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *dbImpl) newDB() error {
	edit := VersionEdit{}
	edit.SetComparator(d.icmp.userCmp.Name())
	edit.SetLogNumber(0)
	edit.SetNextFileNumber(2)
	edit.SetLastSequence(0)

	manifest := DescriptorFileName(d.dbname, 1)
	f, err := d.env.NewWritableFile(manifest)
	if err != nil {
		return err
	}

	defer func() {
		if f != nil {
			f.Close()
		}
		if err != nil {
			d.env.RemoveFile(manifest)
		}
	}()

	writer := log.NewWriter(f)
	record := edit.Append(nil)
	err = writer.AddRecord(record)
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	f = nil

	return SetCurrentFile(d.env, d.dbname, 1)
}

func (d *dbImpl) WriteLevel0Table(mem *MemTable, edit *VersionEdit) error {
	util.AssertMutexHeld(&d.mu)

	// TODO stats

	meta := FileMetaData{
		number: d.versions.NewFileNumber(),
	}
	d.pendingOutputs[meta.number] = struct{}{}

	iter := mem.Iterator()
	// TODO defer iter.Close()

	d.mu.Unlock()
	err := BuildTable(d.dbname, d.env, iter, d.icmp, &d.options, &meta)
	d.mu.Lock()

	if err != nil {
		return err
	}

	if meta.size <= 0 {
		return nil
	}

	level := 0
	// TODO pick level
	edit.AddFile(level, meta.number, meta.size, meta.smallest, meta.largest)

	return nil
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
			d.log = log.NewWriter(f)

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
