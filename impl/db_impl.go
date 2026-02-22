package impl

import (
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
)

type dbImpl struct {
	dbname     string
	options    *db.Options
	icmp       *InternalKeyComparator
	ifilter    db.FilterPolicy
	versions   *VersionSet
	tableCache *TableCache
	snapshots  *SnapshotList
	// tracks file numbers currently being written by compaction or flush
	// protecting them from being deleted by obsolete file cleanup
	// before they are registered in the version set.
	pendingOutputs map[uint64]struct{}
	// current active memtable
	mem *MemTable
	// immutable memtable being flushed to level-0
	imm         *MemTable
	mu          sync.Mutex
	env         db.Env
	log         *log.Writer
	logfile     db.WritableFile
	logfileNum  uint64
	infoLogFile db.WritableFile

	writeSerializer *writeSerializer
	bgWork          *bgWork

	fileLock db.FileLock

	closed bool
	bgErr  error

	logger db.Logger
}

func Open(userOpt *db.Options, dbname string) (db.DB, error) {
	opt, err := validateOption(userOpt)
	if err != nil {
		return nil, err
	}

	icmp := &InternalKeyComparator{
		userCmp: opt.Comparator,
	}
	ifilter := newInternalFilterPolicy(opt.FilterPolicy)
	env := util.DefaultEnv()
	_ = env.CreateDir(dbname)

	logger, infoLogFile, err := openInfoLogger(env, dbname, opt.Logger)
	if err != nil {
		return nil, err
	}

	var bcache *table.BlockCache
	if opt.BlockCacheSize > 0 {
		bcache = table.NewBlockCache(opt.BlockCacheSize)
	}
	tcache := NewTableCache(dbname, env, opt.MaxOpenFiles, icmp, ifilter, bcache, opt.ParanoidChecks)
	vset := NewVersionSet(dbname, icmp, env, tcache)
	snapshots := NewSnapshotList()

	db := &dbImpl{
		dbname:         dbname,
		options:        opt,
		icmp:           icmp,
		ifilter:        ifilter,
		versions:       vset,
		tableCache:     tcache,
		snapshots:      snapshots,
		pendingOutputs: make(map[uint64]struct{}),
		mem:            nil,
		env:            env,

		logger:      logger,
		infoLogFile: infoLogFile,
	}

	db.writeSerializer = db.newWriteSerializer()
	db.writeSerializer.Run()

	db.bgWork = db.newBgWork()
	db.bgWork.Run()

	db.mu.Lock()
	defer db.mu.Unlock()

	edit := VersionEdit{}
	err = db.recover(&edit)
	if err != nil {
		return nil, err
	}

	if db.mem == nil {
		newLogNumber := db.versions.NewFileNumber()
		fname := LogFileName(db.dbname, newLogNumber)
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

	db.CleanupObsoleteFiles()
	db.scheduleCompaction()

	return db, nil
}

func (d *dbImpl) recover(edit *VersionEdit) error {
	util.AssertMutexHeld(&d.mu)

	d.logger.Printf("recovering...")

	d.env.CreateDir(d.dbname)

	lock, err := d.env.LockFile(LockFileName(d.dbname))
	if err != nil {
		return fmt.Errorf("failed to acquire DB lock: %w", err)
	}
	d.fileLock = lock

	dbExists := d.env.FileExists(CurrentFileName(d.dbname))
	if !dbExists {
		if !d.options.CreateIfMissing {
			return fmt.Errorf("%w: db does not exist: %s", db.ErrInvalidArgument, d.dbname)
		}

		d.logger.Printf("Creating DB %s", d.dbname)
		err := d.newDB()
		if err != nil {
			return err
		}
	} else if d.options.ErrorIfExists {
		return fmt.Errorf("%w: db already exists: %s", db.ErrInvalidArgument, d.dbname)
	}

	err = d.versions.Recover()
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

	d.logger.Printf("recovering log %d", logNum)

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
			err := d.WriteLevel0Table(mem, edit, d.versions.NewFileNumber())
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
		err := d.WriteLevel0Table(mem, edit, d.versions.NewFileNumber())
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

func (d *dbImpl) WriteLevel0Table(mem *MemTable, edit *VersionEdit, fnum uint64) error {
	util.AssertMutexHeld(&d.mu)

	// TODO stats
	startTime := time.Now()

	meta := FileMetaData{
		number: fnum,
	}

	iter := mem.Iterator()
	defer iter.Close()

	d.logger.Printf("Level-0 table #%d: started", meta.number)

	d.mu.Unlock()
	err := BuildTable(d.dbname, d.env, iter, d.icmp, d.options, d.ifilter, &meta)
	d.mu.Lock()

	d.logger.Printf("Level-0 table #%d: %d bytes %s", meta.number, meta.size, err)

	if err != nil {
		return err
	}

	if meta.size <= 0 {
		return nil
	}

	level := 0

	// TODO pick level

	edit.AddFile(level, meta.number, meta.size, meta.smallest, meta.largest)

	elapsed := time.Now().Sub(startTime)
	d.logger.Printf("flush time: %s", elapsed)

	return nil
}

func (d *dbImpl) Get(key []byte, options *db.ReadOptions) ([]byte, error) {
	d.mu.Lock()

	var seq uint64
	var verifyChecksum bool
	if options != nil {
		if options.Snapshot != nil {
			seq = options.Snapshot.(*Snapshot).seq
		} else {
			seq = d.versions.GetLastSequence()
		}
		verifyChecksum = options.VerifyChecksum
	} else {
		seq = d.versions.GetLastSequence()
	}

	// NOTE: memtable lifetime is managed by Go GC

	mem := d.mem
	imm := d.imm
	current := d.versions.current
	current.Ref()

	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		current.Unref()
		d.mu.Unlock()
	}()

	var lookupKey LookupKey
	lookupKey.Set(key, seq)

	value, deleted, found := mem.Get(&lookupKey)
	if !found && imm != nil {
		value, deleted, found = imm.Get(&lookupKey)
	}
	if !found {
		var getErr error
		value, getErr = current.Get(&lookupKey, verifyChecksum)
		if errors.Is(getErr, db.ErrNotFound) {
			found = false
		} else if getErr != nil {
			return nil, getErr
		} else {
			found = true
			deleted = false
		}
	}

	// TODO seek compaction?

	if !found || deleted {
		return nil, db.ErrNotFound
	}

	return value, nil
}

func (d *dbImpl) Put(key []byte, value []byte, opt *db.WriteOptions) error {
	batch := NewWriteBatch()
	batch.Put(key, value)
	return d.Write(batch, opt)
}

func (d *dbImpl) Delete(key []byte, opt *db.WriteOptions) error {
	batch := NewWriteBatch()
	batch.Delete(key)
	return d.Write(batch, opt)
}

func (d *dbImpl) Write(updates db.WriteBatch, opt *db.WriteOptions) error {
	if updates == nil {
		return nil
	}
	return d.writeSerializer.Write(updates, opt)
}

func (d *dbImpl) NewIterator(options *db.ReadOptions) (db.Iterator, error) {
	internalIter, latestSnapshot, err := d.newInternalIterator(options)
	if err != nil {
		return nil, err
	}
	var seq uint64
	if options != nil && options.Snapshot != nil {
		seq = options.Snapshot.(*Snapshot).seq
	} else {
		seq = latestSnapshot
	}
	return newDBIter(internalIter, d.icmp.userCmp, seq), nil
}

func (d *dbImpl) newInternalIterator(options *db.ReadOptions) (db.Iterator, uint64, error) {
	iters := make([]db.Iterator, 0, 4)

	d.mu.Lock()
	defer d.mu.Unlock()

	latestSnapshot := d.versions.GetLastSequence()
	current := d.versions.current

	iters = append(iters, d.mem.Iterator())
	if d.imm != nil {
		iters = append(iters, d.imm.Iterator())
	}

	var verifyChecksum bool
	if options != nil {
		verifyChecksum = options.VerifyChecksum
	}

	err := current.AddIterators(&iters, verifyChecksum)
	if err != nil {
		for _, it := range iters {
			_ = it.Close()
		}
		return nil, 0, err
	}

	current.Ref()

	internalIter := newMergingIterator(d.icmp, iters)
	return newCleanupIterator(internalIter, func() {
		d.mu.Lock()
		current.Unref()
		d.mu.Unlock()
	}), latestSnapshot, nil
}

func (d *dbImpl) GetSnapshot() db.Snapshot {
	seq := d.versions.GetLastSequence()
	s := d.snapshots.NewSnapshot(seq, d)
	return s
}

// Close requires callers to release all outstanding iterators/snapshots first.
func (d *dbImpl) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()

	d.logger.Printf("Closing...")

	d.bgWork.Close()
	d.writeSerializer.Close()

	if d.logfile != nil {
		_ = d.logfile.Sync()
		_ = d.logfile.Close()
	}

	if d.infoLogFile != nil {
		_ = d.infoLogFile.Flush()
		_ = d.infoLogFile.Close()
	}

	d.tableCache.Close()
	d.versions.Close()

	if d.fileLock != nil {
		_ = d.env.UnlockFile(d.fileLock)
		d.fileLock = nil
	}

	return nil
}

func openInfoLogger(env db.Env, dbname string, logger db.Logger) (db.Logger, db.WritableFile, error) {
	if logger != nil {
		return logger, nil, nil
	}

	f, err := env.NewAppendableFile(InfoLogFileName(dbname))
	if err != nil {
		return nil, nil, err
	}

	return stdlog.New(f, "", stdlog.LstdFlags), f, nil
}

func SetCurrentFile(env db.Env, dbname string, num uint64) error {
	manifest := DescriptorFileName(dbname, num)
	contents := filepath.Base(manifest)

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

func (d *dbImpl) RegisterPendingOutput(fnum uint64) {
	util.AssertMutexHeld(&d.mu)

	util.AssertFunc(func() bool {
		_, exists := d.pendingOutputs[fnum]
		return exists == false
	})

	d.pendingOutputs[fnum] = struct{}{}
}

func (d *dbImpl) UnregisterPendingOutput(fnum uint64) {
	util.AssertMutexHeld(&d.mu)

	util.AssertFunc(func() bool {
		_, exists := d.pendingOutputs[fnum]
		return exists == true
	})

	delete(d.pendingOutputs, fnum)
}

func (d *dbImpl) CleanupObsoleteFiles() {
	util.AssertMutexHeld(&d.mu)

	filesToDelete := d.FindObsoleteFiles()

	d.mu.Unlock()
	d.DeleteObsoleteFiles(filesToDelete)
	d.mu.Lock()
}

func (d *dbImpl) FindObsoleteFiles() []string {
	util.AssertMutexHeld(&d.mu)

	if d.GetBackgroundError() != nil {
		return nil
	}

	live := d.versions.LiveFiles()
	for num := range d.pendingOutputs {
		live[num] = struct{}{}
	}

	fileNames, err := d.env.GetChildren(d.dbname)
	if err != nil {
		d.logger.Printf("FindObsoleteFiles: %v", err)
		return nil
	}

	ret := []string{}

	for _, fname := range fileNames {
		ftype, fnum, ok := ParseFileName(fname)
		if !ok {
			continue
		}

		keep := true
		switch ftype {
		case FileTypeLog:
			// Keep current log and previous log needed for recovery.
			keep = (fnum >= d.versions.logNumber) || (fnum == d.versions.prevLogNumber)
		case FileTypeDescriptor:
			keep = fnum >= d.versions.manifestFileNumber
		case FileTypeCurrent, FileTypeLock, FileTypeInfoLog:
			keep = true
		case FileTypeTable, FileTypeTemp:
			_, keep = live[fnum]
		default:
			keep = true
		}

		if !keep {
			ret = append(ret, fname)
			d.logger.Printf("Delete type=%d #%d", ftype, fnum)
		}
	}

	return ret
}

func (d *dbImpl) DeleteObsoleteFiles(files []string) {
	for _, fname := range files {
		ftype, fnum, ok := ParseFileName(fname)
		if ok && ftype == FileTypeTable && d.tableCache != nil {
			d.tableCache.Evict(fnum)
		}
		_ = d.env.RemoveFile(filepath.Join(d.dbname, fname))
	}
}

func (d *dbImpl) RecordBackgroundError(err error) {
	util.AssertMutexHeld(&d.mu)
	if d.bgErr == nil {
		d.logger.Printf("RecordBackgroundError %v", err)
		d.bgErr = err
		// TODO signal?
	}
}

func (d *dbImpl) GetBackgroundError() error {
	util.AssertMutexHeld(&d.mu)
	return d.bgErr
}
