package impl

import (
	"errors"
	"sync"
	"time"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/util"
)

type writer struct {
	batch    *WriteBatchImpl
	options  db.WriteOptions
	resultCh chan error
}

// writeSerializer runs a single goroutine that collects concurrent writes and
// applies them as a group, reducing log I/O overhead.
type writeSerializer struct {
	apply    func(*WriteBatchImpl, bool) error
	writerCh chan *writer
	wg       sync.WaitGroup
	// lastWriter holds a writer dequeued but not included in the current batch
	// (sync mismatch or size limit) to be the first writer in the next batch.
	lastWriter *writer
	tempBatch  *WriteBatchImpl
}

func (db *dbImpl) newWriteSerializer() *writeSerializer {
	return &writeSerializer{
		apply:      db.applyBatch,
		writerCh:   make(chan *writer),
		lastWriter: nil,
		tempBatch:  NewWriteBatch(),
	}
}

func (ws *writeSerializer) Write(updates db.WriteBatch, options db.WriteOptions) error {
	if updates == nil {
		return nil
	}
	batch := updates.(*WriteBatchImpl)
	w := &writer{
		batch:    batch,
		options:  options,
		resultCh: make(chan error, 1),
	}

	ws.writerCh <- w
	return <-w.resultCh
}

func (ws *writeSerializer) Run() {
	ws.wg.Add(1)
	go ws.main()
}

func (ws *writeSerializer) main() {
	defer ws.wg.Done()
	for {
		writers := ws.fetchWriters()
		if len(writers) == 0 {
			// closed
			break
		}

		batch := ws.buildBatchGroup(writers)

		err := ws.apply(batch, writers[0].options.Sync)

		if batch == ws.tempBatch {
			ws.tempBatch.clear()
		}

		for _, w := range writers {
			w.resultCh <- err
		}
	}
}

func (ws *writeSerializer) Close() {
	close(ws.writerCh)
	ws.wg.Wait()
}

const maxBatchSize = 1 << 20

func (ws *writeSerializer) fetchWriters() []*writer {
	var first *writer
	if ws.lastWriter != nil {
		first = ws.lastWriter
		ws.lastWriter = nil
	} else {
		r, ok := <-ws.writerCh
		if !ok {
			return nil
		}
		first = r
	}

	util.Assert(first != nil)
	util.Assert(ws.lastWriter == nil)

	size := first.batch.Size()
	writers := []*writer{first}

	for {
		select {
		case r, ok := <-ws.writerCh:
			if !ok {
				return writers
			}
			// Do not mix sync and non-sync writers in the same batch.
			if r.options.Sync != first.options.Sync {
				ws.lastWriter = r
				return writers
			}
			size += r.batch.Size()
			if size > maxBatchSize {
				// Do not make batch too big.
				ws.lastWriter = r
				return writers
			}
			writers = append(writers, r)
		default:
			return writers
		}
	}
}

func (ws *writeSerializer) buildBatchGroup(writers []*writer) *WriteBatchImpl {
	if len(writers) <= 1 {
		return writers[0].batch
	}

	b := ws.tempBatch
	for _, w := range writers {
		b.Append(w.batch)
	}

	return b
}

func (d *dbImpl) applyBatch(batch *WriteBatchImpl, sync bool) error {
	d.mu.Lock()

	err := d.makeRoomForWrite(false)
	if err != nil {
		d.mu.Unlock()
		return err
	}

	lastSeq := d.versions.GetLastSequence()
	batch.setSequence(lastSeq + 1)

	// writeSerializer guarantees only one applyBatch runs at a time,
	// so releasing the mutex here is safe.
	d.mu.Unlock()
	err = d.log.AddRecord(batch.contents())
	if err == nil && sync {
		err = d.logfile.Sync()
	}
	if err == nil {
		err = batch.InsertIntoMemTable(d.mem)
	}

	d.versions.SetLastSequence(lastSeq + uint64(batch.count()))

	return err
}

func (d *dbImpl) makeRoomForWrite(force bool) error {
	util.AssertMutexHeld(&d.mu)

	allowDelay := !force
	for {
		if d.closed {
			return errors.New("db closed")
		} else if bgErr := d.GetBackgroundError(); bgErr != nil {
			return bgErr
		} else if allowDelay && d.versions.NumLevelFiles(0) >= L0SlowDownTrigger {
			d.mu.Unlock()
			time.Sleep(time.Millisecond)
			allowDelay = false // sleep only once
			d.mu.Lock()
		} else if !force && d.mem.ApproximateMemoryUsage() <= d.options.WriteBufferSize {
			// ok
			break
		} else if d.imm != nil {
			d.logger.Printf("Current memtable full; waiting...")
			d.mu.Unlock()
			d.bgWork.writerWaitForFlushDone()
			d.mu.Lock()
		} else if d.versions.NumLevelFiles(0) >= L0StopWritesTrigger {
			d.logger.Printf("Too many L0 files; waiting...")
			d.mu.Unlock()
			d.bgWork.writerWaitForCompactionDone()
			d.mu.Lock()
		} else {
			// switch to a new log file and memtable
			logNum := d.versions.NewFileNumber()
			f, err := d.env.NewWritableFile(LogFileName(d.dbname, logNum))
			if err != nil {
				// TODO reuse file number?
				return err
			}

			err = d.logfile.Close()
			if err != nil {
				d.RecordBackgroundError(err)
			}

			d.logfile = f
			d.logfileNum = logNum
			d.log = log.NewWriter(f)

			d.imm = d.mem
			d.mem = NewMemTable(d.icmp)

			force = false
			d.scheduleFlush()
		}
	}
	return nil
}
