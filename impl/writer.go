package impl

import (
	"sync"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type writer struct {
	batch    *WriteBatchImpl
	options  db.WriteOptions
	resultCh chan error
}

type writeSerializer struct {
	apply      func(*WriteBatchImpl, bool) error
	writerCh   chan *writer
	wg         sync.WaitGroup
	lastWriter *writer
	tempBatch  *WriteBatchImpl
}

func NewWriteSerializer(applyFn func(*WriteBatchImpl, bool) error) *writeSerializer {
	return &writeSerializer{
		apply:      applyFn,
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
