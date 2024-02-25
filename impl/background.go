package impl

import (
	"sync"

	"github.com/ls4154/golsm/util"
)

type bgTaskKind int

const (
	bgTaskFlush bgTaskKind = iota
)

type bgTask struct {
	kind bgTaskKind
	done chan error
}

func (d *dbImpl) scheduleFlush() {
	d.bgWork.ScheduleFlush()
}

func (d *dbImpl) compactMemTable() {
	util.AssertMutexHeld(&d.mu)
	util.Assert(d.imm != nil)

	edit := VersionEdit{}
	err := d.WriteLevel0Table(d.imm, &edit)

	if err == nil {
		edit.SetPrevLogNumber(0)
		edit.SetLogNumber(d.logfileNum)
		err = d.versions.LogAndApply(&edit, &d.mu)
	}

	if err == nil {
		d.imm = nil
		d.RemoveObsoleteFiles()
	} else {
		d.RecordBackgroundError(err)
	}
}

type bgWork struct {
	db *dbImpl

	flushCh     chan struct{}
	compCh      chan struct{}
	flushDoneCh chan struct{}
	compDoneCh  chan struct{}
	wg          sync.WaitGroup
}

func (d *dbImpl) newBgWork() *bgWork {
	return &bgWork{
		db:          d,
		flushCh:     make(chan struct{}, 1),
		compCh:      make(chan struct{}, 1),
		flushDoneCh: make(chan struct{}, 1),
		compDoneCh:  make(chan struct{}, 1),
	}
}

func (bg *bgWork) ScheduleFlush() {
	select {
	case bg.flushCh <- struct{}{}:
	default:
	}
}

func (bg *bgWork) ScheduleCompaction() {
	select {
	case bg.compCh <- struct{}{}:
	default:
	}
}

func (bg *bgWork) Run() {
	bg.wg.Add(2)
	go bg.flushMain()
	go bg.compactionMain()
}

func (bg *bgWork) flushMain() {
	defer bg.wg.Done()
	for range bg.flushCh {
		bg.doFlush()
	}
}

func (bg *bgWork) doFlush() {
	d := bg.db

	d.logger.Printf("doCompaction start")

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.imm != nil {
		d.compactMemTable()
	}

	bg.ScheduleCompaction()

	select {
	case bg.flushDoneCh <- struct{}{}:
	default:
	}
}

// for writeSerializer only
func (bg *bgWork) writerWaitForFlushDone() {
	<-bg.flushDoneCh
}

func (bg *bgWork) compactionMain() {
	defer bg.wg.Done()
	for range bg.compCh {
		bg.doCompaction()
	}
}

func (bg *bgWork) doCompaction() {
	d := bg.db

	d.logger.Printf("doCompaction start")

	d.mu.Lock()
	defer d.mu.Unlock()

	comp := d.versions.PickCompaction()
	d.logger.Printf("picked compaction %+v", comp)
	if comp != nil {
		var err error
		if comp.IsTrivial() {
			err = d.doTrivialMove(comp)
			if err != nil {
				// TODO bg err is already recorded in doTrivialMove
				d.RecordBackgroundError(err)
			}
			comp.Release()
		} else {
			err = d.doCompactionWork(comp)
			if err != nil {
				// TODO bg err is already recorded in doCompactionWork
				d.RecordBackgroundError(err)
			}
			// TODO cleanup
			comp.Release()
			d.RemoveObsoleteFiles()
		}

		if err != nil {
			bg.ScheduleCompaction()
		}
	}

	select {
	case bg.compDoneCh <- struct{}{}:
	default:
	}
}

// for writeSerializer only
func (bg *bgWork) writerWaitForCompactionDone() {
	<-bg.compDoneCh
}

func (bg *bgWork) Close() {
	close(bg.flushCh)
	close(bg.compCh)
	bg.wg.Wait()
}
