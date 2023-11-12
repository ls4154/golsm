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
		// TODO remove obsolete files
	} else {
		// TODO bg error
	}
}

type bgWork struct {
	db *dbImpl

	flushCh      chan struct{}
	compCh       chan struct{}
	writerWaitCh chan struct{}
	wg           sync.WaitGroup
}

func (d *dbImpl) newBgWork() *bgWork {
	return &bgWork{
		db:           d,
		flushCh:      make(chan struct{}, 1),
		compCh:       make(chan struct{}, 1),
		writerWaitCh: make(chan struct{}, 1),
	}
}

func (bg *bgWork) ScheduleFlush() {
	select {
	case bg.flushCh <- struct{}{}:
	default:
	}
}

func (bg *bgWork) Run() {
	bg.wg.Add(1)
	go bg.flushMain()
}

func (bg *bgWork) flushMain() {
	defer bg.wg.Done()
	for range bg.flushCh {
		bg.doFlush()
	}
}

func (bg *bgWork) doFlush() {
	db := bg.db

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.imm != nil {
		db.compactMemTable()
	}

	// TODO maybe schedule compaction

	select {
	case bg.writerWaitCh <- struct{}{}:
	default:
	}
}

func (bg *bgWork) writerWaitForBGDone() {
	<-bg.writerWaitCh
}

func (bg *bgWork) Close() {
	close(bg.flushCh)
	bg.wg.Wait()
}
