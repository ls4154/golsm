package impl

import (
	"sync"

	"github.com/ls4154/golsm/util"
)

func (d *dbImpl) scheduleFlush() {
	d.bgWork.ScheduleFlush()
}

func (d *dbImpl) scheduleCompaction() {
	d.bgWork.ScheduleCompaction()
}

func (d *dbImpl) flushMemTable() error {
	util.AssertMutexHeld(&d.mu)
	util.Assert(d.imm != nil)

	fnum := d.versions.NewFileNumber()
	d.RegisterPendingOutput(fnum)

	edit := VersionEdit{}
	err := d.WriteLevel0Table(d.imm, &edit, fnum)

	if err == nil {
		edit.SetPrevLogNumber(0)
		edit.SetLogNumber(d.logfileNum)
		err = d.versions.LogAndApply(&edit, &d.mu)
	}

	d.UnregisterPendingOutput(fnum)

	if err == nil {
		d.imm = nil
		d.CleanupObsoleteFiles()
	} else {
		d.RecordBackgroundError(err)
	}

	return err
}

type bgWork struct {
	db *dbImpl

	flushCh     chan struct{}
	compCh      chan struct{}
	flushDoneCh chan struct{}
	compDoneCh  chan struct{}
	wg          sync.WaitGroup

	closedCh chan struct{}
}

func (d *dbImpl) newBgWork() *bgWork {
	return &bgWork{
		db:          d,
		flushCh:     make(chan struct{}, 1),
		compCh:      make(chan struct{}, 1),
		flushDoneCh: make(chan struct{}, 1),
		compDoneCh:  make(chan struct{}, 1),

		closedCh: make(chan struct{}),
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

	for {
		select {
		case <-bg.closedCh:
			return
		case <-bg.flushCh:
			err := bg.doFlush()
			if err == nil {
				bg.ScheduleCompaction()
			}

			select {
			case bg.flushDoneCh <- struct{}{}:
			default:
			}
		}
	}
}

func (bg *bgWork) doFlush() error {
	d := bg.db

	d.logger.Printf("doFlush start")

	d.mu.Lock()
	defer d.mu.Unlock()

	if bgErr := d.GetBackgroundError(); bgErr != nil {
		return bgErr
	}

	var err error
	if d.imm != nil {
		err = d.flushMemTable()
	}

	return err
}

// for writeSerializer only
func (bg *bgWork) writerWaitForFlushDone() {
	select {
	case <-bg.flushDoneCh:
	case <-bg.closedCh:
	}
}

func (bg *bgWork) compactionMain() {
	defer bg.wg.Done()

	for {
		select {
		case <-bg.closedCh:
			return
		case <-bg.compCh:
			err := bg.doCompaction()
			if err == nil {
				bg.ScheduleCompaction()
			}

			select {
			case bg.compDoneCh <- struct{}{}:
			default:
			}
		}
	}
}

func (bg *bgWork) doCompaction() error {
	d := bg.db

	d.logger.Printf("doCompaction start")

	d.mu.Lock()
	defer d.mu.Unlock()

	if bgErr := d.GetBackgroundError(); bgErr != nil {
		return bgErr
	}

	comp := d.versions.PickCompaction()
	d.logger.Printf("picked compaction %+v", comp)
	var err error
	if comp != nil {
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
			comp.Release()
			d.CleanupObsoleteFiles()
		}
	}

	return err
}

// for writeSerializer only
func (bg *bgWork) writerWaitForCompactionDone() {
	select {
	case <-bg.compDoneCh:
	case <-bg.closedCh:
	}
}

func (bg *bgWork) Close() {
	close(bg.closedCh)
	bg.wg.Wait()
}
