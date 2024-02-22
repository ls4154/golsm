package impl

import (
	"time"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
)

type compactionState struct {
	c *Compaction
}

func (d *dbImpl) doTrivialMove(c *Compaction) {
	util.AssertMutexHeld(&d.mu)

	f := c.inputs[0][0]
	c.edit.RemoveFile(f.number, c.level)
	c.edit.AddFile(c.level+1, f.number, f.size, f.smallest, f.largest)

	err := d.versions.LogAndApply(&c.edit, &d.mu)
	if err != nil {
		// TODO bg error
	}

	d.logger.Printf("Moved #%d to level-%d %d bytes %v\n", f.number, c.level+1, f.size, err)
}

func (d *dbImpl) doCompactionWork(c *Compaction) {
	util.AssertMutexHeld(&d.mu)

	startTime := time.Now()

	d.logger.Printf("Compacting %d@%d + %d@%d files", len(c.inputs[0]), c.level, len(c.inputs[1]), c.level+1)

	var smallestSnapshot uint64
	if d.snapshots.Empty() {
		smallestSnapshot = d.versions.GetLastSequence()
	} else {
		smallestSnapshot = d.snapshots.Oldest().seq
	}

	input := db.Iterator(nil) // TODO

	d.mu.Unlock()
	var hasCurUserKey bool
	var curUserKey []byte
	var lastSequenceForKey uint64

	var builder *table.TableBuilder

	for input.Valid() {
		// TODO cancel if shutting down

		key := input.Key()

		// TODO check split

		drop := false

		ikey, err := ParseInternalKey(key)
		if err != nil {
			// keep corrupted entry
			hasCurUserKey = false
			curUserKey = curUserKey[:0]
			lastSequenceForKey = MaxSequenceNumber
		} else {
			if !hasCurUserKey || d.icmp.userCmp.Compare(ikey.UserKey, curUserKey) != 0 {
				// new user key
				hasCurUserKey = true
				curUserKey = append(curUserKey[:0], ikey.UserKey...)
				lastSequenceForKey = MaxSequenceNumber
			}

			if lastSequenceForKey <= smallestSnapshot {
				drop = true
			} else if ikey.Type == TypeDeletion { // TODO check if droppable
				drop = true
			}

			lastSequenceForKey = ikey.Sequence
		}

		if !drop {
			if builder == nil {
				// TODO open new
			}

			if builder.NumEntries() == 0 {
			}
		}

		input.Next()
	}
}
