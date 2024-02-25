package impl

import (
	"time"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
)

func (d *dbImpl) doTrivialMove(c *Compaction) error {
	util.AssertMutexHeld(&d.mu)

	f := c.inputs[0][0]
	c.edit.RemoveFile(f.number, c.level)
	c.edit.AddFile(c.level+1, f.number, f.size, f.smallest, f.largest)

	err := d.versions.LogAndApply(&c.edit, &d.mu)
	if err != nil {
		// TODO bg error
	}

	d.logger.Printf("Moved #%d to level-%d %d bytes %v\n", f.number, c.level+1, f.size, err)
	return err
}

func (d *dbImpl) doCompactionWork(c *Compaction) error {
	util.AssertMutexHeld(&d.mu)

	startTime := time.Now()

	d.logger.Printf("Compacting %d@%d + %d@%d files", len(c.inputs[0]), c.level, len(c.inputs[1]), c.level+1)

	var smallestSnapshot uint64
	if d.snapshots.Empty() {
		smallestSnapshot = d.versions.GetLastSequence()
	} else {
		smallestSnapshot = d.snapshots.Oldest().seq
	}

	input, err := c.NewInputIterator()
	if err != nil {
		// TODO bg error
		panic("NewInputIterator error")
	}

	d.mu.Unlock()
	var hasCurUserKey bool
	var curUserKey []byte
	var lastSequenceForKey uint64

	var builder *table.TableBuilder
	var outputs []*FileMetaData
	var curOutput *FileMetaData
	var curOutfile db.WritableFile

	var levelPtrs [NumLevels]int

	input.SeekToFirst()
	for input.Valid() {
		// TODO cancel if shutting down

		key := input.Key()

		// TODO check split

		drop := false

		ikey, parseErr := ParseInternalKey(key)
		if parseErr != nil {
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
			} else if ikey.Type == TypeDeletion && ikey.Sequence <= smallestSnapshot &&
				keyNotExistsInHigherLevel(ikey.UserKey, d.icmp.userCmp, c.inputVersion, c.level+2, &levelPtrs) {
				drop = true
			}

			lastSequenceForKey = ikey.Sequence
		}

		if !drop {
			if builder == nil {
				b, fnum, f, newErr := d.NewCompactionOutputBuilder()
				if newErr != nil {
					err = newErr
					break
				}
				builder = b
				curOutfile = f
				curOutput = &FileMetaData{number: fnum, level: c.level + 1}
				outputs = append(outputs, curOutput)
			}

			if builder.NumEntries() == 0 {
				curOutput.smallest = append(curOutput.smallest[:0], key...)
			}
			curOutput.largest = append(curOutput.largest[:0], key...)

			builder.Add(key, input.Value())

			if builder.FileSize() >= d.options.MaxFileSize {
				finErr := d.FinishCompactionOutputFile(curOutput, curOutfile, builder, input.Error() != nil)
				if finErr != nil {
					err = finErr
					break
				}

				builder = nil
				curOutfile = nil
				curOutput = nil
			}
		}

		input.Next()
	}

	// TODO shutting down

	if err == nil && builder != nil {
		finErr := d.FinishCompactionOutputFile(curOutput, curOutfile, builder, input.Error() != nil)
		if finErr != nil {
			err = finErr
		}

		builder = nil
		curOutfile = nil
		curOutput = nil
	}

	if err == nil {
		err = input.Error()
	}
	_ = input.Close()

	d.mu.Lock()

	elapsed := time.Now().Sub(startTime)
	d.logger.Printf("compaction time: %s", elapsed)

	if err == nil {
		err = d.ApplyCompaction(c, outputs)
	}

	if err != nil {
		// TODO bg error
	}

	return err
}

func (d *dbImpl) NewCompactionOutputBuilder() (*table.TableBuilder, uint64, db.WritableFile, error) {
	d.mu.Lock()

	fileNum := d.versions.NewFileNumber()
	d.pendingOutputs[fileNum] = struct{}{}

	d.mu.Unlock()

	fname := TableFileName(d.dbname, fileNum)
	f, err := d.env.NewWritableFile(fname)
	if err != nil {
		return nil, 0, nil, err
	}

	return table.NewTableBuilder(f, d.icmp, d.options.BlockSize, d.options.Compression, d.options.BlockRestartInterval),
		fileNum, f, nil
}

func (d *dbImpl) FinishCompactionOutputFile(out *FileMetaData, outfile db.WritableFile, builder *table.TableBuilder,
	abandon bool,
) error {
	var err error
	if abandon {
		builder.Abandon()
	} else {
		err = builder.Finish()
	}

	currentEntries := builder.NumEntries()
	currentBytes := builder.FileSize()

	out.size = currentBytes
	// TODO total bytes

	if err == nil {
		err = outfile.Sync()
	}
	if err == nil {
		err = outfile.Close()
	}

	// check table file?

	d.logger.Printf("Generated table #%d@%d: %d keys, %d bytes", out.number, out.level, currentEntries, currentBytes)

	return err
}

func (d *dbImpl) ApplyCompaction(c *Compaction, outputs []*FileMetaData) error {
	util.AssertMutexHeld(&d.mu)

	d.logger.Printf("Compacted %d@%d + %d@%d files => %d bytes",
		len(c.inputs[0]), c.level, len(c.inputs[1]), c.level+1, 12345)

	// delete input files
	for i := 0; i < 2; i++ {
		for _, f := range c.inputs[i] {
			c.edit.RemoveFile(f.number, f.level)
		}
	}

	for _, f := range outputs {
		c.edit.AddFile(f.level, f.number, f.size, f.smallest, f.largest)
	}

	return d.versions.LogAndApply(&c.edit, &d.mu)
}

func keyNotExistsInHigherLevel(userKey []byte, userCmp db.Comparator, v *Version, checkFrom int, levelPtrs *[NumLevels]int) bool {
	for lv := checkFrom; lv < NumLevels; lv++ {
		files := v.files[lv]
		for levelPtrs[lv] < len(files) {
			f := files[levelPtrs[lv]]
			if userCmp.Compare(userKey, ExtractUserKey(f.largest)) <= 0 {
				if userCmp.Compare(userKey, ExtractUserKey(f.smallest)) >= 0 {
					return false
				}
				break
			}
			levelPtrs[lv]++
		}
	}
	return true
}
