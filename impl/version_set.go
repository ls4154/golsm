package impl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/fs"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
)

type Version struct {
	vset  *VersionSet
	files [NumLevels][]*FileMetaData

	compactionScore float64
	compactionLevel Level

	refs int

	next *Version
	prev *Version
}

func (v *Version) Ref() {
	v.refs++
}

func (v *Version) Unref() {
	util.Assert(v.refs >= 1)
	v.refs--
	if v.refs == 0 {
		v.prev.next = v.next
		v.next.prev = v.prev
		v.next = v
		v.prev = v
	}
}

func (v *Version) Get(lkey *LookupKey, verifyChecksum, bypassCache bool) ([]byte, error) {
	icmp := v.vset.icmp

	done := false
	deleted := false
	value := []byte{}
	var fnErr error
	handleResult := func(k, v []byte) {
		parsedKey, err := ParseInternalKey(k)
		if err != nil {
			done = true
			fnErr = err
			return
		}
		if icmp.userCmp.Compare(parsedKey.UserKey, lkey.UserKey()) == 0 {
			done = true
			if parsedKey.Type == TypeDeletion {
				deleted = true
			} else {
				value = append(value, v...)
			}
		}
	}

	ukey := lkey.UserKey()
	ucmp := icmp.userCmp

	// Level-0 files may overlap each other, so we must check all of them
	l0files := make([]*FileMetaData, 0, len(v.files[0]))
	for _, f := range v.files[0] {
		if ucmp.Compare(ukey, ExtractUserKey(f.smallest)) >= 0 && ucmp.Compare(ukey, ExtractUserKey(f.largest)) <= 0 {
			l0files = append(l0files, f)
		}
	}

	// search from newest to oldest to respect sequence number ordering
	sort.Slice(l0files, func(i, j int) bool {
		return l0files[i].number > l0files[j].number
	})

	for _, f := range l0files {
		err := v.vset.tableCache.Get(f.number, f.size, lkey.Key(), handleResult, verifyChecksum, bypassCache)
		if err != nil {
			return nil, err
		}
		if done {
			if fnErr != nil {
				return nil, fnErr
			} else if deleted {
				return nil, db.ErrNotFound
			} else {
				return value, nil
			}
		}
	}

	// Level 1+ files are non-overlapping and sorted, so binary search suffices.
	ikey := lkey.Key()
	for lv := 1; lv < NumLevels; lv++ {
		levelFiles := v.files[lv]
		if len(levelFiles) == 0 {
			continue
		}

		idx := lowerBoundFiles(icmp, levelFiles, ikey)
		if idx >= len(levelFiles) {
			continue
		}

		f := levelFiles[idx]
		if ucmp.Compare(ukey, ExtractUserKey(f.smallest)) < 0 {
			continue
		}

		err := v.vset.tableCache.Get(f.number, f.size, lkey.Key(), handleResult, verifyChecksum, bypassCache)
		if err != nil {
			return nil, err
		}

		if done {
			if fnErr != nil {
				return nil, fnErr
			} else if deleted {
				return nil, db.ErrNotFound
			} else {
				return value, nil
			}
		}
	}

	return nil, db.ErrNotFound
}

func (v *Version) AddIterators(iters *[]db.Iterator, verifyChecksum, bypassCache bool) error {
	for _, f := range v.files[0] {
		it, err := v.vset.tableCache.NewIterator(f.number, f.size, verifyChecksum, bypassCache)
		if err != nil {
			return err
		}

		*iters = append(*iters, it)
	}

	for lv := Level(1); lv < NumLevels; lv++ {
		if len(v.files[lv]) > 0 {
			*iters = append(*iters, v.vset.newConcatIterator(v.files[lv], verifyChecksum, bypassCache))
		}
	}

	return nil
}

func (v *Version) getOverlappingFiles(level Level, begin, end []byte) []*FileMetaData {
	var hasBegin, hasEnd bool
	var userBegin, userEnd []byte
	if len(begin) > 0 {
		hasBegin = true
		userBegin = ExtractUserKey(begin)
	}
	if len(end) > 0 {
		hasEnd = true
		userEnd = ExtractUserKey(end)
	}

	files := []*FileMetaData{}
	userCmp := v.vset.icmp.userCmp

restart:
	for _, f := range v.files[level] {
		fStart := ExtractUserKey(f.smallest)
		fLimit := ExtractUserKey(f.largest)

		if hasBegin && userCmp.Compare(fLimit, userBegin) < 0 {
			// no overlap
		} else if hasEnd && userCmp.Compare(fStart, userEnd) > 0 {
			// no overlap
		} else {
			files = append(files, f)

			// check whether range is expanded for level 0
			if level == 0 {
				if hasBegin && userCmp.Compare(fStart, userBegin) < 0 {
					userBegin = fStart
					files = files[:0]
					goto restart
				} else if hasEnd && userCmp.Compare(fLimit, userEnd) > 0 {
					userEnd = fLimit
					files = files[:0]
					goto restart
				}
			}
		}
	}

	return files
}

type VersionSet struct {
	dbname              string
	tableCache          *TableCache
	icmp                *InternalKeyComparator
	paranoidChecks      bool
	compaction          *compactionPolicy
	maxManifestFileSize uint64
	logger              db.Logger

	nextFileNumber            FileNumber
	manifestFileNumber        FileNumber
	pendingManifestFileNumber FileNumber
	lastSequence              atomic.Uint64
	logNumber                 FileNumber
	prevLogNumber             FileNumber

	descriptorFile fs.WritableFile
	descriptorLog  *log.Writer

	dummyVersions Version
	current       *Version

	compactPointer [NumLevels][]byte

	applyMu sync.Mutex // serialize AppendVersion and AddRecord

	env fs.Env
}

type Compaction struct {
	level Level

	inputVersion *Version
	inputs       [2][]*FileMetaData
	grandparents []*FileMetaData

	maxGrandparentOverlapBytes uint64
	grandparentIndex           int
	seenKey                    bool
	overlappedBytes            uint64

	edit VersionEdit
}

func (c *Compaction) IsTrivial() bool {
	// Large level+2 overlap makes a "cheap" move expensive later.
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0 &&
		totalFileSize(c.grandparents) <= c.maxGrandparentOverlapBytes
}

func (c *Compaction) Release() {
	c.inputVersion.Unref()
}

func (c *Compaction) NewInputIterator() (db.Iterator, error) {
	vset := c.inputVersion.vset

	numIters := 2
	if c.level == 0 {
		numIters = len(c.inputs[0]) + 1
	}
	iters := make([]db.Iterator, 0, numIters)

	for i := Level(0); i < 2; i++ {
		level := c.level + i
		if level == 0 {
			for _, f := range c.inputs[i] {
				it, err := vset.tableCache.NewIterator(f.number, f.size, vset.paranoidChecks, true)
				if err != nil {
					return nil, err
				}
				iters = append(iters, it)
			}
		} else {
			iters = append(iters, vset.newConcatIterator(c.inputs[i], vset.paranoidChecks, true))
		}
	}

	return newMergingIterator(vset.icmp, iters), nil
}

func (c *Compaction) ShouldStopBefore(internalKey []byte) bool {
	icmp := c.inputVersion.vset.icmp
	for c.grandparentIndex < len(c.grandparents) &&
		icmp.Compare(internalKey, c.grandparents[c.grandparentIndex].largest) > 0 {
		if c.seenKey {
			c.overlappedBytes += c.grandparents[c.grandparentIndex].size
		}
		c.grandparentIndex++
	}
	c.seenKey = true

	if c.overlappedBytes > c.maxGrandparentOverlapBytes {
		// Split before one output drags too much level+2 data into the next compaction.
		c.overlappedBytes = 0
		return true
	}
	return false
}

type VersionBuilder struct {
	vset *VersionSet
	base *Version

	deletedFiles [NumLevels]map[FileNumber]struct{}
	addedFiles   [NumLevels][]*FileMetaData
}

func (b *VersionBuilder) Apply(edit *VersionEdit) {
	for _, p := range edit.compactPointers {
		b.vset.compactPointer[p.level] = append(b.vset.compactPointer[p.level], p.internalKey...)
	}

	for del := range edit.deletedFiles {
		b.deletedFiles[del.level][del.number] = struct{}{}
	}

	for _, file := range edit.newFiles {
		f := &FileMetaData{}
		*f = file
		delete(b.deletedFiles[f.level], f.number)
		b.addedFiles[f.level] = append(b.addedFiles[f.level], f)
	}
}

func (b *VersionBuilder) SaveTo(v *Version) {
	for level := Level(0); level < NumLevels; level++ {
		baseFiles := b.base.files[level]
		v.files[level] = make([]*FileMetaData, 0, len(baseFiles)+len(b.addedFiles[level]))

		// Sort by smallest key
		sort.Slice(b.addedFiles[level], func(i, j int) bool {
			return b.vset.icmp.Compare(b.addedFiles[level][i].smallest,
				b.addedFiles[level][j].smallest) < 0
		})

		// Merge added files with existing files
		baseIdx := 0
		for _, f := range b.addedFiles[level] {
			// Add smaller files from the base version
			for baseIdx < len(baseFiles) && b.vset.icmp.Compare(baseFiles[baseIdx].largest, f.smallest) < 0 {
				b.MaybeAddFile(v, level, baseFiles[baseIdx])
				baseIdx++
			}

			b.MaybeAddFile(v, level, f)
		}

		// Add remaining files from the base version
		for baseIdx < len(baseFiles) {
			b.MaybeAddFile(v, level, baseFiles[baseIdx])
			baseIdx++
		}

		// Check there are no overlaps
		util.AssertFunc(func() bool {
			if level > 0 {
				for i := 1; i < len(v.files[level]); i++ {
					if b.vset.icmp.Compare(v.files[level][i-1].largest,
						v.files[level][i].smallest) >= 0 {
						return false
					}
				}
			}
			return true
		})
	}
}

func (b *VersionBuilder) MaybeAddFile(v *Version, level Level, f *FileMetaData) {
	if _, ok := b.deletedFiles[level][f.number]; ok {
		// Deleted file
		return
	}

	util.AssertFunc(func() bool {
		if level > 0 && len(v.files[level]) > 0 {
			// Must not overlap
			last := v.files[level][len(v.files[level])-1]
			return b.vset.icmp.Compare(last.largest, f.smallest) < 0
		}
		return true
	})

	v.files[level] = append(v.files[level], f)
}

func NewVersionSet(dbname string, icmp *InternalKeyComparator, env fs.Env, tableCache *TableCache,
	paranoidChecks bool, maxManifestFileSize uint64, logger db.Logger, compaction *compactionPolicy) *VersionSet {
	util.Assert(compaction != nil)

	vset := &VersionSet{
		dbname:              dbname,
		tableCache:          tableCache,
		icmp:                icmp,
		paranoidChecks:      paranoidChecks,
		compaction:          compaction,
		nextFileNumber:      2,
		manifestFileNumber:  0,
		logNumber:           0,
		maxManifestFileSize: maxManifestFileSize,
		logger:              logger,

		descriptorFile: nil,
		descriptorLog:  nil,

		env: env,
	}

	vset.dummyVersions.prev = &vset.dummyVersions
	vset.dummyVersions.next = &vset.dummyVersions
	v := vset.NewVersion()
	vset.AppendVersion(v)

	return vset
}

func (vs *VersionSet) Close() {
	if vs.descriptorFile != nil {
		vs.descriptorFile.Close()
	}
}

func (vs *VersionSet) LogAndApply(edit *VersionEdit, dbMu *sync.Mutex) error {
	util.AssertMutexHeld(dbMu)

	// Release dbMu before acquiring applyMu to prevent deadlock
	dbMu.Unlock()

	vs.applyMu.Lock()
	defer vs.applyMu.Unlock()

	dbMu.Lock()

	if edit.hasLogNumber {
		util.Assert(edit.logNumber >= vs.logNumber)
		util.Assert(edit.logNumber < vs.nextFileNumber)
	} else {
		edit.SetLogNumber(vs.logNumber)
	}

	if !edit.hasPrevLogNumber {
		edit.SetPrevLogNumber(vs.prevLogNumber)
	}

	v := vs.NewVersion()
	builder := vs.NewBuilder(vs.current)
	builder.Apply(edit)
	builder.SaveTo(v)
	vs.Finalize(v)

	newManifest := false
	var newManifestNumber FileNumber
	var oldManifestNumber FileNumber
	var oldManifestSize uint64
	var snapshotEdit VersionEdit
	writeDescriptorLog := vs.descriptorLog
	writeDescriptorFile := vs.descriptorFile
	var oldDescriptorFile fs.WritableFile

	if writeDescriptorLog == nil {
		util.Assert(vs.manifestFileNumber > 0)
		util.Assert(vs.descriptorFile == nil)
		newManifest = true
		newManifestNumber = vs.manifestFileNumber
	} else if vs.maxManifestFileSize > 0 && vs.descriptorLog.Size() >= vs.maxManifestFileSize {
		newManifest = true
		newManifestNumber = vs.NewFileNumber()
		oldManifestNumber = vs.manifestFileNumber
		oldManifestSize = vs.descriptorLog.Size()
		oldDescriptorFile = vs.descriptorFile
	}

	if newManifest {
		// Capture the snapshot under dbMu. The actual MANIFEST I/O runs after
		// releasing dbMu, so shared version state must not be read after this.
		// build snapshot
		snapshotEdit.SetComparator(vs.icmp.userCmp.Name())
		for lv, cp := range vs.compactPointer {
			if len(cp) > 0 {
				snapshotEdit.SetCompactPointer(Level(lv), cp)
			}
		}
		for lv, files := range vs.current.files {
			for _, f := range files {
				snapshotEdit.AddFile(Level(lv), f.number, f.size, f.smallest, f.largest)
			}
		}

		vs.pendingManifestFileNumber = newManifestNumber
	}

	edit.SetNextFileNumber(vs.nextFileNumber)
	edit.SetLastSequence(vs.GetLastSequence())

	dbMu.Unlock()

	newManifestName := DescriptorFileName(vs.dbname, newManifestNumber)

	var err error
	if newManifest {
		// open new manifest
		writeDescriptorFile, err = vs.env.NewWritableFile(newManifestName)
		if err != nil {
			err = wrapIOError(err, "create manifest %s", newManifestName)
		}

		if err == nil {
			writeDescriptorLog = log.NewWriter(writeDescriptorFile)

			// write snapshot
			record := snapshotEdit.Append(nil)
			err = writeDescriptorLog.AddRecord(record)
		}
	}

	if err == nil {
		record := edit.Append(nil)
		err = writeDescriptorLog.AddRecord(record)
	}

	if err == nil {
		err = syncDirIfSupported(vs.env, vs.dbname)
	}

	if err == nil {
		err = writeDescriptorFile.Sync()
	}

	if err == nil && newManifest {
		err = SetCurrentFile(vs.env, vs.dbname, newManifestNumber)
	}

	if err == nil && oldDescriptorFile != nil {
		if vs.logger != nil {
			vs.logger.Printf("Rotated MANIFEST #%d -> #%d at %d bytes",
				oldManifestNumber, newManifestNumber, oldManifestSize)
		}
		_ = oldDescriptorFile.Close()
	}

	dbMu.Lock()

	if err != nil {
		if newManifest {
			vs.pendingManifestFileNumber = 0
			if writeDescriptorFile != nil {
				_ = writeDescriptorFile.Close()
			}
			_ = vs.env.RemoveFile(newManifestName)
		}
		return wrapIOError(err, "apply version edit")
	}

	if newManifest {
		vs.pendingManifestFileNumber = 0
		vs.manifestFileNumber = newManifestNumber
		vs.descriptorFile = writeDescriptorFile
		vs.descriptorLog = writeDescriptorLog
	}

	vs.AppendVersion(v)
	vs.logNumber = edit.logNumber
	vs.prevLogNumber = edit.prevLogNumber

	return nil
}

func (vs *VersionSet) Recover() error {
	current, err := util.ReadFile(vs.env, CurrentFileName(vs.dbname))
	if err != nil {
		if fs.IsNotExist(err) {
			return fmt.Errorf("%w: missing CURRENT file", db.ErrCorruption)
		}
		return wrapIOError(err, "read CURRENT file %s", CurrentFileName(vs.dbname))
	}

	if len(current) == 0 || current[len(current)-1] != '\n' {
		return fmt.Errorf("%w: CURRENT file does not end with newline", db.ErrCorruption)
	}
	current = current[:len(current)-1]

	dscname := vs.dbname + "/" + string(current)
	file, err := vs.env.NewSequentialFile(dscname)
	if err != nil {
		if fs.IsNotExist(err) {
			return fmt.Errorf("%w: CURRENT points to a non-existent file", db.ErrCorruption)
		}
		return wrapIOError(err, "open manifest %s", dscname)
	}
	defer file.Close()

	builder := vs.NewBuilder(vs.current)
	var recoverErr error
	var haveLogNumber, havePrevLogNumber, haveNextFileNumber, haveLastSequence bool
	var logNumber, prevLogNumber, nextFileNumber FileNumber
	var lastSequence SequenceNumber

	{
		reader := log.NewReader(file)
		var recordBuf []byte

		for {
			recordBuf, err = reader.ReadRecordInto(recordBuf)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				recoverErr = err
				break
			}
			record := recordBuf

			edit := VersionEdit{}
			err = edit.DecodeFrom(record)
			if err != nil {
				recoverErr = err
				break
			}

			if edit.hasComparator && edit.comparator != vs.icmp.userCmp.Name() {
				recoverErr = fmt.Errorf("%w: %s does not match existing comparator %s",
					db.ErrInvalidArgument, edit.comparator, vs.icmp.userCmp.Name())
				break
			}

			builder.Apply(&edit)

			if edit.hasLogNumber {
				logNumber = edit.logNumber
				haveLogNumber = true
			}

			if edit.hasPrevLogNumber {
				prevLogNumber = edit.prevLogNumber
				havePrevLogNumber = true
			}

			if edit.hasNextFileNumber {
				nextFileNumber = edit.nextFileNumber
				haveNextFileNumber = true
			}

			if edit.hasLastSequence {
				lastSequence = edit.lastSequence
				haveLastSequence = true
			}
		}
	}

	if recoverErr != nil {
		return recoverErr
	}

	if !haveNextFileNumber {
		return fmt.Errorf("%w: no meta-nextfile entry in descriptor", db.ErrCorruption)
	} else if !haveLogNumber {
		return fmt.Errorf("%w: no meta-lognumber entry in descriptor", db.ErrCorruption)
	} else if !haveLastSequence {
		return fmt.Errorf("%w: no meta-last-sequence-number entry in descriptor", db.ErrCorruption)
	}

	if !havePrevLogNumber {
		prevLogNumber = 0
	}

	vs.MakeFileNumberUsed(prevLogNumber)
	vs.MakeFileNumberUsed(logNumber)

	v := vs.NewVersion()
	builder.SaveTo(v)
	vs.Finalize(v)
	vs.AppendVersion(v)
	vs.manifestFileNumber = nextFileNumber
	vs.nextFileNumber = nextFileNumber + 1
	vs.lastSequence.Store(uint64(lastSequence))
	vs.logNumber = logNumber
	vs.prevLogNumber = prevLogNumber

	// TODO reuse manifest

	return nil
}

func (vs *VersionSet) GetLastSequence() SequenceNumber {
	return SequenceNumber(vs.lastSequence.Load())
}

func (vs *VersionSet) SetLastSequence(seq SequenceNumber) {
	vs.lastSequence.Store(uint64(seq))
}

func (vs *VersionSet) NewFileNumber() FileNumber {
	n := vs.nextFileNumber
	vs.nextFileNumber++
	return n
}

func (vs *VersionSet) MakeFileNumberUsed(number FileNumber) {
	if number >= vs.nextFileNumber {
		vs.nextFileNumber = number + 1
	}
}

func (vs *VersionSet) NewVersion() *Version {
	v := &Version{
		vset:            vs,
		files:           [7][]*FileMetaData{},
		compactionScore: -1,
		compactionLevel: Level(-1),
	}
	v.next = v
	v.prev = v

	return v
}

func (vs *VersionSet) AppendVersion(v *Version) {
	if vs.current != nil {
		vs.current.Unref()
	}

	v.Ref()
	vs.current = v

	v.prev = vs.dummyVersions.prev
	v.next = &vs.dummyVersions
	v.prev.next = v
	v.next.prev = v
}

func (vs *VersionSet) Finalize(v *Version) {
	bestLevel := Level(-1)
	bestScore := float64(-1)

	// last level cannot be picked as a compaction source
	for level := Level(0); level < NumLevels-1; level++ {
		score := float64(0)
		if level == 0 {
			score = float64(len(v.files[level])) / float64(vs.compaction.l0CompactionTrigger)
		} else {
			levelBytes := totalFileSize(v.files[level])
			score = float64(levelBytes) / float64(vs.compaction.maxBytesForLevel(level))
		}

		if score > bestScore {
			bestLevel = level
			bestScore = score
		}
	}

	v.compactionLevel = bestLevel
	v.compactionScore = bestScore
}

func (vs *VersionSet) NeedsCompaction() bool {
	return vs.current.compactionScore >= 1
}

func (vs *VersionSet) PickCompaction() *Compaction {
	cur := vs.current
	if cur.compactionScore < 1 {
		return nil
	}

	level := cur.compactionLevel
	util.Assert(level >= 0)
	util.Assert(level+1 < NumLevels)

	if len(cur.files[level]) == 0 {
		return nil
	}

	c := &Compaction{
		level:                      level,
		inputVersion:               cur,
		maxGrandparentOverlapBytes: vs.compaction.maxGrandparentOverlapBytes(),
	}
	cur.Ref()

	vs.setupBaseInputs(c)
	vs.setupOtherInputs(c)

	return c
}

func (vs *VersionSet) setupBaseInputs(c *Compaction) {
	level := c.level

	cur := c.inputVersion
	files := cur.files[level]

	util.Assert(len(files) > 0)

	// Use compactPointer for round-robin file selection to compact evenly across the level.
	ptr := vs.compactPointer[level]
	for _, f := range files {
		if len(ptr) <= 0 || vs.icmp.Compare(f.largest, ptr) > 0 {
			c.inputs[0] = append(c.inputs[0], f)
			break
		}
	}

	if len(c.inputs[0]) == 0 {
		c.inputs[0] = append(c.inputs[0], files[0])
	}

	if level == 0 {
		// L0 files may overlap each other, include the full overlapping  first.
		smallest, largest := getRange(vs.icmp, c.inputs[0])
		c.inputs[0] = cur.getOverlappingFiles(Level(0), smallest, largest)
	}
}

func (vs *VersionSet) setupOtherInputs(c *Compaction) {
	level := c.level
	cur := c.inputVersion

	addBoundaryInputs(vs.icmp, cur.files[level], &c.inputs[0])

	smallest, largest := getRange(vs.icmp, c.inputs[0])

	c.inputs[1] = cur.getOverlappingFiles(level+1, smallest, largest)

	addBoundaryInputs(vs.icmp, cur.files[level+1], &c.inputs[1])

	allStart, allLimit := getRange(vs.icmp, append(c.inputs[0], c.inputs[1]...))

	if len(c.inputs[1]) > 0 {
		expanded0 := cur.getOverlappingFiles(level, allStart, allLimit)
		addBoundaryInputs(vs.icmp, cur.files[level], &expanded0)

		inputs1Size := totalFileSize(c.inputs[1])
		expanded0Size := totalFileSize(expanded0)
		if len(expanded0) > len(c.inputs[0]) &&
			inputs1Size+expanded0Size < vs.compaction.expandedCompactionByteSizeLimit() {
			newStart, newLimit := getRange(vs.icmp, expanded0)
			expanded1 := cur.getOverlappingFiles(level+1, newStart, newLimit)
			addBoundaryInputs(vs.icmp, cur.files[level+1], &expanded1)
			if len(expanded1) == len(c.inputs[1]) {
				smallest = newStart
				largest = newLimit
				c.inputs[0] = expanded0
				c.inputs[1] = expanded1
				allStart, allLimit = getRange(vs.icmp, append(c.inputs[0], c.inputs[1]...))
			}
		}
	}

	if level+2 < NumLevels {
		c.grandparents = cur.getOverlappingFiles(level+2, allStart, allLimit)
	}

	vs.compactPointer[level] = largest
	c.edit.SetCompactPointer(level, largest)
}

func (vs *VersionSet) NewBuilder(v *Version) *VersionBuilder {
	builder := &VersionBuilder{
		vset: vs,
		base: v,
	}

	for i := Level(0); i < NumLevels; i++ {
		builder.deletedFiles[i] = make(map[FileNumber]struct{})
	}

	return builder
}

func (vs *VersionSet) LiveFiles() map[FileNumber]struct{} {
	m := map[FileNumber]struct{}{}

	for v := vs.dummyVersions.next; v != &vs.dummyVersions; v = v.next {
		for lv := Level(0); lv < NumLevels; lv++ {
			for _, f := range v.files[lv] {
				m[f.number] = struct{}{}
			}
		}
	}

	return m
}

func (vs *VersionSet) NumLevelFiles(level Level) int {
	return len(vs.current.files[level])
}

func (vs *VersionSet) NumLevelBytes(level Level) uint64 {
	return totalFileSize(vs.current.files[level])
}

func (v *Version) DebugString() string {
	var b strings.Builder
	for level := range NumLevels {
		fmt.Fprintf(&b, "--- level %d ---\n", level)
		files := v.files[level]
		for _, f := range files {
			fmt.Fprintf(&b, " %d:%d[%s .. %s]\n",
				f.number,
				f.size,
				debugInternalKey(f.smallest),
				debugInternalKey(f.largest),
			)
		}
	}
	return b.String()
}

func debugInternalKey(ikey []byte) string {
	parsed, err := ParseInternalKey(ikey)
	if err != nil {
		return "(bad)" + escapeBytes(ikey)
	}
	return "'" + escapeBytes(parsed.UserKey) + "' @ " +
		strconv.FormatUint(uint64(parsed.Sequence), 10) + " : " +
		strconv.Itoa(int(parsed.Type))
}

func escapeBytes(b []byte) string {
	q := strconv.QuoteToASCII(string(b))
	return q[1 : len(q)-1]
}

func totalFileSize(files []*FileMetaData) uint64 {
	var size uint64
	for _, f := range files {
		size += f.size
	}
	return size
}

func getRange(icmp *InternalKeyComparator, files []*FileMetaData) ([]byte, []byte) {
	smallest := files[0].smallest
	largest := files[0].largest
	for i := 1; i < len(files); i++ {
		f := files[i]
		if icmp.Compare(f.smallest, smallest) < 0 {
			smallest = f.smallest
		}
		if icmp.Compare(f.largest, largest) > 0 {
			largest = f.largest
		}
	}
	return smallest, largest
}

// Extend upper boundary to ensure the same user key is not split across multiple sstable files.
func addBoundaryInputs(icmp *InternalKeyComparator, levelFiles []*FileMetaData, compactionFiles *[]*FileMetaData) {
	if len(*compactionFiles) == 0 {
		return
	}

	largest := findLargestKey(icmp, *compactionFiles)

	keepSearch := true
	for keepSearch {
		f := findSmallestBoundaryFile(icmp, levelFiles, largest)
		if f != nil {
			*compactionFiles = append(*compactionFiles, f)
			largest = f.largest
		} else {
			keepSearch = false
		}
	}
}

func findLargestKey(icmp *InternalKeyComparator, files []*FileMetaData) []byte {
	util.Assert(len(files) > 0)

	largest := files[0].largest
	for _, f := range files {
		if icmp.Compare(f.largest, largest) > 0 {
			largest = f.largest
		}
	}
	return largest
}

func findSmallestBoundaryFile(icmp *InternalKeyComparator, files []*FileMetaData, largestKey []byte) *FileMetaData {
	ucmp := icmp.userCmp
	largestUserKey := ExtractUserKey(largestKey)
	var ret *FileMetaData
	for _, f := range files {
		if icmp.Compare(f.smallest, largestKey) > 0 && ucmp.Compare(ExtractUserKey(f.smallest), largestUserKey) == 0 {
			if ret == nil || icmp.Compare(f.smallest, ret.smallest) < 0 {
				ret = f
			}
		}
	}

	return ret
}

func lowerBoundFiles(icmp db.Comparator, files []*FileMetaData, key []byte) int {
	l := 0
	r := len(files)

	for l < r {
		m := (l + r) / 2
		if icmp.Compare(files[m].largest, key) < 0 {
			l = m + 1
		} else {
			r = m
		}
	}

	util.Assert(l == r)
	util.Assert(l <= len(files))

	return l
}

func (vs *VersionSet) newConcatIterator(files []*FileMetaData, verifyChecksum, bypassCache bool) db.Iterator {
	return table.NewTwoLevelIterator(newLevelFileNumIterator(vs.icmp, files), func(fileValue []byte) (db.Iterator, error) {
		fnum := FileNumber(binary.LittleEndian.Uint64(fileValue[0:]))
		fsize := binary.LittleEndian.Uint64(fileValue[8:])
		return vs.tableCache.NewIterator(fnum, fsize, verifyChecksum, bypassCache)
	})
}
