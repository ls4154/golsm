package impl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
)

type Version struct {
	vset  *VersionSet
	files [NumLevels][]*FileMetaData

	compactionScore float64
	compactionLevel int

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

func (v *Version) Get(lkey *LookupKey, verifyChecksum bool) ([]byte, error) {
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
		err := v.vset.tableCache.Get(f.number, f.size, lkey.Key(), handleResult, verifyChecksum)
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

		err := v.vset.tableCache.Get(f.number, f.size, lkey.Key(), handleResult, verifyChecksum)
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

func (v *Version) AddIterators(iters *[]db.Iterator, verifyChecksum bool) error {
	for _, f := range v.files[0] {
		it, err := v.vset.tableCache.NewIterator(f.number, f.size, verifyChecksum)
		if err != nil {
			return err
		}

		*iters = append(*iters, it)
	}

	for lv := 1; lv < NumLevels; lv++ {
		if len(v.files[lv]) > 0 {
			*iters = append(*iters, v.vset.newConcatIterator(v.files[lv], verifyChecksum))
		}
	}

	return nil
}

func (v *Version) getOverlappingFiles(level int, begin, end []byte) []*FileMetaData {
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
	dbname     string
	tableCache *TableCache
	icmp       *InternalKeyComparator

	nextFileNumber     uint64
	manifestFileNumber uint64
	lastSequence       uint64
	logNumber          uint64
	prevLogNumber      uint64

	descriptorFile db.WritableFile
	descriptorLog  *log.Writer

	dummyVersions Version
	current       *Version

	compactPointer [NumLevels][]byte

	applyMu sync.Mutex // serialize AppendVersion and AddRecord

	env db.Env
}

type Compaction struct {
	level int

	inputVersion *Version
	inputs       [2][]*FileMetaData
	edit         VersionEdit
}

func (c *Compaction) IsTrivial() bool {
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0
}

func (c *Compaction) Release() {
	c.inputVersion.Unref()
}

func (c *Compaction) NewInputIterator() (db.Iterator, error) {
	// TODO read opt
	vset := c.inputVersion.vset

	numIters := 2
	if c.level == 0 {
		numIters = len(c.inputs[0]) + 1
	}
	iters := make([]db.Iterator, 0, numIters)

	for i := 0; i < 2; i++ {
		level := c.level + i

		if level == 0 {
			for _, f := range c.inputs[i] {
				it, err := vset.tableCache.NewIterator(f.number, f.size, false)
				if err != nil {
					return nil, err
				}
				iters = append(iters, it)
			}
		} else {
			iters = append(iters, vset.newConcatIterator(c.inputs[i], false))
		}
	}

	return newMergingIterator(vset.icmp, iters), nil
}

type VersionBuilder struct {
	vset *VersionSet
	base *Version

	deletedFiles [NumLevels]map[uint64]struct{}
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
	for level := 0; level < NumLevels; level++ {
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

func (b *VersionBuilder) MaybeAddFile(v *Version, level int, f *FileMetaData) {
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

func NewVersionSet(dbname string, icmp *InternalKeyComparator, env db.Env, tableCache *TableCache) *VersionSet {
	vset := &VersionSet{
		dbname:             dbname,
		tableCache:         tableCache,
		icmp:               icmp,
		nextFileNumber:     2,
		manifestFileNumber: 0,
		lastSequence:       0,
		logNumber:          0,

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

	edit.SetNextFileNumber(vs.nextFileNumber)
	edit.SetLastSequence(vs.lastSequence)

	v := vs.NewVersion()
	builder := vs.NewBuilder(vs.current)
	builder.Apply(edit)
	builder.SaveTo(v)
	vs.Finalize(v)

	setCurrent := false
	if vs.descriptorLog == nil {
		// TODO If manifest rotation is added later, protect the temporary CURRENT file, to avoid obsolete GC races
		setCurrent = true
		newManifestFile := DescriptorFileName(vs.dbname, vs.manifestFileNumber)
		f, err := vs.env.NewWritableFile(newManifestFile)
		if err != nil {
			return err
		}
		vs.descriptorFile = f
		vs.descriptorLog = log.NewWriter(f)

		err = vs.writeSnapshot(vs.descriptorLog)
		if err != nil {
			return err
		}
	}

	dbMu.Unlock()

	record := edit.Append(nil)
	err := vs.descriptorLog.AddRecord(record)
	if err == nil {
		err = vs.descriptorFile.Sync()
	}
	if err == nil && setCurrent {
		err = SetCurrentFile(vs.env, vs.dbname, vs.manifestFileNumber)
	}

	dbMu.Lock()

	if err != nil {
		// TODO remove new manifest file if error occurs
		return err
	}

	vs.AppendVersion(v)
	vs.logNumber = edit.logNumber
	vs.prevLogNumber = edit.prevLogNumber

	return nil
}

func (vs *VersionSet) writeSnapshot(log *log.Writer) error {
	edit := VersionEdit{}
	edit.SetComparator(vs.icmp.userCmp.Name())

	for lv, cp := range vs.compactPointer {
		if len(cp) > 0 {
			edit.SetCompactPointer(lv, cp)
		}
	}

	for lv, files := range vs.current.files {
		for _, f := range files {
			edit.AddFile(lv, f.number, f.size, f.smallest, f.largest)
		}
	}

	record := edit.Append(nil)
	return log.AddRecord(record)
}

func (vs *VersionSet) Recover() error {
	current, err := util.ReadFile(vs.env, CurrentFileName(vs.dbname))
	if err != nil {
		return err
	}

	if len(current) == 0 || current[len(current)-1] != '\n' {
		return fmt.Errorf("%w: CURRENT file does not end with newline", db.ErrCorruption)
	}
	current = current[:len(current)-1]

	dscname := vs.dbname + "/" + string(current)
	file, err := vs.env.NewSequentialFile(dscname)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return fmt.Errorf("%w: CURRENT points to a non-existent file", db.ErrCorruption)
		}
		return err
	}
	defer file.Close()

	builder := vs.NewBuilder(vs.current)
	var recoverErr error
	var haveLogNumber, havePrevLogNumber, haveNextFileNumber, haveLastSequence bool
	var logNumber, prevLogNumber, nextFileNumber, lastSequence uint64

	{
		reader := log.NewReader(file)

		for {
			// TODO record buf reuse
			record, err := reader.ReadRecord()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				recoverErr = err
				break
			}

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
	vs.lastSequence = lastSequence
	vs.logNumber = logNumber
	vs.prevLogNumber = prevLogNumber

	// TODO reuse manifest

	return nil
}

func (vs *VersionSet) GetLastSequence() uint64 {
	return atomic.LoadUint64(&vs.lastSequence)
}

func (vs *VersionSet) SetLastSequence(seq uint64) {
	atomic.StoreUint64(&vs.lastSequence, seq)
}

func (vs *VersionSet) NewFileNumber() uint64 {
	n := vs.nextFileNumber
	vs.nextFileNumber++
	return n
}

func (vs *VersionSet) MakeFileNumberUsed(number uint64) {
	if number >= vs.nextFileNumber {
		vs.nextFileNumber = number + 1
	}
}

func (vs *VersionSet) NewVersion() *Version {
	v := &Version{
		vset:            vs,
		files:           [7][]*FileMetaData{},
		compactionScore: -1,
		compactionLevel: -1,
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
	bestLevel := -1
	bestScore := float64(-1)

	// last level cannot be picked as a compaction source
	for level := 0; level < NumLevels-1; level++ {
		score := float64(0)
		if level == 0 {
			score = float64(len(v.files[level])) / L0CompactionTrigger
		} else {
			levelBytes := totalFileSize(v.files[level])
			score = float64(levelBytes) / maxBytesForlevel(level)
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
		level:        level,
		inputVersion: cur,
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
		c.inputs[0] = cur.getOverlappingFiles(0, smallest, largest)
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
		// TODO try expand base level without exapanding next level
		_ = allStart
		_ = allLimit
	}

	if level+2 < NumLevels {
		// TODO grandparent level
		_ = allStart
		_ = allLimit
	}

	vs.compactPointer[level] = largest
	c.edit.SetCompactPointer(level, largest)
}

func (vs *VersionSet) NewBuilder(v *Version) *VersionBuilder {
	builder := &VersionBuilder{
		vset: vs,
		base: v,
	}

	for i := 0; i < NumLevels; i++ {
		builder.deletedFiles[i] = make(map[uint64]struct{})
	}

	return builder
}

func (vs *VersionSet) LiveFiles() map[uint64]struct{} {
	m := map[uint64]struct{}{}

	for v := vs.dummyVersions.next; v != &vs.dummyVersions; v = v.next {
		for lv := 0; lv < NumLevels; lv++ {
			for _, f := range v.files[lv] {
				m[f.number] = struct{}{}
			}
		}
	}

	return m
}

func (vs *VersionSet) NumLevelFiles(level int) int {
	return len(vs.current.files[level])
}

func totalFileSize(files []*FileMetaData) uint64 {
	var size uint64
	for _, f := range files {
		size += f.size
	}
	return size
}

func maxBytesForlevel(level int) float64 {
	result := 10 * 1048576.0 // 10MB
	for level > 1 {
		result *= 10
		level--
	}
	return result
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
	for i := 0; i < len(files); i++ {
		f := files[i]
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

func (vs *VersionSet) newConcatIterator(files []*FileMetaData, verifyChecksum bool) db.Iterator {
	return table.NewTwoLevelIterator(newLevelFileNumIterator(vs.icmp, files), func(fileValue []byte) (db.Iterator, error) {
		fnum := binary.LittleEndian.Uint64(fileValue[0:])
		fsize := binary.LittleEndian.Uint64(fileValue[8:])
		return vs.tableCache.NewIterator(fnum, fsize, verifyChecksum)
	})
}
