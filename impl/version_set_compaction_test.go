package impl

import (
	"encoding/binary"
	"testing"

	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func compactionInternalKey(user string, seq SequenceNumber) []byte {
	key := []byte(user)
	out := make([]byte, 0, len(key)+8)
	out = append(out, key...)
	out = append(out, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint64(out[len(key):], PackSequenceAndType(seq, TypeValue))
	return out
}

func testFile(level Level, number FileNumber, size uint64, smallest, largest string) *FileMetaData {
	return &FileMetaData{
		number:   number,
		size:     size,
		smallest: compactionInternalKey(smallest, 1),
		largest:  compactionInternalKey(largest, 1),
		level:    level,
	}
}

func testVersionSetForCompaction() *VersionSet {
	return NewVersionSet("db", &InternalKeyComparator{userCmp: util.BytewiseComparator}, nil, nil, false)
}

func TestVersionSetNeedsCompactionL0Trigger(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	v.files[0] = []*FileMetaData{
		testFile(0, 1, 1, "a", "a"),
		testFile(0, 2, 1, "b", "b"),
		testFile(0, 3, 1, "c", "c"),
		testFile(0, 4, 1, "d", "d"),
	}

	vs.Finalize(v)
	vs.AppendVersion(v)

	require.Equal(t, Level(0), v.compactionLevel)
	require.Equal(t, float64(1), v.compactionScore)
	require.True(t, vs.NeedsCompaction())
}

func TestVersionSetNeedsCompactionBySize(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	v.files[1] = []*FileMetaData{
		testFile(1, 1, 11*1024*1024, "a", "z"),
	}

	vs.Finalize(v)
	vs.AppendVersion(v)

	require.Equal(t, Level(1), v.compactionLevel)
	require.Greater(t, v.compactionScore, float64(1))
	require.True(t, vs.NeedsCompaction())
}

func TestVersionSetPickCompactionL0(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	f1 := testFile(0, 1, 1, "a", "c")
	f2 := testFile(0, 2, 1, "b", "e")
	f3 := testFile(0, 3, 1, "k", "l")
	f4 := testFile(0, 4, 1, "m", "n")
	v.files[0] = []*FileMetaData{f1, f2, f3, f4}

	g1 := testFile(1, 11, 1, "a", "d")
	g2 := testFile(1, 12, 1, "x", "z")
	v.files[1] = []*FileMetaData{g1, g2}

	vs.Finalize(v)
	vs.AppendVersion(v)

	c := vs.PickCompaction()
	defer c.Release()
	require.NotNil(t, c)
	require.Equal(t, Level(0), c.level)
	require.Equal(t, []FileNumber{1, 2}, []FileNumber{c.inputs[0][0].number, c.inputs[0][1].number})
	require.Equal(t, []FileNumber{11}, []FileNumber{c.inputs[1][0].number})
	require.Equal(t, f2.largest, vs.compactPointer[0])
}

func TestVersionSetPickCompactionUsesCompactPointer(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	f1 := testFile(1, 1, 1, "a", "b")
	f2 := testFile(1, 2, 1, "c", "d")
	f3 := testFile(1, 3, 1, "e", "f")
	v.files[1] = []*FileMetaData{f1, f2, f3}

	g1 := testFile(2, 21, 1, "c", "c")
	g2 := testFile(2, 22, 1, "d", "e")
	g3 := testFile(2, 23, 1, "x", "z")
	v.files[2] = []*FileMetaData{g1, g2, g3}

	v.compactionLevel = 1
	v.compactionScore = 1.1
	vs.AppendVersion(v)
	vs.compactPointer[1] = append([]byte(nil), f1.largest...)

	c := vs.PickCompaction()
	defer c.Release()
	require.NotNil(t, c)
	require.Equal(t, Level(1), c.level)
	require.Equal(t, []FileNumber{2}, []FileNumber{c.inputs[0][0].number})
	require.Equal(t, []FileNumber{21, 22}, []FileNumber{c.inputs[1][0].number, c.inputs[1][1].number})
	require.Equal(t, f2.largest, vs.compactPointer[1])
}

func TestVersionSetCompactPointerWrapsAround(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	f1 := testFile(1, 1, 1, "a", "b")
	f2 := testFile(1, 2, 1, "c", "d")
	f3 := testFile(1, 3, 1, "e", "f")
	v.files[1] = []*FileMetaData{f1, f2, f3}

	v.compactionLevel = 1
	v.compactionScore = 1.1
	vs.AppendVersion(v)
	// Set compactPointer past all files
	vs.compactPointer[1] = compactionInternalKey("z", 1)

	c := vs.PickCompaction()
	defer c.Release()
	require.NotNil(t, c)
	require.Equal(t, Level(1), c.level)
	// Should fall back to first file
	require.Equal(t, FileNumber(1), c.inputs[0][0].number)
}

func TestPickCompactionReturnsNilWhenNotNeeded(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	// Only 2 L0 files: score = 2/4 = 0.5 < 1
	v.files[0] = []*FileMetaData{
		testFile(0, 1, 1, "a", "b"),
		testFile(0, 2, 1, "c", "d"),
	}

	vs.Finalize(v)
	vs.AppendVersion(v)

	require.Less(t, v.compactionScore, float64(1))
	require.False(t, vs.NeedsCompaction())
	require.Nil(t, vs.PickCompaction())
}

func TestCompactionIsTrivial(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	f1 := testFile(1, 1, 1, "a", "b")
	v.files[1] = []*FileMetaData{f1}

	// No overlapping files in L2
	v.compactionLevel = 1
	v.compactionScore = 1.1
	vs.AppendVersion(v)

	c := vs.PickCompaction()
	defer c.Release()
	require.NotNil(t, c)
	require.Len(t, c.inputs[0], 1)
	require.Len(t, c.inputs[1], 0)
	require.True(t, c.IsTrivial())
}

func TestCompactionIsNotTrivialWithOverlap(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	f1 := testFile(1, 1, 1, "a", "c")
	v.files[1] = []*FileMetaData{f1}

	g1 := testFile(2, 11, 1, "b", "d")
	v.files[2] = []*FileMetaData{g1}

	v.compactionLevel = 1
	v.compactionScore = 1.1
	vs.AppendVersion(v)

	c := vs.PickCompaction()
	defer c.Release()
	require.NotNil(t, c)
	require.Len(t, c.inputs[0], 1)
	require.Len(t, c.inputs[1], 1)
	require.False(t, c.IsTrivial())
}

func TestAddBoundaryInputs(t *testing.T) {
	icmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}

	// f1 ends with user key "b" at seq 2, f2 starts with "b" at seq 1 (same user key, older)
	f1 := &FileMetaData{
		number:   1,
		smallest: compactionInternalKey("a", 3),
		largest:  compactionInternalKey("b", 2),
	}
	f2 := &FileMetaData{
		number:   2,
		smallest: compactionInternalKey("b", 1),
		largest:  compactionInternalKey("c", 1),
	}
	f3 := &FileMetaData{
		number:   3,
		smallest: compactionInternalKey("d", 1),
		largest:  compactionInternalKey("e", 1),
	}

	levelFiles := []*FileMetaData{f1, f2, f3}
	compactionFiles := []*FileMetaData{f1}

	addBoundaryInputs(icmp, levelFiles, &compactionFiles)

	// f2 should be added because its smallest has the same user key "b" as f1's largest
	require.Len(t, compactionFiles, 2)
	require.Equal(t, FileNumber(1), compactionFiles[0].number)
	require.Equal(t, FileNumber(2), compactionFiles[1].number)
}

func TestAddBoundaryInputsNoBoundary(t *testing.T) {
	icmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}

	f1 := &FileMetaData{
		number:   1,
		smallest: compactionInternalKey("a", 1),
		largest:  compactionInternalKey("b", 1),
	}
	f2 := &FileMetaData{
		number:   2,
		smallest: compactionInternalKey("c", 1),
		largest:  compactionInternalKey("d", 1),
	}

	levelFiles := []*FileMetaData{f1, f2}
	compactionFiles := []*FileMetaData{f1}

	addBoundaryInputs(icmp, levelFiles, &compactionFiles)

	// No boundary extension needed
	require.Len(t, compactionFiles, 1)
}

func TestGetOverlappingFilesL0Expands(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()

	// f1 and f2 overlap, f2 and f3 overlap → chain expansion
	f1 := testFile(0, 1, 1, "a", "c")
	f2 := testFile(0, 2, 1, "b", "e")
	f3 := testFile(0, 3, 1, "d", "g")
	f4 := testFile(0, 4, 1, "x", "z")
	v.files[0] = []*FileMetaData{f1, f2, f3, f4}

	vs.AppendVersion(v)

	// Start with range overlapping only f1
	result := v.getOverlappingFiles(0, compactionInternalKey("a", 1), compactionInternalKey("c", 1))

	// Should expand to include f2 (overlaps with a-c) and f3 (overlaps with expanded range a-e)
	require.Len(t, result, 3)
	nums := make([]FileNumber, len(result))
	for i, f := range result {
		nums[i] = f.number
	}
	require.ElementsMatch(t, []FileNumber{1, 2, 3}, nums)
}

func TestFinalizeExcludesLastLevel(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()
	// Put large files only in the last level (NumLevels-1 = 6)
	v.files[NumLevels-1] = []*FileMetaData{
		testFile(NumLevels-1, 1, 1000*1024*1024, "a", "z"),
	}

	vs.Finalize(v)

	// Last level should never be chosen as compaction source
	require.NotEqual(t, Level(NumLevels-1), v.compactionLevel)
	require.Less(t, v.compactionScore, float64(1))
}

func TestFinalizePicksHighestScoreLevel(t *testing.T) {
	vs := testVersionSetForCompaction()
	v := vs.NewVersion()

	// L0: 2 files → score = 2/4 = 0.5
	v.files[0] = []*FileMetaData{
		testFile(0, 1, 1, "a", "b"),
		testFile(0, 2, 1, "c", "d"),
	}

	// L1: 15MB → score = 15/10 = 1.5
	v.files[1] = []*FileMetaData{
		testFile(1, 10, 15*1024*1024, "a", "z"),
	}

	// L2: 50MB → score = 50/100 = 0.5
	v.files[2] = []*FileMetaData{
		testFile(2, 20, 50*1024*1024, "a", "z"),
	}

	vs.Finalize(v)

	require.Equal(t, Level(1), v.compactionLevel)
	require.InDelta(t, 1.5, v.compactionScore, 0.01)
}
