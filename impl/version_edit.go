package impl

import (
	"encoding/binary"
	"fmt"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

const (
	TagComparator     = 1
	TagLogNumber      = 2
	TagNextFileNumber = 3
	TagLastSequence   = 4
	TagCompactPointer = 5
	TagDeletedFile    = 6
	TagNewFile        = 7
	_                 = 8
	TagPrevLogNumber  = 9
)

type FileMetaData struct {
	number   FileNumber
	size     uint64
	smallest []byte
	largest  []byte
	level    Level
}

type DeletedFile struct {
	number FileNumber
	level  Level
}

type CompactPointer struct {
	level       Level
	internalKey []byte
}

type VersionEdit struct {
	comparator        string
	logNumber         FileNumber
	prevLogNumber     FileNumber
	nextFileNumber    FileNumber
	lastSequence      SequenceNumber
	hasComparator     bool
	hasLogNumber      bool
	hasPrevLogNumber  bool
	hasNextFileNumber bool
	hasLastSequence   bool

	compactPointers []CompactPointer
	deletedFiles    map[DeletedFile]struct{} // TODO use slice?
	newFiles        []FileMetaData
}

func (ve *VersionEdit) SetComparator(comparator string) {
	ve.comparator = comparator
	ve.hasComparator = true
}

func (ve *VersionEdit) SetLogNumber(logNumber FileNumber) {
	ve.logNumber = logNumber
	ve.hasLogNumber = true
}

func (ve *VersionEdit) SetPrevLogNumber(prevLogNumber FileNumber) {
	ve.prevLogNumber = prevLogNumber
	ve.hasPrevLogNumber = true
}

func (ve *VersionEdit) SetNextFileNumber(nextFileNumber FileNumber) {
	ve.nextFileNumber = nextFileNumber
	ve.hasNextFileNumber = true
}

func (ve *VersionEdit) SetLastSequence(lastSequence SequenceNumber) {
	ve.lastSequence = lastSequence
	ve.hasLastSequence = true
}

func (ve *VersionEdit) SetCompactPointer(level Level, internalKey []byte) {
	ve.compactPointers = append(ve.compactPointers, CompactPointer{
		level:       level,
		internalKey: append([]byte(nil), internalKey...),
	})
}

func (ve *VersionEdit) RemoveFile(number FileNumber, level Level) {
	if ve.deletedFiles == nil {
		ve.deletedFiles = make(map[DeletedFile]struct{})
	}
	ve.deletedFiles[DeletedFile{number: number, level: level}] = struct{}{}
}

func (ve *VersionEdit) AddFile(level Level, number FileNumber, size uint64, smallest, largest []byte) {
	ve.newFiles = append(ve.newFiles, FileMetaData{
		number:   number,
		size:     size,
		smallest: append([]byte(nil), smallest...),
		largest:  append([]byte(nil), largest...),
		level:    level,
	})
}

func (ve *VersionEdit) Append(dst []byte) []byte {
	if ve.hasComparator {
		dst = binary.AppendUvarint(dst, TagComparator)
		dst = util.AppendLengthPrefixedBytes(dst, []byte(ve.comparator))
	}
	if ve.hasLogNumber {
		dst = binary.AppendUvarint(dst, TagLogNumber)
		dst = binary.AppendUvarint(dst, uint64(ve.logNumber))
	}
	if ve.hasPrevLogNumber {
		dst = binary.AppendUvarint(dst, TagPrevLogNumber)
		dst = binary.AppendUvarint(dst, uint64(ve.prevLogNumber))
	}
	if ve.hasNextFileNumber {
		dst = binary.AppendUvarint(dst, TagNextFileNumber)
		dst = binary.AppendUvarint(dst, uint64(ve.nextFileNumber))
	}
	if ve.hasLastSequence {
		dst = binary.AppendUvarint(dst, TagLastSequence)
		dst = binary.AppendUvarint(dst, uint64(ve.lastSequence))
	}
	for _, c := range ve.compactPointers {
		dst = binary.AppendUvarint(dst, TagCompactPointer)
		dst = binary.AppendUvarint(dst, uint64(c.level))
		dst = util.AppendLengthPrefixedBytes(dst, c.internalKey)
	}
	for df := range ve.deletedFiles {
		dst = binary.AppendUvarint(dst, TagDeletedFile)
		dst = binary.AppendUvarint(dst, uint64(df.level))
		dst = binary.AppendUvarint(dst, uint64(df.number))
	}
	for _, f := range ve.newFiles {
		dst = binary.AppendUvarint(dst, TagNewFile)
		dst = binary.AppendUvarint(dst, uint64(f.level))
		dst = binary.AppendUvarint(dst, uint64(f.number))
		dst = binary.AppendUvarint(dst, f.size)
		dst = util.AppendLengthPrefixedBytes(dst, f.smallest)
		dst = util.AppendLengthPrefixedBytes(dst, f.largest)
	}
	return dst
}

func (ve *VersionEdit) DecodeFrom(src []byte) error {
	for len(src) > 0 {
		tag, n := binary.Uvarint(src)
		if n <= 0 {
			return fmt.Errorf("%w: corrupted tag", db.ErrCorruption)
		}
		src = src[n:]
		switch tag {
		case TagComparator:
			val, read := util.GetLengthPrefixedBytes(src)
			if read <= 0 {
				return fmt.Errorf("%w: corrupted comparator name", db.ErrCorruption)
			}
			ve.comparator = string(val)
			ve.hasComparator = true
			src = src[read:]
		case TagLogNumber:
			val, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted log number", db.ErrCorruption)
			}
			ve.logNumber = FileNumber(val)
			ve.hasLogNumber = true
			src = src[n:]
		case TagPrevLogNumber:
			val, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted prev log number", db.ErrCorruption)
			}
			ve.prevLogNumber = FileNumber(val)
			ve.hasPrevLogNumber = true
			src = src[n:]
		case TagNextFileNumber:
			val, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted next file number", db.ErrCorruption)
			}
			ve.nextFileNumber = FileNumber(val)
			ve.hasNextFileNumber = true
			src = src[n:]
		case TagLastSequence:
			val, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted last sequence", db.ErrCorruption)
			}
			ve.lastSequence = SequenceNumber(val)
			ve.hasLastSequence = true
			src = src[n:]
		case TagCompactPointer:
			level, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted compact pointer", db.ErrCorruption)
			}
			src = src[n:]
			internalKey, read := util.GetLengthPrefixedBytes(src)
			if read <= 0 {
				return fmt.Errorf("%w: corrupted internal key", db.ErrCorruption)
			}
			src = src[read:]
			ve.compactPointers = append(ve.compactPointers, CompactPointer{
				level:       Level(level),
				internalKey: internalKey,
			})
		case TagDeletedFile:
			level, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted deleted file level", db.ErrCorruption)
			}
			src = src[n:]
			number, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted deleted file number", db.ErrCorruption)
			}
			src = src[n:]
			if ve.deletedFiles == nil {
				ve.deletedFiles = make(map[DeletedFile]struct{})
			}
			ve.deletedFiles[DeletedFile{number: FileNumber(number), level: Level(level)}] = struct{}{}
		case TagNewFile:
			level, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted new file level", db.ErrCorruption)
			}
			src = src[n:]
			number, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted new file number", db.ErrCorruption)
			}
			src = src[n:]
			size, n := binary.Uvarint(src)
			if n <= 0 {
				return fmt.Errorf("%w: corrupted new file size", db.ErrCorruption)
			}
			src = src[n:]
			smallest, read := util.GetLengthPrefixedBytes(src)
			if read <= 0 {
				return fmt.Errorf("%w: corrupted new file smallest", db.ErrCorruption)
			}
			src = src[read:]
			largest, read := util.GetLengthPrefixedBytes(src)
			if read <= 0 {
				return fmt.Errorf("%w: corrupted new file largest", db.ErrCorruption)
			}
			src = src[read:]
			ve.newFiles = append(ve.newFiles, FileMetaData{
				number:   FileNumber(number),
				size:     size,
				smallest: append([]byte(nil), smallest...),
				largest:  append([]byte(nil), largest...),
				level:    Level(level),
			})
		default:
			return fmt.Errorf("%w: unknown tag %d", db.ErrCorruption, tag)
		}
	}
	return nil
}
