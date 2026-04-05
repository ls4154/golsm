package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type Reader struct {
	src                  io.Reader
	backingStore         [logBlockSize]byte // fixed read buffer
	buf                  []byte             // unprocessed slice into backingStore
	offset               int
	eof                  bool
	verifyCRC            bool
	skipUntilRecordStart bool

	hasPendingPhysical bool
	pendingPhysical    []byte
	pendingType        logRecordType
	corruptionErr      error
}

func NewReader(src io.Reader) *Reader {
	r := &Reader{
		src:       src,
		offset:    0,
		verifyCRC: true,
	}

	return r
}

// ReadRecord returns one logical WAL record.
//
// The returned bytes are owned by the caller and are safe to modify and retain.
func (r *Reader) ReadRecord() ([]byte, error) {
	return r.ReadRecordInto(nil)
}

// ReadRecordInto decodes one logical WAL record into dst and returns it.
//
// dst's previous contents are discarded, returned slice may alias dst.
// If it returns db.ErrCorruption, the reader has already advanced past the
// corrupt data and may be called again to continue recovery.
func (r *Reader) ReadRecordInto(dst []byte) ([]byte, error) {
	record := dst[:0]
	if record == nil {
		record = make([]byte, 0)
	}
	// Assemble one logical record from FULL or FIRST/MIDDLE/LAST fragments.
	inFragmentedRecord := false
	for {
		fragment, recordType, err := r.readPhysicalRecord()
		if err != nil {
			return nil, err
		}

		if r.skipUntilRecordStart {
			switch recordType {
			case logRecordMiddle:
				continue
			case logRecordLast:
				r.skipUntilRecordStart = false
				continue
			default:
				r.skipUntilRecordStart = false
			}
		}

		switch recordType {
		case logRecordFull:
			if inFragmentedRecord {
				r.setPendingPhysical(fragment, logRecordFull)
				return nil, corruptionf("partial record without end")
			}
			record = append(record[:0], fragment...)
			return record, nil
		case logRecordFirst:
			if inFragmentedRecord {
				r.setPendingPhysical(fragment, logRecordFirst)
				return nil, corruptionf("partial record without end")
			}
			inFragmentedRecord = true
			record = append(record[:0], fragment...)
		case logRecordMiddle:
			if !inFragmentedRecord {
				r.skipUntilRecordStart = true
				return nil, corruptionf("missing start of fragmented record")
			}
			record = append(record, fragment...)
		case logRecordLast:
			if !inFragmentedRecord {
				r.skipUntilRecordStart = true
				return nil, corruptionf("missing start of fragmented record")
			}
			record = append(record, fragment...)
			return record, nil
		case logRecordEof:
			return nil, io.EOF
		case logRecordBad:
			if inFragmentedRecord {
				r.skipUntilRecordStart = true
			}
			return nil, r.takeCorruptionError()
		default:
			if inFragmentedRecord {
				r.skipUntilRecordStart = true
			}
			return nil, corruptionf("unknown record type %d", recordType)
		}
	}
}

func (r *Reader) readPhysicalRecord() ([]byte, logRecordType, error) {
	if r.hasPendingPhysical {
		fragment := r.pendingPhysical
		recordType := r.pendingType
		r.hasPendingPhysical = false
		r.pendingPhysical = nil
		r.pendingType = 0
		return fragment, recordType, nil
	}

	for {
		if len(r.buf) < logHeaderSize {
			if !r.eof {
				n, err := r.src.Read(r.backingStore[:])
				if err != nil && !errors.Is(err, io.EOF) {
					return nil, logRecordBad, util.WrapIOError(err, "read log block")
				}

				r.buf = r.backingStore[:n]
				if n < logBlockSize || errors.Is(err, io.EOF) {
					r.eof = true
				}
				continue
			} else {
				// ignore truncated header at eof
				return nil, logRecordEof, nil
			}
		}

		length := int(r.buf[4]) | (int(r.buf[5]) << 8)
		if logHeaderSize+length > len(r.buf) {
			if !r.eof {
				r.buf = nil
				r.corruptionErr = corruptionf("bad record length")
				return nil, logRecordBad, nil
			} else {
				// ignore truncated record at eof
				return nil, logRecordEof, nil
			}
		}

		t := logRecordType(r.buf[6])

		if r.verifyCRC {
			expectedCRC := util.UnmaskCRC32(binary.LittleEndian.Uint32(r.buf[0:]))
			actualCRC := util.ChecksumCRC32C(r.buf[6 : 6+1+length])
			if expectedCRC != actualCRC {
				r.buf = nil
				r.corruptionErr = corruptionf("checksum mismatch")
				return nil, logRecordBad, nil
			}
		}

		result := r.buf[logHeaderSize : logHeaderSize+length]
		r.buf = r.buf[logHeaderSize+length:]

		return result, t, nil
	}
}

func (r *Reader) setPendingPhysical(fragment []byte, recordType logRecordType) {
	r.pendingPhysical = append(r.pendingPhysical[:0], fragment...)
	r.pendingType = recordType
	r.hasPendingPhysical = true
}

func (r *Reader) takeCorruptionError() error {
	if r.corruptionErr == nil {
		return corruptionf("bad log record")
	}
	err := r.corruptionErr
	r.corruptionErr = nil
	return err
}

func corruptionf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", db.ErrCorruption, fmt.Sprintf(format, args...))
}
