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
	src          io.Reader
	backingStore [logBlockSize]byte // fixed read buffer
	buf          []byte             // unprocessed slice into backingStore
	offset       int
	eof          bool
	verifyCRC    bool
}

func NewReader(src io.Reader) *Reader {
	r := &Reader{
		src:       src,
		offset:    0,
		verifyCRC: true,
	}

	return r
}

// ReadRecord reassembles one logical WAL record from one or more
// physical fragments (FULL/FIRST/MIDDLE/LAST).
// TODO scratch
func (r *Reader) ReadRecord() ([]byte, error) {
	var record []byte
	inFragmentedRecord := false
	for {
		fragment, recordType, err := r.readPhysicalRecord()
		if err != nil {
			return nil, err
		}

		// TODO resyncing?

		switch recordType {
		case logRecordFull:
			if inFragmentedRecord {
				return nil, fmt.Errorf("%w: partial record without end", db.ErrCorruption)
			}
			record = fragment
			return record, nil
		case logRecordFirst:
			if inFragmentedRecord {
				return nil, fmt.Errorf("%w: partial record without end", db.ErrCorruption)
			}
			inFragmentedRecord = true
			record = make([]byte, len(fragment))
			copy(record, fragment)
		case logRecordMiddle:
			if !inFragmentedRecord {
				return nil, fmt.Errorf("%w: missing start of fragmented record", db.ErrCorruption)
			}
			record = append(record, fragment...)
		case logRecordLast:
			if !inFragmentedRecord {
				return nil, fmt.Errorf("%w: missing start of fragmented record", db.ErrCorruption)
			}
			record = append(record, fragment...)
			return record, nil
		case logRecordEof:
			return nil, io.EOF
		case logRecordBad:
			return nil, fmt.Errorf("%w: error in middle of record", db.ErrCorruption)
		default:
			return nil, fmt.Errorf("%w: unknown record type %d", db.ErrCorruption, recordType)
		}
	}
}

func (r *Reader) readPhysicalRecord() ([]byte, logRecordType, error) {
	for {
		if len(r.buf) < logHeaderSize {
			if !r.eof {
				n, err := r.src.Read(r.backingStore[:])
				if err != nil && !errors.Is(err, io.EOF) {
					return nil, logRecordBad, fmt.Errorf("%w: %s", db.ErrIO, err)
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
				return nil, logRecordBad, nil
			} else {
				// ignore truncated record at eof
				return nil, logRecordEof, nil
			}
		}

		t := logRecordType(r.buf[6])

		if t == logRecordZero && length == 0 {
			return nil, logRecordBad, nil
		}

		if r.verifyCRC {
			expectedCRC := util.UnmaskCRC32(binary.LittleEndian.Uint32(r.buf[0:]))
			actualCRC := util.ChecksumCRC32C(r.buf[6 : 6+1+length])
			if expectedCRC != actualCRC {
				return nil, logRecordBad, nil
			}
		}

		result := r.buf[logHeaderSize : logHeaderSize+length]
		r.buf = r.buf[logHeaderSize+length:]

		return result, t, nil
	}
}
