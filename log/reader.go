package log

import (
	"errors"
	"fmt"
	"io"

	"github.com/ls4154/golsm/db"
)

type LogReader struct {
	src          io.Reader
	backingStore [logBlockSize]byte
	buf          []byte
	offset       int
	eof          bool
}

func NewLogReader(src io.Reader) *LogReader {
	r := &LogReader{
		src:    src,
		offset: 0,
	}

	return r
}

// TODO scratch
func (r *LogReader) ReadRecord() ([]byte, error) {
	var record []byte
	inFragmentedRecord := false
	for {
		fragment, recordType := r.readPhysicalRecord()

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

func (r *LogReader) readPhysicalRecord() ([]byte, logRecordType) {
	for {
		if len(r.buf) < logHeaderSize {
			if !r.eof {
				n, err := r.src.Read(r.backingStore[:])
				r.buf = r.backingStore[:n]
				if err != nil {
					if errors.Is(err, io.EOF) {
						r.eof = true
						return nil, logRecordEof
					} else {
						// TODO ??
						panic(err)
					}
				}
				if n < logBlockSize {
					r.eof = true
				}
				continue
			} else {
				return nil, logRecordEof
			}
		}

		length := int(r.buf[4]) | (int(r.buf[5]) << 8)
		if logHeaderSize+length > len(r.buf) {
			if !r.eof {
				return nil, logRecordBad
			}
			return nil, logRecordEof
		}

		t := logRecordType(r.buf[6])

		if t == logRecordZero && length == 0 {
			return nil, logRecordBad
		}

		// TODO crc

		result := r.buf[logHeaderSize : logHeaderSize+length]
		r.buf = r.buf[logHeaderSize+length:]

		return result, t
	}
}
