package log

import (
	"errors"
	"io"
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

func (r *LogReader) ReadRecord() ([]byte, bool) {
	var record []byte
	inFragmentedRecord := false
	for {
		fragment, recordType := r.readPhysicalRecord()

		// TODO resyncing?

		switch recordType {
		case logRecordFull:
			if inFragmentedRecord {
				// TODO courruption
				panic("corruption")
			}
			record = fragment
			return record, true
		case logRecordFirst:
			if inFragmentedRecord {
				// TODO courruption
				panic("corruption")
			}
			inFragmentedRecord = true
			record = make([]byte, len(fragment))
			copy(record, fragment)
		case logRecordMiddle:
			if !inFragmentedRecord {
				// TODO courruption
				panic("corruption")
			}
			record = append(record, fragment...)
		case logRecordLast:
			if !inFragmentedRecord {
				// TODO courruption
				panic("corruption")
			}
			record = append(record, fragment...)
			return record, true
		case logRecordEof:
			return nil, false
		case logRecordBad:
			// TODO
			panic("corruption")
		default:
			// TODO
			panic("corruption")
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
