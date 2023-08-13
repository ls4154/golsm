package log

import (
	"io"

	"github.com/ls4154/goldb/util"
)

type LogWriter struct {
	dest        io.Writer
	blockOffset int
}

func NewLogWriter(dest io.Writer) *LogWriter {
	w := &LogWriter{
		dest:        dest,
		blockOffset: 0,
	}
	return w
}

var zeroArray [logHeaderSize]byte

func (w *LogWriter) AddRecord(data []byte) error {
	left := len(data)
	off := 0
	begin := true
	for begin || left > 0 {
		leftover := logBlockSize - w.blockOffset
		util.Assert(leftover >= 0)

		// fill zeroes and switch to a new block
		if leftover < logHeaderSize {
			w.dest.Write(zeroArray[:leftover])
			w.blockOffset = 0
		}

		avail := logBlockSize - w.blockOffset - logHeaderSize
		fragmentLength := left
		if left > avail {
			fragmentLength = avail
		}

		end := left == fragmentLength
		var recordType logRecordType
		if begin && end {
			recordType = logRecordFull
		} else if begin {
			recordType = logRecordFirst
		} else if end {
			recordType = logRecordLast
		} else {
			recordType = logRecordMiddle
		}

		err := w.emitPhysicalRecord(recordType, data[off:off+fragmentLength])
		if err != nil {
			return err
		}

		off += fragmentLength
		left -= fragmentLength
		begin = false
	}
	return nil
}

func (w *LogWriter) emitPhysicalRecord(t logRecordType, data []byte) error {
	util.Assert(w.blockOffset+logHeaderSize+len(data) <= logBlockSize)

	length := len(data)

	var buf [logHeaderSize]byte
	buf[4] = byte(length & 0xff)
	buf[5] = byte(length >> 8)
	buf[6] = byte(t)

	// TODO crc

	n, err := w.dest.Write(buf[:])
	if err != nil {
		return err
	}
	util.Assert(n == logHeaderSize)
	n, err = w.dest.Write(data)
	if err != nil {
		return err
	}
	util.Assert(n == length)

	w.blockOffset += logHeaderSize + length

	return nil
}
