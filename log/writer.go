package log

import (
	"encoding/binary"
	"io"

	"github.com/ls4154/golsm/util"
)

type Writer struct {
	dest        io.Writer
	blockOffset int
}

func NewWriter(dest io.Writer) *Writer {
	w := &Writer{
		dest:        dest,
		blockOffset: 0,
	}
	return w
}

var zeroArray [logHeaderSize]byte

func (w *Writer) AddRecord(data []byte) error {
	left := len(data)
	off := 0
	begin := true
	for begin || left > 0 {
		leftover := logBlockSize - w.blockOffset
		util.Assert(leftover >= 0)

		// fill zeroes and switch to a new block
		if leftover < logHeaderSize {
			n, err := w.dest.Write(zeroArray[:leftover])
			if err != nil {
				return err
			}
			util.Assert(n == leftover)
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

func (w *Writer) emitPhysicalRecord(t logRecordType, data []byte) error {
	util.Assert(w.blockOffset+logHeaderSize+len(data) <= logBlockSize)

	length := len(data)

	var buf [logHeaderSize]byte
	buf[4] = byte(length & 0xff)
	buf[5] = byte(length >> 8)
	buf[6] = byte(t)

	h := util.NewCRC32C()
	h.Write(buf[6:7])
	h.Write(data)
	crc := h.Sum32()
	binary.LittleEndian.PutUint32(buf[:], util.MaskCRC32(crc))

	n, err := w.dest.Write(buf[0:])
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
