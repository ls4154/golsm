package table

import (
	"encoding/binary"
	"errors"
)

const (
	BlockTrailerSize = 5
	MagicNumber      = 0xdb4775248b80fb57
)

type Table interface{}

const BlockHandleMaxLength = 10 + 10

type BlockHandle struct {
	Offset uint64
	Size   uint64
}

func (h BlockHandle) Append(buf []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(h.Offset))
	buf = binary.AppendUvarint(buf, uint64(h.Size))
	return buf
}

func DecodeBlockHandle(buf []byte) (BlockHandle, error) {
	h := BlockHandle{}
	vint, read := binary.Uvarint(buf)
	if read <= 0 {
		return h, errors.New("bad block handle")
	}
	h.Offset = vint

	vint, read = binary.Uvarint(buf)
	if read <= 0 {
		return h, errors.New("bad block handle")
	}
	h.Size = vint

	return h, nil
}

const FooterLength = 2*BlockHandleMaxLength + 8

type Footer struct {
	MetaindexHandle BlockHandle
	IndexHandle     BlockHandle
}

func (f Footer) Append(buf []byte) []byte {
	buf = f.MetaindexHandle.Append(buf)
	buf = f.IndexHandle.Append(buf)
	// padding
	for len(buf) < 2*BlockHandleMaxLength {
		buf = append(buf, 0)
	}
	buf = binary.LittleEndian.AppendUint64(buf, MagicNumber)
	return buf
}
