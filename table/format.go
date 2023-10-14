package table

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ls4154/golsm/db"
)

const (
	MagicNumber      = 0xdb4775248b80fb57
	BlockTrailerSize = 5
)

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

func DecodeBlockHandle(buf []byte) (BlockHandle, int, error) {
	h := BlockHandle{}
	vint, n1 := binary.Uvarint(buf)
	if n1 <= 0 {
		return h, 0, errors.New("bad block handle")
	}
	h.Offset = vint

	vint, n2 := binary.Uvarint(buf[n1:])
	if n2 <= 0 {
		return h, 0, errors.New("bad block handle")
	}
	h.Size = vint

	return h, n1 + n2, nil
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

func DecodeFooter(buf []byte) (Footer, int, error) {
	if len(buf) < FooterLength {
		return Footer{}, 0, fmt.Errorf("%w: footer too short", db.ErrCorruption)
	}

	if binary.LittleEndian.Uint64(buf[len(buf)-8:]) != MagicNumber {
		return Footer{}, 0, fmt.Errorf("%w: magic number mismatch", db.ErrCorruption)
	}

	metaindexHandle, n1, err := DecodeBlockHandle(buf)
	if err != nil {
		return Footer{}, 0, err
	}
	indexHandle, n2, err := DecodeBlockHandle(buf[n1:])
	if err != nil {
		return Footer{}, 0, err
	}

	return Footer{
		MetaindexHandle: metaindexHandle,
		IndexHandle:     indexHandle,
	}, n1 + n2, nil
}
