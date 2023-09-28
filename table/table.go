package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/env"
	"github.com/ls4154/golsm/util"
)

type Table struct {
	file env.RandomAccessFile
	cmp  db.Comparator

	indexBlock *Block
}

func OpenTable(file env.RandomAccessFile, size uint64, cmp db.Comparator) (*Table, error) {
	// Footer
	var buf [FooterLength]byte
	n, err := file.ReadAt(buf[:], int64(size)-FooterLength)
	if err != nil {
		return nil, err
	}
	if n < FooterLength {
		return nil, errors.New("truncated footer read")
	}
	footer, _, err := DecodeFooter(buf[:])
	if err != nil {
		return nil, err
	}

	// Index block
	indexBlock, err := ReadBlock(file, &footer.IndexHandle)
	if err != nil {
		return nil, err
	}

	// TODO filter block

	return &Table{
		file: file,
		cmp:  cmp,

		indexBlock: indexBlock,
	}, nil
}

func (t *Table) NewIterator() db.Iterator {
	return NewTwoLevelIterator(t.indexBlock.NewBlockIterator(t.cmp), t.NewBlockIteratorFromIndex)
}

func (t *Table) NewBlockIteratorFromIndex(indexValue []byte) (db.Iterator, error) {
	handle, _, err := DecodeBlockHandle(indexValue)
	if err != nil {
		return nil, err
	}
	block, err := ReadBlock(t.file, &handle)
	if err != nil {
		return nil, err
	}

	return block.NewBlockIterator(t.cmp), nil
}

func ReadBlock(f env.RandomAccessFile, handle *BlockHandle) (*Block, error) {
	buf := make([]byte, handle.Size+BlockTrailerSize)
	rd, err := f.ReadAt(buf, int64(handle.Offset))
	if err != nil {
		return nil, err
	}
	if rd != int(handle.Size+BlockTrailerSize) {
		return nil, errors.New("truncated block read")
	}

	// TODO verify checksum option
	if true {
		crc := util.UnmaskCRC32(binary.LittleEndian.Uint32(buf[handle.Size+1:]))

		h := crc32.NewIEEE()
		h.Write(buf[:handle.Size+1])
		actual := h.Sum32()

		if actual != crc {
			return nil, errors.New("block checksum mismatch")
		}
	}

	result := &Block{}

	compressionType := db.CompressionType(buf[handle.Size])
	switch compressionType {
	case db.NoCompression:
		result.contents = buf[:handle.Size]
	case db.SnappyCompression:
		panic("snappy compression not implemented yet")
	default:
		panic(fmt.Sprintf("invalid compression type: %d", compressionType))
	}

	return result, nil
}
