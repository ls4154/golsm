package table

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type Table struct {
	file db.RandomAccessFile
	cmp  db.Comparator

	indexBlock *Block
}

func (t *Table) Close() error {
	// file is not owned
	t.file = nil
	t.cmp = nil
	t.indexBlock = nil
	return nil
}

func OpenTable(file db.RandomAccessFile, size uint64, cmp db.Comparator) (*Table, error) {
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

func ReadBlock(f db.RandomAccessFile, handle *BlockHandle) (*Block, error) {
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

		h := util.NewCRC32C()
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

func (t *Table) InternalGet(key []byte, handleFn func(k, v []byte)) error {
	indexIter := t.indexBlock.NewBlockIterator(t.cmp)

	indexIter.Seek(key)
	if indexIter.Valid() {
		indexValue := indexIter.Value()
		handle, _, err := DecodeBlockHandle(indexValue)
		if err != nil {
			return fmt.Errorf("%w: bad index block handle", db.ErrCorruption)
		}
		_ = handle
		// TODO filter block

		blockIter, err := t.NewBlockIteratorFromIndex(indexValue)
		if err != nil {
			return err
		}
		defer blockIter.Close()

		blockIter.Seek(key)
		if blockIter.Valid() {
			handleFn(blockIter.Key(), blockIter.Value())
		}
		if blockIter.Error() != nil {
			return blockIter.Error()
		}
	}
	if indexIter.Error() != nil {
		return indexIter.Error()
	}

	return nil
}
