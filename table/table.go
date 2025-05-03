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
	cacheID    uint64
	blockCache *BlockCache
}

func (t *Table) Close() error {
	// file is not owned
	t.file = nil
	t.cmp = nil
	t.indexBlock = nil
	return nil
}

func (t *Table) GetCacheID() uint64 {
	return t.cacheID
}

func OpenTable(file db.RandomAccessFile, size uint64, cmp db.Comparator, bcache *BlockCache, cacheID uint64) (*Table, error) {
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
		cacheID:    cacheID,
		blockCache: bcache,
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

	var block *Block
	var blockRelease func()
	if t.blockCache != nil {
		bcacheKey := BuildBlockCacheKey(t.GetCacheID(), handle.Offset)
		block, blockRelease = t.blockCache.Lookup(bcacheKey)
		if block == nil {
			block, err = ReadBlock(t.file, &handle)
			if err != nil {
				return nil, err
			}
			blockRelease = t.blockCache.Insert(bcacheKey, block)
		}
	} else {
		block, err = ReadBlock(t.file, &handle)
		if err != nil {
			return nil, err
		}
	}

	var it db.Iterator = block.NewBlockIterator(t.cmp)
	if blockRelease != nil {
		it = util.NewCleanupIterator(it, blockRelease)
	}

	return it, nil
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

	var contents []byte

	compressionType := db.CompressionType(buf[handle.Size])
	switch compressionType {
	case db.NoCompression:
		contents = buf[:handle.Size]
	case db.SnappyCompression:
		uncompressed, err := util.SnappyUncompress(buf[:handle.Size])
		if err != nil {
			return nil, fmt.Errorf("%w: %s", db.ErrCorruption, err)
		}
		contents = uncompressed
	default:
		return nil, fmt.Errorf("%w: unknown compression type %d", db.ErrCorruption, compressionType)
	}

	return NewBlock(contents)
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
