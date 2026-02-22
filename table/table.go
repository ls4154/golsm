package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type Table struct {
	file db.RandomAccessFile
	cmp  db.Comparator

	indexBlock *Block
	filter     *FilterBlockReader
	cacheID    uint64
	blockCache *BlockCache
}

func (t *Table) Close() error {
	// file is not owned
	t.file = nil
	t.cmp = nil
	t.indexBlock = nil
	t.filter = nil
	return nil
}

func (t *Table) GetCacheID() uint64 {
	return t.cacheID
}

func OpenTable(file db.RandomAccessFile, size uint64, cmp db.Comparator, filterPolicy db.FilterPolicy,
	bcache *BlockCache, cacheID uint64,
) (*Table, error) {
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
	indexBlock, err := ReadBlock(file, &footer.IndexHandle, false)
	if err != nil {
		return nil, err
	}

	var filter *FilterBlockReader
	if filterPolicy != nil {
		metaindexBlock, metaErr := ReadBlock(file, &footer.MetaindexHandle, false)
		if metaErr == nil {
			metaIter := metaindexBlock.NewBlockIterator(util.BytewiseComparator)
			metaIter.Seek([]byte("filter." + filterPolicy.Name()))
			if metaIter.Valid() && string(metaIter.Key()) == "filter."+filterPolicy.Name() {
				fh, _, err := DecodeBlockHandle(metaIter.Value())
				if err == nil {
					filterData, err := readBlockContents(file, &fh, false)
					if err == nil {
						filter = NewFilterBlockReader(filterPolicy, filterData)
					}
				}
			}
			_ = metaIter.Close()
		}
	}

	return &Table{
		file: file,
		cmp:  cmp,

		indexBlock: indexBlock,
		filter:     filter,
		cacheID:    cacheID,
		blockCache: bcache,
	}, nil
}

func (t *Table) NewIterator(verifyChecksum bool) db.Iterator {
	return NewTwoLevelIterator(t.indexBlock.NewBlockIterator(t.cmp), func(indexValue []byte) (db.Iterator, error) {
		return t.newBlockIteratorFromIndex(indexValue, verifyChecksum)
	})
}

func (t *Table) newBlockIteratorFromIndex(indexValue []byte, verifyChecksum bool) (db.Iterator, error) {
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
			block, err = ReadBlock(t.file, &handle, verifyChecksum)
			if err != nil {
				return nil, err
			}
			blockRelease = t.blockCache.Insert(bcacheKey, block)
		}
	} else {
		block, err = ReadBlock(t.file, &handle, verifyChecksum)
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

func ReadBlock(f db.RandomAccessFile, handle *BlockHandle, verifyChecksum bool) (*Block, error) {
	contents, err := readBlockContents(f, handle, verifyChecksum)
	if err != nil {
		return nil, err
	}
	return NewBlock(contents)
}

func readBlockContents(f db.RandomAccessFile, handle *BlockHandle, verifyChecksum bool) ([]byte, error) {
	buf := make([]byte, handle.Size+BlockTrailerSize)
	rd, err := f.ReadAt(buf, int64(handle.Offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if rd != int(handle.Size+BlockTrailerSize) {
		return nil, fmt.Errorf("%w: truncated block read", db.ErrCorruption)
	}

	if verifyChecksum {
		crc := util.UnmaskCRC32(binary.LittleEndian.Uint32(buf[handle.Size+1:]))

		h := util.NewCRC32C()
		h.Write(buf[:handle.Size+1])
		actual := h.Sum32()

		if actual != crc {
			return nil, fmt.Errorf("%w: block checksum mismatch", db.ErrCorruption)
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

	return contents, nil
}

func (t *Table) InternalGet(key []byte, handleFn func(k, v []byte), verifyChecksum bool) error {
	indexIter := t.indexBlock.NewBlockIterator(t.cmp)

	indexIter.Seek(key)
	if indexIter.Valid() {
		indexValue := indexIter.Value()
		handle, _, err := DecodeBlockHandle(indexValue)
		if err != nil {
			return fmt.Errorf("%w: bad index block handle", db.ErrCorruption)
		}
		if t.filter != nil && !t.filter.KeyMightContain(handle.Offset, key) {
			return nil
		}

		blockIter, err := t.newBlockIteratorFromIndex(indexValue, verifyChecksum)
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
