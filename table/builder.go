package table

import (
	"encoding/binary"
	"fmt"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

type TableBuilder struct {
	cmp             db.Comparator
	blockSize       int
	compression     db.CompressionType
	restartInterval int

	file   db.WritableFile
	offset uint64

	block         *BlockBuilder
	indexBlock    *BlockBuilder
	filterBlock   *FilterBlockBuilder
	lastKey       []byte
	numEntries    uint64
	compressedBuf []byte

	// pendingIndexEntry is set after a data block is flushed. The index entry
	// is written on the next Add/Finish call, once the first key of the new block
	// is known, so FindShortestSeparator can shorten the index key.
	pendingIndexEntry bool
	pendingHandle     BlockHandle

	err    error
	closed bool
}

func NewTableBuilder(file db.WritableFile, cmp db.Comparator, blockSize int, compression db.CompressionType,
	restartInterval int, filterPolicy db.FilterPolicy,
) *TableBuilder {
	b := &TableBuilder{
		cmp:             cmp,
		blockSize:       blockSize,
		compression:     compression,
		restartInterval: restartInterval,

		file: file,

		block: NewBlockBuilder(restartInterval),
		// Index block uses restart interval 1 (no prefix compression)
		indexBlock: NewBlockBuilder(1),
	}
	if filterPolicy != nil {
		b.filterBlock = NewFilterBlockBuilder(filterPolicy)
		b.filterBlock.StartBlock(0)
	}
	return b
}

func (b *TableBuilder) Add(key, value []byte) {
	util.Assert(!b.closed)
	if b.err != nil {
		return
	}

	if b.pendingIndexEntry {
		util.Assert(b.block.Empty())
		b.cmp.FindShortestSeparator(&b.lastKey, key)
		buf := [BlockHandleMaxLength]byte{}
		h := b.pendingHandle.Append(buf[:0])
		b.indexBlock.Add(b.lastKey, h)
		b.pendingIndexEntry = false
	}

	if b.filterBlock != nil {
		b.filterBlock.AddKey(key)
	}

	b.lastKey = b.lastKey[:0]
	b.lastKey = append(b.lastKey, key...)
	b.numEntries++
	b.block.Add(key, value)

	if b.block.EstimatedSize() >= b.blockSize {
		b.Flush()
	}
}

func (b *TableBuilder) Flush() {
	util.Assert(!b.closed)
	if b.err != nil {
		return
	}
	if b.block.Empty() {
		return
	}

	b.writeBlock(b.block, &b.pendingHandle)
	if b.err != nil {
		return
	}
	b.pendingIndexEntry = true

	err := b.file.Flush()
	if err != nil {
		b.err = err
	}

	if b.filterBlock != nil {
		b.filterBlock.StartBlock(b.offset)
	}
}

// block format:
//	block data
//	compression type (uint8)
//	crc (uint32)

func (b *TableBuilder) writeBlock(block *BlockBuilder, handle *BlockHandle) {
	raw := block.Finish()

	var blockContents []byte
	compression := b.compression
	switch compression {
	case db.NoCompression:
		blockContents = raw
	case db.SnappyCompression:
		compressed := util.SnappyCompress(raw)
		// Only use compressed data if it is smaller by more than 12.5%.
		if len(compressed) < len(raw)-(len(raw)/8) {
			blockContents = compressed
		} else {
			blockContents = raw
			compression = db.NoCompression
		}
	default:
		util.Assert(false)
	}

	b.writeRawBlock(blockContents, compression, handle)
	block.Reset()
}

func (b *TableBuilder) writeRawBlock(contents []byte, compression db.CompressionType, handle *BlockHandle) {
	handle.Offset = b.offset
	handle.Size = uint64(len(contents))

	n, err := b.file.Write(contents)
	if err != nil {
		b.err = err
		return
	}
	if n < len(contents) {
		b.err = fmt.Errorf("write %d < %d", n, len(contents))
		return
	}

	trailer := [BlockTrailerSize]byte{}
	trailer[0] = byte(compression)

	h := util.NewCRC32C()
	h.Write(contents)
	h.Write(trailer[0:1])
	crc := h.Sum32()
	binary.LittleEndian.PutUint32(trailer[1:5], util.MaskCRC32(crc))

	n, err = b.file.Write(trailer[:])
	if err != nil {
		b.err = err
		return
	}
	if n < len(trailer) {
		b.err = fmt.Errorf("write %d < %d", n, len(contents))
		return
	}

	b.offset += uint64(len(contents) + BlockTrailerSize)
}

func (b *TableBuilder) Error() error {
	return b.err
}

func (b *TableBuilder) Finish() error {
	b.Flush()
	util.Assert(!b.closed)
	b.closed = true

	if b.err != nil {
		return b.err
	}

	var filterBlockHandle, metaindexBlockHandle, indexBlockHandle BlockHandle
	hasFilterBlock := b.filterBlock != nil
	if hasFilterBlock {
		filterContents := b.filterBlock.Finish()
		b.writeRawBlock(filterContents, db.NoCompression, &filterBlockHandle)
		if b.err != nil {
			return b.err
		}
	}

	// metaindex block uses restart interval 1 (same as LevelDB)
	metaIndexBlock := NewBlockBuilder(1)
	if hasFilterBlock {
		metaIndexBlock.Add([]byte("filter."+b.filterBlock.policy.Name()), filterBlockHandle.Append(nil))
	}
	b.writeBlock(metaIndexBlock, &metaindexBlockHandle)
	if b.err != nil {
		return b.err
	}

	// index block
	if b.pendingIndexEntry {
		util.Assert(b.block.Empty())
		b.cmp.FindShortSuccessor(&b.lastKey)
		var buf [BlockHandleMaxLength]byte
		encoded := b.pendingHandle.Append(buf[:0])
		b.indexBlock.Add(b.lastKey, encoded)
		b.pendingIndexEntry = false
	}
	b.writeBlock(b.indexBlock, &indexBlockHandle)
	if b.err != nil {
		return b.err
	}

	// footer
	footer := Footer{
		MetaindexHandle: metaindexBlockHandle,
		IndexHandle:     indexBlockHandle,
	}
	var buf [FooterLength]byte
	encoded := footer.Append(buf[:0])
	n, err := b.file.Write(encoded)
	if err != nil {
		b.err = err
		return b.err
	}
	if n < len(encoded) {
		b.err = fmt.Errorf("write %d < %d", n, len(encoded))
		return b.err
	}
	b.offset += uint64(len(encoded))

	err = b.file.Flush()
	if err != nil {
		b.err = err
		return b.err
	}

	return nil
}

func (b *TableBuilder) Abandon() {
	b.closed = true
}

func (b *TableBuilder) NumEntries() uint64 {
	return b.numEntries
}

func (b *TableBuilder) FileSize() uint64 {
	return uint64(b.offset)
}
