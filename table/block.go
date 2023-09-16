package table

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
)

var (
	ErrInvalidBlock = errors.New("invalid block")
	ErrInvalidEntry = errors.New("invalid entry")
)

type Block struct {
	contents []byte
}

func NewBlock(contents []byte) (*Block, error) {
	if len(contents) < 4 {
		return nil, ErrInvalidBlock
	}

	b := &Block{
		contents: contents,
	}

	maxRestartsAllowed := (len(contents) - 4) / 4
	if int(b.NumRestarts()) > maxRestartsAllowed {
		return nil, ErrInvalidBlock
	}

	return b, nil
}

func (b *Block) NumRestarts() uint32 {
	util.Assert(len(b.contents) >= 4)
	return binary.LittleEndian.Uint32(b.contents[len(b.contents)-4:])
}

func (b *Block) Data() []byte {
	arrLen := b.NumRestarts() * 4
	return b.contents[:len(b.contents)-4-int(arrLen)]
}

func (b *Block) RestartArray() []byte {
	arrLen := b.NumRestarts() * 4
	return b.contents[len(b.contents)-4-int(arrLen) : len(b.contents)-4]
}

func (b *Block) NewBlockIterator(cmp db.Comparator) *BlockIterator {
	data := b.Data()
	restartArray := b.RestartArray()
	numRestart := b.NumRestarts()
	return &BlockIterator{
		cmp: cmp,

		data:         data,
		restartArray: restartArray,
		numRestart:   numRestart,
		restartIndex: numRestart,

		curOffset: len(data),
	}
}

type BlockIterator struct {
	cmp db.Comparator

	data         []byte
	restartArray []byte
	numRestart   uint32
	restartIndex uint32

	// current entry key, owned
	key []byte
	// current entry key, reference to data
	value     []byte
	curOffset int
	curLength int

	err error
}

func (it *BlockIterator) Valid() bool {
	return it.curOffset < len(it.data)
}

func (it *BlockIterator) getRestartPoint(index uint32) int {
	return int(binary.LittleEndian.Uint32(it.restartArray[index*4:]))
}

func (it *BlockIterator) seekToRestartPoint(index uint32) {
	it.key = it.key[:0]
	it.restartIndex = index

	it.curOffset = it.getRestartPoint(index)
	it.curLength = 0
}

func (it *BlockIterator) courrptionError(err error) {
	it.curOffset = len(it.data)
	it.restartIndex = it.numRestart
	it.err = err
	it.key = it.key[:0]
	it.value = nil
}

func (it *BlockIterator) parseNextEntry() bool {
	it.curOffset = it.curOffset + it.curLength
	if it.curOffset >= len(it.data) {
		// no more entries
		it.restartIndex = it.numRestart
		return false
	}

	b := it.data[it.curOffset:]

	// decode entry
	shared, nonShared, valueLen, read, err := decodeBlockEntryLength(b)
	if err != nil {
		it.courrptionError(err)
		return false
	}
	it.curLength = read + int(nonShared) + int(valueLen)
	b = b[read:]

	it.key = it.key[:shared]
	it.key = append(it.key, b[:nonShared]...)
	it.value = b[nonShared : nonShared+valueLen]

	for it.restartIndex+1 < it.numRestart && it.getRestartPoint(it.restartIndex+1) < it.curOffset {
		it.restartIndex++
	}

	return true
}

func decodeBlockEntryLength(b []byte) (uint32, uint32, uint32, int, error) {
	if len(b) < 3 {
		return 0, 0, 0, 0, ErrInvalidEntry
	}

	shared := uint32(b[0])
	nonShared := uint32(b[1])
	valueLen := uint32(b[2])

	if (shared | nonShared | valueLen) < 128 {
		// all three lengths encoded in single byte
		return shared, nonShared, valueLen, 3, nil
	}

	vint, read := binary.Uvarint(b)
	if read <= 0 {
		return 0, 0, 0, 0, ErrInvalidEntry
	}
	shared = uint32(vint)
	b = b[read:]

	vint, read = binary.Uvarint(b)
	if read < 0 {
		return 0, 0, 0, 0, ErrInvalidEntry
	}
	nonShared = uint32(vint)
	b = b[read:]

	vint, read = binary.Uvarint(b)
	if read < 0 {
		return 0, 0, 0, 0, ErrInvalidEntry
	}
	valueLen = uint32(vint)

	return shared, nonShared, valueLen, 0, nil
}

func (it *BlockIterator) SeekToFirst() {
	it.seekToRestartPoint(0)
	it.parseNextEntry()
}

func (it *BlockIterator) Seek(target []byte) {
	left := uint32(0)
	right := it.numRestart - 1
	curKeyCompare := 0

	if it.Valid() {
		curKeyCompare = it.cmp.Compare(it.key, target)
		if curKeyCompare < 0 {
			left = it.restartIndex
		} else if curKeyCompare > 0 {
			right = it.restartIndex
		} else {
			return
		}
	}

	for left < right {
		mid := (left + right + 1) / 2
		regionOffset := it.getRestartPoint(mid)

		b := it.data[regionOffset:]
		shared, nonShared, _, read, err := decodeBlockEntryLength(b)
		if err != nil || shared != 0 {
			it.courrptionError(err)
			return
		}

		midKey := b[read : read+int(nonShared)]
		if it.cmp.Compare(midKey, target) < 0 {
			left = mid
		} else {
			right = mid - 1
		}
	}

	util.Assert(curKeyCompare == 0 || it.Valid())
	skipSeek := left == it.restartIndex && curKeyCompare < 0
	if !skipSeek {
		it.seekToRestartPoint(left)
	}

	for {
		if !it.parseNextEntry() {
			return
		}

		if it.cmp.Compare(it.key, target) >= 0 {
			return
		}
	}
}

func (it *BlockIterator) SeekToLast() {
	it.seekToRestartPoint(it.numRestart - 1)
	for it.parseNextEntry() && it.curOffset+it.curLength < len(it.data) {
		// keep skipping
	}
}

func (it *BlockIterator) Next() {
	util.Assert(it.Valid())
	it.parseNextEntry()
}

func (it *BlockIterator) Prev() {
	util.Assert(it.Valid())

	original := it.curOffset
	for it.getRestartPoint(it.restartIndex) >= original {
		if it.restartIndex == 0 {
			// no more entries
			it.curOffset = len(it.data)
			it.restartIndex = it.numRestart
			return
		}
		it.restartIndex--
	}

	it.seekToRestartPoint(it.restartIndex)
	for it.parseNextEntry() && it.curOffset+it.curLength < original {
		// loop until original entry
	}
}

func (it *BlockIterator) Key() []byte {
	util.Assert(it.Valid())
	return it.key
}

func (it *BlockIterator) Value() []byte {
	util.Assert(it.Valid())
	return it.value
}

func (it *BlockIterator) Error() error {
	return it.err
}

type BlockBuilder struct {
	lastKey         []byte
	restartPoints   []int
	restartInterval int
	restartCounter  int
	buf             []byte
	done            bool
}

func NewBlockBuilder(restartInterval int) *BlockBuilder {
	return &BlockBuilder{
		restartPoints:   []int{0}, // first restart point
		restartInterval: restartInterval,
	}
}

func (b *BlockBuilder) Add(key, value []byte) {
	util.Assert(!b.done)

	shared := 0
	if b.restartCounter < b.restartInterval {
		lim := util.MinInt(len(key), len(b.lastKey))
		for shared < lim && b.lastKey[shared] == key[shared] {
			shared++
		}
	} else {
		b.restartPoints = append(b.restartPoints, len(b.buf))
		b.restartCounter = 0
	}

	b.buf = binary.AppendUvarint(b.buf, uint64(shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(key)-shared))
	b.buf = binary.AppendUvarint(b.buf, uint64(len(value)))

	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)

	b.lastKey = b.lastKey[:shared]
	b.lastKey = append(b.lastKey, key[shared:]...)
	util.Assert(bytes.Equal(b.lastKey, key))
	b.restartCounter++
}

func (b *BlockBuilder) Finish() []byte {
	util.Assert(!b.done)
	for _, r := range b.restartPoints {
		b.buf = binary.LittleEndian.AppendUint32(b.buf, uint32(r))
	}
	b.buf = binary.LittleEndian.AppendUint32(b.buf, uint32(len(b.restartPoints)))
	b.done = true
	return b.buf
}

func (b *BlockBuilder) EstimatedSize() int {
	return len(b.buf) + len(b.restartPoints)*4 + 4
}

func (b *BlockBuilder) Reset() {
	b.lastKey = b.lastKey[:0]
	b.restartPoints = b.restartPoints[:0]
	b.restartPoints = append(b.restartPoints, 0) // first restart point
	b.restartCounter = 0
	b.buf = b.buf[:0]
	b.done = false
}

func (b *BlockBuilder) Empty() bool {
	return len(b.buf) == 0
}
