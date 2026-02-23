package skiplist

import (
	"unsafe"

	"github.com/ls4154/golsm/util"
)

type Arena struct {
	blocks       [][]byte
	currentBlock []byte
	allocPos     int
	bytesLeft    int
	blockSize    int
	memUsage     int
}

func NewArena(blockSize int) *Arena {
	util.Assert(blockSize > 0)
	return &Arena{
		blockSize: blockSize,
	}
}

func (a *Arena) Allocate(bytes int) []byte {
	if a.bytesLeft < bytes {
		return a.allocateFallBack(bytes)
	}

	m := a.currentBlock[a.allocPos : a.allocPos+bytes]
	a.allocPos += bytes
	a.bytesLeft -= bytes
	return m
}

const (
	wordSize = int(unsafe.Sizeof(uintptr(0)))
	align    = int(unsafe.Alignof(uintptr(0)))
)

func (a *Arena) AllocateAligned(bytes int) []byte {
	padSize := 0
	if mod := a.allocPos % align; mod != 0 {
		padSize = align - mod
	}

	needBytes := padSize + bytes
	if a.bytesLeft < needBytes {
		return a.allocateFallBack(bytes)
	}

	m := a.currentBlock[a.allocPos : a.allocPos+needBytes]
	a.allocPos += needBytes
	a.bytesLeft -= needBytes
	return m[padSize:]
}

func (a *Arena) allocateFallBack(bytes int) []byte {
	if bytes > a.blockSize/4 {
		return a.allocateNewBlock(bytes)
	}

	a.currentBlock = a.allocateNewBlock(a.blockSize)
	a.allocPos = 0
	a.bytesLeft = a.blockSize

	m := a.currentBlock[a.allocPos : a.allocPos+bytes]
	a.allocPos += bytes
	a.bytesLeft -= bytes
	return m
}

func (a *Arena) allocateNewBlock(bytes int) []byte {
	words := divRoundUp(bytes, wordSize)
	// Allocate as []uintptr to guarantee pointer alignment, then cast to []byte.
	mem := make([]uintptr, words)
	block := unsafe.Slice((*byte)(unsafe.Pointer(&mem[0])), bytes)

	util.Assert(uintptr(unsafe.Pointer(&block[0]))%uintptr(align) == 0)

	a.blocks = append(a.blocks, block)
	a.memUsage += words * wordSize
	return block
}

func divRoundUp(n, d int) int {
	return (n + d - 1) / d
}

func (a *Arena) MemoryUsage() int {
	return a.memUsage
}

func bytesToPtr(m []byte) unsafe.Pointer {
	return unsafe.Pointer(&m[0])
}
