package table

import (
	"bytes"
	"encoding/binary"

	"github.com/ls4154/golsm/db"
)

const filterBaseLg = 11

type FilterBlockBuilder struct {
	policy        db.FilterPolicy
	keys          [][]byte
	result        []byte
	filterOffsets []uint32
}

func NewFilterBlockBuilder(policy db.FilterPolicy) *FilterBlockBuilder {
	return &FilterBlockBuilder{
		policy: policy,
	}
}

func (b *FilterBlockBuilder) StartBlock(blockOffset uint64) {
	filterIndex := int(blockOffset >> filterBaseLg)
	for filterIndex > len(b.filterOffsets) {
		b.generateFilter()
	}
}

func (b *FilterBlockBuilder) AddKey(key []byte) {
	b.keys = append(b.keys, bytes.Clone(key))
}

func (b *FilterBlockBuilder) Finish() []byte {
	if len(b.keys) > 0 {
		b.generateFilter()
	}

	arrayOffset := uint32(len(b.result))
	for _, off := range b.filterOffsets {
		b.result = binary.LittleEndian.AppendUint32(b.result, off)
	}
	b.result = binary.LittleEndian.AppendUint32(b.result, arrayOffset)
	b.result = append(b.result, byte(filterBaseLg))

	return b.result
}

func (b *FilterBlockBuilder) generateFilter() {
	if len(b.keys) == 0 {
		b.filterOffsets = append(b.filterOffsets, uint32(len(b.result)))
		return
	}

	b.filterOffsets = append(b.filterOffsets, uint32(len(b.result)))
	b.result = b.policy.AppendFilter(b.keys, b.result)
	for i := range b.keys {
		b.keys[i] = nil
	}
	b.keys = b.keys[:0]
}

type FilterBlockReader struct {
	policy      db.FilterPolicy
	data        []byte
	offsetStart uint32
	numOffsets  uint32
	baseLg      uint8
	valid       bool
}

func NewFilterBlockReader(policy db.FilterPolicy, contents []byte) *FilterBlockReader {
	r := &FilterBlockReader{
		policy: policy,
		data:   contents,
	}
	n := len(contents)
	if n < 5 {
		return r
	}

	r.baseLg = contents[n-1]
	r.offsetStart = binary.LittleEndian.Uint32(contents[n-5:])
	if r.offsetStart > uint32(n-5) {
		return r
	}

	offsetBytes := uint32(n-5) - r.offsetStart
	if offsetBytes%4 != 0 {
		return r
	}

	r.numOffsets = offsetBytes / 4
	r.valid = true
	return r
}

func (r *FilterBlockReader) KeyMightContain(blockOffset uint64, key []byte) bool {
	// Treat malformed filters as potential matches.
	if !r.valid {
		return true
	}

	index := blockOffset >> r.baseLg
	if index >= uint64(r.numOffsets) {
		return true
	}

	startPos := r.offsetStart + uint32(index)*4
	limitPos := startPos + 4

	start := binary.LittleEndian.Uint32(r.data[startPos:])
	limit := binary.LittleEndian.Uint32(r.data[limitPos:])

	if start == limit {
		return false
	}
	if start > limit || limit > r.offsetStart {
		return true
	}

	return r.policy.MightContain(key, r.data[start:limit])
}
