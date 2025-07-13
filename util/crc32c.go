package util

import (
	"hash"
	"hash/crc32"
)

var crc32cTable *crc32.Table

func init() {
	crc32cTable = crc32.MakeTable(crc32.Castagnoli)
}

type crc32cHash struct {
	hash.Hash32
}

func NewCRC32C() hash.Hash32 {
	h := crc32.New(crc32cTable)
	return &crc32cHash{h}
}

func ChecksumCRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}
