package util

import "github.com/ls4154/golsm/db"

type BloomFilterPolicy struct {
	bitsPerKey int
	k          int
}

func (b *BloomFilterPolicy) AppendFilter(keys [][]byte, dst []byte) []byte {
	bits := len(keys) * b.bitsPerKey
	if bits < 64 {
		bits = 64
	}

	bytes := (bits + 7) / 8
	bits = bytes * 8

	initLen := len(dst)
	dst = append(dst, make([]byte, bytes+1)...)

	filter := dst[initLen:]
	Assert(len(filter) == bytes+1)
	filter[bytes] = byte(b.k)

	for _, key := range keys {
		h := LDBHash(key, 0xbc9f1d34)
		delta := (h >> 17) | (h << 15)

		for i := 0; i < b.k; i++ {
			bitpos := h % uint32(bits)
			setBit(filter, bitpos)
			h += delta
		}
	}

	return dst
}

func (b *BloomFilterPolicy) MightContain(key []byte, filter []byte) bool {
	if len(filter) < 2 {
		return false
	}

	bits := (len(filter) - 1) * 8
	k := int(filter[len(filter)-1])

	if k > 30 {
		return true
	}

	h := LDBHash(key, 0xbc9f1d34)
	delta := (h >> 17) | (h << 15)

	for i := 0; i < k; i++ {
		bitpos := h % uint32(bits)
		if !isBitSet(filter, bitpos) {
			return false
		}
		h += delta
	}

	return true
}

func (b *BloomFilterPolicy) Name() string {
	return "leveldb.BuiltinBloomFilter2"
}

func NewBloomFilterPolicy(bitsPerKey int) db.FilterPolicy {
	k := int(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	return &BloomFilterPolicy{
		bitsPerKey: bitsPerKey,
		k:          k,
	}
}

func setBit(arr []byte, pos uint32) {
	arr[pos/8] |= (1 << (pos % 8))
}

func isBitSet(arr []byte, pos uint32) bool {
	return ((arr[pos/8] >> (pos % 8)) & 1) == 1
}
