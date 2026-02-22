package util

import (
	"encoding/binary"
)

func VarintLength(x uint64) int {
	l := 1
	for x >= 0x80 {
		x >>= 7
		l++
	}
	return l
}

func AppendLengthPrefixedBytes(dest, value []byte) []byte {
	dest = binary.AppendUvarint(dest, uint64(len(value)))
	dest = append(dest, value...)
	return dest
}

func GetLengthPrefixedBytes(input []byte) ([]byte, int) {
	length, n := binary.Uvarint(input)
	if n <= 0 {
		return nil, 0
	}
	if len(input)-n < int(length) {
		return nil, 0
	}
	return input[n : n+int(length)], n + int(length)
}

func MinInt(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

const MaskDelta = 0xa282ead8

func MaskCRC32(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + MaskDelta
}

func UnmaskCRC32(masked uint32) uint32 {
	rot := masked - MaskDelta
	return (rot >> 17) | (rot << 15)
}
