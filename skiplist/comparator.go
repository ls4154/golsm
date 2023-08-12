package skiplist

import (
	"bytes"
	"encoding/binary"
)

type Comparator interface {
	Compare(a []byte, b []byte) int
}

var ByteComparator byteComparator

type byteComparator struct{}

func (byteComparator) Compare(a []byte, b []byte) int {
	return bytes.Compare(a, b)
}

var Uint64Comparator uint64Comparator

type uint64Comparator struct{}

func (uint64Comparator) Compare(a []byte, b []byte) int {
	numA := binary.NativeEndian.Uint64(a)
	numB := binary.NativeEndian.Uint64(b)

	if numA < numB {
		return -1
	} else if numA == numB {
		return 0
	} else {
		return 1
	}
}
