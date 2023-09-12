package golsm

import "bytes"

type Comparator interface {
	Compare(a, b []byte) int
	Name() string
	// TODO FindShortestSeparator(start []byte) []byte
}

var BytewiseComparator = bytewiseComparator{}

type bytewiseComparator struct{}

func (bytewiseComparator) Compare(a []byte, b []byte) int {
	return bytes.Compare(a, b)
}

func (bytewiseComparator) Name() string {
	return "leveldb.BytewiseComparator"
}
