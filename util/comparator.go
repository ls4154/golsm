package util

import "bytes"

var BytewiseComparator = bytewiseComparator{}

type bytewiseComparator struct{}

func (bytewiseComparator) Compare(a []byte, b []byte) int {
	return bytes.Compare(a, b)
}

func (bytewiseComparator) Name() string {
	return "leveldb.BytewiseComparator"
}
