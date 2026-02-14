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

func (c bytewiseComparator) FindShortestSeparator(start *[]byte, limit []byte) {
	minLength := MinInt(len(*start), len(limit))
	diffIndex := 0

	// find common prefix length
	for diffIndex < minLength && (*start)[diffIndex] == limit[diffIndex] {
		diffIndex++
	}

	if diffIndex >= minLength {
		return
	}

	// NOTE: This follows a conservative shortening rule.
	// More aggressive shortening is possible by handling boundary cases
	// (e.g. diffByte+1 == limit[diffIndex]) and scanning subsequent bytes
	// for a non-0xff increment point.
	diffByte := (*start)[diffIndex]
	if diffByte < 0xff && diffByte+1 < limit[diffIndex] {
		(*start)[diffIndex]++
		*start = (*start)[:diffIndex+1]
		Assert(c.Compare(*start, limit) < 0)
	}
}

func (bytewiseComparator) FindShortSuccessor(key *[]byte) {
	for i := 0; i < len(*key); i++ {
		if (*key)[i] != 0xff {
			(*key)[i] += 1
			*key = (*key)[:i+1]
			return
		}
	}
}
