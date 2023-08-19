package util

import (
	"encoding/binary"
	"fmt"
	"runtime"
)

func VarintLength(x uint64) int {
	l := 1
	for x >= 0x80 {
		x >>= 7
		l++
	}
	return l
}

func Assert(cond bool) {
	if cond {
		return
	}
	pc, file, no, ok := runtime.Caller(1)
	if ok {
		name := runtime.FuncForPC(pc).Name()
		panic(fmt.Sprintf("assertion failed (%s:%d:%s)", file, no, name))
	}
	panic("assertion failed")
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
	return input[n : n+int(length)], n + int(length)
}
