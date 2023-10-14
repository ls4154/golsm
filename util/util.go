package util

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
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

func AssertFunc(fn func() bool) {
	if fn() {
		return
	}
	pc, file, no, ok := runtime.Caller(1)
	if ok {
		name := runtime.FuncForPC(pc).Name()
		panic(fmt.Sprintf("assertion failed (%s:%d:%s)", file, no, name))
	}
	panic("assertion failed")
}

func AssertMutexHeld(mu *sync.Mutex) {
	if mu.TryLock() {
		mu.Unlock()
		pc, file, no, ok := runtime.Caller(1)
		if ok {
			name := runtime.FuncForPC(pc).Name()
			panic(fmt.Sprintf("assertion failed (%s:%d:%s): mutex not held", file, no, name))
		}
		panic("assertion failed: mutex not held")
	}
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
