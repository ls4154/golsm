package util

import (
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
