//go:build debug

package util

import (
	"fmt"
	"runtime"
	"sync"
)

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
