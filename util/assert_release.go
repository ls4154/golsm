//go:build !debug

package util

import "sync"

func Assert(_ bool) {}

func AssertFunc(_ func() bool) {}

func AssertMutexHeld(_ *sync.Mutex) {}
