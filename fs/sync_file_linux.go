//go:build linux

package fs

import (
	"errors"
	"os"
	"syscall"
)

func syncFile(f *os.File) error {
	fd := int(f.Fd())
	return ignoreEINTR(func() error {
		return syscall.Fdatasync(fd)
	})
}

func ignoreEINTR(fn func() error) error {
	for {
		err := fn()
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}
