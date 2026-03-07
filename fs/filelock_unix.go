//go:build linux || darwin

package fs

import (
	"os"
	"syscall"
	"unsafe"
)

func lockFile(f *os.File) error {
	lock := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: 0, // SEEK_SET
		Start:  0,
		Len:    0, // 0 = whole file
	}
	_, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		f.Fd(),
		syscall.F_SETLK,
		uintptr(unsafe.Pointer(&lock)),
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func unlockFile(f *os.File) error {
	lock := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
	}
	_, _, errno := syscall.Syscall(
		syscall.SYS_FCNTL,
		f.Fd(),
		syscall.F_SETLK,
		uintptr(unsafe.Pointer(&lock)),
	)
	if errno != 0 {
		return errno
	}
	return nil
}
