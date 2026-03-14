//go:build !linux

package fs

import "os"

func syncFile(f *os.File) error {
	return f.Sync()
}
