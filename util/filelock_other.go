//go:build !linux && !darwin

package util

import "os"

func lockFile(f *os.File) error {
	return nil
}

func unlockFile(f *os.File) error {
	return nil
}
