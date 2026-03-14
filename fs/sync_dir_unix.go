//go:build unix

package fs

import "os"

func (e *OSEnv) SyncDir(name string) error {
	// Sync the parent directory so file create/rename updates become durable.
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	return f.Sync()
}
