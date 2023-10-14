package env

import (
	"bufio"
	"errors"
	"fmt"
	"os"

	"github.com/ls4154/golsm/db"
)

const writableFileBufferSize = 64 * 1024 // 64 KB

type bufWritableFile struct {
	f *os.File
	w *bufio.Writer
}

func newBufWritableFile(f *os.File) *bufWritableFile {
	return &bufWritableFile{
		f: f,
		w: bufio.NewWriterSize(f, writableFileBufferSize),
	}
}

func (b *bufWritableFile) Write(p []byte) (int, error) {
	return b.w.Write(p)
}

func (b *bufWritableFile) Flush() error {
	return b.w.Flush()
}

func (b *bufWritableFile) Sync() error {
	if err := b.w.Flush(); err != nil {
		return err
	}
	return b.f.Sync()
}

func (b *bufWritableFile) Close() error {
	err1 := b.w.Flush()
	err2 := b.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type FileLock struct{}

type GenericEnv struct{}

var globalEnv *GenericEnv

func init() {
	globalEnv = &GenericEnv{}
}

func DefaultEnv() *GenericEnv {
	return globalEnv
}

func (e *GenericEnv) NewSequentialFile(name string) (db.SequentialFile, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, osError(err)
	}
	return f, nil
}

func (e *GenericEnv) NewRandomAccessFile(name string) (db.RandomAccessFile, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, osError(err)
	}
	return f, nil
}

func (e *GenericEnv) NewWritableFile(name string) (db.WritableFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, osError(err)
	}
	return newBufWritableFile(f), nil
}

func (e *GenericEnv) NewAppendableFile(name string) (db.WritableFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, osError(err)
	}
	return newBufWritableFile(f), nil
}

func (e *GenericEnv) RemoveFile(name string) error {
	return osError(os.Remove(name))
}

func (e *GenericEnv) RenameFile(src, target string) error {
	return osError(os.Rename(src, target))
}

func (e *GenericEnv) FileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (e *GenericEnv) GetFileSize(name string) (uint64, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return 0, osError(err)
	}
	return uint64(stat.Size()), nil
}

func (e *GenericEnv) GetChildren(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, osError(err)
	}
	defer f.Close()

	dents, err := f.ReadDir(0)
	if err != nil {
		return nil, err
	}

	children := make([]string, 0, len(dents))
	for _, e := range dents {
		children = append(children, e.Name())
	}
	return children, nil
}

func (e *GenericEnv) CreateDir(name string) error {
	return osError(os.Mkdir(name, 0o755))
}

func (e *GenericEnv) RemoveDir(name string) error {
	return osError(os.Remove(name))
}

func (e *GenericEnv) LockFile(name string) (db.FileLock, error) {
	// TODO
	return &FileLock{}, nil
}

func (e *GenericEnv) UnlockFile(lock db.FileLock) error {
	// TODO
	return nil
}

func osError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return db.ErrNotFound
	}
	return fmt.Errorf("%w: %s", db.ErrIO, err)
}
