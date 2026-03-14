package fs

import (
	"bufio"
	"errors"
	stdfs "io/fs"
	"os"
)

var ErrNotExist = stdfs.ErrNotExist

func IsNotExist(err error) bool {
	return errors.Is(err, ErrNotExist)
}

type Env interface {
	NewSequentialFile(name string) (SequentialFile, error)
	NewRandomAccessFile(name string) (RandomAccessFile, error)
	NewWritableFile(name string) (WritableFile, error)
	NewAppendableFile(name string) (WritableFile, error)
	RemoveFile(name string) error
	RenameFile(src, target string) error
	FileExists(name string) bool
	GetFileSize(name string) (uint64, error)

	GetChildren(path string) ([]string, error)
	CreateDir(name string) error
	RemoveDir(name string) error

	LockFile(name string) (FileLock, error)
	UnlockFile(lock FileLock) error
}

type SequentialFile interface {
	Read(p []byte) (n int, err error)
	Close() error
}

type RandomAccessFile interface {
	ReadAt(p []byte, off int64) (n int, err error)
	Close() error
}

type WritableFile interface {
	Write(p []byte) (n int, err error)
	Close() error
	Flush() error
	Sync() error
}

type FileLock interface{}

type bufWritableFile struct {
	f *os.File
	w *bufio.Writer
}

const writableFileBufferSize = 64 * 1024 // 64 KB

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
	return syncFile(b.f)
}

func (b *bufWritableFile) Close() error {
	err1 := b.w.Flush()
	err2 := b.f.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type FileHandle struct {
	f *os.File
}

type OSEnv struct{}

var defaultEnv = &OSEnv{}

func Default() *OSEnv {
	return defaultEnv
}

func (e *OSEnv) NewSequentialFile(name string) (SequentialFile, error) {
	return os.Open(name)
}

func (e *OSEnv) NewRandomAccessFile(name string) (RandomAccessFile, error) {
	return os.Open(name)
}

func (e *OSEnv) NewWritableFile(name string) (WritableFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return newBufWritableFile(f), nil
}

func (e *OSEnv) NewAppendableFile(name string) (WritableFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return newBufWritableFile(f), nil
}

func (e *OSEnv) RemoveFile(name string) error {
	return os.Remove(name)
}

func (e *OSEnv) RenameFile(src, target string) error {
	return os.Rename(src, target)
}

func (e *OSEnv) FileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (e *OSEnv) GetFileSize(name string) (uint64, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), nil
}

func (e *OSEnv) GetChildren(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
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

func (e *OSEnv) CreateDir(name string) error {
	return os.Mkdir(name, 0o755)
}

func (e *OSEnv) RemoveDir(name string) error {
	return os.Remove(name)
}

func (e *OSEnv) LockFile(name string) (FileLock, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &FileHandle{f: f}, nil
}

func (e *OSEnv) UnlockFile(lock FileLock) error {
	fl := lock.(*FileHandle)
	if err := unlockFile(fl.f); err != nil {
		return err
	}
	return fl.f.Close()
}
