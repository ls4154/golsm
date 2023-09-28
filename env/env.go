package env

import (
	"bufio"
	"io"
	"os"
)

type Env interface {
	NewReadableFile(name string) (SequentialFile, error)
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

	LockFile(name string) (*FileLock, error)
	UnlockFile(lock *FileLock) error
}

type SequentialFile interface {
	io.Reader
	io.Closer
}

type RandomAccessFile interface {
	io.ReaderAt
	io.Closer
}

type WritableFile interface {
	io.Writer
	io.Closer
	Flush() error
	Sync() error
}

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

func (e *GenericEnv) NewReadableFile(name string) (SequentialFile, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (e *GenericEnv) NewRandomAccessFile(name string) (RandomAccessFile, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (e *GenericEnv) NewWritableFile(name string) (WritableFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return newBufWritableFile(f), nil
}

func (e *GenericEnv) NewAppendableFile(name string) (WritableFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return newBufWritableFile(f), nil
}

func (e *GenericEnv) RemoveFile(name string) error {
	return os.Remove(name)
}

func (e *GenericEnv) RenameFile(src, target string) error {
	return os.Rename(src, target)
}

func (e *GenericEnv) FileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (e *GenericEnv) GetFileSize(name string) (uint64, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), nil
}

func (e *GenericEnv) GetChildren(path string) ([]string, error) {
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

func (e *GenericEnv) CreateDir(name string) error {
	return os.Mkdir(name, 0o755)
}

func (e *GenericEnv) RemoveDir(name string) error {
	return os.Remove(name)
}

func (e *GenericEnv) LockFile(name string) (*FileLock, error) {
	// TODO
	return &FileLock{}, nil
}

func (e *GenericEnv) UnlockFile(lock *FileLock) error {
	// TODO
	return nil
}
