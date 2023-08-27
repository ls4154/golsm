package env

import (
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
	Sync() error
	// TODO Flush
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
	return f, nil
}

func (e *GenericEnv) NewAppendableFile(name string) (WritableFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (e *GenericEnv) RemoveFile(name string) error {
	return os.Remove(name)
}

func (e *GenericEnv) RenameFile(src, target string) error {
	return os.Rename(src, target)
}

func (e *GenericEnv) FileExists(name string) bool {
	_, err := os.Stat(name)
	if err != nil {
		return false
	}
	return true
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
