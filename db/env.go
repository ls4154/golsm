package db

import "io"

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

type FileLock interface{}
