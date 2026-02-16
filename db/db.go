package db

import (
	"errors"
)

type DB interface {
	Get(key []byte, options *ReadOptions) ([]byte, error)
	Put(key, value []byte, options *WriteOptions) error
	Delete(key []byte, options *WriteOptions) error
	Write(batch WriteBatch, options *WriteOptions) error
	NewIterator(options *ReadOptions) (Iterator, error)
	GetSnapshot() Snapshot
	Close() error
}

type Iterator interface {
	Valid() bool
	SeekToFirst()
	SeekToLast()
	Seek(target []byte)
	Next()
	Prev()
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

type Snapshot interface {
	Release()
}

type CompressionType uint8

const (
	NoCompression CompressionType = iota
	SnappyCompression
)

type Options struct {
	CreateIfMissing      bool
	ErrorIfExists        bool
	BlockSize            int
	BlockRestartInterval int
	MaxFileSize          uint64
	WriteBufferSize      int
	MaxOpenFiles         int
	BlockCacheSize       int
	FilterPolicy         FilterPolicy
	Compression          CompressionType
	Comparator           Comparator
	Logger               Logger
}

func DefaultOptions() *Options {
	return &Options{
		CreateIfMissing:      true,
		ErrorIfExists:        false,
		BlockSize:            4 * 1024,
		BlockRestartInterval: 16,
		MaxFileSize:          4 * 1024 * 1024,
		WriteBufferSize:      4 * 1024 * 1024,
		MaxOpenFiles:         1000,
		BlockCacheSize:       64 << 20,
		Compression:          NoCompression,
	}
}

type ReadOptions struct {
	Snapshot       Snapshot
	VerifyChecksum bool
	BypassCache    bool
}

type WriteOptions struct {
	Sync bool
}

var (
	ErrNotFound        = errors.New("not found")
	ErrCorruption      = errors.New("corrupted")
	ErrNotSupported    = errors.New("not supported")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrIO              = errors.New("io error")
)
