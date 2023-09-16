package db

import (
	"errors"

	"github.com/ls4154/golsm/util"
)

type DB interface {
	Get(key []byte, options *ReadOptions) ([]byte, error)
	Put(key, value []byte, options WriteOptions) error
	Delete(key []byte, options WriteOptions) error
	Write(batch WriteBatch, options WriteOptions) error
	NewIterator() (Iterator, error)
	GetSnapshot() Snapshot
	ReleaseSnapshot(snap Snapshot)
	Close() error
}

type Iterator interface {
	Valid() bool
	Next()
	Prev()
	SeekToFirst()
	SeekToLast()
	Seek(target []byte)
	Key() []byte
	Value() []byte
}

type Snapshot interface {
	Snapshot()
}

type CompressionType uint8

const (
	NoCompression CompressionType = iota
	SnappyCompression
)

type Options struct {
	BlockSize       int
	MaxFileSize     int
	WriteBufferSize int
	Compression     CompressionType
	Comparator      Comparator
}

type ReadOptions struct {
	VerifyChecksum bool
	BypassCache    bool
	Snapshot       Snapshot
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

func DefaultOptions() *Options {
	return &Options{
		BlockSize:       4 * 1024,
		MaxFileSize:     4 * 1024 * 1024,
		WriteBufferSize: 4 * 1024 * 1024,
		Compression:     SnappyCompression,
		Comparator:      util.BytewiseComparator,
	}
}
