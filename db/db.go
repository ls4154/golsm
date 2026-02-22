package db

import (
	"errors"
)

type DB interface {
	// Get returns the latest value for key.
	// It returns ErrNotFound if the key does not exist.
	// options may be nil.
	Get(key []byte, options *ReadOptions) ([]byte, error)
	// Put stores value for key.
	// If the key already exists, Put overwrites it.
	// options may be nil.
	Put(key, value []byte, options *WriteOptions) error
	// Delete removes key.
	// It is not an error if the key does not exist.
	// options may be nil.
	Delete(key []byte, options *WriteOptions) error
	// Write applies batch atomically.
	// options may be nil.
	Write(batch WriteBatch, options *WriteOptions) error
	// NewIterator creates an iterator over the logical DB view.
	// The returned iterator is initially invalid; call a Seek method first.
	// Call Close when done to release resources.
	// options may be nil.
	NewIterator(options *ReadOptions) (Iterator, error)
	// GetSnapshot returns a handle to the current DB state.
	// Pass it via ReadOptions.Snapshot for consistent reads.
	// The snapshot must be released by calling Release when no longer needed.
	GetSnapshot() Snapshot
	// Close shuts down the database and releases all remaining resources.
	// Callers must stop issuing new reads/writes and release all outstanding iterators/snapshots before calling Close.
	Close() error
}

type Iterator interface {
	// Valid reports whether the iterator is positioned at a valid entry.
	Valid() bool
	// SeekToFirst moves to the first entry.
	SeekToFirst()
	// SeekToLast moves to the last entry.
	SeekToLast()
	// Seek moves to the first entry with key >= target (by Comparator).
	Seek(target []byte)
	// Next moves to the next entry.
	Next()
	// Prev moves to the previous entry.
	Prev()
	// Key returns the current entry key.
	// It must be called only when Valid() is true.
	Key() []byte
	// Value returns the current entry value.
	// It must be called only when Valid() is true.
	Value() []byte
	// Error returns the terminal error seen during iteration, if any.
	Error() error
	// Close releases resources held by the iterator.
	Close() error
}

type Snapshot interface {
	// Release frees the snapshot reference.
	Release()
}

type CompressionType uint8

const (
	NoCompression CompressionType = iota
	SnappyCompression
)

type Options struct {
	// CreateIfMissing creates the DB on Open when it does not exist.
	CreateIfMissing bool
	// ErrorIfExists fails Open when the DB already exists.
	ErrorIfExists bool
	// ParanoidChecks enables stricter checksum/format verification.
	ParanoidChecks bool
	// BlockSize is the target SSTable data block size in bytes.
	BlockSize int
	// BlockRestartInterval is the number of keys between restart points in a block (for key prefix compression).
	BlockRestartInterval int
	// MaxFileSize is the target maximum SSTable file size in bytes.
	MaxFileSize uint64
	// WriteBufferSize is the memtable size limit in bytes.
	// Exceeding this limit triggers flush/compaction flow.
	WriteBufferSize int
	// MaxOpenFiles is the upper bound for table-cache/file-handle usage.
	MaxOpenFiles int
	// BlockCacheSize is the block-cache capacity in bytes.
	// Set to 0 to disable block caching.
	BlockCacheSize int
	// FilterPolicy controls filter-block creation/lookups (for example Bloom filters).
	// Pass util.NewBloomFilterPolicy(bitsPerKey) to enable Bloom filtering.
	// A common starting point is util.NewBloomFilterPolicy(10).
	FilterPolicy FilterPolicy
	// Compression sets the SSTable block compression mode.
	// DefaultOptions uses SnappyCompression.
	Compression CompressionType
	// Comparator defines user-key ordering/equality semantics.
	// If nil, util.BytewiseComparator is used.
	// For an existing DB, it must have the same Name() and ordering as before.
	// Changing its Name() breaks format compatibility; changing its ordering corrupts data.
	Comparator Comparator
	// Logger is the sink used for internal log output.
	// If nil, Open uses a default logger backed by "<dbname>/LOG".
	Logger Logger
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
		Compression:          SnappyCompression,
	}
}

// ReadOptions is optional; passing nil uses default read behavior.
// A nil options value is equivalent to a zero-value ReadOptions.
type ReadOptions struct {
	// Snapshot selects a point-in-time read view when non-nil.
	// The snapshot must come from the same DB and must not be released.
	Snapshot Snapshot
	// VerifyChecksum enables checksum verification during reads.
	VerifyChecksum bool
	// BypassCache skips populating the block cache with data read during this operation.
	// Useful for bulk scans that would otherwise evict more frequently accessed data.
	BypassCache bool
}

// WriteOptions is optional; passing nil uses default write behavior.
// A nil options value is equivalent to a zero-value WriteOptions.
type WriteOptions struct {
	// Sync fsyncs the WAL when true for stronger durability.
	// When false, writes can be faster but recent updates may be lost on crash.
	Sync bool
}

var (
	ErrNotFound        = errors.New("not found")
	ErrCorruption      = errors.New("corrupted")
	ErrNotSupported    = errors.New("not supported")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrIO              = errors.New("io error")
)
