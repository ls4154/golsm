package log

const (
	logBlockSize = 32 * 1024

	// Physical record header format:
	//   checksum(4B), length(2B), type(1B)
	logHeaderSize = 4 + 2 + 1
)

type logRecordType byte

const (
	// Physical types persisted in WAL.

	logRecordZero   logRecordType = 0
	logRecordFull   logRecordType = 1
	logRecordFirst  logRecordType = 2
	logRecordMiddle logRecordType = 3
	logRecordLast   logRecordType = 4

	// Reader-only sentinel values (not persisted on disk).

	logRecordEof logRecordType = 5
	logRecordBad logRecordType = 6
)
