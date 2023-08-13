package log

const (
	logBlockSize = 32 * 1024

	// checksum(4B), length(2B), type(1B)
	logHeaderSize = 4 + 2 + 1
)

type logRecordType byte

const (
	logRecordZero   logRecordType = 0
	logRecordFull   logRecordType = 1
	logRecordFirst  logRecordType = 2
	logRecordMiddle logRecordType = 3
	logRecordLast   logRecordType = 4

	logRecordEof logRecordType = 5
	logRecordBad logRecordType = 6
)
