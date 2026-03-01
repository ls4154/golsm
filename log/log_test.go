package log

import (
	"bytes"
	"io"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

type flushBuffer struct {
	bytes.Buffer
}

func (b *flushBuffer) Flush() error {
	return nil
}

func TestLog(t *testing.T) {
	buf := new(flushBuffer)

	records := [][]byte{
		[]byte("abc"),
		[]byte("xyz"),
		[]byte("12345678"),
		[]byte(""),
	}

	writer := NewWriter(buf)

	for _, r := range records {
		err := writer.AddRecord(r)
		require.NoError(t, err)
	}

	reader := NewReader(buf)

	for _, r := range records {
		record, err := reader.ReadRecord()
		require.NoError(t, err)
		require.Equal(t, r, record)
	}

	_, err := reader.ReadRecord()
	require.ErrorIs(t, err, io.EOF)
}

func TestLogFragmented(t *testing.T) {
	buf := new(flushBuffer)

	records := make([][]byte, 4)

	records[0] = repeatedBytes([]byte("xyz"), 10000)
	records[1] = repeatedBytes([]byte("abcde"), 10000)
	records[2] = repeatedBytes([]byte("xxxxxx"), 10000)
	records[3] = repeatedBytes([]byte("0123456789"), 10000)

	writer := NewWriter(buf)

	for _, r := range records {
		err := writer.AddRecord(r)
		require.NoError(t, err)
	}

	reader := NewReader(buf)

	for _, r := range records {
		record, err := reader.ReadRecord()
		require.NoError(t, err)
		require.Equal(t, r, record)
	}

	_, err := reader.ReadRecord()
	require.ErrorIs(t, err, io.EOF)
}

func TestLogReadRecordIntoReuseBuffer(t *testing.T) {
	buf := new(flushBuffer)
	records := [][]byte{
		[]byte("small-record"),
		repeatedBytes([]byte("fragmented"), 5000),
		[]byte("tail"),
	}

	writer := NewWriter(buf)
	for _, r := range records {
		err := writer.AddRecord(r)
		require.NoError(t, err)
	}

	reader := NewReader(buf)
	recordBuf := make([]byte, 0, 64)
	for _, r := range records {
		var err error
		recordBuf, err = reader.ReadRecordInto(recordBuf)
		require.NoError(t, err)
		require.Equal(t, r, recordBuf)
	}

	_, err := reader.ReadRecordInto(recordBuf)
	require.ErrorIs(t, err, io.EOF)
}

func repeatedBytes(input []byte, n int) []byte {
	r := make([]byte, 0, len(input)*n)
	for i := 0; i < n; i++ {
		r = append(r, input...)
	}
	return r
}

func TestLogCRCChecksumMismatch(t *testing.T) {
	var buf flushBuffer
	writer := NewWriter(&buf)
	require.NoError(t, writer.AddRecord([]byte("test-log-record")))

	raw := buf.Bytes()
	raw[logHeaderSize] ^= 0x01 // corrupt payload to trigger CRC mismatch

	reader := NewReader(bytes.NewReader(raw))
	_, err := reader.ReadRecord()
	require.ErrorIs(t, err, db.ErrCorruption)
}

func TestWriterSizeTracksBytes(t *testing.T) {
	var buf flushBuffer
	writer := NewWriter(&buf)
	require.Equal(t, uint64(0), writer.Size())

	records := [][]byte{
		[]byte("a"),
		repeatedBytes([]byte("b"), 100),
		repeatedBytes([]byte("c"), 50000),
	}

	for _, r := range records {
		require.NoError(t, writer.AddRecord(r))
		require.Equal(t, uint64(buf.Len()), writer.Size())
	}
}

func TestWriterWithInitialOffsetTracksLogicalSize(t *testing.T) {
	var buf flushBuffer
	initial := uint64(logBlockSize - 3) // trailer zone: forces zero-padding before next record
	writer := NewWriterWithOffset(&buf, initial)
	require.Equal(t, initial, writer.Size())

	require.NoError(t, writer.AddRecord([]byte("x")))

	expectedNewBytes := 3 + logHeaderSize + 1
	require.Equal(t, expectedNewBytes, buf.Len())
	require.Equal(t, initial+uint64(expectedNewBytes), writer.Size())
	require.Equal(t, []byte{0, 0, 0}, buf.Bytes()[:3])
}
