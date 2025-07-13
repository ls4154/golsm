package log

import (
	"bytes"
	"io"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	buf := new(bytes.Buffer)

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
	buf := new(bytes.Buffer)

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

func repeatedBytes(input []byte, n int) []byte {
	r := make([]byte, 0, len(input)*n)
	for i := 0; i < n; i++ {
		r = append(r, input...)
	}
	return r
}

func TestLogCRCChecksumMismatch(t *testing.T) {
	var buf bytes.Buffer
	writer := NewWriter(&buf)
	require.NoError(t, writer.AddRecord([]byte("test-log-record")))

	raw := buf.Bytes()
	raw[logHeaderSize] ^= 0x01 // corrupt payload to trigger CRC mismatch

	reader := NewReader(bytes.NewReader(raw))
	_, err := reader.ReadRecord()
	require.ErrorIs(t, err, db.ErrCorruption)
}
