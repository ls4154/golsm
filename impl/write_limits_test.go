package impl

import (
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

func TestValidateWriteLength(t *testing.T) {
	require.NoError(t, validateWriteLength("key", maxUserKeyLength, maxUserKeyLength))
	require.NoError(t, validateWriteLength("value", maxValueLength, maxValueLength))

	err := validateWriteLength("key", maxUserKeyLength+1, maxUserKeyLength)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
	require.ErrorContains(t, err, "key is too large")

	err = validateWriteLength("value", maxValueLength+1, maxValueLength)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
	require.ErrorContains(t, err, "value is too large")
}

func TestWriteBatchPutRejectsOversizedKey(t *testing.T) {
	batch := NewWriteBatch()

	err := batch.Put(make([]byte, int(maxUserKeyLength+1)), []byte("x"))
	require.ErrorIs(t, err, db.ErrInvalidArgument)
	require.ErrorContains(t, err, "key is too large")
}

func TestWriteBatchPutRejectsOversizedValue(t *testing.T) {
	batch := NewWriteBatch()

	err := batch.Put([]byte("k"), make([]byte, int(maxValueLength+1)))
	require.ErrorIs(t, err, db.ErrInvalidArgument)
	require.ErrorContains(t, err, "value is too large")
}

func TestWriteBatchDeleteRejectsOversizedKey(t *testing.T) {
	batch := NewWriteBatch()

	err := batch.Delete(make([]byte, int(maxUserKeyLength+1)))
	require.ErrorIs(t, err, db.ErrInvalidArgument)
	require.ErrorContains(t, err, "key is too large")
}

func setLargeBufferSentinels(buf []byte) {
	if len(buf) == 0 {
		return
	}
	buf[0] = 0x11
	buf[len(buf)/2] = 0x77
	buf[len(buf)-1] = 0xee
}

func requireLargeBufferSentinels(t *testing.T, buf []byte) {
	t.Helper()
	require.NotEmpty(t, buf)
	require.Equal(t, byte(0x11), buf[0])
	require.Equal(t, byte(0x77), buf[len(buf)/2])
	require.Equal(t, byte(0xee), buf[len(buf)-1])
}

func requireIterKeyValue(t *testing.T, ldb db.DB, seekKey, wantValue []byte) {
	t.Helper()

	it, err := ldb.NewIterator(nil)
	require.NoError(t, err)
	defer it.Close()

	it.Seek(seekKey)
	require.True(t, it.Valid())
	require.Equal(t, seekKey, it.Key())
	require.Equal(t, wantValue, it.Value())
	require.NoError(t, it.Error())
}

func requireIterValueSentinels(t *testing.T, ldb db.DB, seekKey []byte, valueLen int) {
	t.Helper()

	it, err := ldb.NewIterator(nil)
	require.NoError(t, err)
	defer it.Close()

	it.Seek(seekKey)
	require.True(t, it.Valid())
	require.Equal(t, seekKey, it.Key())
	require.Len(t, it.Value(), valueLen)
	requireLargeBufferSentinels(t, it.Value())
	require.NoError(t, it.Error())
}

func freeLargeBuffer[T ~[]byte](buf *T) {
	*buf = nil
	runtime.GC()
	debug.FreeOSMemory()
}

func TestManualMaxSizedKeyRoundTrip(t *testing.T) {
	t.Skip("manual large-memory test; remove Skip to run on a machine with ample RAM")

	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	key := make([]byte, int(maxUserKeyLength))
	setLargeBufferSentinels(key)
	defer freeLargeBuffer(&key)

	value := []byte("ok")
	require.NoError(t, ldb.Put(key, value, nil))

	got, err := ldb.Get(key, nil)
	require.NoError(t, err)
	require.Equal(t, value, got)
	requireIterKeyValue(t, ldb, key, value)
}

func TestManualOversizedKeyIsRejected(t *testing.T) {
	t.Skip("manual large-memory test; remove Skip to run on a machine with ample RAM")

	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	key := make([]byte, int(maxUserKeyLength+1))
	setLargeBufferSentinels(key)
	defer freeLargeBuffer(&key)

	err = ldb.Put(key, []byte("x"), nil)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
	require.ErrorContains(t, err, "key is too large")
}

func TestManualMaxSizedValueRoundTrip(t *testing.T) {
	t.Skip("manual large-memory test; remove Skip to run on a machine with ample RAM")

	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	value := make([]byte, int(maxValueLength))
	setLargeBufferSentinels(value)
	defer freeLargeBuffer(&value)

	require.NoError(t, ldb.Put([]byte("k"), value, nil))

	got, err := ldb.Get([]byte("k"), nil)
	require.NoError(t, err)
	require.Len(t, got, len(value))
	requireLargeBufferSentinels(t, got)
	requireIterValueSentinels(t, ldb, []byte("k"), len(value))
}

func TestManualOversizedValueIsRejected(t *testing.T) {
	t.Skip("manual large-memory test; remove Skip to run on a machine with ample RAM")

	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	value := make([]byte, int(maxValueLength+1))
	setLargeBufferSentinels(value)
	defer freeLargeBuffer(&value)

	err = ldb.Put([]byte("k"), value, nil)
	require.ErrorIs(t, err, db.ErrInvalidArgument)
	require.ErrorContains(t, err, "value is too large")
}
