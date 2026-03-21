package impl

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/util"
	"github.com/stretchr/testify/require"
)

func sizedBatch(size int) *WriteBatchImpl {
	if size < writeBatchHeaderSize {
		size = writeBatchHeaderSize
	}
	return &WriteBatchImpl{rep: make([]byte, size)}
}

func TestWriteSerializerSeparatesSyncAndNonSyncWriters(t *testing.T) {
	ws := &writeSerializer{
		writerCh:  make(chan *writer, writerQueueSize),
		closedCh:  make(chan struct{}),
		tempBatch: NewWriteBatch(),
	}

	first := &writer{batch: sizedBatch(writeBatchHeaderSize + 16)}
	second := &writer{batch: sizedBatch(writeBatchHeaderSize + 16), sync: true}
	third := &writer{batch: sizedBatch(writeBatchHeaderSize + 16), sync: true}

	ws.writerCh <- first
	ws.writerCh <- second
	ws.writerCh <- third

	group := ws.fetchWriters()
	require.Len(t, group, 1)
	require.Same(t, first, group[0])
	require.Same(t, second, ws.lastWriter)

	group = ws.fetchWriters()
	require.Len(t, group, 2)
	require.Same(t, second, group[0])
	require.Same(t, third, group[1])
	require.Nil(t, ws.lastWriter)
}

func TestWriteSerializerSplitsBatchAtMaxBatchSize(t *testing.T) {
	ws := &writeSerializer{
		writerCh:  make(chan *writer, writerQueueSize),
		closedCh:  make(chan struct{}),
		tempBatch: NewWriteBatch(),
	}

	first := &writer{batch: sizedBatch(maxBatchSize)}
	second := &writer{batch: sizedBatch(writeBatchHeaderSize + 1)}

	ws.writerCh <- first
	ws.writerCh <- second

	group := ws.fetchWriters()
	require.Len(t, group, 1)
	require.Same(t, first, group[0])
	require.Same(t, second, ws.lastWriter)

	group = ws.fetchWriters()
	require.Len(t, group, 1)
	require.Same(t, second, group[0])
	require.Nil(t, ws.lastWriter)
}

func TestWriteFailsFastAfterBackgroundError(t *testing.T) {
	testDir := t.TempDir()
	ldb, err := Open(db.DefaultOptions(), testDir)
	require.NoError(t, err)
	defer ldb.Close()

	dbi := ldb.(*dbImpl)
	bgErr := errors.New("background failed")

	dbi.mu.Lock()
	dbi.RecordBackgroundError(bgErr)
	dbi.mu.Unlock()

	err = ldb.Put([]byte("key"), []byte("value"), nil)
	require.ErrorIs(t, err, bgErr)
}

func newMakeRoomTestDB() *dbImpl {
	opt := db.DefaultOptions()
	opt.Compaction = db.DefaultCompactionOptions(opt.WriteBufferSize)

	icmp := &InternalKeyComparator{userCmp: util.BytewiseComparator}
	versions := NewVersionSet("db", icmp, nil, nil, false, opt.MaxManifestFileSize, nopLogger{},
		newCompactionPolicy(opt.Compaction, opt.MaxFileSize))

	return &dbImpl{
		options:  opt,
		icmp:     icmp,
		versions: versions,
		mem:      NewMemTable(icmp),
		bgWork: &bgWork{
			flushDoneCh: make(chan struct{}, 1),
			compDoneCh:  make(chan struct{}, 1),
			closedCh:    make(chan struct{}),
		},
		logger: nopLogger{},
	}
}

func TestMakeRoomForWriteWaitsForFlushDone(t *testing.T) {
	d := newMakeRoomTestDB()
	d.options.WriteBufferSize = 1
	d.mem.Put(1, []byte("key"), bytes.Repeat([]byte("v"), 32))
	d.imm = NewMemTable(d.icmp)

	errCh := make(chan error, 1)
	go func() {
		d.mu.Lock()
		err := d.makeRoomForWrite(false)
		d.mu.Unlock()
		errCh <- err
	}()

	require.Never(t, func() bool {
		select {
		case err := <-errCh:
			require.NoError(t, err)
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, 5*time.Millisecond)

	d.mu.Lock()
	d.imm = nil
	d.options.WriteBufferSize = d.mem.ApproximateMemoryUsage() + 1
	d.mu.Unlock()

	d.bgWork.flushDoneCh <- struct{}{}

	require.Eventually(t, func() bool {
		select {
		case err := <-errCh:
			require.NoError(t, err)
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
}

func TestMakeRoomForWriteWaitsForCompactionDone(t *testing.T) {
	d := newMakeRoomTestDB()
	d.options.WriteBufferSize = 1
	d.mem.Put(1, []byte("key"), bytes.Repeat([]byte("v"), 32))
	d.versions.compaction.l0StopWritesTrigger = 1

	v := d.versions.NewVersion()
	v.files[0] = []*FileMetaData{
		testFile(0, 1, 1, "a", "a"),
	}
	d.versions.AppendVersion(v)

	errCh := make(chan error, 1)
	go func() {
		d.mu.Lock()
		err := d.makeRoomForWrite(false)
		d.mu.Unlock()
		errCh <- err
	}()

	require.Never(t, func() bool {
		select {
		case err := <-errCh:
			require.NoError(t, err)
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, 5*time.Millisecond)

	d.mu.Lock()
	d.options.WriteBufferSize = d.mem.ApproximateMemoryUsage() + 1
	d.mu.Unlock()

	d.bgWork.compDoneCh <- struct{}{}

	require.Eventually(t, func() bool {
		select {
		case err := <-errCh:
			require.NoError(t, err)
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
}
