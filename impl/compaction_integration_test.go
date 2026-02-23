package impl

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ls4154/golsm/db"
	"github.com/stretchr/testify/require"
)

type recordingLogger struct {
	mu   sync.Mutex
	logs []string
}

func (l *recordingLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, fmt.Sprintf(format, v...))
}

func (l *recordingLogger) Contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, msg := range l.logs {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

func forceFlushAndWait(t *testing.T, d *dbImpl) {
	t.Helper()

	d.mu.Lock()
	err := d.makeRoomForWrite(true)
	d.mu.Unlock()

	require.NoError(t, err)
	d.bgWork.writerWaitForFlushDone()
}

func TestBackgroundCompactionCoversOverlappingL0(t *testing.T) {
	testDir := t.TempDir()
	opt := db.DefaultOptions()
	opt.WriteBufferSize = 64 << 10
	opt.MaxFileSize = 1 << 20
	logs := &recordingLogger{}
	opt.Logger = logs

	ldb, err := Open(opt, testDir)
	require.NoError(t, err)
	defer ldb.Close()

	dbi := ldb.(*dbImpl)

	const rounds = 6
	const keyCount = 64

	for round := 0; round < rounds; round++ {
		val := []byte(fmt.Sprintf("value-%02d", round))
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("k%03d", i))
			require.NoError(t, ldb.Put(key, val, nil))
		}
		forceFlushAndWait(t, dbi)
	}

	dbi.scheduleCompaction()

	require.Eventually(t, func() bool {
		dbi.mu.Lock()
		defer dbi.mu.Unlock()
		return dbi.versions.NumLevelFiles(1) > 0
	}, 5*time.Second, 20*time.Millisecond)
	require.True(t, logs.Contains("Compacting"))

	expected := []byte(fmt.Sprintf("value-%02d", rounds-1))
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		got, getErr := ldb.Get(key, nil)
		require.NoError(t, getErr)
		require.Equal(t, expected, got)
	}
}
