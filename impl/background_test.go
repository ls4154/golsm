package impl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBGWorkWriterWaitForCompactionDoneUnblocksOnSignal(t *testing.T) {
	bg := &bgWork{
		compDoneCh: make(chan struct{}),
		closedCh:   make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		bg.writerWaitForCompactionDone()
		close(done)
	}()

	require.Never(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, 5*time.Millisecond)

	bg.compDoneCh <- struct{}{}

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
}

func TestBGWorkWriterWaitForCompactionDoneUnblocksOnClose(t *testing.T) {
	bg := &bgWork{
		compDoneCh: make(chan struct{}),
		closedCh:   make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		bg.writerWaitForCompactionDone()
		close(done)
	}()

	close(bg.closedCh)

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
}
