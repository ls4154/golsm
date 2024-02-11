package impl

import "github.com/ls4154/golsm/db"

type cleanupIterator struct {
	db.Iterator
	cleanup func()
	closed  bool
}

func newCleanupIterator(iter db.Iterator, cleanup func()) db.Iterator {
	return &cleanupIterator{
		Iterator: iter,
		cleanup:  cleanup,
	}
}

func (it *cleanupIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true

	err := it.Iterator.Close()
	it.cleanup()
	return err
}
