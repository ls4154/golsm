package impl

import "github.com/ls4154/golsm/db"

type mergingIterator struct {
	cmp      *InternalKeyComparator
	children []db.Iterator
	current  db.Iterator
	dir      iterDirection
}

func newMergingIterator(cmp *InternalKeyComparator, children []db.Iterator) *mergingIterator {
	return &mergingIterator{
		cmp:      cmp,
		children: children,
		dir:      directionForward,
	}
}

func (it *mergingIterator) Valid() bool {
	return it.current != nil && it.current.Valid()
}

func (it *mergingIterator) SeekToFirst() {
	for _, child := range it.children {
		child.SeekToFirst()
	}
	it.dir = directionForward
	it.findSmallest()
}

func (it *mergingIterator) SeekToLast() {
	for _, child := range it.children {
		child.SeekToLast()
	}
	it.dir = directionReverse
	it.findLargest()
}

func (it *mergingIterator) Seek(target []byte) {
	for _, child := range it.children {
		child.Seek(target)
	}
	it.dir = directionForward
	it.findSmallest()
}

func (it *mergingIterator) Next() {
	if !it.Valid() {
		return
	}

	if it.dir != directionForward {
		key := it.current.Key()
		for _, child := range it.children {
			if child == it.current {
				continue
			}
			child.Seek(key)
			if child.Valid() && it.cmp.Compare(child.Key(), key) == 0 {
				child.Next()
			}
		}
		it.dir = directionForward
	}

	it.current.Next()
	it.findSmallest()
}

func (it *mergingIterator) Prev() {
	if !it.Valid() {
		return
	}

	if it.dir != directionReverse {
		key := it.current.Key()
		for _, child := range it.children {
			if child == it.current {
				continue
			}
			child.Seek(key)
			if child.Valid() {
				child.Prev()
			} else {
				child.SeekToLast()
			}
		}
		it.dir = directionReverse
	}

	it.current.Prev()
	it.findLargest()
}

func (it *mergingIterator) Key() []byte {
	return it.current.Key()
}

func (it *mergingIterator) Value() []byte {
	return it.current.Value()
}

func (it *mergingIterator) Error() error {
	if it.current != nil && it.current.Error() != nil {
		return it.current.Error()
	}
	for _, child := range it.children {
		if err := child.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (it *mergingIterator) Close() error {
	var err error
	for _, child := range it.children {
		if cerr := child.Close(); err == nil {
			err = cerr
		}
	}
	it.children = nil
	it.current = nil
	return err
}

func (it *mergingIterator) findSmallest() {
	var smallest db.Iterator
	for _, child := range it.children {
		if !child.Valid() {
			continue
		}
		if smallest == nil || it.cmp.Compare(child.Key(), smallest.Key()) < 0 {
			smallest = child
		}
	}
	it.current = smallest
}

func (it *mergingIterator) findLargest() {
	var largest db.Iterator
	for _, child := range it.children {
		if !child.Valid() {
			continue
		}
		if largest == nil || it.cmp.Compare(child.Key(), largest.Key()) > 0 {
			largest = child
		}
	}
	it.current = largest
}
