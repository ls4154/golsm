package impl

type SnapshotList struct {
	head Snapshot
}

type Snapshot struct {
	seq uint64

	prev *Snapshot
	next *Snapshot
}

func (s *Snapshot) Release() {
	s.prev.next = s.next
	s.next.prev = s.prev
	s.next = nil
	s.prev = nil
}

func NewSnapshotList() *SnapshotList {
	l := &SnapshotList{}
	l.head.next = &l.head
	l.head.prev = &l.head
	return l
}

func (l *SnapshotList) NewSnapshot(seq uint64) *Snapshot {
	s := &Snapshot{
		seq: seq,
	}
	s.next = &l.head
	s.prev = l.head.prev
	s.prev.next = s
	s.next.prev = s
	return s
}

func (l *SnapshotList) Empty() bool {
	return l.head.next == &l.head
}

func (l *SnapshotList) Oldest() *Snapshot {
	return l.head.next
}
