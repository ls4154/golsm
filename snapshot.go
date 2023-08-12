package goldb

type Snapshot struct {
	seq uint64

	prev *Snapshot
	next *Snapshot
}

func (s *Snapshot) Release() {
	// TODO
}
