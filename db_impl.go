package goldb

import (
	"sync"
)

type dbImpl struct {
	dbname   string
	icmp     *InternalKeyComparator
	versions *VersionSet
	mem      *MemTable
	mu       sync.Mutex
}

func open(options *Options, dbname string) (DB, error) {
	icmp := &InternalKeyComparator{
		userCmp: options.Comparator,
	}
	vset := NewVersionSet(dbname)
	db := &dbImpl{
		dbname:   dbname,
		icmp:     icmp,
		versions: vset,
		mem:      NewMemTable(icmp),
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	return db, nil
}

func (db *dbImpl) Get(key []byte, options *ReadOptions) ([]byte, error) {
	var seq uint64
	if options != nil && options.Snapshot != nil {
		seq = options.Snapshot.seq
	} else {
		db.mu.Lock()
		seq = db.versions.GetLastSequence()
		db.mu.Unlock()
	}

	value, deleted, exist := db.mem.Get(seq, key)
	if exist {
		if deleted {
			return nil, ErrNotFound
		}
		return value, nil
	}

	return nil, ErrNotFound
}

func (db *dbImpl) Put(key []byte, value []byte, options WriteOptions) error {
	if key == nil {
		return ErrInvalidArgument
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	seq := db.versions.GetLastSequence() + 1
	db.mem.Put(seq, key, value)
	db.versions.SetLastSequence(seq)

	return nil
}

func (db *dbImpl) Write(batch *WriteBatch, options WriteOptions) error {
	panic("unimplemented")
}

func (db *dbImpl) Delete(key []byte, options WriteOptions) error {
	if key == nil {
		return ErrInvalidArgument
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	seq := db.versions.GetLastSequence() + 1
	db.mem.Delete(seq, key)
	db.versions.SetLastSequence(seq)

	return nil
}

func (db *dbImpl) NewIterator() (Iterator, error) {
	panic("unimplemented")
}

func (db *dbImpl) GetSnapshot() *Snapshot {
	s := &Snapshot{}

	db.mu.Lock()
	defer db.mu.Unlock()

	s.seq = db.versions.GetLastSequence()

	return s
}

func (*dbImpl) Close() error {
	return nil
}
