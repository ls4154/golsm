package golsm

import "sync/atomic"

type VersionSet struct {
	dbname string

	lastSequence uint64
}

func NewVersionSet(dbname string) *VersionSet {
	vset := &VersionSet{
		dbname:       dbname,
		lastSequence: 0,
	}
	return vset
}

func (vs *VersionSet) GetLastSequence() uint64 {
	return atomic.LoadUint64(&vs.lastSequence)
}

func (vs *VersionSet) SetLastSequence(seq uint64) {
	atomic.StoreUint64(&vs.lastSequence, seq)
}
