package goldb

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
	return vs.lastSequence
}

func (vs *VersionSet) SetLastSequence(seq uint64) {
	vs.lastSequence = seq
}
