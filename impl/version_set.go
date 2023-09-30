package impl

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/log"
	"github.com/ls4154/golsm/util"
)

type VersionSet struct {
	dbname string

	nextFileNumber uint64
	lastSequence   uint64

	env db.Env
}

func NewVersionSet(dbname string, env db.Env) *VersionSet {
	vset := &VersionSet{
		dbname:         dbname,
		nextFileNumber: 2,
		lastSequence:   0,

		env: env,
	}
	return vset
}

func (vs *VersionSet) Recover() error {
	current, err := util.ReadFile(vs.env, CurrentFileName(vs.dbname))
	if err != nil {
		return err
	}

	if len(current) == 0 || current[len(current)-1] != '\n' {
		return fmt.Errorf("%w: CURRENT file does not end with newline", db.ErrCorruption)
	}
	current = current[:len(current)-1]

	dscname := vs.dbname + "/" + string(current)
	file, err := vs.env.NewSequentialFile(dscname)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return fmt.Errorf("%w: CURRENT points to a non-existent file", db.ErrCorruption)
		}
		return err
	}

	// TODO
	{
		reader := log.NewLogReader(file)

		for {
			record, err := reader.ReadRecord()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				panic("TODO")
			}

			// TODO edit decodefrom
		}
	}
}

func (vs *VersionSet) GetLastSequence() uint64 {
	return atomic.LoadUint64(&vs.lastSequence)
}

func (vs *VersionSet) SetLastSequence(seq uint64) {
	atomic.StoreUint64(&vs.lastSequence, seq)
}

func (vs *VersionSet) NewFileNumber() uint64 {
	n := vs.nextFileNumber
	vs.nextFileNumber++
	return n
}
