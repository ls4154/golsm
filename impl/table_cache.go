package impl

import (
	"sync"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/table"
)

type TableCache struct {
	dbname  string
	options *db.Options
	env     db.Env
	cmp     db.Comparator

	tables map[uint64]*table.Table
	mu     sync.Mutex
}

func NewTableCache(dbname string, options *db.Options, env db.Env, cmp db.Comparator) *TableCache {
	return &TableCache{
		dbname:  dbname,
		options: options,
		env:     env,
		cmp:     cmp,

		tables: make(map[uint64]*table.Table),
	}
}

func (tc *TableCache) Get(num, size uint64, key []byte, handleFn func(k, v []byte)) error {
	tbl, err := tc.findTable(num, size)
	if err != nil {
		return err
	}

	err = tbl.InternalGet(key, handleFn)
	return err
}

func (tc *TableCache) findTable(num, size uint64) (*table.Table, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tbl, ok := tc.tables[num]; ok {
		return tbl, nil
	}

	fname := TableFileName(tc.dbname, num)
	f, err := tc.env.NewRandomAccessFile(fname)
	if err != nil {
		oldFname := SSTTableFileName(tc.dbname, num)
		f, err = tc.env.NewRandomAccessFile(oldFname)
		if err != nil {
			return nil, err
		}
	}

	tbl, err := table.OpenTable(f, size, tc.cmp)
	if err != nil {
		return nil, err
	}

	tc.tables[num] = tbl
	return tbl, nil
}
