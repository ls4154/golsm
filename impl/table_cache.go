package impl

import (
	"encoding/binary"
	"sync"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
)

type tableAndFile struct {
	file  db.RandomAccessFile
	table *table.Table
}

type TableCache struct {
	dbname string
	env    db.Env
	cmp    db.Comparator

	cache *util.LRUCache[tableAndFile]
	mu    sync.Mutex
}

func NewTableCache(dbname string, env db.Env, size int, cmp db.Comparator) *TableCache {
	cache := util.NewLRUCache[tableAndFile](size)
	cache.SetOnEvict(func(_ []byte, v *tableAndFile) {
		v.table.Close()
		v.file.Close()
	})

	return &TableCache{
		dbname: dbname,
		env:    env,
		cmp:    cmp,
		cache:  cache,
	}
}

func (tc *TableCache) Get(num, size uint64, key []byte, handleFn func(k, v []byte)) error {
	handle, err := tc.findTable(num, size)
	if err != nil {
		return err
	}
	defer tc.cache.Release(handle)

	tbl := handle.Value().table
	return tbl.InternalGet(key, handleFn)
}

func (tc *TableCache) NewIterator(num, size uint64) (db.Iterator, error) {
	handle, err := tc.findTable(num, size)
	if err != nil {
		return nil, err
	}
	tbl := handle.Value().table
	iter := tbl.NewIterator()
	return newCleanupIterator(iter, func() {
		tc.cache.Release(handle)
	}), nil
}

func (tc *TableCache) findTable(num, size uint64) (*util.LRUHandle[tableAndFile], error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, num)

	if h := tc.cache.Lookup(key); h != nil {
		return h, nil
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
		_ = f.Close()
		return nil, err
	}

	return tc.cache.Insert(key, tableAndFile{
		file:  f,
		table: tbl,
	}, 1), nil
}
