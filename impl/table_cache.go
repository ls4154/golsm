package impl

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
)

type tableAndFile struct {
	file  db.RandomAccessFile
	table *table.Table
}

type TableCache struct {
	dbname         string
	env            db.Env
	cmp            db.Comparator
	filter         db.FilterPolicy
	paranoidChecks bool

	lru *util.LRUCache[tableAndFile]

	bcache  *table.BlockCache
	cacheID atomic.Uint64
}

func NewTableCache(dbname string, env db.Env, size int, cmp db.Comparator, filter db.FilterPolicy,
	bcache *table.BlockCache, paranoidChecks bool) *TableCache {
	lru := util.NewLRUCache[tableAndFile](size)
	lru.SetOnEvict(func(_ []byte, v *tableAndFile) {
		v.table.Close()
		v.file.Close()
	})

	return &TableCache{
		dbname:         dbname,
		env:            env,
		cmp:            cmp,
		filter:         filter,
		paranoidChecks: paranoidChecks,
		lru:            lru,
		bcache:         bcache,
	}
}

func (tc *TableCache) Get(num FileNumber, size uint64, key []byte, handleFn func(k, v []byte), verifyChecksum, bypassCache bool) error {
	handle, err := tc.findTable(num, size)
	if err != nil {
		return err
	}
	defer tc.lru.Release(handle)

	tbl := handle.Value().table
	return tbl.InternalGet(key, handleFn, verifyChecksum, bypassCache)
}

func (tc *TableCache) NewIterator(num FileNumber, size uint64, verifyChecksum, bypassCache bool) (db.Iterator, error) {
	handle, err := tc.findTable(num, size)
	if err != nil {
		return nil, err
	}
	tbl := handle.Value().table
	iter := tbl.NewIterator(verifyChecksum, bypassCache)
	return newCleanupIterator(iter, func() {
		tc.lru.Release(handle)
	}), nil
}

func (tc *TableCache) Close() {
	tc.lru.Close()
}

func (tc *TableCache) Evict(num FileNumber) {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(num))
	tc.lru.Erase(key)
}

func (tc *TableCache) findTable(num FileNumber, size uint64) (*util.LRUHandle[tableAndFile], error) {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(num))

	if h := tc.lru.Lookup(key); h != nil {
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

	cacheID := tc.cacheID.Add(1)
	tbl, err := table.OpenTable(f, size, tc.cmp, tc.filter, tc.bcache, cacheID, tc.paranoidChecks)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return tc.lru.Insert(key, tableAndFile{
		file:  f,
		table: tbl,
	}, 1), nil
}
