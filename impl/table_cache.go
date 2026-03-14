package impl

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/ls4154/golsm/db"
	"github.com/ls4154/golsm/fs"
	"github.com/ls4154/golsm/table"
	"github.com/ls4154/golsm/util"
)

type tableAndFile struct {
	file  fs.RandomAccessFile
	table *table.Table
}

type TableCache struct {
	dbname         string
	env            fs.Env
	cmp            db.Comparator
	filter         db.FilterPolicy
	paranoidChecks bool

	lru *util.LRUCache[tableAndFile]

	bcache  *table.BlockCache
	cacheID atomic.Uint64
}

func NewTableCache(dbname string, env fs.Env, size int, cmp db.Comparator, filter db.FilterPolicy,
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
	return wrapIOError(tbl.InternalGet(key, handleFn, verifyChecksum, bypassCache), "read table #%d", num)
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

	return tc.lru.LookupOrLoad(key, func() (tableAndFile, int, error) {
		fname := TableFileName(tc.dbname, num)
		f, err := tc.env.NewRandomAccessFile(fname)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return tableAndFile{}, 0, wrapIOError(err, "open table file %s", fname)
			}
			oldFname := SSTTableFileName(tc.dbname, num)
			f, err = tc.env.NewRandomAccessFile(oldFname)
			if err != nil {
				return tableAndFile{}, 0, wrapIOError(err, "open table file %s", oldFname)
			}
		}

		cacheID := tc.cacheID.Add(1)
		tbl, err := table.OpenTable(f, size, tc.cmp, tc.filter, tc.bcache, cacheID, tc.paranoidChecks)
		if err != nil {
			_ = f.Close()
			return tableAndFile{}, 0, wrapIOError(err, "open table #%d", num)
		}

		return tableAndFile{file: f, table: tbl}, 1, nil
	})
}
