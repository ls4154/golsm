# golsm

A Go implementation of an LSM-tree based key-value store with LevelDB-compatible on-disk formats.

## Features

- Put / Get / Delete
- Range scan via bidirectional iterators
- Atomic batch writes
- Snapshots
- Background memtable flush and multi-level compaction
- Snappy block compression
- Block cache (LRU)
- LevelDB-compatible on-disk format (WAL, MANIFEST, SSTable / `.ldb`)
- Bloom filter

## Usage

```bash
go get github.com/ls4154/golsm
```

```go
package main

import (
    "log"

    "github.com/ls4154/golsm"
    "github.com/ls4154/golsm/db"
)

func main() {
    opt := db.DefaultOptions()

    ldb, err := golsm.Open(opt, "./mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer ldb.Close()

    wo := &db.WriteOptions{}
    ro := &db.ReadOptions{}

    // Write
    if err := ldb.Put([]byte("hello"), []byte("world"), wo); err != nil {
        log.Fatal(err)
    }

    // Read
    val, err := ldb.Get([]byte("hello"), ro)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("hello = %s", val)

    // Delete
    if err := ldb.Delete([]byte("hello"), wo); err != nil {
        log.Fatal(err)
    }

    // Scan
    iter, err := ldb.NewIterator(ro)
    if err != nil {
        log.Fatal(err)
    }
    defer iter.Close()

    for iter.SeekToFirst(); iter.Valid(); iter.Next() {
        log.Printf("%s = %s", iter.Key(), iter.Value())
    }

    // Snapshot
    snap := ldb.GetSnapshot()
    defer snap.Release()

    snapVal, err := ldb.Get([]byte("key"), &db.ReadOptions{Snapshot: snap})
    _ = snapVal
    _ = err
}
```

## Benchmark

```bash
go run ./cmd/dbbench \
    -benchmarks=fillseq,overwrite,readseq,readreverse,readrandom \
    -num=1000000 -threads=4 -value_size=100 -write_buffer_size=4194304 -max_file_size=4194304 \
    -cache_size=67108864 -compression=snappy -bloom_bits=10 \
    -db=./bench-db -histogram=1
```

## License

BSD 3-Clause
