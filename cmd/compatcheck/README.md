# compatcheck

Internal compatibility checker between golsm and C++ LevelDB (`./leveldb`).

## Purpose

Validate that both engines produce the same logical DB state from the same
on-disk data, including cross-engine WAL recovery behavior.

## What It Compares

`compatcheck` generates deterministic workload/query files, executes both
engines, and compares binary transcript SHA-256 hashes.

Transcript contains:
- full forward iteration
- full reverse iteration
- point gets
- seek + immediate prev

## Coverage Matrix

Default checks:
- `golsm(no-compression) -> leveldb`
- `leveldb(snappy) -> golsm`

With `-full-matrix=true` (default), also:
- `golsm(snappy) -> leveldb`
- `leveldb(no-compression) -> golsm`

Crash-recovery checks (`-crash-recovery=true`, default):
- writer intentionally exits without `Close()`
- verify pre-recovery `.log` is non-empty
- opposite engine opens first (WAL replay)
- transcript hashes must match

## Prerequisites

- Go toolchain
- CMake + C++ compiler
- local LevelDB source at `./leveldb`

If `-leveldb-tool` is not provided, `compatcheck` builds
`cmd/compatcheck/cpp/leveldb_kvtool.cc` automatically.

## Example

```bash
GOCACHE=/tmp/go-build go run ./cmd/compatcheck \
  -ops=200000 \
  -keyspace=50000 \
  -value-size=200 \
  -point-queries=4000 \
  -seek-queries=4000 \
  -reopen-every=0 \
  -full-matrix=true \
  -crash-recovery=true \
  -crash-after=180000
```

## Useful Flags

- `-ops`: number of write operations
- `-keyspace`: key universe size
- `-value-size`: base value bytes
- `-point-queries`: number of point get queries
- `-seek-queries`: number of seek queries
- `-reopen-every`: periodic reopen interval during normal apply (`0` disables)
- `-full-matrix`: run all compression direction pairs
- `-crash-recovery`: run crash-recovery matrix
- `-crash-after`: op index to crash at (`0` means auto, ~90% of ops)
- `-keep`: keep generated artifacts under workdir
- `-workdir`: output working directory
- `-leveldb-tool`: use prebuilt `leveldb_kvtool` binary
