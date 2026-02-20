# stressrw

Long-running concurrent read/write stress tool for golsm.

## Why

`compatcheck` validates on-disk format compatibility.
`stressrw` validates runtime stability under sustained concurrent access.

## Key Design

- Fixed keyspace (`-keyspace`) to cap live data size.
- Mixed writes (`put` + `delete`) to avoid monotonic growth.
- Concurrent `Get`, `Seek/Prev`, iterator probes.
- Periodic snapshot-based internal consistency checks.
- Optional periodic `Close/Open` (`-reopen-interval`) for repeated recovery cycles.
- Optional disk guard (`-max-disk-mb`) to stop before using too much space.

## Quick Start

```bash
GOCACHE=/tmp/go-build go run ./cmd/stressrw \
  -db=/tmp/golsm-stress \
  -duration=30m \
  -reopen-interval=10m \
  -writers=8 \
  -readers=24 \
  -iterators=8 \
  -keyspace=50000 \
  -value-size=64 \
  -value-jitter=32 \
  -writer-delay=500us \
  -compression=snappy \
  -bloom-bits=10 \
  -max-disk-mb=2048
```

## Recommended Long Soak (example)

```bash
GOCACHE=/tmp/go-build go run ./cmd/stressrw \
  -db=/tmp/golsm-stress-soak \
  -duration=1h \
  -reopen-interval=15m \
  -writers=8 \
  -readers=32 \
  -iterators=8 \
  -keyspace=100000 \
  -value-size=80 \
  -value-jitter=40 \
  -writer-delay=1ms \
  -compression=no \
  -max-disk-mb=4096
```

## Important Flags

- `-duration`: total runtime.
- `-reopen-interval`: periodic close/open interval (`0` disables).
- `-writers`, `-readers`, `-iterators`: worker counts.
- `-keyspace`: fixed key universe size.
- `-writer-delay`: per-write sleep to control write pressure.
- `-max-disk-mb`: fail when DB dir exceeds limit (`0` disables).
- `-final-check`: final keyspace oracle verification.
- `-final-max-mismatches`: fail fast threshold for final oracle mismatches (`0` disables fail-fast).
- `-final-progress-every`: progress print interval (keys) during final oracle check.
- `-partitioned-writers`: assign disjoint key ranges per writer (recommended for deterministic oracle tracking).
- `-reuse-db`: keep existing DB at start (skips strict final oracle check).
