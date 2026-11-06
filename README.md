# FrostDB (not production ready yet)

FrostDB is a Windows-first, drop-in replacement for Redis — built from scratch in Go with zero POSIX dependencies.

It started with queue, worker, and delayed-job workloads as the first target, and now covers the full Redis command surface needed for production use: strings, lists, hashes, sets, sorted sets, streams, transactions, pub/sub, and RDB compatibility.

## Why This Exists

Redis is excellent software, but its implementation model is strongly shaped by Unix and POSIX behavior. That becomes a problem on Windows for two reasons:

1. `fork()` is a Unix primitive and does not exist on Windows.
2. Several Redis workflows rely on child-process semantics for background persistence and rewrite paths.

FrostDB takes the opposite approach:

- No `fork()` dependency
- No child-process based persistence model
- No global lock around the entire database
- Sharded in-memory execution from day one (256 shards)
- RESP2 / RESP3 protocol support (fully compatible with `redis-py`)
- Forkless WAL-based persistence
- RDB import/export for seamless migration from Redis
- Drop-in backend replacement for most worker framework

## Features

### Commands

| Category | Commands |
|----------|----------|
| Strings | GET, SET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, MGET, MSET, MSETNX, SETNX, SETEX, PSETEX, GETSET, GETEX, GETDEL |
| Keys | DEL, UNLINK, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, TTL, PTTL, SCAN, KEYS, PING, ECHO, INFO, EXISTS, TYPE, RENAME, RENAMENX, RANDOMKEY, DBSIZE, FLUSHDB, FLUSHALL, SELECT |
| Lists | LPUSH, RPUSH, LPOP, RPOP, BLPOP, BRPOP, LRANGE, LMOVE, LREM, LLEN, LINDEX, LSET, LINSERT |
| Hashes | HSET, HSETNX, HGET, HDEL, HGETALL, HLEN, HINCRBY, HINCRBYFLOAT, HEXISTS, HKEYS, HVALS, HMGET |
| Sets | SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SINTER, SINTERSTORE, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE |
| Sorted Sets | ZADD, ZREM, ZRANGE, ZRANGEBYSCORE, ZREMRANGEBYSCORE, ZPOPMIN |
| Streams | XADD, XREVRANGE, XREADGROUP, XACK, XPENDING, XAUTOCLAIM, XGROUP |
| Transactions | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| PubSub | SUBSCRIBE, UNSUBSCRIBE, PUBLISH |
| Server | HELLO, CLIENT, CONFIG, SAVE, BGSAVE, OBJECT, MEMORY, CLUSTER, DBSIZE |

### Persistence

FrostDB uses a forkless, CRC32-checked write-ahead log (WAL) plus periodic checkpoints. Recovery loads the latest checkpoint and replays the later WAL frames; an incomplete final frame from an interrupted write is ignored after all earlier checksum-verified frames are recovered. WAL segments rotate automatically at 64 MiB.

**Current write boundary:** a command handler updates memory first, then the dispatcher appends its successful deterministic or canonical WAL frame. This is intentionally documented because it is not log-before-apply atomic: if that first synchronous append fails, the client receives `ERR persistence failure` after the in-memory mutation may already have occurred. With the default policy, later writes are rejected while the WAL remains unhealthy.

#### Durability and failure policy

`appendfsync` controls flush behavior (default: `everysec`):

- `always` — flush and fsync after every accepted WAL frame; this is the only per-command durable mode.
- `everysec` — fsync pending frames about once per second; a power loss can lose roughly the most recent second of accepted writes.
- `no` — defer to operating-system writeback during normal operation; a controlled shutdown still performs a final sync, but an unclean shutdown can lose more data.

`wal-error-policy` controls write admission after WAL I/O fails (default: `stop-writes`):

- `stop-writes` — reject subsequent mutations with `MISCONF` until WAL health recovers.
- `continue` — retain availability and allow writes despite unhealthy persistence; use only when the resulting durability risk is acceptable.

Configure either setting with CLI flags, environment variables, or a FrostDB config file:

```bash
./frost --appendfsync always --wal-error-policy stop-writes
FROST_APPEND_FSYNC=everysec FROST_WAL_ERROR_POLICY=stop-writes ./frost
```

```conf
appendfsync everysec
wal-error-policy stop-writes
```

Use `INFO Persistence` to monitor `frostdb_wal_fsync_policy`, `frostdb_wal_error_policy`, `frostdb_wal_write_admission`, WAL last-sync/failure/error fields, and checkpoint in-progress, result, count, and error fields.

#### Replay compatibility

WAL persistence is deliberately fail-closed for mutations without proven deterministic replay: those commands return an error while WAL persistence is enabled instead of acknowledging a write that a crash could lose. Supported replay covers the common string, expiry, list, set, sorted-set, hash, stream, rename, and flush paths, including canonical frames for random `SPOP` removals and empty sorted-set pops. Absolute TTL forms are preserved across restart.

Some mutating command families remain unavailable with WAL enabled until their snapshot and replay semantics are completed—for example scripting/functions, bit and array mutations, copy/restore/sort, advanced stream mutations, and hash-field TTL operations. Existing WAL files produced by older FrostDB builds that logged raw `SPOP` frames cannot be made deterministic retroactively; take a checkpoint or migrate to a fresh WAL before relying on this version's canonical `SREM`/`DEL` frames.

### RDB Import / Export

Full Redis RDB file compatibility for migration:

```bash
# Auto-loads ./data/dump.rdb on startup (standard Redis behavior)
./frost.exe

# Or specify a custom RDB file to import on startup
./frost.exe --import-rdb /path/to/dump.rdb

# Export live state while server is running
redis-cli SAVE
redis-cli BGSAVE

# Export from persisted state without starting the server
./frost.exe --export-rdb /path/to/backup.rdb
```

- Automatically loads `dump.rdb` from data directory on boot (like Redis)
- Supports all data types: Strings, Lists, Sets, Hashes, Sorted Sets, Streams
- Preserves TTLs, consumer groups, and pending entries
- Uses [hdt3213/rdb](https://github.com/HDT3213/rdb) (RDB v1–v12, up to Redis 7.2)
- `SAVE` blocks until complete; `BGSAVE` runs in background
- `--export-rdb` reads from disk (WAL + checkpoint), does not start the server

### Architecture

```mermaid
graph TD
    Client[Client Connection] -->|TCP| Parser[RESP2/RESP3 Parser]
    
    subgraph Core Engine
        Parser -->|Parsed Command| Dispatcher[Command Registry/Dispatcher]
        Dispatcher -->|Engine API| Shards[256 Engine Shards]
        
        Shards -->|Get/Set| Objects[Memory Objects: Strings, Lists, Hashes, ZSets, Streams]
    end
    
    subgraph Durability
        Dispatcher -->|LogCommand| WAL[Write-Ahead Log]
        WAL -->|Sync| Disk[(Disk Storage)]
        Checkpoint[Background Checkpointer] -->|Periodic Snapshot| Disk
    end
```

Commands are plugins. They receive a context (which tracks transaction state and `HELLO` protocol version), call the engine API, and return RESP values. No command touches the network or storage layer directly.

## Build

Requires [Go 1.22+](https://go.dev/doc/install).

```bash
go build -o frost.exe ./cmd/frost
```

## Run

```bash
./frost.exe
```

Listens on port `6379` by default. Data is persisted to `./data/` (override with `FROST_DATA_DIR` env var).

### Logging

Structured leveled logging (built on Go's `log/slog`):

```bash
# Set log level (debug, info, warn, error)
./frost.exe --loglevel debug

# JSON output (for log aggregators like Loki, Datadog, etc.)
./frost.exe --log-json
```

### Graceful Shutdown

FrostDB handles `SIGINT` / `SIGTERM` (or Ctrl+C) gracefully:

1. Stops accepting new connections
2. Drains in-flight client requests
3. Flushes and closes the WAL
4. Exits cleanly

No data loss on controlled shutdown.

### Memory Eviction

```bash
# Start with 256MB memory limit and LRU eviction
./frost.exe --maxmemory 268435456 --maxmemory-policy allkeys-lru
```

Configure at runtime via `CONFIG SET`:

```bash
redis-cli CONFIG SET maxmemory 100mb
redis-cli CONFIG SET maxmemory-policy allkeys-lfu
redis-cli CONFIG GET maxmemory*
```

Supported policies: `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`.

### Keyspace Notifications

Redis-compatible keyspace notifications via PubSub:

```bash
# Enable all keyspace notifications
redis-cli CONFIG SET notify-keyspace-events KEA

# Subscribe to expired key events
redis-cli SUBSCRIBE __keyevent@0__:expired

# Subscribe to all events on a specific key
redis-cli SUBSCRIBE __keyspace@0__:mykey
```

Flags: `K` (keyspace), `E` (keyevent), `g` (generic), `$` (string), `l` (list), `s` (set), `h` (hash), `z` (zset), `x` (expired), `e` (evicted), `t` (stream), `A` (all).

## Test

```bash
go test ./...
```

### Test Coverage

14 test packages, 35+ test cases covering:

| Package | Tests |
|---------|-------|
| command/generic | PING, ECHO, DEL, EXPIRE, INFO, SCAN, CLIENT |
| command/string | GET, SET, wrong type handling |
| command/listcmd | Push/Pop, blocking BLPOP |
| command/zsetcmd | ZADD, ZRANGE, ZPOPMIN, ZRANGEBYSCORE, flags |
| command/streamcmd | XADD, XREADGROUP, XACK, XPENDING full flow |
| command/tx | MULTI/EXEC, DISCARD, nested MULTI, error abort |
| command/pubsub | PUBLISH with 0/1/N subscribers, UNSUBSCRIBE |
| engine/core | Get/Set, Del, Expire, persistence round-trip, checkpoint+recover |
| engine/object | List, Stream (add/read/ack/autoclaim), ZSet |
| persistence/wal | Append/Read, rotation, checksum integrity |
| persistence/checkpoint | Write/Read, latest discovery, invalid magic |
| persistence/recovery | Empty, checkpoint-only, WAL-only, combined |
| resp/parser | SimpleString, Array, inline commands |

## Benchmarks

Run the command dispatch benchmarks with:

```bash
go test -bench . -benchmem ./internal/command/registry
```

### Verified on this development environment

Measured directly via `go test -bench . -benchmem` on Intel Core i5-6500T @ 2.50GHz, Linux, 4 cores, exercising the full `registry.Dispatch` path (parsing → ACL → striped key locking → command execution):

| Benchmark | ns/op | allocs/op |
|-----------|-------|-----------|
| GET | 463 | 3 |
| SET | 1,010 | 11 |
| GET (parallel) | 259 | 3 |
| SET (parallel) | 1,143 | 11 |

These reflect a round of allocation-focused fixes: removing dead command-serialization code, ACL fast paths for the default (unrestricted) user, lazily allocating the striped-lock release/reacquire closures only for commands that actually block (BLPOP, XREAD, etc.), and zero-allocation single/two-key lock paths. Before these changes, GET was 12 allocs/op (869 ns/op) and SET was 20 allocs/op (1,451 ns/op) on the same machine — a 75% and 45% allocation reduction respectively.

### Historical reference (Windows)

The table below predates the environment above and was measured on Intel Core i5 @ 2.50GHz, Windows, 4 cores. It has not been re-verified on Linux and is kept for historical comparison only:

| Benchmark | ops/sec | ns/op | allocs/op |
|-----------|---------|-------|-----------|
| TCP Server (SET) | ~97,686 | 12,409 | - |
| SET | 2,589,223 | 486 | 5 |
| GET | 8,543,374 | 150 | 1 |
| SET (parallel) | 3,203,698 | 378 | 11 |
| List queue (RPUSH+LPOP) | 1,000,000 | 1,154 | 14 |
| List queue (parallel) | 1,505,142 | 723 | 16 |
| Delayed queue (ZADD+ZPOPMIN) | 409,173 | 2,688 | 27 |
| XADD | 1,000,000 | 1,046 | 14 |
| Stream consumer (XADD+XREADGROUP+XACK) | ~281,041 | 4,085 | 50 |

### Performance Architecture
- **Zero-Alloc Hot Paths**: Pre-allocated RESP types, `unsafe.String` casting, and inline FNV-1a hashing eliminate garbage collection pauses on hot keys. FNV-1a was benchmarked against Go's runtime `hash/maphash` for typical short Redis keys and kept — the AES-accelerated runtime hash is only faster on long keys (>~40 bytes), not the common case.
- **Fast-Path Key Locking**: Single-key commands (GET, SET, INCR, ...) and two-key commands (RENAME, LMOVE, SMOVE, ...) resolve and lock striped key slots without allocating a slice or map; only commands capable of releasing/reacquiring locks mid-execution (BLPOP, XREAD, DEBUG, ...) pay for the lock-callback closures.
- **Data Structure Pooling**: `sync.Pool` extensively used for `ListNode`, `SkiplistNode`, and `WAL` byte buffers.
- **Radix-like BTree Streams**: Streams are internally backed by `btree.BTreeG` blocks of packed arrays (Listpacks) combined with zero-alloc `StreamID` structs. This reduces pointers by a factor of 100x and avoids massive GC pauses when scanning millions of stream entries.
- **O(1) Stream Consumers**: Consumers track precise byte offsets and `StreamID` structs rather than performing O(N) string-parsing scans.
- **Forkless Persistence**: Zero-overhead dense binary WAL serialization.


## Ecosystem Compatibility
- **redis-py**: Fully supports connecting, handshaking (`HELLO 3`), and communicating with Python's official Redis client.
- **rq (Redis Queue)**: Fully implements all complex internal List, Set, Hash, and Sorted Set commands (like `LMOVE`, `ZREMRANGEBYSCORE`, `HINCRBY`) needed to run Python `rq` jobs out of the box (requires using `SimpleWorker` on Windows due to OS fork limitations).

## Usage

Connect with any Redis client:

```bash
redis-cli PING
redis-cli SET mykey "Hello World"
redis-cli GET mykey
```

Blocking queue:

```bash
# Terminal 1
redis-cli BLPOP myqueue 0

# Terminal 2
redis-cli LPUSH myqueue "Task 1"
```

Stream consumer group:

```bash
redis-cli XGROUP CREATE mystream workers 0 MKSTREAM
redis-cli XADD mystream '*' task "process-order" id "123"
redis-cli XREADGROUP GROUP workers w1 COUNT 1 STREAMS mystream '>'
redis-cli XACK mystream workers 1234567890-0
```

Transactions:

```bash
redis-cli MULTI
redis-cli SET counter 1
redis-cli SET status active
redis-cli EXEC
```

## Design Principles

- Commands are plugins
- Engine is isolated behind an interface
- Streams are first class
- Persistence is forkless
- Everything sharded from day 1
- Files stay under 300 lines
- WAL write failures propagate to the client (`ERR persistence failure`) — no silent data loss
- Panic recovery per connection — one bad request never crashes the server
- Structured logging with levels — no noisy output in production, full detail in debug


## Redis test cases passing:

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/list-3

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/list-2

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/list

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/stream

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/string

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/sort

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/pubsub

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/scan

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/limits

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/wait

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/bitfield

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/bitops

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/incr

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/array

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/hash

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/set

./runtest --host 127.0.0.1 --port 6379 --verbose --single unit/type/zset

## Benchmarks

| Metric              | **FrostDB** | **Redis 8** |   **Valkey** | **DragonflyDB** |
| ------------------- | ----------: | ----------: | -----------: | --------------: |
| **SET ops/s**       |  **79,039** | **107,821** |  **109,077** |     **107,613** |
| **GET ops/s**       |  **86,522** | **105,472** |  **111,500** |     **104,824** |
| **SET avg latency** |    2.314 ms |    1.699 ms |     1.683 ms |    **1.321 ms** |
| **GET avg latency** |    2.039 ms |    1.690 ms |     1.638 ms |    **1.354 ms** |
| **SET P50**         |    1.791 ms |    1.591 ms |     1.591 ms |    **1.063 ms** |
| **GET P50**         |    1.527 ms |    1.559 ms |     1.559 ms |    **1.143 ms** |
| **SET P95**         |    6.359 ms |    2.695 ms | **2.543 ms** |        3.487 ms |
| **GET P95**         |    5.807 ms |    2.863 ms | **2.495 ms** |        3.455 ms |
| **SET P99**         |   10.175 ms |    4.127 ms | **3.799 ms** |        5.631 ms |
| **GET P99**         |    9.223 ms |    5.431 ms | **3.799 ms** |        5.487 ms |
| **Max latency**     |     51.1 ms |     42.9 ms |      43.9 ms |     **24.7 ms** |
