# TQCache

A High-Performance Persistent Session Storage Server

Maurits van der Schee

---

# What is TQCache?

- High-performance, persistent session storage server
- Memcached-compatible interface (text & binary protocols)
- Disk-based persistence that survives restarts
- No LRU eviction support. Not intended as a cache.
- Ideal for (PHP) session storage

---

# Concurrency Model

| Component        | Description                              |
| ---------------- | ---------------------------------------- |
| **ShardedCache** | Routes keys to shards via FNV-1a hash    |
| **Worker**       | Single goroutine per shard               |
| **Storage**      | Per-shard files (no locks needed)        |
| **Sync Worker**  | Periodic fsync across all shards         |

---

# Request Flow: Incoming

1. **TCP Client** connects to server
2. **Server.Start** accepts connection, spawns goroutine
3. **handleConnection** peeks first byte to detect protocol
4. **Binary (0x80) or Text** protocol parses the command
5. **ShardedCache** uses FNV-1a hash to determine shard index
6. **Request** sent to the buffered channel of the Shard Worker

---

# Request Flow: Shard Worker

1. **Worker.run()** receives from channel (or expiry ticker)
2. **handleRequest()** routes to handleGet/handleSet/etc.
3. **Index** (B-Tree) looks up key to find keyId
4. **Storage** reads/writes key metadata from keys file
5. **Storage** reads/writes value data from data file
6. **Min-Heap** insert on SET (if TTL), pop on expiry tick
7. **Sync Worker** notify periodically to fsync files to disk
8. **Response** sent back through response channel

---

# On-Disk Storage

- Not append-only, uses `fseek` for random access
- Uses fixed-size records, to avoid fragmentation
- **Keys file**: Fixed 1051-byte records
- **Data files**: 16 buckets (1KB, 2KB, 4KB, ... 64MB)
- Chooses the bucket based on the value size
- Unused space leads to ~25-33% disk space overhead

---

# Keys File Format

```
┌──────────┬──────────────┬─────────┬──────────┬────────┬─────────┐
│  keyLen  │     key      │   cas   │  expiry  │ bucket │ slotIdx │
│ 2 bytes  │  1024 bytes  │ 8 bytes │ 8 bytes  │ 1 byte │ 8 bytes │
└──────────┴──────────────┴─────────┴──────────┴────────┴─────────┘
           Total: 1051 bytes per record
```

---

# Data File Format

- 16 bucket files with doubling sizes
- Sizes: 1KB, 2KB, 4KB, ... up to 64MB

```
┌──────────┬────────────────────┐
│  length  │        data        │
│ 4 bytes  │ [bucketSize] bytes │
└──────────┴────────────────────┘
```

---

# On-Disk Storage Layout

```
data/
├── shard_00/
│   ├── keys           # key metadata (1051 bytes each)
│   ├── data_00        # 1KB slots
│   ├── data_01        # 2KB slots
│   ├── ...
│   └── data_15        # 64MB slots
├── shard_01/
├── ...
└── shard_15/
```

---

# Continuous Defragmentation

**On delete/expiry:**

1. Move tail slot data to freed slot position
2. Update the moved entry's index
3. Truncate file by one slot

**Benefits:** Always compact, O(1) allocation, no fragmentation

---

# In-Memory Structures

1. **B-Tree Index**: Fast key to keyId lookup, stores:

  `{ key, keyId, bucket, slotIdx, length, expiry, cas }`

2. **Expiry Min-Heap**: TTL invalidation without scanning

| Operation       | Complexity | Description                      |
| --------------- | ---------- | -------------------------------- |
| `PeekMin()`     | O(1)       | Check if root is expired         |
| `Insert()`      | O(log n)   | Add new item with TTL            |
| `Remove(keyId)` | O(log n)   | Remove by keyId                  |

---

# Performance Comparison

| Reference              | SET (RPS) | GET (RPS) | Memory (MB) | CPU Usage |
| :--------------------- | :-------- | :-------- | :---------- | :-------- |
| **Memcached** (Memory) | ~126k     | ~275k     | ~1073MB     | ~2.5 core |
| **Redis** (Periodic)   | ~62k      | ~107k     | ~1339MB     | ~1 core   |
| **TQCache** (Periodic) | ~92k      | ~176k     | ~70MB       | ~4 core   |

---

# Performance Highlights

- **SET**: +49% faster than Redis (~92k vs ~62k RPS)
- **GET**: +64% faster than Redis (~176k vs ~107k RPS)
- **Memory**: ~19x less than Redis (~70MB vs ~1339MB)
- **Uses OS disk cache** instead of in-process memory

---

# PHP Configuration

```ini
session.save_handler = memcached
session.save_path = "localhost:11211"
```

Remember: No LRU eviction support.

Use `max-ttl` to limit diskspace usage.

---

# Configuration Options

| Parameter       | Default    | Description                       |
| --------------- | ---------- | ----------------------------------|
| `listen`        | `:11211`   | Address to listen on              |
| `data-dir`      | `data`     | Persistent data directory         |
| `shards`        | `16`       | Number of shards                  |
| `default-ttl`   | `0`        | For keys (`0` = no expiry)        |
| `max-ttl`       | `24h`      | Cap for any key (`0` = unlimited) |
| `sync-mode`     | `periodic` | `none`, `periodic`, or `always`   |
| `sync-interval` | `1s`       | Interval between fsync calls      |

---

# Summary

- Drop-in Memcached replacement with persistence
- Faster and more memory-efficient than Redis
- Lock-free architecture for predictable latency
- Optimized for SSD with OS disk caching
- Simple deployment and configuration

---

# Disclaimer

- Built in a weekend (AI assisted)
- Work in progress (optimizing further)
- No production use (yet)
- No warranties (use at your own risk)

---

# Thank You

**Repository:** github.com/mevdschee/tqcache

**Blog post:** tqdev.com/2026-tqcache-memcache-redis-alternative
