# Project Brief: TQSession

TQSession is a high-performance, persistent caching system implemented in Go. It allows
usage both as an **embeddable Go library** and as a **standalone server application** 
(CLI). The server application behaves like Memcached, accepting a configuration file
(defaulting to `tqsession.conf`) for easy deployment. It allows 
**configurable persistence guarantees** (e.g., FSYNC on every write, periodic sync, 
or OS-buffered) to balance durability vs. performance. It implements 
**both the Memcached text protocol and the Binary Protocol**, ensuring compatibility
with standard clients.

## Architecture

### Concurrency Model

Uses a **sharded architecture** with RWMutex per shard for concurrent access:

| Component         | Description                                      |
|-------------------|--------------------------------------------------|
| **ShardedCache**  | Routes keys to shards via FNV-1a hash            |
| **Shard (Cache)** | Independent cache with own mutex, index, storage |
| **Storage**       | Per-shard files with bucket-level RWMutex        |
| **Sync Worker**   | Periodic fsync across all shards (configurable)  |

**Sharding benefits**:
- 16 shards (default) allow parallel access to different keys
- RWMutex allows concurrent reads within each shard
- Write lock only held briefly for index updates

---

### On-Disk Storage System

Uses **fixed-size records** with `fseek` for random access (not append-only).

| File   | Purpose                                  |
|--------|------------------------------------------|
| `keys` | Fixed-size key records (1049 bytes each) |
| `data` | Variable-size value records              |

---

#### Keys File Format (`keys`)

Each record is exactly **1059 bytes** at offset `keyId * 1059`:

```
┌──────────┬──────────────┬──────────────┬─────────┬──────────┬────────┬─────────┐
│  keyLen  │     key      │ lastAccessed │   cas   │  expiry  │ bucket │ slotIdx │
│ 2 bytes  │  1024 bytes  │   8 bytes    │ 8 bytes │ 8 bytes  │ 1 byte │ 8 bytes │
└──────────┴──────────────┴──────────────┴─────────┴──────────┴────────┴─────────┘
         Total: 1059 bytes per record
```

| Field          | Size    | Description                                               |
|----------------|---------|-----------------------------------------------------------|
| `keyLen`       | 2 bytes | Actual key length (uint16, 0-1024)                        |
| `key`          | 1024 bytes | Key string, null-padded                                |
| `lastAccessed` | 8 bytes | Unix timestamp (int64), for LRU                           |
| `cas`          | 8 bytes | CAS token (uint64)                                        |
| `expiry`       | 8 bytes | Unix timestamp in **milliseconds** (int64), 0 = no expiry |
| `bucket`       | 1 byte  | Data bucket index (0-15)                                  |
| `slotIdx`      | 8 bytes | Slot index within the bucket (int64)                      |

**keyId** = record index = file offset / 1059

---

#### Data File Format (`data`)

Create 16 data files each holding a different size bucket. 
Start at 1024 bytes and double the size for each file.

```
┌──────────┬─────────────────────┐
│  length  │        data         │
│ 4 bytes  │   [length] bytes    │
└──────────┴─────────────────────┘
```

| Field          | Size    | Description                            |
|----------------|---------|----------------------------------------|
| `length`       | 4 bytes | Data length (uint32), max bucket size  |
| `data`         | bucket size | Raw value bytes                    |

**Slot sizes**: Total slot = `4 + bucket_size` bytes. Buckets: 1KB, 2KB, ..., 64MB.

---

### In-Memory Structures

#### 1. B-Tree Index (Fast Read Access)
- **In-memory only**: Built on startup by scanning `keys` file
- **Lookup**: key string → keyId (record index)
- **Stores**: `{ key, keyId, dataOffset, dataLength, expiry, cas, lastAccessed }`

#### 2. Expiry Min-Heap (TTL Invalidation Without Scanning)
- **Structure**: Binary min-heap ordered by `expiry` field
- **In-memory only**: Built on startup from keys file
- **Entries**: `(expiry, keyId)` — pointers into keys file

**Operations**:
| Operation      | Complexity | Description                     |
|----------------|------------|---------------------------------|
| `PeekMin()`    | O(1)       | Check if root is expired        |
| `PopExpired()` | O(log n)   | Remove expired item, reheap     |
| `Insert()`     | O(log n)   | Add new item with TTL           |
| `Remove(keyId)` | O(log n) | Remove by keyId (with index map) |

**Invalidation Flow** (no scanning required):
1. Background goroutine calls `PeekMin()`
2. If `root.expiry <= now`: pop, compact file slots via continuous defrag
3. Repeat until root is not expired or heap is empty

#### 3. LRU List (Access-Order Eviction)
- **In-memory**: Doubly-linked list ordered by `lastAccessed`
- **On update**: Move node to head, update `lastAccessed` in keys file via fseek
- **On eviction**: Remove from tail, compact file slots via continuous defrag

#### 4. Continuous Defragmentation (Always Compact Files)

Instead of free lists, uses **continuous defragmentation**:

**On delete/eviction**:
1. Move tail slot data to freed slot position
2. Update the moved entry's index to point to new slot
3. Truncate file by one slot

**Benefits**:
- Files are always compact, no wasted space
- O(1) allocation (always append to end)
- No fragmentation over time

## Success Criteria
1.  **Persistence**: Data survives process restarts
2.  **Performance**: Fast lookups via B-Tree, 100k+ RPS SET/GET with sharding
3.  **Protocol**: Compatible with Memcached clients (Text and Binary protocols)
4.  **Simplicity**: Easy to understand and maintain, small code base
5.  **Compatibility**: Works with PHP Memcached session handler

**Files**: Each shard has its own folder (`shard_00/` to `shard_15/`) containing `keys` and `data_*` files.
