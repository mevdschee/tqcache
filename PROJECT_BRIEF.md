# Project Brief: TQSession

TQSession is a high-performance, persistent caching system implemented in Go. It allows usage both as an **embeddable Go library** and as a **standalone server application** (CLI). 
The server application behaves like Memcached, accepting a configuration file (defaulting to `tqsession.conf`) for easy deployment.
It allows **configurable persistence guarantees** (e.g., FSYNC on every write, periodic sync, or OS-buffered) to balance durability vs. performance.
It implements **both the Memcached text protocol and the Binary Protocol**, ensuring compatibility with standard clients.

## Architecture

### Concurrency Model

Uses a **single-worker architecture** for storage and sync, with no locks:

| Goroutine   | Responsibility                                          |
|-------------|---------------------------------------------------------|
| **Server**  | Accepts client connections, talks to Storage worker     |
| **Storage** | Handles all file operations (reads/writes) sequentially |
| **Sync**    | Calls `fsync` periodically (interval configurable)      |

Operations are sent to the storage worker via channels, eliminating lock contention.

---

### On-Disk Storage System

Uses **fixed-size records** with `fseek` for random access (not append-only).

| File   | Purpose                                  |
|--------|------------------------------------------|
| `keys` | Fixed-size key records (1049 bytes each) |
| `data` | Variable-size value records              |

---

#### Keys File Format (`keys`)

Each record is exactly **1060 bytes** at offset `keyId * 1060`:

```
┌─────────┬──────────┬──────────────┬──────────────┬─────────┬──────────┬────────┬─────────┐
│  free   │  keyLen  │     key      │ lastAccessed │   cas   │  expiry  │ bucket │ slotIdx │
│ 1 byte  │ 2 bytes  │  1024 bytes  │   8 bytes    │ 8 bytes │ 8 bytes  │ 1 byte │ 8 bytes │
└─────────┴──────────┴──────────────┴──────────────┴─────────┴──────────┴────────┴─────────┘
         Total: 1060 bytes per record
```

| Field          | Size    | Description                                               |
|----------------|---------|-----------------------------------------------------------|
| `free`         | 1 byte  | `0x00` = in use, `0x01` = deleted/free                    |
| `keyLen`       | 2 bytes | Actual key length (uint16, 0-1024)                        |
| `key`          | 1024 bytes | Key string, null-padded                                |
| `lastAccessed` | 8 bytes | Unix timestamp (int64), for LRU                           |
| `cas`          | 8 bytes | CAS token (uint64)                                        |
| `expiry`       | 8 bytes | Unix timestamp in **milliseconds** (int64), 0 = no expiry |
| `bucket`       | 1 byte  | Data bucket index (0-15)                                  |
| `slotIdx`      | 8 bytes | Slot index within the bucket (int64)                      |

**keyId** = record index = file offset / 1060

---

#### Data File Format (`data`)

Create 16 data files each holding a different size bucket. 
Start at 1024 bytes and double the size for each file.

```
┌─────────┬──────────┬─────────────────────┐
│  free   │  length  │        data         │
│ 1 byte  │ 4 bytes  │   [length] bytes    │
└─────────┴──────────┴─────────────────────┘
```

| Field          | Size    | Description                            |
|----------------|---------|----------------------------------------|
| `free`         | 1 byte  | `0x00` = in use, `0x01` = deleted/free |
| `length`       | 4 bytes | Data length (uint32), max bucket size  |
| `data`         | bucket size | Raw value bytes                    |

**Slot sizes**: Total slot = `5 + bucket_size` bytes. Buckets: 1KB, 2KB, ..., 64MB.

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
2. If `root.expiry <= now`: pop, mark keys record as free, mark data record as free
3. Repeat until root is not expired or heap is empty

#### 3. LRU List (Access-Order Eviction)
- **In-memory**: Doubly-linked list ordered by `lastAccessed`
- **On update**: Move node to head, update `lastAccessed` in keys file via fseek
- **On eviction**: Remove from tail, mark records as free

#### 4. In-Memory Free Lists (O(1) Allocation)

| File | Free List Structure    | Allocation Strategy                         |
|------|------------------------|---------------------------------------------|
| `keys` | Stack of free keyIds | Pop from stack (any slot works, fixed size) |
| `data` | Size-bucketed lists  | Best-fit from smallest sufficient bucket    |

**On delete**: Push freed slot onto appropriate free list  
**On insert**: Pop from free list, or append to file if empty

## Success Criteria
1.  **Persistence**: Data survives process restarts
2.  **Performance**: Fast lookups via B-Tree, 100k RPS GET, 50k RPS SET (hot keys)
3.  **Protocol**: Compatible with Memcached clients (Text and Binary protocols)
4.  **Simplicity**: Easy to understand and maintain, small code base
5.  **Compatibility**: Works with PHP Memcached session handler

**Files**: `keys` and `data` in the configured data directory.
