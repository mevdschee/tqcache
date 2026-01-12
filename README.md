# TQSession

TQSession is a high-performance, persistent session storage server for PHP. It provides a Memcached-compatible interface
with disk-based persistence, making it ideal for session storage that survives restarts.

## Features

- **Persistent Storage**: Data stored on disk, survives server restarts
- **Memcached Compatible**: Works with PHP's native `memcached` session handler
- **Protocol Support**: Both Memcached text and binary protocols
- **TTL Support**: Millisecond-precision key expiration
- **CAS Operations**: Compare-and-swap for atomic updates
- **Single-worker architecture**: Simple and predictable performance

## Requirements

- Go 1.21 or later

## Installation

```bash
go install github.com/mevdschee/tqsession/cmd/tqsession@latest
```

Or build from source:

```bash
git clone https://github.com/mevdschee/tqsession.git
cd tqsession
go build -o tqsession ./cmd/tqsession
```

## Usage

```bash
tqsession [options]
```

### Command-Line Flags

| Flag             | Default     | Description                                                    |
|------------------|-------------|----------------------------------------------------------------|
| `-config`        |             | Path to INI config file (overrides other flags)                |
| `-listen`        | `:11211`    | Address to listen on (`[host]:port`)                           |
| `-data-dir`      | `data`      | Directory for persistent data files                            |
| `-shards`        | `16`        | Number of shards for parallel access                           |
| `-default-ttl`   | `0`         | Default TTL for keys (`0` = no expiry)                         |
| `-max-ttl`       | `24h`       | Maximum TTL cap for any key (`0` = unlimited)                  |
| `-max-data-size` | `64MB`      | Max live data size in bytes for LRU eviction (`0` = unlimited) |
| `-sync-mode`     | `periodic`  | Sync mode: `none`, `periodic`, `always`                        |
| `-sync-interval` | `1s`        | Interval between fsync calls (when periodic)                   |

**Fixed limits:** Max key size is 1KB, max value size is 64MB.

NB: You may also use a config file instead of CLI flags (see [cmd/tqsession/tqsession.conf](cmd/tqsession/tqsession.conf)).

## PHP Configuration

Configure PHP to use TQSession as the session handler:

```ini
session.save_handler = memcached
session.save_path = "localhost:11211"
```

NB: Set "max-data-size = 0" and "max-ttl = 24h" to prevent data loss and prevent disk space exhaustion.

## Performance

**TQSession vs Redis vs Memcached**

Benchmarks were run on a local development environment (Linux, Loopback).
100,000 keys were used for the benchmark, each with a size of 1KB.

### Benchmark Results

![Performance Benchmark](benchmarks/getset/getset_benchmark.png)

### Performance Highlights
- **Write (SET)**: TQSession outperforms Redis (~104k RPS vs ~52k RPS) in writing persistence-backed data (Periodic Sync).
- **Read (GET)**: TQSession outperforms Redis (~137k RPS vs ~114k RPS) in reading from memory.
- **Memory Efficiency**: TQSession uses 14x less memory than Redis (~58MB vs ~784MB) as it only stores keys+offsets in RAM.

### Summary Table

| Reference | SET (RPS) | GET (RPS) | Memory (MB) |
| :--- | :--- | :--- | :--- |
| **Memcached** (Memory) | ~138k | ~303k | ~681MB |
| **Redis** (Periodic) | ~52k | ~114k | ~784MB |
| **TQSession** (Periodic) | ~104k | ~137k | ~58MB |

## Testing

```bash
go test ./pkg/tqsession/...
```

## Profiling

A pprof server runs on `localhost:6062` for profiling:

```bash
go tool pprof http://localhost:6062/debug/pprof/profile?seconds=30
```

## Architecture

TQSession stores session data on disk in a fixed-size record formats and
holds several memory data structures to speed up access. It assumes SSD
performance with good random I/O and enough free memory to let the OS keep
the disk blocks in the cache. It does not perform any disk I/O optimization
and does not use append-only files. 

See [PROJECT_BRIEF.md](PROJECT_BRIEF.md) for detailed architecture.