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
| `-data-dir`      | `data`      | Directory for persistent data files                            |
| `-listen`        | `:11211`    | Address to listen on (`[host]:port`)                           |
| `-sync-mode`     | `periodic`  | Sync mode: `none`, `periodic`                                  |
| `-sync-interval` | `1s`        | Interval between fsync calls (when periodic)                   |
| `-default-ttl`   | `0`         | Default TTL for keys (`0` = no expiry)                         |
| `-max-data-size` | `67108864`  | Max live data size in bytes for LRU eviction (`0` = unlimited) |

**Fixed limits:** Max key size is 1KB, max value size is 64MB.

### Config File

You can use an INI-style config file instead of CLI flags:

```ini
# tqsession.conf
[server]
port = :11211

[storage]
data_dir = data
default_expiry = 0s
max_data_size = 64MB
sync_strategy = periodic
sync_interval = 1s
```

See [cmd/tqsession/tqsession.conf](cmd/tqsession/tqsession.conf) for a complete example.

### Examples

```bash
# Start with defaults
tqsession

# Use config file
tqsession -config /etc/tqsession.conf

# Custom port and data directory
tqsession -port :11212 -data-dir /var/lib/tqsession

# Set default TTL to 24 hours
tqsession -default-ttl 24h

# Increase max data size to 1GB
tqsession -max-data-size 1073741824
```

## PHP Configuration

Configure PHP to use TQSession as the session handler:

```ini
session.save_handler = memcached
session.save_path = "localhost:11211"
```

Or with PHP-FPM:

```ini
php_admin_value[session.save_handler] = memcached
php_admin_value[session.save_path] = "localhost:11211"
```

## Benchmarks

Run the included benchmark:

```bash
cd benchmarks/getset
./getset_benchmark.sh
```

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

TQSession uses a **single-worker architecture** for simplicity and predictable performance:

| Goroutine   | Responsibility                                             |
|-------------|------------------------------------------------------------|
| **Server**  | Accepts client connections, routes to Storage worker       |
| **Storage** | Handles all file operations sequentially (no locks needed) |
| **Sync**    | Calls `fsync` periodically (configurable interval)         |

### Storage Format

**Keys file** (`keys`): Fixed 1060-byte records
- 2-byte key length, key string (1024 bytes), CAS, expiry (ms), bucket/slot pointers

**Data files** (`data_00` - `data_15`): 16 size-bucketed files (1KB → 64MB)
- Each slot stores: free flag + length + data

### In-Memory Structures

- **B-tree index**: O(log n) key lookups
- **Expiry min-heap**: Efficient TTL cleanup without scanning
- **LRU list**: Evicts least recently used items when size limit reached (expired first)
- **Free lists**: O(1) slot allocation via stack-based recycling

See [PROJECT_BRIEF.md](PROJECT_BRIEF.md) for detailed architecture.