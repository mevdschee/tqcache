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
listen = :11211

[storage]
data-dir = data
default-ttl = 0s
max-data-size = 64MB
sync-mode = periodic
sync-interval = 1s
```

See [cmd/tqsession/tqsession.conf](cmd/tqsession/tqsession.conf) for a complete example.

## PHP Configuration

Configure PHP to use TQSession as the session handler:

```ini
session.save_handler = memcached
session.save_path = "localhost:11211"
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

TQSession stores session data on disk in a fixed-size record formats and
holds several memory data structures to speed up access. It assumes SSD
performance with good random I/O and enough free memory to let the OS keep
the disk blocks in the cache. It does not perform any disk I/O optimization
and does not use append-only files. 

See [PROJECT_BRIEF.md](PROJECT_BRIEF.md) for detailed architecture.