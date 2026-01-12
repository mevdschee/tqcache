# TQSession

TQSession is a high-performance, persistent session storage system implemented in Go. It allows usage both as an **embeddable Go library** and as a **standalone server application** (CLI).

Blog post: 

## Features

- **Persistence**: Built around disk storage
- **Protocol Support**:
    - Memcached Text Protocol (Legacy & Debugging)
    - Memcached Binary Protocol (Modern standard, default for PHP 7+)
- **Session Handler Ready**: Fully compatible with PHP's native `memcached` session handler.
- **TTL Support**: Built-in support for key expiration (Time To Live).

## Architecture

See [PROJECT_BRIEF.md](PROJECT_BRIEF.md) for architectural details.
