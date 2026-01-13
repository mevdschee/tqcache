package tqcache

import "time"

// SyncStrategy defines how strictly the cache should be persisted to disk
type SyncStrategy int

const (
	// SyncNone lets the OS decide when to flush modifications to disk
	SyncNone SyncStrategy = iota
	// SyncAlways forces an fsync after every write
	SyncAlways
	// SyncPeriodic forces an fsync at a regular interval
	SyncPeriodic
)

// Default configuration values (single source of truth)
const (
	DefaultShardCount      = 16
	DefaultChannelCapacity = 1000
	DefaultSyncInterval    = 1 * time.Second
)

// Config holds the configuration for TQCache
type Config struct {
	DataDir         string
	DefaultTTL      time.Duration
	MaxTTL          time.Duration
	MaxKeySize      int
	MaxValueSize    int
	SyncStrategy    SyncStrategy
	SyncInterval    time.Duration
	ChannelCapacity int // Request channel capacity per worker (default 1000)
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		DataDir:         "data",
		DefaultTTL:      0,
		MaxTTL:          7 * 24 * time.Hour,
		MaxKeySize:      1 << 10, // 1KB
		MaxValueSize:    1 << 20, // 1MB
		SyncStrategy:    SyncPeriodic,
		SyncInterval:    DefaultSyncInterval,
		ChannelCapacity: DefaultChannelCapacity,
	}
}
