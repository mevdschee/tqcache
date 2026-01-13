package tqsession

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

// Config holds the configuration for TQSession
type Config struct {
	DataDir         string
	DefaultTTL      time.Duration
	MaxTTL          time.Duration
	MaxKeySize      int
	MaxValueSize    int
	MaxDataSize     int64
	SyncStrategy    SyncStrategy
	SyncInterval    time.Duration
	ChannelCapacity int // Request channel capacity per worker (default 1000)
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		DataDir:         "/tmp/tqsession",
		DefaultTTL:      24 * time.Hour,
		MaxTTL:          7 * 24 * time.Hour,
		MaxKeySize:      250,
		MaxValueSize:    1 << 20, // 1MB
		MaxDataSize:     0,       // Unlimited
		SyncStrategy:    SyncPeriodic,
		SyncInterval:    1 * time.Second,
		ChannelCapacity: 1000, // Default channel capacity
	}
}
