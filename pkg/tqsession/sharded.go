package tqsession

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"time"
)

const (
	// DefaultShardCount is the default number of shards
	DefaultShardCount = 16
)

// ShardedCache wraps multiple Cache instances for concurrent access.
// Keys are distributed across shards using FNV-1a hash.
type ShardedCache struct {
	shards    []*Cache
	config    Config
	stopSync  chan struct{}
	StartTime time.Time
}

// NewSharded creates a new sharded cache with the number of shards from config.
// Each shard gets its own subfolder (shard_00, shard_01, ...).
func NewSharded(cfg Config, shardCount int) (*ShardedCache, error) {
	if shardCount <= 0 {
		shardCount = DefaultShardCount
	}

	sc := &ShardedCache{
		shards:    make([]*Cache, shardCount),
		config:    cfg,
		stopSync:  make(chan struct{}),
		StartTime: time.Now(),
	}

	// Create a shard for each index
	for i := 0; i < shardCount; i++ {
		shardDir := filepath.Join(cfg.DataDir, fmt.Sprintf("shard_%02d", i))
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			// Cleanup on failure
			for j := 0; j < i; j++ {
				sc.shards[j].Close()
			}
			return nil, fmt.Errorf("failed to create shard dir %d: %w", i, err)
		}

		shardCfg := cfg
		shardCfg.DataDir = shardDir
		// Disable per-shard sync workers - we'll run one at the top level
		shardCfg.SyncStrategy = SyncNone

		shard, err := New(shardCfg)
		if err != nil {
			for j := 0; j < i; j++ {
				sc.shards[j].Close()
			}
			return nil, fmt.Errorf("failed to create shard %d: %w", i, err)
		}
		sc.shards[i] = shard
	}

	// Start sync worker if periodic
	if cfg.SyncStrategy == SyncPeriodic {
		go sc.runSyncWorker()
	}

	return sc, nil
}

// shardFor returns the shard index for the given key using FNV-1a hash.
func (sc *ShardedCache) shardFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % len(sc.shards)
}

func (sc *ShardedCache) runSyncWorker() {
	ticker := time.NewTicker(sc.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for _, shard := range sc.shards {
				shard.storage.Sync()
			}
		case <-sc.stopSync:
			return
		}
	}
}

// Close closes all shards.
func (sc *ShardedCache) Close() error {
	if sc.config.SyncStrategy == SyncPeriodic {
		close(sc.stopSync)
	}

	var err error
	for _, shard := range sc.shards {
		if e := shard.Close(); e != nil {
			err = e
		}
	}
	return err
}

// Get retrieves a value from the cache.
func (sc *ShardedCache) Get(key string) ([]byte, uint64, error) {
	return sc.shards[sc.shardFor(key)].Get(key)
}

// Set stores a value in the cache.
func (sc *ShardedCache) Set(key string, value []byte, ttl time.Duration) (uint64, error) {
	return sc.shards[sc.shardFor(key)].Set(key, value, ttl)
}

// Add stores a value only if it doesn't already exist.
func (sc *ShardedCache) Add(key string, value []byte, ttl time.Duration) (uint64, error) {
	return sc.shards[sc.shardFor(key)].Add(key, value, ttl)
}

// Replace stores a value only if it already exists.
func (sc *ShardedCache) Replace(key string, value []byte, ttl time.Duration) (uint64, error) {
	return sc.shards[sc.shardFor(key)].Replace(key, value, ttl)
}

// Cas stores a value only if CAS matches.
func (sc *ShardedCache) Cas(key string, value []byte, ttl time.Duration, cas uint64) (uint64, error) {
	return sc.shards[sc.shardFor(key)].Cas(key, value, ttl, cas)
}

// Delete removes a key from the cache.
func (sc *ShardedCache) Delete(key string) error {
	return sc.shards[sc.shardFor(key)].Delete(key)
}

// Touch updates the TTL of an existing item.
func (sc *ShardedCache) Touch(key string, ttl time.Duration) (uint64, error) {
	return sc.shards[sc.shardFor(key)].Touch(key, ttl)
}

// Increment increments a numeric value.
func (sc *ShardedCache) Increment(key string, delta uint64) (uint64, uint64, error) {
	return sc.shards[sc.shardFor(key)].Increment(key, delta)
}

// Decrement decrements a numeric value.
func (sc *ShardedCache) Decrement(key string, delta uint64) (uint64, uint64, error) {
	return sc.shards[sc.shardFor(key)].Decrement(key, delta)
}

// Append appends data to an existing value.
func (sc *ShardedCache) Append(key string, value []byte) (uint64, error) {
	return sc.shards[sc.shardFor(key)].Append(key, value)
}

// Prepend prepends data to an existing value.
func (sc *ShardedCache) Prepend(key string, value []byte) (uint64, error) {
	return sc.shards[sc.shardFor(key)].Prepend(key, value)
}

// FlushAll invalidates all items.
func (sc *ShardedCache) FlushAll() {
	for _, shard := range sc.shards {
		shard.FlushAll()
	}
}

// Stats returns cache statistics.
func (sc *ShardedCache) Stats() map[string]string {
	totalItems := 0
	totalBytes := int64(0)

	for _, shard := range sc.shards {
		totalItems += shard.index.Count()
		totalBytes += shard.liveDataSize
	}

	stats := make(map[string]string)
	stats["curr_items"] = fmt.Sprintf("%d", totalItems)
	stats["bytes"] = fmt.Sprintf("%d", totalBytes)
	return stats
}

// LiveDataSize returns the total live data size across all shards.
func (sc *ShardedCache) LiveDataSize() int64 {
	var total int64
	for _, shard := range sc.shards {
		total += shard.LiveDataSize()
	}
	return total
}

// GetStartTime returns when the cache was started
func (sc *ShardedCache) GetStartTime() time.Time {
	return sc.StartTime
}
