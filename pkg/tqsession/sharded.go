package tqsession

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	// DefaultShardCount is the default number of shards
	DefaultShardCount = 16
)

// ShardedCache wraps multiple Worker instances for concurrent access.
// Keys are distributed across shards using FNV-1a hash.
// Each shard is operated by a dedicated goroutine, eliminating lock contention.
type ShardedCache struct {
	workers   []*Worker
	config    Config
	syncChan  chan int // Channel for sync requests (worker index)
	stopSync  chan struct{}
	StartTime time.Time
}

// NewSharded creates a new sharded cache with the number of shards from config.
// Each shard gets its own subfolder (shard_00, shard_01, ...) and a dedicated worker goroutine.
func NewSharded(cfg Config, shardCount int) (*ShardedCache, error) {
	if shardCount <= 0 {
		shardCount = DefaultShardCount
	}

	// Set GOMAXPROCS for optimal parallelism: max(min(cpucount,shards/4), 1)
	gomaxprocs := runtime.NumCPU()
	if gomaxprocs > shardCount/4 {
		gomaxprocs = shardCount / 4
	}
	if gomaxprocs < 1 {
		gomaxprocs = 1
	}
	runtime.GOMAXPROCS(gomaxprocs)

	sc := &ShardedCache{
		workers:   make([]*Worker, shardCount),
		config:    cfg,
		syncChan:  make(chan int, shardCount*2), // Buffered to avoid blocking workers
		stopSync:  make(chan struct{}),
		StartTime: time.Now(),
	}

	// Create a worker for each shard
	for i := 0; i < shardCount; i++ {
		shardDir := filepath.Join(cfg.DataDir, fmt.Sprintf("shard_%02d", i))
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			// Cleanup on failure
			for j := 0; j < i; j++ {
				sc.workers[j].Close()
			}
			return nil, fmt.Errorf("failed to create shard dir %d: %w", i, err)
		}

		// Create storage for this shard
		storage, err := NewStorage(shardDir, cfg.SyncStrategy == SyncAlways)
		if err != nil {
			for j := 0; j < i; j++ {
				sc.workers[j].Close()
			}
			return nil, fmt.Errorf("failed to create storage for shard %d: %w", i, err)
		}

		// Create worker with storage
		worker, err := NewWorker(storage, cfg.DefaultTTL, cfg.MaxDataSize/int64(shardCount), cfg.ChannelCapacity)
		if err != nil {
			storage.Close()
			for j := 0; j < i; j++ {
				sc.workers[j].Close()
			}
			return nil, fmt.Errorf("failed to create worker for shard %d: %w", i, err)
		}

		// Set up sync notification for periodic mode
		if cfg.SyncStrategy == SyncPeriodic {
			workerIdx := i // Capture for closure
			worker.SetSyncInterval(cfg.SyncInterval)
			worker.SetSyncNotify(func() {
				// Non-blocking send to sync channel
				select {
				case sc.syncChan <- workerIdx:
				default:
					// Channel full, sync already pending
				}
			})
		}

		// Start the worker goroutine
		worker.Start()
		sc.workers[i] = worker
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
	return int(h.Sum32()) % len(sc.workers)
}

// runSyncWorker processes sync requests from workers
func (sc *ShardedCache) runSyncWorker() {
	for {
		select {
		case workerIdx := <-sc.syncChan:
			worker := sc.workers[workerIdx]
			worker.Sync()
			worker.MarkSynced()
		case <-sc.stopSync:
			return
		}
	}
}

// Close closes all workers.
func (sc *ShardedCache) Close() error {
	if sc.config.SyncStrategy == SyncPeriodic {
		close(sc.stopSync)
	}

	var err error
	for _, worker := range sc.workers {
		if e := worker.Close(); e != nil {
			err = e
		}
	}
	return err
}

// sendRequest sends a request to the appropriate worker and waits for response.
func (sc *ShardedCache) sendRequest(shardIdx int, req *Request) *Response {
	req.RespChan = make(chan *Response, 1)
	sc.workers[shardIdx].RequestChan() <- req
	return <-req.RespChan
}

// Get retrieves a value from the cache.
func (sc *ShardedCache) Get(key string) ([]byte, uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:  OpGet,
		Key: key,
	})
	return resp.Value, resp.Cas, resp.Err
}

// Set stores a value in the cache.
func (sc *ShardedCache) Set(key string, value []byte, ttl time.Duration) (uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:    OpSet,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
	return resp.Cas, resp.Err
}

// Add stores a value only if it doesn't already exist.
func (sc *ShardedCache) Add(key string, value []byte, ttl time.Duration) (uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:    OpAdd,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
	return resp.Cas, resp.Err
}

// Replace stores a value only if it already exists.
func (sc *ShardedCache) Replace(key string, value []byte, ttl time.Duration) (uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:    OpReplace,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
	return resp.Cas, resp.Err
}

// Cas stores a value only if CAS matches.
func (sc *ShardedCache) Cas(key string, value []byte, ttl time.Duration, cas uint64) (uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:    OpCas,
		Key:   key,
		Value: value,
		TTL:   ttl,
		Cas:   cas,
	})
	return resp.Cas, resp.Err
}

// Delete removes a key from the cache.
func (sc *ShardedCache) Delete(key string) error {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:  OpDelete,
		Key: key,
	})
	return resp.Err
}

// Touch updates the TTL of an existing item.
func (sc *ShardedCache) Touch(key string, ttl time.Duration) (uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:  OpTouch,
		Key: key,
		TTL: ttl,
	})
	return resp.Cas, resp.Err
}

// Increment increments a numeric value.
func (sc *ShardedCache) Increment(key string, delta uint64) (uint64, uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:    OpIncr,
		Key:   key,
		Delta: delta,
	})
	// Parse value as uint64
	var val uint64
	for _, b := range resp.Value {
		if b >= '0' && b <= '9' {
			val = val*10 + uint64(b-'0')
		}
	}
	return val, resp.Cas, resp.Err
}

// Decrement decrements a numeric value.
func (sc *ShardedCache) Decrement(key string, delta uint64) (uint64, uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:    OpDecr,
		Key:   key,
		Delta: delta,
	})
	// Parse value as uint64
	var val uint64
	for _, b := range resp.Value {
		if b >= '0' && b <= '9' {
			val = val*10 + uint64(b-'0')
		}
	}
	return val, resp.Cas, resp.Err
}

// Append appends data to an existing value.
func (sc *ShardedCache) Append(key string, value []byte) (uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:    OpAppend,
		Key:   key,
		Value: value,
	})
	return resp.Cas, resp.Err
}

// Prepend prepends data to an existing value.
func (sc *ShardedCache) Prepend(key string, value []byte) (uint64, error) {
	resp := sc.sendRequest(sc.shardFor(key), &Request{
		Op:    OpPrepend,
		Key:   key,
		Value: value,
	})
	return resp.Cas, resp.Err
}

// FlushAll invalidates all items.
func (sc *ShardedCache) FlushAll() {
	for i := range sc.workers {
		sc.sendRequest(i, &Request{Op: OpFlushAll})
	}
}

// Stats returns cache statistics.
func (sc *ShardedCache) Stats() map[string]string {
	totalItems := 0
	totalBytes := int64(0)

	for _, worker := range sc.workers {
		totalItems += worker.Index().Count()
		totalBytes += worker.LiveDataSize()
	}

	stats := make(map[string]string)
	stats["curr_items"] = fmt.Sprintf("%d", totalItems)
	stats["bytes"] = fmt.Sprintf("%d", totalBytes)
	return stats
}

// LiveDataSize returns the total live data size across all shards.
func (sc *ShardedCache) LiveDataSize() int64 {
	var total int64
	for _, worker := range sc.workers {
		total += worker.LiveDataSize()
	}
	return total
}

// GetStartTime returns when the cache was started
func (sc *ShardedCache) GetStartTime() time.Time {
	return sc.StartTime
}
