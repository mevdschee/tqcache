package tqsession

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// SyncStrategy defines how strictly the cache should be persisted to disk
type SyncStrategy int

const (
	// SyncNone lets the OS decide when to flush modifications to disk
	SyncNone SyncStrategy = iota
	// SyncAlways forces an fsync after every write (not used in single-worker model)
	SyncAlways
	// SyncPeriodic forces an fsync at a regular interval
	SyncPeriodic
)

// Config holds the configuration for TQSession
type Config struct {
	DataDir       string
	DefaultExpiry time.Duration
	MaxKeySize    int
	MaxValueSize  int
	MaxDataSize   int64 // Maximum live data size in bytes before LRU eviction (0=unlimited)
	SyncStrategy  SyncStrategy
	SyncInterval  time.Duration
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		DataDir:       "data",
		DefaultExpiry: 0,
		MaxKeySize:    250,
		MaxValueSize:  1024 * 1024, // 1MB
		MaxDataSize:   0,           // Unlimited
		SyncStrategy:  SyncPeriodic,
		SyncInterval:  1 * time.Second,
	}
}

// Cache is the main TQSession cache
type Cache struct {
	config    Config
	storage   *Storage
	worker    *Worker
	stopSync  chan struct{}
	wg        sync.WaitGroup
	StartTime time.Time
}

// New creates a new TQSession cache
func New(cfg Config) (*Cache, error) {
	storage, err := NewStorage(cfg.DataDir, cfg.SyncStrategy == SyncAlways)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	worker, err := NewWorker(storage, cfg.DefaultExpiry, cfg.MaxDataSize)
	if err != nil {
		storage.Close()
		return nil, fmt.Errorf("failed to create worker: %w", err)
	}

	c := &Cache{
		config:    cfg,
		storage:   storage,
		worker:    worker,
		stopSync:  make(chan struct{}),
		StartTime: time.Now(),
	}

	// Start the worker
	worker.Start()

	// Start sync worker if periodic
	if cfg.SyncStrategy == SyncPeriodic {
		c.wg.Add(1)
		go c.runSyncWorker()
	}

	return c, nil
}

// runSyncWorker periodically syncs files to disk
func (c *Cache) runSyncWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.storage.Sync()
		case <-c.stopSync:
			return
		}
	}
}

// Close shuts down the cache
func (c *Cache) Close() error {
	// Stop worker and wait for it
	c.worker.Stop()

	// Stop sync worker and wait for it
	if c.config.SyncStrategy == SyncPeriodic {
		close(c.stopSync)
		c.wg.Wait()
	}

	// Final sync
	c.storage.Sync()

	return c.storage.Close()
}

// sendRequest sends a request to the worker and waits for response
func (c *Cache) sendRequest(req *Request) *Response {
	req.RespChan = make(chan *Response, 1)
	c.worker.RequestChan() <- req
	return <-req.RespChan
}

// Get retrieves a value from the cache
func (c *Cache) Get(key string) ([]byte, uint64, error) {
	resp := c.sendRequest(&Request{
		Op:  OpGet,
		Key: key,
	})
	if resp.Err == ErrKeyNotFound {
		return nil, 0, os.ErrNotExist
	}
	return resp.Value, resp.Cas, resp.Err
}

// Set stores a value in the cache
func (c *Cache) Set(key string, value []byte, ttl time.Duration) (uint64, error) {
	resp := c.sendRequest(&Request{
		Op:    OpSet,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
	return resp.Cas, resp.Err
}

// Add stores a value only if it doesn't already exist
func (c *Cache) Add(key string, value []byte, ttl time.Duration) (uint64, error) {
	resp := c.sendRequest(&Request{
		Op:    OpAdd,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
	if resp.Err == ErrKeyExists {
		return 0, os.ErrExist
	}
	return resp.Cas, resp.Err
}

// Replace stores a value only if it already exists
func (c *Cache) Replace(key string, value []byte, ttl time.Duration) (uint64, error) {
	resp := c.sendRequest(&Request{
		Op:    OpReplace,
		Key:   key,
		Value: value,
		TTL:   ttl,
	})
	if resp.Err == ErrKeyNotFound {
		return 0, os.ErrNotExist
	}
	return resp.Cas, resp.Err
}

// Cas stores a value only if CAS matches
func (c *Cache) Cas(key string, value []byte, ttl time.Duration, cas uint64) (uint64, error) {
	resp := c.sendRequest(&Request{
		Op:    OpCas,
		Key:   key,
		Value: value,
		TTL:   ttl,
		Cas:   cas,
	})
	if resp.Err == ErrCasMismatch {
		return 0, os.ErrExist
	}
	if resp.Err == ErrKeyNotFound {
		return 0, os.ErrNotExist
	}
	return resp.Cas, resp.Err
}

// Delete removes a key from the cache
func (c *Cache) Delete(key string) error {
	resp := c.sendRequest(&Request{
		Op:  OpDelete,
		Key: key,
	})
	if resp.Err == ErrKeyNotFound {
		return os.ErrNotExist
	}
	return resp.Err
}

// Touch updates the TTL of an existing item
func (c *Cache) Touch(key string, ttl time.Duration) (uint64, error) {
	resp := c.sendRequest(&Request{
		Op:  OpTouch,
		Key: key,
		TTL: ttl,
	})
	if resp.Err == ErrKeyNotFound {
		return 0, os.ErrNotExist
	}
	return resp.Cas, resp.Err
}

// Increment increments a numeric value
func (c *Cache) Increment(key string, delta uint64) (uint64, uint64, error) {
	resp := c.sendRequest(&Request{
		Op:    OpIncr,
		Key:   key,
		Delta: delta,
	})
	if resp.Err == ErrKeyNotFound {
		return 0, 0, os.ErrNotExist
	}
	// Parse the value
	var val uint64
	for _, b := range resp.Value {
		if b >= '0' && b <= '9' {
			val = val*10 + uint64(b-'0')
		}
	}
	return val, resp.Cas, resp.Err
}

// Decrement decrements a numeric value
func (c *Cache) Decrement(key string, delta uint64) (uint64, uint64, error) {
	resp := c.sendRequest(&Request{
		Op:    OpDecr,
		Key:   key,
		Delta: delta,
	})
	if resp.Err == ErrKeyNotFound {
		return 0, 0, os.ErrNotExist
	}
	// Parse the value
	var val uint64
	for _, b := range resp.Value {
		if b >= '0' && b <= '9' {
			val = val*10 + uint64(b-'0')
		}
	}
	return val, resp.Cas, resp.Err
}

// Append appends data to an existing value
func (c *Cache) Append(key string, value []byte) (uint64, error) {
	resp := c.sendRequest(&Request{
		Op:    OpAppend,
		Key:   key,
		Value: value,
	})
	if resp.Err == ErrKeyNotFound {
		return 0, os.ErrNotExist
	}
	return resp.Cas, resp.Err
}

// Prepend prepends data to an existing value
func (c *Cache) Prepend(key string, value []byte) (uint64, error) {
	resp := c.sendRequest(&Request{
		Op:    OpPrepend,
		Key:   key,
		Value: value,
	})
	if resp.Err == ErrKeyNotFound {
		return 0, os.ErrNotExist
	}
	return resp.Cas, resp.Err
}

// FlushAll invalidates all items
func (c *Cache) FlushAll() {
	c.sendRequest(&Request{Op: OpFlushAll})
}

// Stats returns cache statistics
func (c *Cache) Stats() map[string]string {
	resp := c.sendRequest(&Request{Op: OpStats})
	return resp.Stats
}
