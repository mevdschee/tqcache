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
	// SyncAlways forces an fsync after every write
	SyncAlways
	// SyncPeriodic forces an fsync at a regular interval
	SyncPeriodic
)

// Config holds the configuration for TQSession
type Config struct {
	DataDir       string
	DefaultExpiry time.Duration
	MaxTTL        time.Duration // Maximum TTL for any key (0=unlimited)
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
		MaxTTL:        0, // Unlimited
		MaxKeySize:    250,
		MaxValueSize:  1024 * 1024, // 1MB
		MaxDataSize:   0,           // Unlimited
		SyncStrategy:  SyncPeriodic,
		SyncInterval:  1 * time.Second,
	}
}

// Cache is the main TQSession cache with RWMutex-based concurrency
type Cache struct {
	config  Config
	storage *Storage
	index   *Index

	// RWMutex for index access
	mu sync.RWMutex

	// Slot counters
	nextKeyId  int64
	nextSlotId [NumBuckets]int64

	// State
	liveDataSize  int64
	maxDataSize   int64
	defaultExpiry time.Duration
	maxTTL        time.Duration
	StartTime     time.Time

	// Sync control
	stopSync chan struct{}
	wg       sync.WaitGroup
}

// New creates a new TQSession cache
func New(cfg Config) (*Cache, error) {
	storage, err := NewStorage(cfg.DataDir, cfg.SyncStrategy == SyncAlways)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	c := &Cache{
		config:        cfg,
		storage:       storage,
		index:         NewIndex(),
		maxDataSize:   cfg.MaxDataSize,
		defaultExpiry: cfg.DefaultExpiry,
		maxTTL:        cfg.MaxTTL,
		stopSync:      make(chan struct{}),
		StartTime:     time.Now(),
	}

	// Recover index from disk
	if err := c.recoverIndex(); err != nil {
		storage.Close()
		return nil, fmt.Errorf("failed to recover index: %w", err)
	}

	// Start sync worker if periodic
	if cfg.SyncStrategy == SyncPeriodic {
		c.wg.Add(1)
		go c.runSyncWorker()
	}

	return c, nil
}

// recoverIndex rebuilds the in-memory index from disk
func (c *Cache) recoverIndex() error {
	keyCount, err := c.storage.KeyCount()
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()

	for keyId := int64(0); keyId < keyCount; keyId++ {
		rec, err := c.storage.ReadKeyRecord(keyId)
		if err != nil {
			continue
		}

		// Extract key
		keyBytes := rec.Key[:]
		nullIdx := 0
		for i, b := range keyBytes {
			if b == 0 {
				nullIdx = i
				break
			}
			if i == len(keyBytes)-1 {
				nullIdx = len(keyBytes)
			}
		}
		key := string(keyBytes[:nullIdx])

		// Skip expired entries
		if rec.Expiry > 0 && rec.Expiry <= now {
			continue
		}

		entry := &IndexEntry{
			Key:          key,
			KeyId:        keyId,
			Bucket:       int(rec.Bucket),
			SlotIdx:      rec.SlotIdx,
			Expiry:       rec.Expiry,
			Cas:          rec.Cas,
			LastAccessed: rec.LastAccessed,
		}
		c.index.Set(entry)

		// Track data slot usage
		if rec.SlotIdx >= c.nextSlotId[rec.Bucket] {
			c.nextSlotId[rec.Bucket] = rec.SlotIdx + 1
		}

		// Estimate live data size
		c.liveDataSize += int64(c.storage.SlotSize(int(rec.Bucket)))
	}

	c.nextKeyId = keyCount
	return nil
}

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
	if c.config.SyncStrategy == SyncPeriodic {
		close(c.stopSync)
		c.wg.Wait()
	}
	c.storage.Sync()
	return c.storage.Close()
}

// Get retrieves a value from the cache
func (c *Cache) Get(key string) ([]byte, uint64, error) {
	c.mu.Lock()
	entry, exists := c.index.Get(key)
	if !exists {
		c.mu.Unlock()
		return nil, 0, os.ErrNotExist
	}

	// Check if expired
	now := time.Now().UnixMilli()
	if entry.Expiry > 0 && entry.Expiry <= now {
		c.deleteEntry(entry)
		c.mu.Unlock()
		return nil, 0, os.ErrNotExist
	}

	bucket := entry.Bucket
	slotIdx := entry.SlotIdx
	cas := entry.Cas

	// Update last accessed synchronously for correct LRU ordering
	entry.LastAccessed = time.Now().Unix()
	c.index.Set(entry)
	c.mu.Unlock()

	// Read data with bucket lock
	c.storage.dataMu[bucket].RLock()
	data, err := c.storage.ReadDataSlot(bucket, slotIdx)
	c.storage.dataMu[bucket].RUnlock()

	if err != nil {
		return nil, 0, err
	}

	return data, cas, nil
}

// Set stores a value in the cache
func (c *Cache) Set(key string, value []byte, ttl time.Duration) (uint64, error) {
	return c.doSet(key, value, ttl, 0, false)
}

func (c *Cache) doSet(key string, value []byte, ttl time.Duration, existingCas uint64, checkCas bool) (uint64, error) {
	if len(key) > MaxKeySize {
		return 0, ErrKeyTooLarge
	}

	bucket, err := c.storage.BucketForSize(len(value))
	if err != nil {
		return 0, err
	}

	now := time.Now()
	var expiry int64
	if ttl > 0 {
		// Apply max TTL cap if configured
		if c.maxTTL > 0 && ttl > c.maxTTL {
			ttl = c.maxTTL
		}
		expiry = now.Add(ttl).UnixMilli()
	} else if c.defaultExpiry > 0 {
		expiry = now.Add(c.defaultExpiry).UnixMilli()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if key exists
	existing, exists := c.index.Get(key)
	if checkCas {
		if !exists {
			return 0, ErrKeyNotFound
		}
		if existing.Cas != existingCas {
			return 0, ErrCasMismatch
		}
	}

	// Update live data size and evict if needed
	c.liveDataSize += int64(len(value))
	if exists {
		c.liveDataSize -= int64(existing.Length)
	}
	c.lruEvict()

	// Compact old slot if bucket changed
	if exists && existing.Bucket != bucket {
		c.compactDataSlot(existing.Bucket, existing.SlotIdx)
	}

	// Allocate key slot
	var keyId int64
	if exists {
		keyId = existing.KeyId
	} else {
		keyId = c.nextKeyId
		c.nextKeyId++
	}

	// Allocate data slot
	var slotIdx int64
	if exists && existing.Bucket == bucket {
		slotIdx = existing.SlotIdx
	} else {
		slotIdx = c.nextSlotId[bucket]
		c.nextSlotId[bucket]++
	}

	cas := uint64(now.UnixNano())

	// Write key record
	keyRec := &KeyRecord{
		KeyLen:       uint16(len(key)),
		LastAccessed: now.Unix(),
		Cas:          cas,
		Expiry:       expiry,
		Bucket:       byte(bucket),
		SlotIdx:      slotIdx,
	}
	copy(keyRec.Key[:], key)

	c.storage.keysMu.Lock()
	err = c.storage.WriteKeyRecord(keyId, keyRec)
	c.storage.keysMu.Unlock()
	if err != nil {
		return 0, err
	}

	// Write data
	c.storage.dataMu[bucket].Lock()
	err = c.storage.WriteDataSlot(bucket, slotIdx, value)
	c.storage.dataMu[bucket].Unlock()
	if err != nil {
		return 0, err
	}

	// Update index
	entry := &IndexEntry{
		Key:          key,
		KeyId:        keyId,
		Bucket:       bucket,
		SlotIdx:      slotIdx,
		Length:       len(value),
		Expiry:       expiry,
		Cas:          cas,
		LastAccessed: now.Unix(),
	}
	c.index.Set(entry)

	return cas, nil
}

// compactDataSlot moves tail to fill gap (call with mu locked)
func (c *Cache) compactDataSlot(bucket int, freedSlotIdx int64) {
	tailIdx := c.nextSlotId[bucket] - 1
	if tailIdx < 0 {
		return
	}

	if freedSlotIdx == tailIdx {
		c.nextSlotId[bucket]--
		c.storage.dataMu[bucket].Lock()
		c.storage.TruncateDataFile(bucket, c.nextSlotId[bucket])
		c.storage.dataMu[bucket].Unlock()
		return
	}

	c.storage.dataMu[bucket].Lock()
	tailData, err := c.storage.ReadDataSlot(bucket, tailIdx)
	if err != nil {
		c.storage.dataMu[bucket].Unlock()
		return
	}
	if err := c.storage.WriteDataSlot(bucket, freedSlotIdx, tailData); err != nil {
		c.storage.dataMu[bucket].Unlock()
		return
	}
	c.storage.dataMu[bucket].Unlock()

	// Update index for moved entry
	tailEntry := c.index.GetByBucketSlot(bucket, tailIdx)
	if tailEntry != nil {
		c.index.UpdateSlotIdx(tailEntry, freedSlotIdx)
		c.storage.keysMu.Lock()
		c.storage.UpdateSlotIdx(tailEntry.KeyId, freedSlotIdx)
		c.storage.keysMu.Unlock()
	}

	c.nextSlotId[bucket]--
	c.storage.dataMu[bucket].Lock()
	c.storage.TruncateDataFile(bucket, c.nextSlotId[bucket])
	c.storage.dataMu[bucket].Unlock()
}

// compactKeySlot moves tail key to fill gap (call with mu locked)
func (c *Cache) compactKeySlot(freedKeyId int64) {
	tailKeyId := c.nextKeyId - 1
	if tailKeyId < 0 {
		return
	}

	if freedKeyId == tailKeyId {
		c.nextKeyId--
		c.storage.keysMu.Lock()
		c.storage.TruncateKeysFile(c.nextKeyId)
		c.storage.keysMu.Unlock()
		return
	}

	c.storage.keysMu.Lock()
	tailRec, err := c.storage.ReadKeyRecord(tailKeyId)
	if err != nil {
		c.storage.keysMu.Unlock()
		return
	}
	if err := c.storage.WriteKeyRecord(freedKeyId, tailRec); err != nil {
		c.storage.keysMu.Unlock()
		return
	}
	c.storage.keysMu.Unlock()

	tailEntry := c.index.GetByKeyId(tailKeyId)
	if tailEntry != nil {
		c.index.UpdateKeyId(tailEntry, freedKeyId)
	}

	c.nextKeyId--
	c.storage.keysMu.Lock()
	c.storage.TruncateKeysFile(c.nextKeyId)
	c.storage.keysMu.Unlock()
}

// lruEvict evicts until under max data size (call with mu locked)
func (c *Cache) lruEvict() {
	if c.maxDataSize <= 0 {
		return
	}
	now := time.Now().UnixMilli()

	// Phase 1: Evict expired first
	for c.liveDataSize > c.maxDataSize {
		entry := c.index.expiryHeap.PeekMin()
		if entry == nil || entry.Expiry > now || entry.Expiry == 0 {
			break
		}
		indexEntry := c.index.GetByKeyId(entry.KeyId)
		if indexEntry != nil {
			c.deleteEntry(indexEntry)
		} else {
			c.index.expiryHeap.Remove(entry.KeyId)
		}
	}

	// Phase 2: LRU eviction
	for c.liveDataSize > c.maxDataSize {
		node := c.index.lruList.PopTail()
		if node == nil {
			break
		}
		indexEntry := c.index.GetByKeyId(node.KeyId)
		if indexEntry != nil {
			c.deleteEntry(indexEntry)
		}
	}
}

// deleteEntry removes an entry (call with mu locked)
func (c *Cache) deleteEntry(entry *IndexEntry) {
	c.index.Delete(entry.Key)
	c.liveDataSize -= int64(entry.Length)
	c.compactDataSlot(entry.Bucket, entry.SlotIdx)
	c.compactKeySlot(entry.KeyId)
}

// Delete removes a key from the cache
func (c *Cache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.index.Get(key)
	if !exists {
		return os.ErrNotExist
	}
	c.deleteEntry(entry)
	return nil
}

// Add stores a value only if it doesn't already exist
func (c *Cache) Add(key string, value []byte, ttl time.Duration) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index.Get(key); exists {
		return 0, os.ErrExist
	}

	c.mu.Unlock()
	cas, err := c.doSet(key, value, ttl, 0, false)
	c.mu.Lock() // Re-lock for deferred unlock
	return cas, err
}

// Replace stores a value only if it already exists
func (c *Cache) Replace(key string, value []byte, ttl time.Duration) (uint64, error) {
	c.mu.Lock()
	if _, exists := c.index.Get(key); !exists {
		c.mu.Unlock()
		return 0, os.ErrNotExist
	}
	c.mu.Unlock()
	return c.doSet(key, value, ttl, 0, false)
}

// Cas stores a value only if CAS matches
func (c *Cache) Cas(key string, value []byte, ttl time.Duration, cas uint64) (uint64, error) {
	newCas, err := c.doSet(key, value, ttl, cas, true)
	if err == ErrCasMismatch {
		return 0, os.ErrExist
	}
	if err == ErrKeyNotFound {
		return 0, os.ErrNotExist
	}
	return newCas, err
}

// Touch updates the TTL of an existing item
func (c *Cache) Touch(key string, ttl time.Duration) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.index.Get(key)
	if !exists {
		return 0, os.ErrNotExist
	}

	now := time.Now()
	var expiry int64
	if ttl > 0 {
		expiry = now.Add(ttl).UnixMilli()
	}

	entry.Cas = uint64(now.UnixNano())
	entry.LastAccessed = now.Unix()
	entry.Expiry = expiry
	c.index.Set(entry)

	return entry.Cas, nil
}

// Increment increments a numeric value
func (c *Cache) Increment(key string, delta uint64) (uint64, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.index.Get(key)
	if !exists {
		return 0, 0, os.ErrNotExist
	}

	c.storage.dataMu[entry.Bucket].RLock()
	data, err := c.storage.ReadDataSlot(entry.Bucket, entry.SlotIdx)
	c.storage.dataMu[entry.Bucket].RUnlock()
	if err != nil {
		return 0, 0, err
	}

	// Parse number
	var val uint64
	for _, b := range data {
		if b >= '0' && b <= '9' {
			val = val*10 + uint64(b-'0')
		}
	}
	val += delta

	// Convert back to bytes
	newVal := fmt.Sprintf("%d", val)
	c.mu.Unlock()
	cas, err := c.doSet(key, []byte(newVal), 0, 0, false)
	c.mu.Lock()
	return val, cas, err
}

// Decrement decrements a numeric value
func (c *Cache) Decrement(key string, delta uint64) (uint64, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.index.Get(key)
	if !exists {
		return 0, 0, os.ErrNotExist
	}

	c.storage.dataMu[entry.Bucket].RLock()
	data, err := c.storage.ReadDataSlot(entry.Bucket, entry.SlotIdx)
	c.storage.dataMu[entry.Bucket].RUnlock()
	if err != nil {
		return 0, 0, err
	}

	var val uint64
	for _, b := range data {
		if b >= '0' && b <= '9' {
			val = val*10 + uint64(b-'0')
		}
	}
	if delta > val {
		val = 0
	} else {
		val -= delta
	}

	newVal := fmt.Sprintf("%d", val)
	c.mu.Unlock()
	cas, err := c.doSet(key, []byte(newVal), 0, 0, false)
	c.mu.Lock()
	return val, cas, err
}

// Append appends data to an existing value
func (c *Cache) Append(key string, value []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.index.Get(key)
	if !exists {
		return 0, os.ErrNotExist
	}

	c.storage.dataMu[entry.Bucket].RLock()
	data, err := c.storage.ReadDataSlot(entry.Bucket, entry.SlotIdx)
	c.storage.dataMu[entry.Bucket].RUnlock()
	if err != nil {
		return 0, err
	}

	newData := append(data, value...)
	c.mu.Unlock()
	cas, err := c.doSet(key, newData, 0, 0, false)
	c.mu.Lock()
	return cas, err
}

// Prepend prepends data to an existing value
func (c *Cache) Prepend(key string, value []byte) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.index.Get(key)
	if !exists {
		return 0, os.ErrNotExist
	}

	c.storage.dataMu[entry.Bucket].RLock()
	data, err := c.storage.ReadDataSlot(entry.Bucket, entry.SlotIdx)
	c.storage.dataMu[entry.Bucket].RUnlock()
	if err != nil {
		return 0, err
	}

	newData := append(value, data...)
	c.mu.Unlock()
	cas, err := c.doSet(key, newData, 0, 0, false)
	c.mu.Lock()
	return cas, err
}

// FlushAll invalidates all items
func (c *Cache) FlushAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.index = NewIndex()
	c.liveDataSize = 0

	c.storage.keysMu.Lock()
	c.storage.TruncateKeysFile(0)
	c.storage.keysMu.Unlock()

	for bucket := 0; bucket < NumBuckets; bucket++ {
		c.storage.dataMu[bucket].Lock()
		c.storage.TruncateDataFile(bucket, 0)
		c.storage.dataMu[bucket].Unlock()
	}

	c.nextKeyId = 0
	for i := range c.nextSlotId {
		c.nextSlotId[i] = 0
	}
}

// Stats returns cache statistics
func (c *Cache) Stats() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := make(map[string]string)
	stats["curr_items"] = fmt.Sprintf("%d", c.index.Count())
	stats["bytes"] = fmt.Sprintf("%d", c.liveDataSize)
	return stats
}

// LiveDataSize returns the current live data size (for testing)
func (c *Cache) LiveDataSize() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.liveDataSize
}

// GetStartTime returns when the cache was started
func (c *Cache) GetStartTime() time.Time {
	return c.StartTime
}
