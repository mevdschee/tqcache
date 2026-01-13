package tqsession

import (
	"strings"
	"sync"
	"time"
)

// Operation types
type OpType int

const (
	OpGet OpType = iota
	OpSet
	OpAdd
	OpReplace
	OpDelete
	OpTouch
	OpCas
	OpIncr
	OpDecr
	OpAppend
	OpPrepend
	OpFlushAll
	OpStats
)

// Request represents a cache operation request
type Request struct {
	Op       OpType
	Key      string
	Value    []byte
	TTL      time.Duration
	Cas      uint64
	Delta    uint64
	RespChan chan *Response
}

// Response represents a cache operation response
type Response struct {
	Value []byte
	Cas   uint64
	Err   error
	Stats map[string]string
}

// Worker is the single-threaded storage worker
type Worker struct {
	storage  *Storage
	index    *Index
	reqChan  chan *Request
	stopChan chan struct{}
	wg       sync.WaitGroup

	nextKeyId  int64
	nextSlotId [NumBuckets]int64
	startTime  time.Time

	DefaultTTL   time.Duration
	maxDataSize  int64 // Maximum live data size (0 = unlimited)
	liveDataSize int64 // Current live data size in bytes

	// Sync tracking for periodic mode
	lastSync     time.Time
	syncInterval time.Duration
	syncNotify   func() // Called when sync is needed
}

func NewWorker(storage *Storage, DefaultTTL time.Duration, maxDataSize int64, channelCapacity int) (*Worker, error) {
	if channelCapacity <= 0 {
		channelCapacity = 1000 // Default
	}
	w := &Worker{
		storage:      storage,
		index:        NewIndex(),
		reqChan:      make(chan *Request, channelCapacity),
		stopChan:     make(chan struct{}),
		startTime:    time.Now(),
		DefaultTTL:   DefaultTTL,
		maxDataSize:  maxDataSize,
		lastSync:     time.Now(),
		syncInterval: time.Second, // Default 1 second
	}

	// Recover state from disk
	if err := w.recover(); err != nil {
		return nil, err
	}

	return w, nil
}

// recover rebuilds in-memory structures from disk
func (w *Worker) recover() error {
	keyCount, err := w.storage.KeyCount()
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()

	for keyId := int64(0); keyId < keyCount; keyId++ {
		rec, err := w.storage.ReadKeyRecord(keyId)
		if err != nil {
			continue // Skip unreadable records
		}

		// With continuous compaction, all records in file are valid

		// Extract key (null-terminated)
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

		// Skip expired entries (they will be compacted on first access/write)
		if rec.Expiry > 0 && rec.Expiry <= now {
			continue
		}

		// Use bucket/slotIdx from the persisted record
		entry := &IndexEntry{
			Key:          key,
			KeyId:        keyId,
			Bucket:       int(rec.Bucket),
			SlotIdx:      rec.SlotIdx,
			Expiry:       rec.Expiry,
			Cas:          rec.Cas,
			LastAccessed: rec.LastAccessed,
		}
		w.index.Set(entry)
	}

	w.nextKeyId = keyCount

	// Also scan data files for slot tracking
	for bucket := 0; bucket < NumBuckets; bucket++ {
		count, err := w.storage.SlotCount(bucket)
		if err != nil {
			return err
		}
		w.nextSlotId[bucket] = count
	}

	return nil
}

// Start starts the worker goroutine
func (w *Worker) Start() {
	w.wg.Add(1)
	go w.run()
}

// Stop stops the worker and waits for it to finish
func (w *Worker) Stop() {
	close(w.stopChan)
	w.wg.Wait()
}

// RequestChan returns the request channel
func (w *Worker) RequestChan() chan *Request {
	return w.reqChan
}

// SetSyncNotify sets the callback for sync notifications
func (w *Worker) SetSyncNotify(notify func()) {
	w.syncNotify = notify
}

// SetSyncInterval sets the sync interval
func (w *Worker) SetSyncInterval(interval time.Duration) {
	w.syncInterval = interval
}

// checkSync checks if sync is needed and triggers it if so
func (w *Worker) checkSync() {
	if w.syncNotify == nil {
		return
	}
	if time.Since(w.lastSync) >= w.syncInterval {
		w.syncNotify()
	}
}

// MarkSynced updates the last sync time
func (w *Worker) MarkSynced() {
	w.lastSync = time.Now()
}

func (w *Worker) run() {
	defer w.wg.Done()

	// Ticker for expiry cleanup
	expiryTicker := time.NewTicker(100 * time.Millisecond)
	defer expiryTicker.Stop()

	for {
		select {
		case req := <-w.reqChan:
			w.handleRequest(req)
		case <-expiryTicker.C:
			w.cleanupExpired()
		case <-w.stopChan:
			return
		}
	}
}

func (w *Worker) handleRequest(req *Request) {
	var resp *Response

	switch req.Op {
	case OpGet:
		resp = w.handleGet(req)
	case OpSet:
		resp = w.handleSet(req)
	case OpAdd:
		resp = w.handleAdd(req)
	case OpReplace:
		resp = w.handleReplace(req)
	case OpDelete:
		resp = w.handleDelete(req)
	case OpTouch:
		resp = w.handleTouch(req)
	case OpCas:
		resp = w.handleCas(req)
	case OpIncr:
		resp = w.handleIncr(req)
	case OpDecr:
		resp = w.handleDecr(req)
	case OpAppend:
		resp = w.handleAppend(req)
	case OpPrepend:
		resp = w.handlePrepend(req)
	case OpFlushAll:
		resp = w.handleFlushAll(req)
	case OpStats:
		resp = w.handleStats(req)
	default:
		resp = &Response{Err: ErrKeyNotFound}
	}

	if req.RespChan != nil {
		req.RespChan <- resp
	}
}

func (w *Worker) handleGet(req *Request) *Response {
	entry, ok := w.index.Get(req.Key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}

	// Check expiry
	if entry.Expiry > 0 && entry.Expiry <= time.Now().UnixMilli() {
		w.deleteEntry(entry)
		return &Response{Err: ErrKeyNotFound}
	}

	// Read data
	data, err := w.storage.ReadDataSlot(entry.Bucket, entry.SlotIdx)
	if err != nil {
		return &Response{Err: err}
	}

	// Update last accessed
	now := time.Now()
	entry.LastAccessed = now.Unix()
	w.index.Touch(req.Key, now)
	w.storage.UpdateLastAccessed(entry.KeyId, now)

	return &Response{Value: data, Cas: entry.Cas}
}

func (w *Worker) handleSet(req *Request) *Response {
	resp := w.doSet(req.Key, req.Value, req.TTL, 0, false)
	w.checkSync()
	return resp
}

func (w *Worker) handleAdd(req *Request) *Response {
	// Only set if key doesn't exist
	if _, ok := w.index.Get(req.Key); ok {
		return &Response{Err: ErrKeyExists}
	}
	resp := w.doSet(req.Key, req.Value, req.TTL, 0, false)
	w.checkSync()
	return resp
}

func (w *Worker) handleReplace(req *Request) *Response {
	// Only set if key exists
	if _, ok := w.index.Get(req.Key); !ok {
		return &Response{Err: ErrKeyNotFound}
	}
	resp := w.doSet(req.Key, req.Value, req.TTL, 0, false)
	w.checkSync()
	return resp
}

func (w *Worker) handleCas(req *Request) *Response {
	entry, ok := w.index.Get(req.Key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}
	if entry.Cas != req.Cas {
		return &Response{Err: ErrCasMismatch}
	}
	resp := w.doSet(req.Key, req.Value, req.TTL, 0, false)
	w.checkSync()
	return resp
}

func (w *Worker) doSet(key string, value []byte, ttl time.Duration, existingCas uint64, checkCas bool) *Response {
	if len(key) > MaxKeySize {
		return &Response{Err: ErrKeyTooLarge}
	}

	// Find bucket for value
	bucket, err := w.storage.BucketForSize(len(value))
	if err != nil {
		return &Response{Err: err}
	}

	now := time.Now()
	var expiry int64
	if ttl > 0 {
		expiry = now.Add(ttl).UnixMilli()
	} else if w.DefaultTTL > 0 {
		expiry = now.Add(w.DefaultTTL).UnixMilli()
	}

	// Check if key exists
	existing, exists := w.index.Get(key)
	if exists {
		if checkCas && existing.Cas != existingCas {
			return &Response{Err: ErrCasMismatch}
		}
	}

	// Update live data size early and trigger LRU eviction BEFORE allocating slots
	// This ensures eviction doesn't truncate the slot we're about to use
	w.liveDataSize += int64(len(value))
	if exists {
		w.liveDataSize -= int64(existing.Length)
	}
	w.lruEvict()

	// Now compact old data slot if bucket changed (after eviction is done)
	if exists && existing.Bucket != bucket {
		w.compactDataSlot(existing.Bucket, existing.SlotIdx)
	}

	// Allocate key slot - always append with continuous compaction
	var keyId int64
	if exists {
		keyId = existing.KeyId
	} else {
		keyId = w.nextKeyId
		w.nextKeyId++
	}

	// Allocate data slot - always append (continuous defrag keeps files compact)
	var slotIdx int64
	if exists && existing.Bucket == bucket {
		// Reuse same slot if bucket unchanged
		slotIdx = existing.SlotIdx
	} else {
		// Append to the bucket
		slotIdx = w.nextSlotId[bucket]
		w.nextSlotId[bucket]++
	}

	// Generate new CAS
	cas := uint64(now.UnixNano())

	// Write key record (including bucket/slotIdx for recovery)
	keyRec := &KeyRecord{
		KeyLen:       uint16(len(key)),
		LastAccessed: now.Unix(),
		Cas:          cas,
		Expiry:       expiry,
		Bucket:       byte(bucket),
		SlotIdx:      slotIdx,
	}
	copy(keyRec.Key[:], key)
	if err := w.storage.WriteKeyRecord(keyId, keyRec); err != nil {
		return &Response{Err: err}
	}

	// Write data
	if err := w.storage.WriteDataSlot(bucket, slotIdx, value); err != nil {
		return &Response{Err: err}
	}

	// Live data size was already updated above before allocation

	// Trigger LRU eviction if needed (already done above, but re-check in case of edge cases)
	// w.lruEvict() -- moved to before allocation

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
	w.index.Set(entry)

	return &Response{Cas: cas}
}

func (w *Worker) handleDelete(req *Request) *Response {
	entry, ok := w.index.Get(req.Key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}

	w.deleteEntry(entry)
	w.checkSync()
	return &Response{}
}

func (w *Worker) deleteEntry(entry *IndexEntry) {
	// Remove from index FIRST (clears slotIndex before compactDataSlot moves another entry there)
	w.index.Delete(entry.Key)

	// Update live data size
	w.liveDataSize -= int64(entry.Length)

	// Compact data slot: move tail to freed slot and truncate
	w.compactDataSlot(entry.Bucket, entry.SlotIdx)

	// Compact key slot: move tail to freed slot and truncate
	w.compactKeySlot(entry.KeyId)
}

// compactDataSlot moves the tail slot to fill the freed slot, then truncates the file
func (w *Worker) compactDataSlot(bucket int, freedSlotIdx int64) {
	tailIdx := w.nextSlotId[bucket] - 1
	if tailIdx < 0 {
		return // Empty file
	}

	if freedSlotIdx == tailIdx {
		// Already the tail, just decrement and truncate
		w.nextSlotId[bucket]--
		w.storage.TruncateDataFile(bucket, w.nextSlotId[bucket])
		return
	}

	// Read tail slot data
	tailData, err := w.storage.ReadDataSlot(bucket, tailIdx)
	if err != nil {
		return // Can't read, skip compaction
	}

	// Write tail data to freed slot
	if err := w.storage.WriteDataSlot(bucket, freedSlotIdx, tailData); err != nil {
		return // Can't write, skip compaction
	}

	// Find and update the entry that points to the tail slot
	tailEntry := w.index.GetByBucketSlot(bucket, tailIdx)
	if tailEntry != nil {
		// Update index and storage to point to new slot
		w.index.UpdateSlotIdx(tailEntry, freedSlotIdx)
		w.storage.UpdateSlotIdx(tailEntry.KeyId, freedSlotIdx)
	}

	// Truncate file
	w.nextSlotId[bucket]--
	w.storage.TruncateDataFile(bucket, w.nextSlotId[bucket])
}

// compactKeySlot moves the tail key record to fill the freed slot, then truncates the file
func (w *Worker) compactKeySlot(freedKeyId int64) {
	tailKeyId := w.nextKeyId - 1
	if tailKeyId < 0 {
		return // Empty file
	}

	if freedKeyId == tailKeyId {
		// Already the tail, just decrement and truncate
		w.nextKeyId--
		w.storage.TruncateKeysFile(w.nextKeyId)
		return
	}

	// Read tail key record
	tailRec, err := w.storage.ReadKeyRecord(tailKeyId)
	if err != nil {
		return // Can't read, skip compaction
	}

	// Write tail record to freed slot
	if err := w.storage.WriteKeyRecord(freedKeyId, tailRec); err != nil {
		return // Can't write, skip compaction
	}

	// Find and update the entry that has tailKeyId
	tailEntry := w.index.GetByKeyId(tailKeyId)
	if tailEntry != nil {
		// Update index to point to new keyId
		w.index.UpdateKeyId(tailEntry, freedKeyId)
	}

	// Truncate file
	w.nextKeyId--
	w.storage.TruncateKeysFile(w.nextKeyId)
}

func (w *Worker) handleTouch(req *Request) *Response {
	entry, ok := w.index.Get(req.Key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}

	now := time.Now()
	var expiry int64
	if req.TTL > 0 {
		expiry = now.Add(req.TTL).UnixMilli()
	}

	// Update key record
	rec, err := w.storage.ReadKeyRecord(entry.KeyId)
	if err != nil {
		return &Response{Err: err}
	}
	rec.LastAccessed = now.Unix()
	rec.Expiry = expiry
	if err := w.storage.WriteKeyRecord(entry.KeyId, rec); err != nil {
		return &Response{Err: err}
	}

	// Update index
	entry.LastAccessed = now.Unix()
	entry.Expiry = expiry
	w.index.Set(entry)

	w.checkSync()
	return &Response{Cas: entry.Cas}
}

func (w *Worker) handleIncr(req *Request) *Response {
	return w.doIncrDecr(req.Key, req.Delta, true)
}

func (w *Worker) handleDecr(req *Request) *Response {
	return w.doIncrDecr(req.Key, req.Delta, false)
}

func (w *Worker) doIncrDecr(key string, delta uint64, incr bool) *Response {
	entry, ok := w.index.Get(key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}

	// Read current value
	data, err := w.storage.ReadDataSlot(entry.Bucket, entry.SlotIdx)
	if err != nil {
		return &Response{Err: err}
	}

	// Parse as number
	var val uint64
	for _, b := range data {
		if b >= '0' && b <= '9' {
			val = val*10 + uint64(b-'0')
		}
	}

	// Apply delta
	if incr {
		val += delta
	} else {
		if delta > val {
			val = 0
		} else {
			val -= delta
		}
	}

	// Convert back to string
	newData := []byte(strings.TrimLeft(string(data), "0123456789") +
		func() string {
			if val == 0 {
				return "0"
			}
			s := ""
			for v := val; v > 0; v /= 10 {
				s = string('0'+byte(v%10)) + s
			}
			return s
		}())

	// Actually, let's just store the number
	newData = []byte(func() string {
		if val == 0 {
			return "0"
		}
		s := ""
		for v := val; v > 0; v /= 10 {
			s = string('0'+byte(v%10)) + s
		}
		return s
	}())

	// Write back
	if err := w.storage.WriteDataSlot(entry.Bucket, entry.SlotIdx, newData); err != nil {
		return &Response{Err: err}
	}

	// Update CAS
	now := time.Now()
	entry.Cas = uint64(now.UnixNano())
	entry.LastAccessed = now.Unix()
	entry.Length = len(newData)
	w.index.Set(entry)

	w.checkSync()
	return &Response{Value: newData, Cas: entry.Cas}
}

func (w *Worker) handleAppend(req *Request) *Response {
	return w.doAppendPrepend(req.Key, req.Value, true)
}

func (w *Worker) handlePrepend(req *Request) *Response {
	return w.doAppendPrepend(req.Key, req.Value, false)
}

func (w *Worker) doAppendPrepend(key string, value []byte, append bool) *Response {
	entry, ok := w.index.Get(key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}

	// Read current value
	data, err := w.storage.ReadDataSlot(entry.Bucket, entry.SlotIdx)
	if err != nil {
		return &Response{Err: err}
	}

	// Combine
	var newData []byte
	if append {
		newData = make([]byte, len(data)+len(value))
		copy(newData, data)
		copy(newData[len(data):], value)
	} else {
		newData = make([]byte, len(value)+len(data))
		copy(newData, value)
		copy(newData[len(value):], data)
	}

	// Check if we need a new bucket
	newBucket, err := w.storage.BucketForSize(len(newData))
	if err != nil {
		return &Response{Err: err}
	}

	// Compact old slot and allocate new if bucket changed
	if newBucket != entry.Bucket {
		w.compactDataSlot(entry.Bucket, entry.SlotIdx)

		// Append to the new bucket
		entry.Bucket = newBucket
		entry.SlotIdx = w.nextSlotId[newBucket]
		w.nextSlotId[newBucket]++
	}

	// Write new data
	if err := w.storage.WriteDataSlot(entry.Bucket, entry.SlotIdx, newData); err != nil {
		return &Response{Err: err}
	}

	// Update entry
	now := time.Now()
	entry.Cas = uint64(now.UnixNano())
	entry.LastAccessed = now.Unix()
	entry.Length = len(newData)
	w.index.Set(entry)

	w.checkSync()
	return &Response{Cas: entry.Cas}
}

func (w *Worker) handleFlushAll(req *Request) *Response {
	// Reset in-memory structures
	w.index = NewIndex()
	w.liveDataSize = 0

	// Truncate all files to reclaim space
	w.storage.TruncateKeysFile(0)
	for bucket := 0; bucket < NumBuckets; bucket++ {
		w.storage.TruncateDataFile(bucket, 0)
	}

	// Reset slot counters
	w.nextKeyId = 0
	for i := range w.nextSlotId {
		w.nextSlotId[i] = 0
	}

	w.checkSync()
	return &Response{}
}

func (w *Worker) handleStats(req *Request) *Response {
	stats := make(map[string]string)
	stats["curr_items"] = func() string {
		count := w.index.Count()
		s := ""
		if count == 0 {
			return "0"
		}
		for v := count; v > 0; v /= 10 {
			s = string('0'+byte(v%10)) + s
		}
		return s
	}()
	return &Response{Stats: stats}
}

func (w *Worker) cleanupExpired() {
	now := time.Now().UnixMilli()

	// Peek at expired entries and delete them properly
	for {
		entry := w.index.expiryHeap.PeekMin()
		if entry == nil || entry.Expiry > now || entry.Expiry == 0 {
			break
		}

		// Find and delete the entry from the B-tree by keyId
		// We need to iterate the B-tree to find by keyId
		// For now, just remove from heap - Get() will handle the actual deletion
		// when it finds the expired key
		w.index.expiryHeap.Remove(entry.KeyId)
	}
}

// StartTime returns when the worker was started
func (w *Worker) StartTime() time.Time {
	return w.startTime
}

// lruEvict evicts items until liveDataSize <= maxDataSize.
// Phase 1: Evict expired items first
// Phase 2: Evict from LRU tail (least recently used)
func (w *Worker) lruEvict() {
	if w.maxDataSize <= 0 {
		return // No limit set
	}

	now := time.Now().UnixMilli()

	// Phase 1: Evict expired items first
	for w.liveDataSize > w.maxDataSize {
		entry := w.index.expiryHeap.PeekMin()
		if entry == nil || entry.Expiry > now || entry.Expiry == 0 {
			break
		}

		// Find the index entry by keyId and delete it
		indexEntry := w.index.GetByKeyId(entry.KeyId)
		if indexEntry != nil {
			w.deleteEntry(indexEntry)
		} else {
			// Just remove from heap if entry not found
			w.index.expiryHeap.Remove(entry.KeyId)
		}
	}

	// Phase 2: LRU eviction from tail
	for w.liveDataSize > w.maxDataSize {
		node := w.index.lruList.PopTail()
		if node == nil {
			break
		}

		// Find and delete the entry by keyId
		indexEntry := w.index.GetByKeyId(node.KeyId)
		if indexEntry != nil {
			w.deleteEntry(indexEntry)
		}
	}
}

// LiveDataSize returns the current live data size in bytes
func (w *Worker) LiveDataSize() int64 {
	return w.liveDataSize
}

// Sync syncs the worker's storage to disk
func (w *Worker) Sync() error {
	return w.storage.Sync()
}

// Storage returns the worker's storage for direct access
func (w *Worker) Storage() *Storage {
	return w.storage
}

// Index returns the worker's index for stats access
func (w *Worker) Index() *Index {
	return w.index
}

// Close stops the worker and closes storage
func (w *Worker) Close() error {
	w.Stop()
	return w.storage.Close()
}
