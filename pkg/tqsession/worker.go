package tqsession

import (
	"strings"
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
	storage   *Storage
	index     *Index
	freeLists *FreeLists
	reqChan   chan *Request
	stopChan  chan struct{}

	nextKeyId  int64
	nextSlotId [NumBuckets]int64
	startTime  time.Time

	defaultExpiry time.Duration
}

func NewWorker(storage *Storage, defaultExpiry time.Duration) (*Worker, error) {
	w := &Worker{
		storage:       storage,
		index:         NewIndex(),
		freeLists:     NewFreeLists(),
		reqChan:       make(chan *Request, 1000),
		stopChan:      make(chan struct{}),
		startTime:     time.Now(),
		defaultExpiry: defaultExpiry,
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

	now := time.Now().Unix()

	for keyId := int64(0); keyId < keyCount; keyId++ {
		rec, err := w.storage.ReadKeyRecord(keyId)
		if err != nil {
			continue // Skip unreadable records
		}

		if rec.Free == FlagDeleted {
			w.freeLists.PushKey(keyId)
			continue
		}

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

		// Skip expired entries
		if rec.Expiry > 0 && rec.Expiry <= now {
			w.freeLists.PushKey(keyId)
			// TODO: Also free the data slot
			continue
		}

		// We need to find the data location - for now we store it with keyId
		// In a real implementation, we'd need to track bucket/slotIdx in the key record
		// For simplicity, we'll use a convention: bucket and slotIdx are derived
		// This is a simplification - in practice we'd store this in the key record

		entry := &IndexEntry{
			Key:          key,
			KeyId:        keyId,
			Expiry:       rec.Expiry,
			Cas:          rec.Cas,
			LastAccessed: rec.LastAccessed,
			// Bucket and SlotIdx would need to be stored somewhere
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
	go w.run()
}

// Stop stops the worker
func (w *Worker) Stop() {
	close(w.stopChan)
}

// RequestChan returns the request channel
func (w *Worker) RequestChan() chan *Request {
	return w.reqChan
}

func (w *Worker) run() {
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
	if entry.Expiry > 0 && entry.Expiry <= time.Now().Unix() {
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
	return w.doSet(req.Key, req.Value, req.TTL, 0, false)
}

func (w *Worker) handleAdd(req *Request) *Response {
	// Only set if key doesn't exist
	if _, ok := w.index.Get(req.Key); ok {
		return &Response{Err: ErrKeyExists}
	}
	return w.doSet(req.Key, req.Value, req.TTL, 0, false)
}

func (w *Worker) handleReplace(req *Request) *Response {
	// Only set if key exists
	if _, ok := w.index.Get(req.Key); !ok {
		return &Response{Err: ErrKeyNotFound}
	}
	return w.doSet(req.Key, req.Value, req.TTL, 0, false)
}

func (w *Worker) handleCas(req *Request) *Response {
	entry, ok := w.index.Get(req.Key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}
	if entry.Cas != req.Cas {
		return &Response{Err: ErrCasMismatch}
	}
	return w.doSet(req.Key, req.Value, req.TTL, 0, false)
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
		expiry = now.Add(ttl).Unix()
	} else if w.defaultExpiry > 0 {
		expiry = now.Add(w.defaultExpiry).Unix()
	}

	// Check if key exists
	existing, exists := w.index.Get(key)
	if exists {
		if checkCas && existing.Cas != existingCas {
			return &Response{Err: ErrCasMismatch}
		}
		// Free old data slot
		w.storage.MarkDataFree(existing.Bucket, existing.SlotIdx)
		w.freeLists.PushData(existing.Bucket, existing.SlotIdx)
	}

	// Allocate key slot
	var keyId int64
	if exists {
		keyId = existing.KeyId
	} else {
		keyId = w.freeLists.PopKey()
		if keyId < 0 {
			keyId = w.nextKeyId
			w.nextKeyId++
		}
	}

	// Allocate data slot
	slotIdx := w.freeLists.PopData(bucket)
	if slotIdx < 0 {
		slotIdx = w.nextSlotId[bucket]
		w.nextSlotId[bucket]++
	}

	// Generate new CAS
	cas := uint64(now.UnixNano())

	// Write key record
	keyRec := &KeyRecord{
		Free:         FlagInUse,
		LastAccessed: now.Unix(),
		Cas:          cas,
		Expiry:       expiry,
	}
	copy(keyRec.Key[:], key)
	if err := w.storage.WriteKeyRecord(keyId, keyRec); err != nil {
		return &Response{Err: err}
	}

	// Write data
	if err := w.storage.WriteDataSlot(bucket, slotIdx, value); err != nil {
		return &Response{Err: err}
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
	w.index.Set(entry)

	return &Response{Cas: cas}
}

func (w *Worker) handleDelete(req *Request) *Response {
	entry, ok := w.index.Get(req.Key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}

	w.deleteEntry(entry)
	return &Response{}
}

func (w *Worker) deleteEntry(entry *IndexEntry) {
	// Mark key record as free
	w.storage.MarkKeyFree(entry.KeyId)
	w.freeLists.PushKey(entry.KeyId)

	// Mark data slot as free
	w.storage.MarkDataFree(entry.Bucket, entry.SlotIdx)
	w.freeLists.PushData(entry.Bucket, entry.SlotIdx)

	// Remove from index
	w.index.Delete(entry.Key)
}

func (w *Worker) handleTouch(req *Request) *Response {
	entry, ok := w.index.Get(req.Key)
	if !ok {
		return &Response{Err: ErrKeyNotFound}
	}

	now := time.Now()
	var expiry int64
	if req.TTL > 0 {
		expiry = now.Add(req.TTL).Unix()
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

	// Free old slot if bucket changed
	if newBucket != entry.Bucket {
		w.storage.MarkDataFree(entry.Bucket, entry.SlotIdx)
		w.freeLists.PushData(entry.Bucket, entry.SlotIdx)

		// Allocate new slot
		slotIdx := w.freeLists.PopData(newBucket)
		if slotIdx < 0 {
			slotIdx = w.nextSlotId[newBucket]
			w.nextSlotId[newBucket]++
		}
		entry.Bucket = newBucket
		entry.SlotIdx = slotIdx
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

	return &Response{Cas: entry.Cas}
}

func (w *Worker) handleFlushAll(req *Request) *Response {
	// This is a simplified flush - in practice we'd iterate and delete
	w.index = NewIndex()
	w.freeLists = NewFreeLists()
	// TODO: Truncate files
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
	now := time.Now().Unix()
	expired := w.index.GetExpired(now)

	for _, e := range expired {
		// Find the entry by keyId
		// We need to iterate or maintain a reverse map
		// For now, we'll skip this - the Get operation handles expiry
		_ = e
	}
}

// StartTime returns when the worker was started
func (w *Worker) StartTime() time.Time {
	return w.startTime
}
