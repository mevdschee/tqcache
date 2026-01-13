package tqcache

import (
	"container/heap"

	"github.com/google/btree"
)

// IndexEntry represents an entry in the B-tree index
type IndexEntry struct {
	Key     string
	KeyId   int64
	Bucket  int
	SlotIdx int64
	Length  int
	Expiry  int64 // Unix timestamp, 0 = no expiry
	Cas     uint64
}

// Less implements btree.Item
func (e IndexEntry) Less(than btree.Item) bool {
	return e.Key < than.(IndexEntry).Key
}

// ExpiryEntry represents an entry in the expiry heap
type ExpiryEntry struct {
	Expiry int64 // Unix timestamp
	KeyId  int64
	index  int // heap index for updates
}

// ExpiryHeap is a min-heap ordered by expiry time
type ExpiryHeap struct {
	entries  []*ExpiryEntry
	keyIndex map[int64]int // keyId → heap index for O(log n) removal
}

func NewExpiryHeap() *ExpiryHeap {
	return &ExpiryHeap{
		entries:  make([]*ExpiryEntry, 0),
		keyIndex: make(map[int64]int),
	}
}

func (h *ExpiryHeap) Len() int { return len(h.entries) }

func (h *ExpiryHeap) Less(i, j int) bool {
	return h.entries[i].Expiry < h.entries[j].Expiry
}

func (h *ExpiryHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.entries[i].index = i
	h.entries[j].index = j
	h.keyIndex[h.entries[i].KeyId] = i
	h.keyIndex[h.entries[j].KeyId] = j
}

func (h *ExpiryHeap) Push(x interface{}) {
	entry := x.(*ExpiryEntry)
	entry.index = len(h.entries)
	h.entries = append(h.entries, entry)
	h.keyIndex[entry.KeyId] = entry.index
}

func (h *ExpiryHeap) Pop() interface{} {
	n := len(h.entries)
	entry := h.entries[n-1]
	h.entries = h.entries[:n-1]
	delete(h.keyIndex, entry.KeyId)
	return entry
}

// PeekMin returns the entry with the smallest expiry without removing it
func (h *ExpiryHeap) PeekMin() *ExpiryEntry {
	if len(h.entries) == 0 {
		return nil
	}
	return h.entries[0]
}

// Insert adds or updates an entry
func (h *ExpiryHeap) Insert(keyId int64, expiry int64) {
	if idx, ok := h.keyIndex[keyId]; ok {
		// Update existing
		h.entries[idx].Expiry = expiry
		heap.Fix(h, idx)
	} else {
		// Insert new
		heap.Push(h, &ExpiryEntry{Expiry: expiry, KeyId: keyId})
	}
}

// Remove removes an entry by keyId
func (h *ExpiryHeap) Remove(keyId int64) {
	if idx, ok := h.keyIndex[keyId]; ok {
		heap.Remove(h, idx)
	}
}

// PopExpired removes and returns expired entries
func (h *ExpiryHeap) PopExpired(now int64) []*ExpiryEntry {
	var expired []*ExpiryEntry
	for len(h.entries) > 0 && h.entries[0].Expiry <= now && h.entries[0].Expiry > 0 {
		entry := heap.Pop(h).(*ExpiryEntry)
		expired = append(expired, entry)
	}
	return expired
}

// Index holds all in-memory data structures
type Index struct {
	btree      *btree.BTree
	expiryHeap *ExpiryHeap
	keyIdMap   map[int64]string         // keyId → key for reverse lookup
	slotIndex  map[int]map[int64]string // bucket → slotIdx → key for defrag
}

func NewIndex() *Index {
	idx := &Index{
		btree:      btree.New(32), // degree 32 for good performance
		expiryHeap: NewExpiryHeap(),
		keyIdMap:   make(map[int64]string),
		slotIndex:  make(map[int]map[int64]string),
	}
	for i := 0; i < NumBuckets; i++ {
		idx.slotIndex[i] = make(map[int64]string)
	}
	return idx
}

// Get retrieves an entry by key
func (idx *Index) Get(key string) (*IndexEntry, bool) {
	item := idx.btree.Get(IndexEntry{Key: key})
	if item == nil {
		return nil, false
	}
	entry := item.(IndexEntry)
	return &entry, true
}

// Set inserts or updates an entry
func (idx *Index) Set(entry *IndexEntry) {
	// Remove old slot index entry if bucket/slot changed
	if oldEntry, ok := idx.Get(entry.Key); ok {
		if oldEntry.Bucket != entry.Bucket || oldEntry.SlotIdx != entry.SlotIdx {
			delete(idx.slotIndex[oldEntry.Bucket], oldEntry.SlotIdx)
		}
	}

	idx.btree.ReplaceOrInsert(*entry)
	idx.keyIdMap[entry.KeyId] = entry.Key
	idx.slotIndex[entry.Bucket][entry.SlotIdx] = entry.Key

	// Update expiry heap
	if entry.Expiry > 0 {
		idx.expiryHeap.Insert(entry.KeyId, entry.Expiry)
	} else {
		idx.expiryHeap.Remove(entry.KeyId)
	}
}

// Delete removes an entry by key
func (idx *Index) Delete(key string) *IndexEntry {
	item := idx.btree.Delete(IndexEntry{Key: key})
	if item == nil {
		return nil
	}
	entry := item.(IndexEntry)
	delete(idx.keyIdMap, entry.KeyId)
	delete(idx.slotIndex[entry.Bucket], entry.SlotIdx)
	idx.expiryHeap.Remove(entry.KeyId)
	return &entry
}

// GetByKeyId retrieves an entry by keyId
func (idx *Index) GetByKeyId(keyId int64) *IndexEntry {
	key, ok := idx.keyIdMap[keyId]
	if !ok {
		return nil
	}
	entry, _ := idx.Get(key)
	return entry
}

// Count returns the number of entries
func (idx *Index) Count() int {
	return idx.btree.Len()
}

// GetExpired returns entries that have expired
func (idx *Index) GetExpired(now int64) []*ExpiryEntry {
	return idx.expiryHeap.PopExpired(now)
}

// GetByBucketSlot retrieves an entry by bucket and slot index
func (idx *Index) GetByBucketSlot(bucket int, slotIdx int64) *IndexEntry {
	key, ok := idx.slotIndex[bucket][slotIdx]
	if !ok {
		return nil
	}
	entry, _ := idx.Get(key)
	return entry
}

// UpdateSlotIdx updates the slot index for an entry (used during defrag)
func (idx *Index) UpdateSlotIdx(entry *IndexEntry, newSlotIdx int64) {
	// Remove old slot index
	delete(idx.slotIndex[entry.Bucket], entry.SlotIdx)
	// Update entry
	entry.SlotIdx = newSlotIdx
	// Add new slot index
	idx.slotIndex[entry.Bucket][newSlotIdx] = entry.Key
	// Update btree
	idx.btree.ReplaceOrInsert(*entry)
}

// UpdateKeyId updates the keyId for an entry (used during key file defrag)
func (idx *Index) UpdateKeyId(entry *IndexEntry, newKeyId int64) {
	// Remove old keyId mapping
	delete(idx.keyIdMap, entry.KeyId)
	// Update expiry heap
	if heapIdx, ok := idx.expiryHeap.keyIndex[entry.KeyId]; ok {
		delete(idx.expiryHeap.keyIndex, entry.KeyId)
		idx.expiryHeap.entries[heapIdx].KeyId = newKeyId
		idx.expiryHeap.keyIndex[newKeyId] = heapIdx
	}
	// Update expiry heap
	if heapIdx, ok := idx.expiryHeap.keyIndex[entry.KeyId]; ok {
		delete(idx.expiryHeap.keyIndex, entry.KeyId)
		idx.expiryHeap.entries[heapIdx].KeyId = newKeyId
		idx.expiryHeap.keyIndex[newKeyId] = heapIdx
	}
	// Update entry
	entry.KeyId = newKeyId
	// Add new keyId mapping
	idx.keyIdMap[newKeyId] = entry.Key
	// Update btree
	idx.btree.ReplaceOrInsert(*entry)
}
