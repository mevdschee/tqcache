package tqsession

import (
	"container/heap"
	"time"

	"github.com/google/btree"
)

// IndexEntry represents an entry in the B-tree index
type IndexEntry struct {
	Key          string
	KeyId        int64
	Bucket       int
	SlotIdx      int64
	Length       int
	Expiry       int64 // Unix timestamp, 0 = no expiry
	Cas          uint64
	LastAccessed int64
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

// LRUNode represents a node in the LRU list
type LRUNode struct {
	KeyId        int64
	LastAccessed int64
	prev         *LRUNode
	next         *LRUNode
}

// LRUList is a doubly-linked list for LRU eviction
type LRUList struct {
	head    *LRUNode
	tail    *LRUNode
	nodeMap map[int64]*LRUNode // keyId → node
	count   int
}

func NewLRUList() *LRUList {
	return &LRUList{
		nodeMap: make(map[int64]*LRUNode),
	}
}

// Len returns the number of items in the list
func (l *LRUList) Len() int {
	return l.count
}

// Add adds a new item to the front (most recently used)
func (l *LRUList) Add(keyId int64, lastAccessed int64) {
	node := &LRUNode{
		KeyId:        keyId,
		LastAccessed: lastAccessed,
	}
	l.nodeMap[keyId] = node
	l.count++

	if l.head == nil {
		l.head = node
		l.tail = node
		return
	}

	node.next = l.head
	l.head.prev = node
	l.head = node
}

// Touch moves an item to the front and updates its timestamp
func (l *LRUList) Touch(keyId int64, lastAccessed int64) {
	node, ok := l.nodeMap[keyId]
	if !ok {
		return
	}

	node.LastAccessed = lastAccessed

	// Already at front
	if node == l.head {
		return
	}

	// Remove from current position
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	if node == l.tail {
		l.tail = node.prev
	}

	// Move to front
	node.prev = nil
	node.next = l.head
	l.head.prev = node
	l.head = node
}

// Remove removes an item from the list
func (l *LRUList) Remove(keyId int64) {
	node, ok := l.nodeMap[keyId]
	if !ok {
		return
	}

	delete(l.nodeMap, keyId)
	l.count--

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}
}

// PopTail removes and returns the least recently used item
func (l *LRUList) PopTail() *LRUNode {
	if l.tail == nil {
		return nil
	}

	node := l.tail
	l.Remove(node.KeyId)
	return node
}

// Index holds all in-memory data structures
type Index struct {
	btree      *btree.BTree
	expiryHeap *ExpiryHeap
	lruList    *LRUList
}

func NewIndex() *Index {
	return &Index{
		btree:      btree.New(32), // degree 32 for good performance
		expiryHeap: NewExpiryHeap(),
		lruList:    NewLRUList(),
	}
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
	idx.btree.ReplaceOrInsert(*entry)

	// Update expiry heap
	if entry.Expiry > 0 {
		idx.expiryHeap.Insert(entry.KeyId, entry.Expiry)
	} else {
		idx.expiryHeap.Remove(entry.KeyId)
	}

	// Update LRU list
	if _, exists := idx.lruList.nodeMap[entry.KeyId]; exists {
		idx.lruList.Touch(entry.KeyId, entry.LastAccessed)
	} else {
		idx.lruList.Add(entry.KeyId, entry.LastAccessed)
	}
}

// Delete removes an entry by key
func (idx *Index) Delete(key string) *IndexEntry {
	item := idx.btree.Delete(IndexEntry{Key: key})
	if item == nil {
		return nil
	}
	entry := item.(IndexEntry)
	idx.expiryHeap.Remove(entry.KeyId)
	idx.lruList.Remove(entry.KeyId)
	return &entry
}

// Touch updates the last accessed time for an entry
func (idx *Index) Touch(key string, lastAccessed time.Time) *IndexEntry {
	entry, ok := idx.Get(key)
	if !ok {
		return nil
	}
	entry.LastAccessed = lastAccessed.Unix()
	idx.btree.ReplaceOrInsert(*entry)
	idx.lruList.Touch(entry.KeyId, entry.LastAccessed)
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

// PopLRU removes and returns the least recently used entry's keyId
func (idx *Index) PopLRU() *LRUNode {
	return idx.lruList.PopTail()
}
