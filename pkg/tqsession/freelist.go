package tqsession

// FreeList manages free slots for reuse
type FreeList struct {
	slots []int64
}

func NewFreeList() *FreeList {
	return &FreeList{
		slots: make([]int64, 0),
	}
}

// Push adds a free slot
func (f *FreeList) Push(slotIdx int64) {
	f.slots = append(f.slots, slotIdx)
}

// Pop returns a free slot, or -1 if none available
func (f *FreeList) Pop() int64 {
	if len(f.slots) == 0 {
		return -1
	}
	idx := f.slots[len(f.slots)-1]
	f.slots = f.slots[:len(f.slots)-1]
	return idx
}

// Len returns the number of free slots
func (f *FreeList) Len() int {
	return len(f.slots)
}

// FreeLists manages all free lists (keys + 16 data buckets)
type FreeLists struct {
	keys *FreeList
	data [NumBuckets]*FreeList
}

func NewFreeLists() *FreeLists {
	f := &FreeLists{
		keys: NewFreeList(),
	}
	for i := 0; i < NumBuckets; i++ {
		f.data[i] = NewFreeList()
	}
	return f
}

// PushKey adds a free key slot
func (f *FreeLists) PushKey(keyId int64) {
	f.keys.Push(keyId)
}

// PopKey returns a free key slot, or -1 if none
func (f *FreeLists) PopKey() int64 {
	return f.keys.Pop()
}

// PushData adds a free data slot for a bucket
func (f *FreeLists) PushData(bucket int, slotIdx int64) {
	f.data[bucket].Push(slotIdx)
}

// PopData returns a free data slot for a bucket, or -1 if none
func (f *FreeLists) PopData(bucket int) int64 {
	return f.data[bucket].Pop()
}
