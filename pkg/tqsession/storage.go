package tqsession

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Record sizes
const (
	KeyRecordSize  = 1059 // 2 + 1024 + 8 + 8 + 8 + 1 + 8 (keyLen, key, lastAccessed, cas, expiry, bucket, slotIdx)
	MaxKeySize     = 1024
	DataHeaderSize = 1 + 4 // free + length (data files still have free flag)
)

// Bucket configuration: 16 buckets from 1KB to 64MB (doubling each time)
const (
	NumBuckets    = 16
	MinBucketSize = 1024             // 1KB
	MaxBucketSize = 64 * 1024 * 1024 // 64MB
)

// Free flags (for data files only - key files use continuous compaction)
const (
	FlagInUse   = 0x00
	FlagDeleted = 0x01
)

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrKeyTooLarge   = errors.New("key too large")
	ErrValueTooLarge = errors.New("value too large")
	ErrKeyExists     = errors.New("key already exists")
	ErrCasMismatch   = errors.New("cas mismatch")
)

// KeyRecord represents a fixed-size record in the keys file
type KeyRecord struct {
	KeyLen       uint16 // Actual key length (0-1024)
	Key          [MaxKeySize]byte
	LastAccessed int64
	Cas          uint64
	Expiry       int64
	Bucket       byte
	SlotIdx      int64
}

// Storage handles all file I/O for the cache
type Storage struct {
	dataDir    string
	keysFile   *os.File
	dataFiles  [NumBuckets]*os.File
	syncAlways bool // If true, fsync after every write

	// Bucket sizes: 1KB, 2KB, 4KB, ..., 64MB
	bucketSizes [NumBuckets]int
}

// NewStorage creates a new storage instance
func NewStorage(dataDir string, syncAlways bool) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	s := &Storage{
		dataDir:    dataDir,
		syncAlways: syncAlways,
	}

	// Calculate bucket sizes
	size := MinBucketSize
	for i := 0; i < NumBuckets; i++ {
		s.bucketSizes[i] = size
		size *= 2
	}

	// Open keys file
	keysPath := filepath.Join(dataDir, "keys")
	keysFile, err := os.OpenFile(keysPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open keys file: %w", err)
	}
	s.keysFile = keysFile

	// Open data bucket files
	for i := 0; i < NumBuckets; i++ {
		dataPath := filepath.Join(dataDir, fmt.Sprintf("data_%02d", i))
		dataFile, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("failed to open data file %d: %w", i, err)
		}
		s.dataFiles[i] = dataFile
	}

	return s, nil
}

// Close closes all file handles
func (s *Storage) Close() error {
	var firstErr error
	if s.keysFile != nil {
		if err := s.keysFile.Close(); err != nil {
			firstErr = err
		}
	}
	for i := 0; i < NumBuckets; i++ {
		if s.dataFiles[i] != nil {
			if err := s.dataFiles[i].Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Sync fsyncs all files
func (s *Storage) Sync() error {
	if err := s.keysFile.Sync(); err != nil {
		return err
	}
	for i := 0; i < NumBuckets; i++ {
		if err := s.dataFiles[i].Sync(); err != nil {
			return err
		}
	}
	return nil
}

// BucketForSize returns the bucket index for a given value size
func (s *Storage) BucketForSize(size int) (int, error) {
	for i := 0; i < NumBuckets; i++ {
		if size <= s.bucketSizes[i] {
			return i, nil
		}
	}
	return -1, ErrValueTooLarge
}

// BucketSize returns the slot size for a bucket (excluding header)
func (s *Storage) BucketSize(bucket int) int {
	return s.bucketSizes[bucket]
}

// SlotSize returns the total slot size for a bucket (including header)
func (s *Storage) SlotSize(bucket int) int {
	return DataHeaderSize + s.bucketSizes[bucket]
}

// ReadKeyRecord reads a key record at the given keyId
func (s *Storage) ReadKeyRecord(keyId int64) (*KeyRecord, error) {
	offset := keyId * KeyRecordSize
	buf := make([]byte, KeyRecordSize)

	n, err := s.keysFile.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if n != KeyRecordSize {
		return nil, fmt.Errorf("short read: got %d, want %d", n, KeyRecordSize)
	}

	rec := &KeyRecord{
		KeyLen:       binary.LittleEndian.Uint16(buf[0:2]),
		LastAccessed: int64(binary.LittleEndian.Uint64(buf[1026:1034])),
		Cas:          binary.LittleEndian.Uint64(buf[1034:1042]),
		Expiry:       int64(binary.LittleEndian.Uint64(buf[1042:1050])),
		Bucket:       buf[1050],
		SlotIdx:      int64(binary.LittleEndian.Uint64(buf[1051:1059])),
	}
	copy(rec.Key[:], buf[2:1026])

	return rec, nil
}

// WriteKeyRecord writes a key record at the given keyId
func (s *Storage) WriteKeyRecord(keyId int64, rec *KeyRecord) error {
	offset := keyId * KeyRecordSize
	buf := make([]byte, KeyRecordSize)

	binary.LittleEndian.PutUint16(buf[0:2], rec.KeyLen)
	copy(buf[2:1026], rec.Key[:])
	binary.LittleEndian.PutUint64(buf[1026:1034], uint64(rec.LastAccessed))
	binary.LittleEndian.PutUint64(buf[1034:1042], rec.Cas)
	binary.LittleEndian.PutUint64(buf[1042:1050], uint64(rec.Expiry))
	buf[1050] = rec.Bucket
	binary.LittleEndian.PutUint64(buf[1051:1059], uint64(rec.SlotIdx))

	_, err := s.keysFile.WriteAt(buf, offset)
	if err == nil && s.syncAlways {
		err = s.keysFile.Sync()
	}
	return err
}

// ReadDataSlot reads data from a bucket slot
func (s *Storage) ReadDataSlot(bucket int, slotIdx int64) ([]byte, error) {
	slotSize := s.SlotSize(bucket)
	offset := slotIdx * int64(slotSize)

	// Read header
	header := make([]byte, DataHeaderSize)
	if _, err := s.dataFiles[bucket].ReadAt(header, offset); err != nil {
		return nil, err
	}

	if header[0] == FlagDeleted {
		return nil, ErrKeyNotFound
	}

	length := binary.LittleEndian.Uint32(header[1:5])

	// Read data
	data := make([]byte, length)
	if _, err := s.dataFiles[bucket].ReadAt(data, offset+DataHeaderSize); err != nil {
		return nil, err
	}

	return data, nil
}

// WriteDataSlot writes data to a bucket slot
func (s *Storage) WriteDataSlot(bucket int, slotIdx int64, data []byte) error {
	slotSize := s.SlotSize(bucket)
	offset := slotIdx * int64(slotSize)

	// Prepare buffer with header + data (padded to slot size)
	buf := make([]byte, slotSize)
	buf[0] = FlagInUse
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(data)))
	copy(buf[DataHeaderSize:], data)

	_, err := s.dataFiles[bucket].WriteAt(buf, offset)
	if err == nil && s.syncAlways {
		err = s.dataFiles[bucket].Sync()
	}
	return err
}

// MarkDataFree marks a data slot as free
func (s *Storage) MarkDataFree(bucket int, slotIdx int64) error {
	slotSize := s.SlotSize(bucket)
	offset := slotIdx * int64(slotSize)
	_, err := s.dataFiles[bucket].WriteAt([]byte{FlagDeleted}, offset)
	return err
}

// KeysFileSize returns the current size of the keys file
func (s *Storage) KeysFileSize() (int64, error) {
	info, err := s.keysFile.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// DataFileSize returns the current size of a data bucket file
func (s *Storage) DataFileSize(bucket int) (int64, error) {
	info, err := s.dataFiles[bucket].Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// KeyCount returns the number of key slots in the file
func (s *Storage) KeyCount() (int64, error) {
	size, err := s.KeysFileSize()
	if err != nil {
		return 0, err
	}
	return size / KeyRecordSize, nil
}

// SlotCount returns the number of slots in a data bucket file
func (s *Storage) SlotCount(bucket int) (int64, error) {
	size, err := s.DataFileSize(bucket)
	if err != nil {
		return 0, err
	}
	return size / int64(s.SlotSize(bucket)), nil
}

// UpdateLastAccessed updates only the lastAccessed field
func (s *Storage) UpdateLastAccessed(keyId int64, lastAccessed time.Time) error {
	offset := keyId*KeyRecordSize + 1 + 2 + MaxKeySize // Skip free + keyLen + key
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(lastAccessed.Unix()))
	_, err := s.keysFile.WriteAt(buf, offset)
	return err
}

// UpdateSlotIdx updates only the slotIdx field in a key record
func (s *Storage) UpdateSlotIdx(keyId int64, slotIdx int64) error {
	offset := keyId*KeyRecordSize + 1 + 2 + MaxKeySize + 8 + 8 + 8 + 1 // Skip to slotIdx
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(slotIdx))
	_, err := s.keysFile.WriteAt(buf, offset)
	return err
}

// TruncateDataFile truncates a data bucket file to the given slot count
func (s *Storage) TruncateDataFile(bucket int, slotCount int64) error {
	newSize := slotCount * int64(s.SlotSize(bucket))
	return s.dataFiles[bucket].Truncate(newSize)
}

// TruncateKeysFile truncates the keys file to the given key count
func (s *Storage) TruncateKeysFile(keyCount int64) error {
	newSize := keyCount * KeyRecordSize
	return s.keysFile.Truncate(newSize)
}
