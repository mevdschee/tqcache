package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
)

// Constants from storage.go
const (
	KeyRecordSize  = 1059 // 2 + 1024 + 8 + 8 + 8 + 1 + 8
	MaxKeySize     = 1024
	DataHeaderSize = 1 + 4 // free + length
	NumBuckets     = 16
	MinBucketSize  = 1024
	FlagInUse      = 0x00
	FlagDeleted    = 0x01
)

// KeyRecord represents a fixed-size record in the keys file
type KeyRecord struct {
	KeyLen       uint16
	Key          [MaxKeySize]byte
	LastAccessed int64
	Cas          uint64
	Expiry       int64
	Bucket       byte
	SlotIdx      int64
}

// ValidEntry holds a validated key record and its data
type ValidEntry struct {
	KeyRecord KeyRecord
	Data      []byte
}

// TargetShard holds the output files and state for a target shard
type TargetShard struct {
	shardDir   string
	keysFile   *os.File
	dataFiles  [NumBuckets]*os.File
	keyCount   int64
	slotCounts [NumBuckets]int64
}

func main() {
	srcDir := flag.String("src-dir", "data", "Source directory containing shard data")
	dstDir := flag.String("dst-dir", "", "Destination directory for cleaned/resharded data (required)")
	targetShards := flag.Int("target-shards", 0, "Number of target shards (0 = same as source)")
	dryRun := flag.Bool("dry-run", false, "Only report errors, don't write clean files")
	verbose := flag.Bool("verbose", false, "Print progress information")
	flag.Parse()

	if *dstDir == "" {
		log.Fatal("ERROR: -dst-dir is required")
	}

	// Prevent overwriting source
	absSrc, _ := filepath.Abs(*srcDir)
	absDst, _ := filepath.Abs(*dstDir)
	if absSrc == absDst {
		log.Fatal("ERROR: -dst-dir must be different from -src-dir")
	}

	log.Printf("TQSession Cleanup/Reshard Tool")
	log.Printf("Source directory: %s", *srcDir)
	log.Printf("Destination directory: %s", *dstDir)

	// Discover source shards
	sourceShards, err := discoverShards(*srcDir)
	if err != nil {
		log.Fatalf("Failed to discover shards: %v", err)
	}
	if len(sourceShards) == 0 {
		log.Fatal("No shard directories found in source")
	}
	log.Printf("Found %d source shards: %v", len(sourceShards), sourceShards)

	// Determine target shard count
	numTargetShards := *targetShards
	if numTargetShards <= 0 {
		numTargetShards = len(sourceShards)
	}
	log.Printf("Target shards: %d", numTargetShards)

	if *dryRun {
		log.Printf("DRY RUN mode - no changes will be made")
	}

	// Phase 1: Read all valid entries from all source shards
	allEntries := make([]ValidEntry, 0)
	totalKeys := 0
	totalSkipped := 0

	for _, shardIdx := range sourceShards {
		shardDir := filepath.Join(*srcDir, fmt.Sprintf("shard_%02d", shardIdx))
		entries, keys, skipped, err := readShard(shardDir, *verbose)
		if err != nil {
			log.Printf("Shard %02d: ERROR - %v", shardIdx, err)
			continue
		}

		allEntries = append(allEntries, entries...)
		totalKeys += keys
		totalSkipped += skipped

		if *verbose || skipped > 0 {
			log.Printf("Shard %02d: %d keys processed, %d skipped", shardIdx, keys, skipped)
		}
	}

	log.Printf("Phase 1 complete: %d total keys read, %d skipped", totalKeys, totalSkipped)

	if *dryRun {
		log.Printf("DRY RUN: Would write %d entries to %d shards", len(allEntries), numTargetShards)
		return
	}

	// Phase 2: Create target shards and write entries
	if err := os.MkdirAll(*dstDir, 0755); err != nil {
		log.Fatalf("Failed to create destination directory: %v", err)
	}

	// Initialize target shards
	targetShardObjs := make([]*TargetShard, numTargetShards)
	for i := 0; i < numTargetShards; i++ {
		ts, err := newTargetShard(*dstDir, i)
		if err != nil {
			log.Fatalf("Failed to create target shard %d: %v", i, err)
		}
		targetShardObjs[i] = ts
	}
	defer func() {
		for _, ts := range targetShardObjs {
			ts.Close()
		}
	}()

	// Write entries to target shards
	bucketSizes := makeBucketSizes()
	for _, entry := range allEntries {
		// Determine target shard by hashing the key
		keyStr := string(entry.KeyRecord.Key[:entry.KeyRecord.KeyLen])
		targetIdx := hashKey(keyStr, numTargetShards)

		ts := targetShardObjs[targetIdx]
		bucket := int(entry.KeyRecord.Bucket)

		// Write data slot first
		slotIdx := ts.slotCounts[bucket]
		if err := writeDataSlot(ts.dataFiles[bucket], bucket, slotIdx, entry.Data, bucketSizes[bucket]); err != nil {
			log.Fatalf("Failed to write data to shard %d: %v", targetIdx, err)
		}
		ts.slotCounts[bucket]++

		// Update key record with new slot index
		newRec := entry.KeyRecord
		newRec.SlotIdx = slotIdx

		// Write key record
		if err := writeKeyRecord(ts.keysFile, ts.keyCount, &newRec); err != nil {
			log.Fatalf("Failed to write key to shard %d: %v", targetIdx, err)
		}
		ts.keyCount++
	}

	// Report results
	log.Printf("Phase 2 complete: %d entries written to %d shards", len(allEntries), numTargetShards)
	for i, ts := range targetShardObjs {
		if ts.keyCount > 0 {
			log.Printf("  Shard %02d: %d keys", i, ts.keyCount)
		}
	}
}

// discoverShards scans a directory for shard_XX subdirectories
func discoverShards(dataDir string) ([]int, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	shardPattern := regexp.MustCompile(`^shard_(\d+)$`)
	var shards []int

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		matches := shardPattern.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}
		var idx int
		fmt.Sscanf(matches[1], "%d", &idx)
		shards = append(shards, idx)
	}

	sort.Ints(shards)
	return shards, nil
}

// hashKey returns a shard index for the given key
func hashKey(key string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(numShards))
}

// makeBucketSizes creates the bucket size array
func makeBucketSizes() [NumBuckets]int {
	var sizes [NumBuckets]int
	size := MinBucketSize
	for i := 0; i < NumBuckets; i++ {
		sizes[i] = size
		size *= 2
	}
	return sizes
}

// newTargetShard creates a new target shard directory and files
func newTargetShard(dstDir string, shardIdx int) (*TargetShard, error) {
	shardDir := filepath.Join(dstDir, fmt.Sprintf("shard_%02d", shardIdx))
	if err := os.MkdirAll(shardDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create shard dir: %w", err)
	}

	ts := &TargetShard{
		shardDir: shardDir,
	}

	// Create keys file
	keysPath := filepath.Join(shardDir, "keys")
	keysFile, err := os.Create(keysPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create keys file: %w", err)
	}
	ts.keysFile = keysFile

	// Create data files
	for i := 0; i < NumBuckets; i++ {
		dataPath := filepath.Join(shardDir, fmt.Sprintf("data_%02d", i))
		dataFile, err := os.Create(dataPath)
		if err != nil {
			ts.Close()
			return nil, fmt.Errorf("failed to create data file %d: %w", i, err)
		}
		ts.dataFiles[i] = dataFile
	}

	return ts, nil
}

// Close closes all files in the target shard
func (ts *TargetShard) Close() {
	if ts.keysFile != nil {
		ts.keysFile.Close()
	}
	for _, f := range ts.dataFiles {
		if f != nil {
			f.Close()
		}
	}
}

// readShard reads all valid entries from a source shard
func readShard(shardDir string, verbose bool) ([]ValidEntry, int, int, error) {
	keysPath := filepath.Join(shardDir, "keys")
	keysFile, err := os.Open(keysPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, fmt.Errorf("failed to open keys file: %w", err)
	}
	defer keysFile.Close()

	// Get key count
	info, err := keysFile.Stat()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to stat keys file: %w", err)
	}
	keyCount := info.Size() / KeyRecordSize

	// Open data files for reading
	bucketSizes := makeBucketSizes()
	dataFiles := make([]*os.File, NumBuckets)
	for i := 0; i < NumBuckets; i++ {
		dataPath := filepath.Join(shardDir, fmt.Sprintf("data_%02d", i))
		dataFiles[i], err = os.Open(dataPath)
		if err != nil && !os.IsNotExist(err) {
			return nil, 0, 0, fmt.Errorf("failed to open data file %d: %w", i, err)
		}
	}
	defer func() {
		for _, f := range dataFiles {
			if f != nil {
				f.Close()
			}
		}
	}()

	// Collect valid entries
	validEntries := make([]ValidEntry, 0, keyCount)
	keys := 0
	skipped := 0

	for keyId := int64(0); keyId < keyCount; keyId++ {
		rec, err := readKeyRecord(keysFile, keyId)
		if err != nil {
			if verbose {
				log.Printf("  Key %d: failed to read record: %v", keyId, err)
			}
			skipped++
			continue
		}

		// Validate key length
		if rec.KeyLen > MaxKeySize {
			if verbose {
				log.Printf("  Key %d: invalid key length %d", keyId, rec.KeyLen)
			}
			skipped++
			continue
		}

		// Validate bucket
		if int(rec.Bucket) >= NumBuckets {
			if verbose {
				log.Printf("  Key %d: invalid bucket %d", keyId, rec.Bucket)
			}
			skipped++
			continue
		}

		bucket := int(rec.Bucket)
		slotIdx := rec.SlotIdx

		// Validate slot exists in data file
		if dataFiles[bucket] == nil {
			if verbose {
				log.Printf("  Key %d: data file for bucket %d does not exist", keyId, bucket)
			}
			skipped++
			continue
		}

		// Read and validate data slot
		data, err := readDataSlot(dataFiles[bucket], bucket, slotIdx, bucketSizes[bucket])
		if err != nil {
			if verbose {
				log.Printf("  Key %d: failed to read data slot (bucket=%d, slot=%d): %v", keyId, bucket, slotIdx, err)
			}
			skipped++
			continue
		}

		// Entry is valid
		validEntries = append(validEntries, ValidEntry{
			KeyRecord: *rec,
			Data:      data,
		})
		keys++
	}

	return validEntries, keys, skipped, nil
}

func readKeyRecord(f *os.File, keyId int64) (*KeyRecord, error) {
	offset := keyId * KeyRecordSize
	buf := make([]byte, KeyRecordSize)

	n, err := f.ReadAt(buf, offset)
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

func writeKeyRecord(f *os.File, keyId int64, rec *KeyRecord) error {
	offset := keyId * KeyRecordSize
	buf := make([]byte, KeyRecordSize)

	binary.LittleEndian.PutUint16(buf[0:2], rec.KeyLen)
	copy(buf[2:1026], rec.Key[:])
	binary.LittleEndian.PutUint64(buf[1026:1034], uint64(rec.LastAccessed))
	binary.LittleEndian.PutUint64(buf[1034:1042], rec.Cas)
	binary.LittleEndian.PutUint64(buf[1042:1050], uint64(rec.Expiry))
	buf[1050] = rec.Bucket
	binary.LittleEndian.PutUint64(buf[1051:1059], uint64(rec.SlotIdx))

	_, err := f.WriteAt(buf, offset)
	return err
}

func slotSize(bucket int, bucketSize int) int {
	return DataHeaderSize + bucketSize
}

func readDataSlot(f *os.File, bucket int, slotIdx int64, bucketSize int) ([]byte, error) {
	slotSz := slotSize(bucket, bucketSize)
	offset := slotIdx * int64(slotSz)

	// Check if offset is valid
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if offset >= info.Size() {
		return nil, fmt.Errorf("slot index %d out of range (file size: %d, slot size: %d)", slotIdx, info.Size(), slotSz)
	}

	// Read header
	header := make([]byte, DataHeaderSize)
	if _, err := f.ReadAt(header, offset); err != nil {
		return nil, err
	}

	if header[0] == FlagDeleted {
		return nil, fmt.Errorf("slot is marked as deleted")
	}

	length := binary.LittleEndian.Uint32(header[1:5])
	if int(length) > bucketSize {
		return nil, fmt.Errorf("invalid data length %d exceeds bucket size %d", length, bucketSize)
	}

	// Read data
	data := make([]byte, length)
	if _, err := f.ReadAt(data, offset+DataHeaderSize); err != nil {
		if err == io.EOF && length == 0 {
			return data, nil
		}
		return nil, err
	}

	return data, nil
}

func writeDataSlot(f *os.File, bucket int, slotIdx int64, data []byte, bucketSize int) error {
	slotSz := slotSize(bucket, bucketSize)
	offset := slotIdx * int64(slotSz)

	// Prepare buffer with header + data (padded to slot size)
	buf := make([]byte, slotSz)
	buf[0] = FlagInUse
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(data)))
	copy(buf[DataHeaderSize:], data)

	_, err := f.WriteAt(buf, offset)
	return err
}
