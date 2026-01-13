package tqcache

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupTestCache(t *testing.T) (*ShardedCache, func()) {
	tmpDir, err := os.MkdirTemp("", "tqcache_test")
	if err != nil {
		t.Fatal(err)
	}

	config := DefaultConfig()
	config.DataDir = tmpDir
	config.SyncStrategy = SyncNone

	c, err := NewSharded(config, 4) // Use 4 shards for tests
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatal(err)
	}

	return c, func() {
		c.Close()
		os.RemoveAll(tmpDir)
	}
}

func TestSetGet(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Set a key
	cas, err := c.Set("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Get the key
	val, getCas, err := c.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", val)
	}
	if getCas != cas {
		t.Errorf("CAS mismatch: set=%d, get=%d", cas, getCas)
	}

	// Overwrite
	newCas, err := c.Set("key1", []byte("value2"), 0)
	if err != nil {
		t.Fatalf("Set overwrite failed: %v", err)
	}
	if newCas == cas {
		t.Error("CAS should change on overwrite")
	}

	// Verify new value
	val, _, _ = c.Get("key1")
	if string(val) != "value2" {
		t.Errorf("Expected 'value2', got '%s'", val)
	}
}

func TestAdd(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Add to non-existent key should succeed
	cas, err := c.Add("key1", []byte("value1"), 0)
	if err != nil {
		t.Fatalf("Add to new key failed: %v", err)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Verify value
	val, _, err := c.Get("key1")
	if err != nil || string(val) != "value1" {
		t.Errorf("Get after Add failed: val=%s, err=%v", val, err)
	}

	// Add to existing key should fail with ErrKeyExists
	_, err = c.Add("key1", []byte("value2"), 0)
	if err != ErrKeyExists {
		t.Errorf("Expected ErrKeyExists for Add on existing key, got %v", err)
	}

	// Verify original value unchanged
	val, _, _ = c.Get("key1")
	if string(val) != "value1" {
		t.Errorf("Value changed after failed Add: %s", val)
	}
}

func TestReplace(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Replace on non-existent key should fail with ErrKeyNotFound
	_, err := c.Replace("key1", []byte("value1"), 0)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for Replace on missing key, got %v", err)
	}

	// Set a key first
	c.Set("key1", []byte("original"), 0)

	// Replace should succeed
	cas, err := c.Replace("key1", []byte("replaced"), 0)
	if err != nil {
		t.Fatalf("Replace failed: %v", err)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Verify value changed
	val, _, _ := c.Get("key1")
	if string(val) != "replaced" {
		t.Errorf("Expected 'replaced', got '%s'", val)
	}
}

func TestCas(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// CAS on non-existent key should fail with ErrKeyNotFound
	_, err := c.Cas("key1", []byte("value"), 0, 12345)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for CAS on missing key, got %v", err)
	}

	// Set a key
	originalCas, _ := c.Set("key1", []byte("original"), 0)

	// CAS with wrong token should fail with ErrCasMismatch
	_, err = c.Cas("key1", []byte("wrong"), 0, originalCas+1)
	if err != ErrCasMismatch {
		t.Errorf("Expected ErrCasMismatch for CAS mismatch, got %v", err)
	}

	// Verify value unchanged
	val, _, _ := c.Get("key1")
	if string(val) != "original" {
		t.Errorf("Value changed after failed CAS: %s", val)
	}

	// CAS with correct token should succeed
	newCas, err := c.Cas("key1", []byte("updated"), 0, originalCas)
	if err != nil {
		t.Fatalf("CAS with correct token failed: %v", err)
	}
	if newCas == originalCas {
		t.Error("CAS should return new token")
	}

	// Verify value changed
	val, _, _ = c.Get("key1")
	if string(val) != "updated" {
		t.Errorf("Expected 'updated', got '%s'", val)
	}
}

func TestCasConcurrency(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	const numGoroutines = 100
	const key = "counter"

	// Initialize counter to 0
	c.Set(key, []byte("0"), 0)

	// Launch goroutines that each increment the counter using CAS
	var wg sync.WaitGroup
	successCount := int64(0)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each goroutine tries to increment until it succeeds
			for {
				// Get current value and CAS token
				val, cas, err := c.Get(key)
				if err != nil {
					continue
				}

				// Parse current value
				current := 0
				fmt.Sscanf(string(val), "%d", &current)

				// Try to increment with CAS
				newVal := fmt.Sprintf("%d", current+1)
				_, err = c.Cas(key, []byte(newVal), 0, cas)
				if err == nil {
					// CAS succeeded, increment success counter
					atomic.AddInt64(&successCount, 1)
					return
				}
				// CAS failed (concurrent modification), retry
			}
		}()
	}

	wg.Wait()

	// Verify final counter value equals number of goroutines
	val, _, _ := c.Get(key)
	finalValue := 0
	fmt.Sscanf(string(val), "%d", &finalValue)

	if finalValue != numGoroutines {
		t.Errorf("Expected counter=%d, got %d (CAS race condition!)", numGoroutines, finalValue)
	}

	if successCount != numGoroutines {
		t.Errorf("Expected %d successful CAS operations, got %d", numGoroutines, successCount)
	}

	t.Logf("CAS concurrency test passed: %d goroutines, final counter=%d", numGoroutines, finalValue)
}

func TestDelete(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Delete non-existent key should fail with ErrKeyNotFound
	err := c.Delete("key1")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for Delete on missing key, got %v", err)
	}

	// Set a key
	c.Set("key1", []byte("value"), 0)

	// Delete should succeed
	err = c.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get should fail with ErrKeyNotFound
	_, _, err = c.Get("key1")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after Delete, got %v", err)
	}

	// Delete again should fail with ErrKeyNotFound
	err = c.Delete("key1")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for second Delete, got %v", err)
	}
}

func TestTouch(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Touch non-existent key should fail with ErrKeyNotFound
	_, err := c.Touch("key1", time.Hour)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for Touch on missing key, got %v", err)
	}

	// Set a key with short TTL
	c.Set("key1", []byte("value"), 1*time.Second)

	// Touch to extend TTL
	cas, err := c.Touch("key1", 1*time.Hour)
	if err != nil {
		t.Fatalf("Touch failed: %v", err)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Verify value still accessible
	val, _, err := c.Get("key1")
	if err != nil || string(val) != "value" {
		t.Errorf("Get after Touch failed")
	}
}

func TestFlushAll(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Set multiple keys
	c.Set("key1", []byte("value1"), 0)
	c.Set("key2", []byte("value2"), 0)
	c.Set("key3", []byte("value3"), 0)

	// Verify they exist
	_, _, err := c.Get("key1")
	if err != nil {
		t.Fatal("Key1 should exist before flush")
	}

	// Flush all
	c.FlushAll()

	// All keys should be gone (ErrKeyNotFound)
	_, _, err = c.Get("key1")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after FlushAll, got %v", err)
	}
	_, _, err = c.Get("key2")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after FlushAll, got %v", err)
	}
	_, _, err = c.Get("key3")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after FlushAll, got %v", err)
	}
}

func TestIncrement(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Increment non-existent key should fail with ErrKeyNotFound
	_, _, err := c.Increment("counter", 1)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for Increment on missing key, got %v", err)
	}

	// Set numeric value
	c.Set("counter", []byte("10"), 0)

	// Increment
	newVal, cas, err := c.Increment("counter", 5)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if newVal != 15 {
		t.Errorf("Expected 15, got %d", newVal)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Verify stored value
	val, _, _ := c.Get("counter")
	if string(val) != "15" {
		t.Errorf("Expected '15', got '%s'", val)
	}
}

func TestDecrement(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Set numeric value
	c.Set("counter", []byte("10"), 0)

	// Decrement
	newVal, cas, err := c.Decrement("counter", 3)
	if err != nil {
		t.Fatalf("Decrement failed: %v", err)
	}
	if newVal != 7 {
		t.Errorf("Expected 7, got %d", newVal)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Decrement below zero should floor at 0
	newVal, _, err = c.Decrement("counter", 100)
	if err != nil {
		t.Fatalf("Decrement failed: %v", err)
	}
	if newVal != 0 {
		t.Errorf("Expected 0 (floor), got %d", newVal)
	}

	// Verify stored value
	val, _, _ := c.Get("counter")
	if string(val) != "0" {
		t.Errorf("Expected '0', got '%s'", val)
	}
}

func TestAppend(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Append to non-existent key should fail with ErrKeyNotFound
	_, err := c.Append("key1", []byte("suffix"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for Append on missing key, got %v", err)
	}

	// Set a key
	c.Set("key1", []byte("hello"), 0)

	// Append
	cas, err := c.Append("key1", []byte(" world"))
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Verify
	val, _, _ := c.Get("key1")
	if string(val) != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", val)
	}
}

func TestPrepend(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Prepend to non-existent key should fail with ErrKeyNotFound
	_, err := c.Prepend("key1", []byte("prefix"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for Prepend on missing key, got %v", err)
	}

	// Set a key
	c.Set("key1", []byte("world"), 0)

	// Prepend
	cas, err := c.Prepend("key1", []byte("hello "))
	if err != nil {
		t.Fatalf("Prepend failed: %v", err)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Verify
	val, _, _ := c.Get("key1")
	if string(val) != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", val)
	}
}

func TestStats(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Empty cache
	stats := c.Stats()
	if stats["curr_items"] != "0" {
		t.Errorf("Expected 0 items, got %s", stats["curr_items"])
	}

	// Add items
	c.Set("key1", []byte("value1"), 0)
	c.Set("key2", []byte("value2"), 0)

	stats = c.Stats()
	if stats["curr_items"] != "2" {
		t.Errorf("Expected 2 items, got %s", stats["curr_items"])
	}
}

func TestExpiry(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Set a key with short TTL (now works with millisecond precision)
	cas, setErr := c.Set("expiry_key", []byte("expiry_value"), 200*time.Millisecond)
	if setErr != nil {
		t.Fatalf("Set failed: %v", setErr)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	// Should be accessible immediately
	val, _, err := c.Get("expiry_key")
	if err != nil {
		t.Fatalf("Key should be accessible immediately: err=%v", err)
	}
	if string(val) != "expiry_value" {
		t.Errorf("Expected 'expiry_value', got '%s'", val)
	}

	// Wait for expiry
	time.Sleep(300 * time.Millisecond)

	// Should be gone due to expiry check in Get (ErrKeyNotFound)
	_, _, err = c.Get("expiry_key")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after expiry, got %v", err)
	}
}

func TestMaxTTL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tqcache-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultConfig()
	config.DataDir = tmpDir
	config.SyncStrategy = SyncNone
	config.MaxTTL = 200 * time.Millisecond // Max TTL of 200ms

	c, err := NewSharded(config, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Set with TTL larger than max - should be capped
	_, err = c.Set("capped_key", []byte("capped_value"), 10*time.Second)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Key should exist initially
	val, _, err := c.Get("capped_key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "capped_value" {
		t.Errorf("Expected 'capped_value', got '%s'", val)
	}

	// Wait for max TTL to expire (not the requested 10s)
	time.Sleep(300 * time.Millisecond)

	// Should be gone because TTL was capped to 200ms
	_, _, err = c.Get("capped_key")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after max TTL expiry, got %v (MaxTTL cap not applied?)", err)
	}

	t.Log("MaxTTL correctly caps requested TTL values")
}

func TestLargeValue(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Test with 1KB value (should fit in bucket 0)
	val1K := make([]byte, 1024)
	for i := range val1K {
		val1K[i] = byte(i % 256)
	}

	cas, err := c.Set("key1k", val1K, 0)
	if err != nil {
		t.Fatalf("Set 1K value failed: %v", err)
	}
	if cas == 0 {
		t.Error("Expected non-zero CAS")
	}

	retrieved, _, err := c.Get("key1k")
	if err != nil {
		t.Fatalf("Get 1K value failed: %v", err)
	}
	if len(retrieved) != 1024 {
		t.Errorf("Expected 1024 bytes, got %d", len(retrieved))
	}
	for i := 0; i < 1024; i++ {
		if retrieved[i] != byte(i%256) {
			t.Errorf("Byte mismatch at %d: expected %d, got %d", i, byte(i%256), retrieved[i])
			break
		}
	}

	// Test with 10KB value (should fit in bucket 3: 8KB < 10KB <= 16KB)
	val10K := make([]byte, 10*1024)
	for i := range val10K {
		val10K[i] = byte((i * 7) % 256)
	}

	_, err = c.Set("key10k", val10K, 0)
	if err != nil {
		t.Fatalf("Set 10K value failed: %v", err)
	}

	retrieved, _, err = c.Get("key10k")
	if err != nil {
		t.Fatalf("Get 10K value failed: %v", err)
	}
	if len(retrieved) != 10*1024 {
		t.Errorf("Expected 10240 bytes, got %d", len(retrieved))
	}
}

func TestPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tqcache_persistence_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultConfig()
	config.DataDir = tmpDir
	config.SyncStrategy = SyncAlways // Ensure persistence

	// Phase 1: Create cache, write items
	c, err := NewSharded(config, 4)
	if err != nil {
		t.Fatal(err)
	}

	// Write some items
	c.Set("key1", []byte("value1"), 0)
	c.Set("key2", []byte("value2"), 0)
	c.Set("key3", []byte("value3"), 0)

	// Close the cache
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: Reopen cache and verify data persisted
	c2, err := NewSharded(config, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// Verify all items exist
	val, _, err := c2.Get("key1")
	if err != nil {
		t.Errorf("key1 should exist after restart: %v", err)
	} else if string(val) != "value1" {
		t.Errorf("key1 value mismatch: expected 'value1', got '%s'", val)
	}

	val, _, err = c2.Get("key2")
	if err != nil {
		t.Errorf("key2 should exist after restart: %v", err)
	} else if string(val) != "value2" {
		t.Errorf("key2 value mismatch: expected 'value2', got '%s'", val)
	}

	val, _, err = c2.Get("key3")
	if err != nil {
		t.Errorf("key3 should exist after restart: %v", err)
	} else if string(val) != "value3" {
		t.Errorf("key3 value mismatch: expected 'value3', got '%s'", val)
	}

	t.Log("Persistence test passed: data survives restart")
}

func TestMultipleKeys(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Write 100 items
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", i)
		value := []byte("value" + key)
		if _, err := c.Set(key, value, 0); err != nil {
			t.Fatalf("Set failed for %s: %v", key, err)
		}
	}

	// Verify all items exist
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%02d", i)
		expected := "value" + key
		val, _, err := c.Get(key)
		if err != nil {
			t.Errorf("Get failed for %s: %v", key, err)
			continue
		}
		if string(val) != expected {
			t.Errorf("Value mismatch for %s: expected '%s', got '%s'", key, expected, val)
		}
	}

	// Verify stats
	stats := c.Stats()
	if stats["curr_items"] != "100" {
		t.Errorf("Expected 100 items, got %s", stats["curr_items"])
	}
}

func TestBucketSelection(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Test values of different sizes to ensure bucket selection works
	testCases := []struct {
		name   string
		size   int
		bucket int // expected bucket (0=1KB, 1=2KB, 2=4KB, etc.)
	}{
		{"tiny", 100, 0},   // < 1KB → bucket 0
		{"1KB", 1024, 0},   // = 1KB → bucket 0
		{"1.5KB", 1536, 1}, // > 1KB → bucket 1 (2KB)
		{"4KB", 4096, 2},   // = 4KB → bucket 2
		{"10KB", 10240, 4}, // > 8KB → bucket 4 (16KB)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value := make([]byte, tc.size)
			for i := range value {
				value[i] = byte(i % 256)
			}

			key := "key_" + tc.name
			if _, err := c.Set(key, value, 0); err != nil {
				t.Fatalf("Set failed for %s: %v", key, err)
			}

			retrieved, _, err := c.Get(key)
			if err != nil {
				t.Fatalf("Get failed for %s: %v", key, err)
			}

			if len(retrieved) != tc.size {
				t.Errorf("Size mismatch for %s: expected %d, got %d", key, tc.size, len(retrieved))
			}

			// Verify data integrity
			for i := 0; i < tc.size; i++ {
				if retrieved[i] != byte(i%256) {
					t.Errorf("Data corruption in %s at byte %d", key, i)
					break
				}
			}
		})
	}
}

func TestOverwrite(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Set initial value
	cas1, _ := c.Set("overwrite_key", []byte("initial"), 0)

	// Overwrite with new value
	cas2, _ := c.Set("overwrite_key", []byte("updated"), 0)

	// CAS should change
	if cas1 == cas2 {
		t.Error("CAS should change on overwrite")
	}

	// Value should be updated
	val, _, _ := c.Get("overwrite_key")
	if string(val) != "updated" {
		t.Errorf("Expected 'updated', got '%s'", val)
	}

	// Stats should show only 1 item
	stats := c.Stats()
	if stats["curr_items"] != "1" {
		t.Errorf("Expected 1 item after overwrite, got %s", stats["curr_items"])
	}
}

func TestKeyWithNullBytes(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Create a key with null bytes (3 null bytes)
	keyWithNulls := "\x00\x00\x00"
	value := []byte("value for null key")

	// Set should work
	_, err := c.Set(keyWithNulls, value, 0)
	if err != nil {
		t.Fatalf("Set with null byte key failed: %v", err)
	}

	// Get should return the same value
	retrieved, _, err := c.Get(keyWithNulls)
	if err != nil {
		t.Fatalf("Get with null byte key failed: %v", err)
	}
	if string(retrieved) != string(value) {
		t.Errorf("Expected %q, got %q", value, retrieved)
	}

	// A different key (e.g., 4 nulls) should not match
	_, _, err = c.Get("\x00\x00\x00\x00")
	if err == nil {
		t.Errorf("Different null key should not match")
	}

	// Key with null in middle should work
	keyWithMiddleNull := "abc\x00def"
	_, err = c.Set(keyWithMiddleNull, []byte("middle null"), 0)
	if err != nil {
		t.Fatalf("Set with middle null failed: %v", err)
	}

	retrieved, _, err = c.Get(keyWithMiddleNull)
	if err != nil {
		t.Fatalf("Get with middle null failed: %v", err)
	}
	if string(retrieved) != "middle null" {
		t.Errorf("Expected 'middle null', got %q", retrieved)
	}

	t.Log("Keys with null bytes work correctly")
}

func TestKeyNotTrimmed(t *testing.T) {
	c, cleanup := setupTestCache(t)
	defer cleanup()

	// Key with trailing spaces
	keyWithSpaces := "mykey   "
	value := []byte("value with spaces")

	_, err := c.Set(keyWithSpaces, value, 0)
	if err != nil {
		t.Fatalf("Set with trailing spaces failed: %v", err)
	}

	// Get with exact key (including spaces) should work
	retrieved, _, err := c.Get(keyWithSpaces)
	if err != nil {
		t.Fatalf("Get with exact key failed: %v", err)
	}
	if string(retrieved) != string(value) {
		t.Errorf("Expected %q, got %q", value, retrieved)
	}

	// Get with trimmed key should NOT work (different key)
	_, _, err = c.Get("mykey")
	if err == nil {
		t.Errorf("Trimmed key should not match key with trailing spaces")
	}

	// Key with leading spaces
	keyLeading := "   leading"
	_, err = c.Set(keyLeading, []byte("leading spaces"), 0)
	if err != nil {
		t.Fatalf("Set with leading spaces failed: %v", err)
	}

	_, _, err = c.Get("leading")
	if err == nil {
		t.Errorf("Key without leading spaces should not match")
	}

	t.Log("Keys are preserved exactly without trimming")
}
