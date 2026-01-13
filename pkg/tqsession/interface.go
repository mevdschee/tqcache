package tqsession

import "time"

// CacheInterface defines the interface for ShardedCache.
// Allows server to work with the cache implementation.
type CacheInterface interface {
	Get(key string) ([]byte, uint64, error)
	Set(key string, value []byte, ttl time.Duration) (uint64, error)
	Add(key string, value []byte, ttl time.Duration) (uint64, error)
	Replace(key string, value []byte, ttl time.Duration) (uint64, error)
	Cas(key string, value []byte, ttl time.Duration, cas uint64) (uint64, error)
	Delete(key string) error
	Touch(key string, ttl time.Duration) (uint64, error)
	Increment(key string, delta uint64) (uint64, uint64, error)
	Decrement(key string, delta uint64) (uint64, uint64, error)
	Append(key string, value []byte) (uint64, error)
	Prepend(key string, value []byte) (uint64, error)
	FlushAll()
	Stats() map[string]string
	Close() error
	GetStartTime() time.Time
}

// Ensure ShardedCache implements CacheInterface
var _ CacheInterface = (*ShardedCache)(nil)
