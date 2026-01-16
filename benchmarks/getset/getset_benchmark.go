package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
)

var (
	host       = flag.String("host", "localhost:11211", "Host to benchmark")
	protocol   = flag.String("protocol", "memc-txt", "Protocol: memc-txt, memc-bin, or redis")
	csvOutput  = flag.Bool("csv", false, "Output results in CSV format")
	label      = flag.String("label", "Target", "Label for the backend (used in CSV)")
	mode       = flag.String("mode", "default", "Sync mode label (used in CSV)")
	clients    = flag.Int("clients", 10, "Number of concurrent clients")
	requests   = flag.Int("requests", 100000, "Total number of requests")
	valueSize  = flag.Int("size", 1024, "Value size in bytes")
	keys       = flag.Int("keys", 100000, "Key space size")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	operation  = flag.String("op", "both", "operation to benchmark: set, get, or both")
	sequential = flag.Bool("sequential", true, "Sequential key access (vs random)")
)

// Benchmarker defines the interface for benchmarking different cache backends
type Benchmarker interface {
	Set(key string, value []byte) error
	Get(key string) error
	Close() error
}

// MemcacheClient implements Benchmarker for Memcached
type MemcacheClient struct {
	client *memcache.Client
}

func NewMemcacheClient(server string) *MemcacheClient {
	return &MemcacheClient{
		client: memcache.New(server),
	}
}

func (m *MemcacheClient) Set(key string, value []byte) error {
	return m.client.Set(&memcache.Item{Key: key, Value: value})
}

func (m *MemcacheClient) Get(key string) error {
	_, err := m.client.Get(key)
	return err
}

func (m *MemcacheClient) Close() error {
	// gomemcache client doesn't explicitly need closing as it manages connections lazily,
	// but strictly speaking we can't close the *Client itself, just the connections it holds.
	// For this benchmark where we create one per routine, it's fine.
	return nil
}

// RedisClient implements Benchmarker for Redis
type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr string) *RedisClient {
	return &RedisClient{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
	}
}

func (r *RedisClient) Set(key string, value []byte) error {
	return r.client.Set(context.Background(), key, value, 0).Err()
}

func (r *RedisClient) Get(key string) error {
	return r.client.Get(context.Background(), key).Err()
}

func (r *RedisClient) Close() error {
	return r.client.Close()
}

func main() {
	flag.Parse()

	// Wait for server to come up (manual check)
	conn, err := net.DialTimeout("tcp", *host, 2*time.Second)
	if err != nil {
		log.Fatalf("Cannot connect to %s: %v", *host, err)
	}
	conn.Close()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if !*csvOutput {
		fmt.Printf("Benchmarking %s (%s) with %d clients, %d requests, %d byte values, %d keys...\n", *host, *protocol, *clients, *requests, *valueSize, *keys)
	}

	// Pre-generate keys/values to avoid creating garbage during critical path
	keyParams := make([]string, *keys)
	for i := 0; i < *keys; i++ {
		keyParams[i] = fmt.Sprintf("key_%d", i)
	}

	val := make([]byte, *valueSize)
	rand.Read(val)

	// Factory function to create new clients based on protocol
	// Note: For benchmarks, creating a new client per goroutine (or sharing one) depends on the driver.
	// gomemcache is typically thread-safe but here we mimicked the original behavior of one client per routine.
	// go-redis is also thread-safe and uses a pool.
	// To strictly emulate "concurrent clients", creating separate instances is cleaner for isolation,
	// but might just be multiple connections from one pool in go-redis case.
	// For simplicity, we create a new "Benchmarker" wrapper per routine.

	// Factory function to create new clients based on protocol
	// Note: For benchmarks, creating a new client per goroutine (or sharing one) depends on the driver.
	// gomemcache is typically thread-safe but here we mimicked the original behavior of one client per routine.
	// go-redis is also thread-safe and uses a pool.
	// To strictly emulate "concurrent clients", creating separate instances is cleaner for isolation,
	// but might just be multiple connections from one pool in go-redis case.
	// For simplicity, we create a new "Benchmarker" wrapper per routine.

	clientFactory := func() Benchmarker {
		switch *protocol {
		case "memc-txt":
			return NewMemcacheClient(*host)
		case "memc-bin":
			return NewBinaryMemcacheClient(*host)
		case "redis":
			return NewRedisClient(*host)
		default:
			log.Fatalf("Unknown protocol: %s", *protocol)
			return nil
		}
	}

	// SET Benchmark
	if *operation == "set" || *operation == "both" {
		start := time.Now()
		if *sequential {
			runBenchmarkSequential("SET", clientFactory, keyParams, func(b Benchmarker, key string) error {
				return b.Set(key, val)
			})
		} else {
			runBenchmarkRandom("SET", clientFactory, keyParams, func(b Benchmarker, key string) error {
				return b.Set(key, val)
			})
		}
		elapsed := time.Since(start)
		printResults("SET", elapsed)
	}

	// GET Benchmark
	if *operation == "get" || *operation == "both" {
		start := time.Now()
		if *sequential {
			runBenchmarkSequential("GET", clientFactory, keyParams, func(b Benchmarker, key string) error {
				return b.Get(key)
			})
		} else {
			runBenchmarkRandom("GET", clientFactory, keyParams, func(b Benchmarker, key string) error {
				return b.Get(key)
			})
		}
		elapsed := time.Since(start)
		printResults("GET", elapsed)
	}
}

// BinaryMemcacheClient implements Benchmarker for Memcached Binary Protocol
type BinaryMemcacheClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewBinaryMemcacheClient(server string) *BinaryMemcacheClient {
	conn, err := net.Dial("tcp", server)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", server, err)
	}
	return &BinaryMemcacheClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (c *BinaryMemcacheClient) Set(key string, value []byte) error {
	// Header (24 bytes) + Extras (8 bytes) + Key + Value
	totalLen := 8 + len(key) + len(value)
	// We can write components directly to bufio to avoid alloc of full buffer
	// Or just alloc small header and write rest.

	// Header
	reqHeader := make([]byte, 24)
	reqHeader[0] = 0x80
	reqHeader[1] = 0x01 // SET
	reqHeader[2] = byte(len(key) >> 8)
	reqHeader[3] = byte(len(key))
	reqHeader[4] = 8 // Extras length
	reqHeader[8] = byte(totalLen >> 24)
	reqHeader[9] = byte(totalLen >> 16)
	reqHeader[10] = byte(totalLen >> 8)
	reqHeader[11] = byte(totalLen)

	if _, err := c.writer.Write(reqHeader); err != nil {
		return err
	}

	// Extras (8 bytes of zeros)
	// We can just write 8 zeros
	zeros := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	if _, err := c.writer.Write(zeros); err != nil {
		return err
	}

	// Key
	if _, err := c.writer.WriteString(key); err != nil {
		return err
	}
	// Value
	if _, err := c.writer.Write(value); err != nil {
		return err
	}

	if err := c.writer.Flush(); err != nil {
		return err
	}

	// Read response header (24 bytes)
	respHeader := make([]byte, 24)
	if _, err := io.ReadFull(c.reader, respHeader); err != nil {
		return err
	}

	// Check status (bytes 6-7)
	status := uint16(respHeader[6])<<8 | uint16(respHeader[7])
	if status != 0 {
		return fmt.Errorf("memcache error status: %d", status)
	}

	bodyLen := uint32(respHeader[8])<<24 | uint32(respHeader[9])<<16 | uint32(respHeader[10])<<8 | uint32(respHeader[11])
	if bodyLen > 0 {
		// Discard body
		discard := make([]byte, bodyLen)
		if _, err := io.ReadFull(c.reader, discard); err != nil {
			return err
		}
	}

	return nil
}

func (c *BinaryMemcacheClient) Get(key string) error {
	// Header (24 bytes) + Key
	totalLen := len(key)

	reqHeader := make([]byte, 24)
	reqHeader[0] = 0x80
	reqHeader[1] = 0x00 // GET
	reqHeader[2] = byte(len(key) >> 8)
	reqHeader[3] = byte(len(key))
	reqHeader[4] = 0 // Extra len
	reqHeader[8] = byte(totalLen >> 24)
	reqHeader[9] = byte(totalLen >> 16)
	reqHeader[10] = byte(totalLen >> 8)
	reqHeader[11] = byte(totalLen)

	if _, err := c.writer.Write(reqHeader); err != nil {
		return err
	}
	if _, err := c.writer.WriteString(key); err != nil {
		return err
	}
	if err := c.writer.Flush(); err != nil {
		return err
	}

	// Read response header
	respHeader := make([]byte, 24)
	if _, err := io.ReadFull(c.reader, respHeader); err != nil {
		return err
	}

	status := uint16(respHeader[6])<<8 | uint16(respHeader[7])

	// Always consume body even on error (prevents protocol desync)
	bodyLen := uint32(respHeader[8])<<24 | uint32(respHeader[9])<<16 | uint32(respHeader[10])<<8 | uint32(respHeader[11])
	if bodyLen > 0 {
		trash := make([]byte, bodyLen)
		if _, err := io.ReadFull(c.reader, trash); err != nil {
			return err
		}
	}

	// Key not found (0x0001) is acceptable for GET benchmark
	if status != 0 && status != 1 {
		return fmt.Errorf("memcache error status: %d", status)
	}

	return nil
}

func (c *BinaryMemcacheClient) Close() error {
	return c.conn.Close()
}

func runBenchmarkSequential(name string, factory func() Benchmarker, keyParams []string, op func(Benchmarker, string) error) {
	var wg sync.WaitGroup
	requestsPerClient := *requests / *clients
	numKeys := len(keyParams)

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		startIdx := i * requestsPerClient
		go func(start int) {
			defer wg.Done()
			client := factory()
			defer client.Close()

			for j := 0; j < requestsPerClient; j++ {
				keyIdx := (start + j) % numKeys
				_ = op(client, keyParams[keyIdx])
			}
		}(startIdx)
	}
	wg.Wait()
}

func runBenchmarkRandom(name string, factory func() Benchmarker, keyParams []string, op func(Benchmarker, string) error) {
	var wg sync.WaitGroup
	requestsPerClient := *requests / *clients
	numKeys := len(keyParams)

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := factory()
			defer client.Close()

			for j := 0; j < requestsPerClient; j++ {
				_ = op(client, keyParams[rand.Intn(numKeys)])
			}
		}()
	}
	wg.Wait()
}

func printResults(op string, elapsed time.Duration) {
	rps := float64(*requests) / elapsed.Seconds()
	if *csvOutput {
		// Mode,Backend,Protocol,Operation,RPS,TimePerReq(ms)
		fmt.Printf("%s,%s,%s,%s,%.2f,%.4f\n", *mode, *label, *protocol, op, rps, elapsed.Seconds()*1000/float64(*requests))
	} else {
		fmt.Printf("%-5s: %.2f req/sec (Time: %s)\n", op, rps, elapsed)
	}
}
