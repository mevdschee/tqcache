package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mevdschee/tqsession/internal/config"
	"github.com/mevdschee/tqsession/pkg/server"
	"github.com/mevdschee/tqsession/pkg/tqsession"
)

func main() {
	configFile := flag.String("config", "", "Path to config file (INI format)")
	dataDir := flag.String("data-dir", "data", "Directory for data files")
	listen := flag.String("listen", ":11211", "Address to listen on ([host]:port)")
	syncMode := flag.String("sync-mode", "periodic", "Sync mode: none, periodic")
	syncInterval := flag.Duration("sync-interval", time.Second, "Sync interval for periodic fsync")
	defaultTTL := flag.Duration("default-ttl", 0, "Default TTL for keys without explicit expiry (0 = no expiry)")
	maxDataSize := flag.Int64("max-data-size", 64*1024*1024, "Maximum live data size in bytes for LRU eviction (0 = unlimited)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	var cfg tqsession.Config
	var serverPort string

	// Load config file if specified
	if *configFile != "" {
		fileCfg, err := config.Load(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config file: %v", err)
		}
		cfg, err = fileCfg.ToTQSessionConfig()
		if err != nil {
			log.Fatalf("Invalid config: %v", err)
		}
		serverPort = fileCfg.Server.Listen
		if serverPort == "" {
			serverPort = ":11211"
		}
		log.Printf("Loaded config from %s", *configFile)
	} else {
		// Use command-line flags
		var syncStrategy tqsession.SyncStrategy
		switch *syncMode {
		case "none":
			syncStrategy = tqsession.SyncNone
		case "periodic":
			syncStrategy = tqsession.SyncPeriodic
		default:
			log.Fatalf("Invalid sync-mode: %s (valid: none, periodic)", *syncMode)
		}

		cfg = tqsession.Config{
			DataDir:       *dataDir,
			DefaultExpiry: *defaultTTL,
			MaxKeySize:    1024,             // 1KB max key size
			MaxValueSize:  64 * 1024 * 1024, // 64MB
			MaxDataSize:   *maxDataSize,
			SyncStrategy:  syncStrategy,
			SyncInterval:  *syncInterval,
		}
		serverPort = *listen
	}

	cache, err := tqsession.New(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize TQSession: %v", err)
	}
	defer cache.Close()

	srv := server.New(cache, serverPort)
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Start pprof server
	go func() {
		log.Println("Starting pprof server on :6062")
		if err := http.ListenAndServe("localhost:6062", nil); err != nil {
			log.Println("Pprof failed:", err)
		}
	}()

	// Set up signal handling
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	log.Printf("TQSession started on %s", serverPort)
	<-quit
	log.Println("Shutting down TQSession...")
}
