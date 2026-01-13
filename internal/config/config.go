package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mevdschee/tqcache/pkg/tqcache"
)

// Config represents the application configuration.
// It maps to the INI config file and converts to tqcache.Config.
type Config struct {
	Server struct {
		Listen string // Address to listen on (e.g., :11211 or localhost:11211)
	}
	Storage struct {
		DataDir         string
		Shards          string // e.g., "16"
		DefaultTTL      string // e.g., "0s", "1h"
		MaxTTL          string // e.g., "0s" (unlimited), "24h"
		SyncStrategy    string // "none", "periodic"
		SyncInterval    string // e.g., "1s"
		ChannelCapacity string // e.g., "100" or "1000"
	}
}

// Load reads an INI configuration file from the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return parseINI(string(data))
}

func parseINI(data string) (*Config, error) {
	cfg := &Config{}

	lines := strings.Split(data, "\n")
	currentSection := ""

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.ToLower(line[1 : len(line)-1])
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(strings.ToLower(parts[0]))
		value := strings.TrimSpace(parts[1])
		// Remove inline comments
		if idx := strings.Index(value, " #"); idx != -1 {
			value = strings.TrimSpace(value[:idx])
		}

		switch currentSection {
		case "server":
			switch key {
			case "listen":
				cfg.Server.Listen = value
			}
		case "storage":
			switch key {
			case "data-dir":
				cfg.Storage.DataDir = value
			case "shards":
				cfg.Storage.Shards = value
			case "default-ttl":
				cfg.Storage.DefaultTTL = value
			case "max-ttl":
				cfg.Storage.MaxTTL = value
			case "sync-mode":
				cfg.Storage.SyncStrategy = value
			case "sync-interval":
				cfg.Storage.SyncInterval = value
			case "channel-capacity":
				cfg.Storage.ChannelCapacity = value
			}
		}
	}

	return cfg, nil
}

// ToTQCacheConfig converts the file-based configuration to the library's config struct.
func (c *Config) ToTQCacheConfig() (tqcache.Config, error) {
	cfg := tqcache.DefaultConfig()

	if c.Storage.DataDir != "" {
		cfg.DataDir = c.Storage.DataDir
	}

	if c.Storage.DefaultTTL != "" {
		dur, err := time.ParseDuration(c.Storage.DefaultTTL)
		if err != nil {
			return cfg, fmt.Errorf("invalid default_expiry: %w", err)
		}
		cfg.DefaultTTL = dur
	}

	if c.Storage.MaxTTL != "" {
		dur, err := time.ParseDuration(c.Storage.MaxTTL)
		if err != nil {
			return cfg, fmt.Errorf("invalid max-ttl: %w", err)
		}
		cfg.MaxTTL = dur
	}

	if c.Storage.SyncStrategy != "" {
		switch c.Storage.SyncStrategy {
		case "always":
			cfg.SyncStrategy = tqcache.SyncAlways
		case "periodic":
			cfg.SyncStrategy = tqcache.SyncPeriodic
		case "none":
			cfg.SyncStrategy = tqcache.SyncNone
		default:
			return cfg, fmt.Errorf("invalid sync_strategy: %s (valid: none, periodic)", c.Storage.SyncStrategy)
		}
	}

	if c.Storage.SyncInterval != "" {
		dur, err := time.ParseDuration(c.Storage.SyncInterval)
		if err != nil {
			return cfg, fmt.Errorf("invalid sync_interval: %w", err)
		}
		cfg.SyncInterval = dur
	}

	if c.Storage.ChannelCapacity != "" {
		n, err := strconv.Atoi(c.Storage.ChannelCapacity)
		if err != nil {
			return cfg, fmt.Errorf("invalid channel-capacity: %w", err)
		}
		cfg.ChannelCapacity = n
	}

	return cfg, nil
}

// Shards returns the configured number of shards
func (c *Config) Shards() int {
	if c.Storage.Shards == "" {
		return tqcache.DefaultShardCount
	}
	n, err := strconv.Atoi(c.Storage.Shards)
	if err != nil || n <= 0 {
		return tqcache.DefaultShardCount
	}
	return n
}
