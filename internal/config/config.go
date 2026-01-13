package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mevdschee/tqsession/pkg/tqsession"
)

// Config represents the application configuration.
// It maps to the INI config file and converts to tqsession.Config.
type Config struct {
	Server struct {
		Listen string // Address to listen on (e.g., :11211 or localhost:11211)
	}
	Storage struct {
		DataDir         string
		Shards          string // e.g., "16"
		DefaultTTL      string // e.g., "0s", "1h"
		MaxTTL          string // e.g., "0s" (unlimited), "24h"
		MaxDataSize     string // e.g., "64MB" - max live data before LRU eviction
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
			case "max-data-size":
				cfg.Storage.MaxDataSize = value
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

// ToTQSessionConfig converts the file-based configuration to the library's config struct.
func (c *Config) ToTQSessionConfig() (tqsession.Config, error) {
	cfg := tqsession.DefaultConfig()

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

	if c.Storage.MaxDataSize != "" {
		size, err := parseBytes64(c.Storage.MaxDataSize)
		if err != nil {
			return cfg, fmt.Errorf("invalid max_data_size: %w", err)
		}
		cfg.MaxDataSize = size
	}

	if c.Storage.SyncStrategy != "" {
		switch c.Storage.SyncStrategy {
		case "always":
			cfg.SyncStrategy = tqsession.SyncAlways
		case "periodic":
			cfg.SyncStrategy = tqsession.SyncPeriodic
		case "none":
			cfg.SyncStrategy = tqsession.SyncNone
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

// Shards returns the configured number of shards (default 16)
func (c *Config) Shards() int {
	if c.Storage.Shards == "" {
		return 16
	}
	n, err := strconv.Atoi(c.Storage.Shards)
	if err != nil || n <= 0 {
		return 16
	}
	return n
}

func parseBytes64(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, nil
	}

	var multiplier int64 = 1
	if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	} else if strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "G")
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	} else if strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "M")
	} else if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	} else if strings.HasSuffix(s, "K") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "K")
	} else if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}

	val, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}

	return val * multiplier, nil
}
