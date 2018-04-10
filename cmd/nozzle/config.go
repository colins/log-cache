package main

import (
	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a LogCache.
type Config struct {
	LogCacheAddr string `env:"LOG_CACHE_ADDR, required"`
	LogCacheTLS  tls.TLS
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		LogCacheAddr: ":8080",
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
