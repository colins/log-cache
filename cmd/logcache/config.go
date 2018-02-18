package main

import (
	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a LogCache.
type Config struct {
	Addr       string `env:"ADDR, required"`
	TLS        tls.TLS
	HealthPort int `env:"HEALTH_PORT"`

	// MinimumSize sets the lower bound for pruning. It will not prune beyond
	// the set size. Defaults to 500000.
	MinimumSize int `env:"MINIMUM_SIZE"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		Addr:        ":8080",
		HealthPort:  6060,
		MinimumSize: 500000,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
