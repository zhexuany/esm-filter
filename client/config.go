package client

import (
	"log"

	"github.com/BurntSushi/toml"
	"time"
)

const (
	// DefaultHostName is the default hostname if one is not provided.
	DefaultHostName = "localhost"
	// DefaultBindAddress is the default address to bind to
	DefaultBindAddress = "55555"

	DefaultDownstream = "localhost:8086"

	DefaultTicket = 10
)

type Config struct {
	HostName    string `toml:"hostname"`
	BindAddress string `toml:"bind-address"`
	Downstream  string `toml:"downstream"`

	Ticket time.Duration `toml:"expired-time"`
}

func ParseConfig(path string) (*Config, error) {
	if path == "" {
		log.Println("no configuraion privoded, using default settings")
		return NewDemoConfig(), nil
	}
	config := Config{}
	if _, err := toml.DecodeFile(path, config); err != nil {
		return nil, err
	}

	return &config, nil
}

func NewDemoConfig() *Config {
	return &Config{
		HostName:    DefaultHostName,
		BindAddress: DefaultBindAddress,
		Downstream:  DefaultDownstream,
		Ticket:      DefaultTicket,
	}
}
