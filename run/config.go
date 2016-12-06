package main

import (
	"log"

	"github.com/BurntSushi/toml"
)

const (
	// DefaultHostName is the default hostname if one is not provided.
	DefaultHostName = "localhost"
	// DefaultBindAddress is the default address to bind to
	DefaultBindAddress = "55555"
)

type Config struct {
	HostName    string `toml:"http-hostname"`
	BindAddress string `toml:"http-bind-address"`
}

func ParseConfig(path string) (*Config, error) {
	if path == "" {
		log.Println("no configuraion privoded, using default settings")
		return NewDemoConfig()
	}
	config := Config{}
	if err := toml.DecodeFile(path, config); err != nil {
		return nil, err
	}

	return config, nil
}

func NewDemoConfig() *Config {
	return *Config{
		HostName:    DefaultHostName,
		BindAddress: DefaultBindAddress,
	}
}
