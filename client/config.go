package client

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	// DefaultHostName is the default hostname if one is not provided.
	DefaultHostName = "localhost"
	// DefaultBindAddress is the default address to bind to
	DefaultBindAddress = ":8286"

	DefaultDownstream = "localhost:8086"

	DefaultTicket = 10
)

type Config struct {
	HostName    string `toml:"hostname"`
	BindAddress string `toml:"bind-address"`
	Downstream  string `toml:"downstream"`

	Ticket time.Duration `toml:"expired-time"`
}

func (c *Config) ApplyEnvOverrides() error {
	return c.applyEnvOverrides("ESM_FILTER", reflect.ValueOf(c))
}

func (c *Config) applyEnvOverrides(prefix string, spec reflect.Value) error {
	s := spec
	if spec.Kind() == reflect.Ptr {
		s = spec.Elem()
	}

	// make sure we have a struct
	if s.Kind() != reflect.Struct {
		return nil
	}

	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		//Get the toml tag to determine what env name to use
		configName := typeOfSpec.Field(i).Tag.Get("toml")
		// Replace hyphens with underscores to avoid issues with shells
		configName = strings.Replace(configName, "-", "_", -1)
		fieldKey := typeOfSpec.Field(i).Name
		// Skip any fields that we cannot set
		if f.CanSet() || f.Kind() == reflect.Slice {

			// Use the upper-case prefix and toml name for the env var
			key := strings.ToUpper(configName)
			if prefix != "" {
				key = strings.ToUpper(fmt.Sprintf("%s_%s", prefix, configName))
			}
			value := os.Getenv(key)

			if f.Kind() == reflect.Slice || f.Kind() == reflect.Array {
				for i := 0; i < f.Len(); i++ {
					if err := c.applyEnvOverrides(key, f.Index(i)); err != nil {
						return err
					}

					if err := c.applyEnvOverrides(fmt.Sprintf("%s_%d", key, i), f.Index(i)); err != nil {
						return err
					}
				}
				continue
			}

			//If it's a sub-config, recursively apply
			if f.Kind() == reflect.Struct || f.Kind() == reflect.Ptr {
				if err := c.applyEnvOverrides(key, f); err != nil {
					return err
				}
				continue
			}

			//Skip any fields we don't have a value to set
			if value == "" {
				continue
			}

			switch f.Kind() {
			case reflect.String:
				f.SetString(value)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

				var intValue int64

				// Handle toml.Duration
				if f.Type().Name() == "Duration" {
					dur, err := time.ParseDuration(value)
					if err != nil {
						return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)
					}
					intValue = dur.Nanoseconds()
				} else {
					var err error
					intValue, err = strconv.ParseInt(value, 0, f.Type().Bits())
					if err != nil {
						return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)
					}
				}

				f.SetInt(intValue)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				var intValue uint64
				var err error
				intValue, err = strconv.ParseUint(value, 0, f.Type().Bits())
				if err != nil {
					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)
				}

				f.SetUint(intValue)
			case reflect.Bool:
				boolValue, err := strconv.ParseBool(value)
				if err != nil {
					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)

				}
				f.SetBool(boolValue)
			case reflect.Float32, reflect.Float64:
				floatValue, err := strconv.ParseFloat(value, f.Type().Bits())
				if err != nil {
					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldKey, f.Type().String(), value)

				}
				f.SetFloat(floatValue)
			default:
				if err := c.applyEnvOverrides(key, f); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ParseConfig(path string) (*Config, error) {
	if path == "" {
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
