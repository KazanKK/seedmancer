package utils

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the shape of seedmancer.yaml (project or ~/.seedmancer/config.yaml).
type Config struct {
	StoragePath  string `yaml:"storage_path"`
	DatabaseName string `yaml:"database_name,omitempty"`
	DatabaseURL  string `yaml:"database_url,omitempty"`
}

// LoadConfig reads and parses the full seedmancer config file.
func LoadConfig(configPath string) (Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("reading config file: %v", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parsing config file: %v", err)
	}
	return cfg, nil
}
