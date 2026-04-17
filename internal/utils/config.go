package utils

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/KazanKK/seedmancer/internal/ui"
	"gopkg.in/yaml.v3"
)

// Config is the shape of seedmancer.yaml (project or ~/.seedmancer/config.yaml).
//
// In the pure schema-first model a project no longer pins itself to a named
// schema — the schema is derived from the fingerprint of the dumped
// schema.json every time. All we persist is storage/API plumbing.
type Config struct {
	StoragePath string `yaml:"storage_path"`
	DatabaseURL string `yaml:"database_url,omitempty"`
	APIToken    string `yaml:"api_token,omitempty"`
	APIURL      string `yaml:"api_url,omitempty"`
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

// SaveAPIToken persists the Seedmancer dashboard API token to ~/.seedmancer/config.yaml.
func SaveAPIToken(token string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("getting home directory: %v", err)
	}
	configDir := filepath.Join(homeDir, ".seedmancer")
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("creating config directory: %v", err)
	}
	configPath := filepath.Join(configDir, "config.yaml")
	var cfg Config
	if data, readErr := os.ReadFile(configPath); readErr == nil {
		_ = yaml.Unmarshal(data, &cfg)
	}
	cfg.APIToken = token
	data, err := yaml.Marshal(&cfg)
	if err != nil {
		return fmt.Errorf("marshalling config: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		return fmt.Errorf("writing config: %v", err)
	}
	ui.Debug("API token saved to %s", configPath)
	return nil
}
