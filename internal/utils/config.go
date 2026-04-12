package utils

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/KazanKK/seedmancer/internal/ui"
	"gopkg.in/yaml.v3"
)

// Config is the shape of seedmancer.yaml (project or ~/.seedmancer/config.yaml).
type Config struct {
	StoragePath  string `yaml:"storage_path"`
	DatabaseName string `yaml:"database_name,omitempty"`
	DatabaseURL  string `yaml:"database_url,omitempty"`
	OpenAIAPIKey string `yaml:"openai_api_key,omitempty"`
	APIToken     string `yaml:"api_token,omitempty"`
	APIURL       string `yaml:"api_url,omitempty"` // e.g. https://api.seedmancer.dev (HTTP API host)
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

// SaveOpenAIKey persists the OpenAI API key to ~/.seedmancer/config.yaml.
func SaveOpenAIKey(apiKey string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("getting home directory: %v", err)
	}

	configDir := filepath.Join(homeDir, ".seedmancer")
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("creating config directory: %v", err)
	}

	configPath := filepath.Join(configDir, "config.yaml")

	// Merge with any existing config so other fields are preserved.
	var cfg Config
	if data, readErr := os.ReadFile(configPath); readErr == nil {
		_ = yaml.Unmarshal(data, &cfg)
	}
	cfg.OpenAIAPIKey = apiKey

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshalling config: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		return fmt.Errorf("writing config: %v", err)
	}

	ui.Debug("OpenAI API key saved to %s", configPath)
	return nil
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
	return nil
}
