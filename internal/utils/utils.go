package utils

import (
	"fmt"
	"os"
	"path/filepath"
)

// FindConfigFile tries to find the seedmancer config file in the current directory
// or any parent directory, falling back to the global config if needed
func FindConfigFile() (string, error) {
	// Try to find config in current directory and parents
	dir, err := os.Getwd()
	for {
		configPath := filepath.Join(dir, "seedmancer.yaml")
		if _, err := os.Stat(configPath); err == nil {
			return configPath, nil
		}
		
		parent := filepath.Dir(dir)
		if parent == dir {
			break // Reached root directory
		}
		dir = parent
	}

	// Fall back to global config
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("getting home directory: %v", err)
	}
	
	globalConfig := filepath.Join(homeDir, ".seedmancer", "config.yaml")
	if _, err := os.Stat(globalConfig); err == nil {
		return globalConfig, nil
	}

	return "", fmt.Errorf("no config file found in project or ~/.seedmancer/config.yaml")
}

// GetVersionPath returns the path to a specific database version
func GetVersionPath(projectRoot, storagePath, databaseName, version string) string {
	if version == "" {
		return filepath.Join(projectRoot, storagePath, "databases", databaseName, "unversioned")
	}
	return filepath.Join(projectRoot, storagePath, "databases", databaseName, version)
}

// GetBaseURL returns the appropriate API base URL based on the version
func GetBaseURL() string {
	// return "https://seedmancer.com" // Replace with your production domain
	return "http://localhost:1234"
}

// ReadConfig reads the seedmancer config file and returns the storage path
func ReadConfig(configPath string) (string, error) {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return "", err
	}
	return cfg.StoragePath, nil
}