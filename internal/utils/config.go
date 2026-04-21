package utils

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the shape of seedmancer.yaml (project or ~/.seedmancer/config.yaml).
//
// In the pure schema-first model a project no longer pins itself to a named
// schema — the schema is derived from the fingerprint of the dumped
// schema.json every time. All we persist is storage/API plumbing.
//
// APIToken is kept on the struct for read-only backward compatibility: older
// versions wrote `api_token:` here, and we still accept it so existing
// installs don't get signed out. New writes go to ~/.seedmancer/credentials
// (see credentials.go); `seedmancer login` no longer touches config files.
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

// LocalSchemaMeta is the user-editable sidecar that sits next to schema.json.
// We intentionally keep it tiny so it can grow over time (description, tags,
// etc.) without breaking readers: any unknown field is ignored by
// yaml.Unmarshal.
type LocalSchemaMeta struct {
	DisplayName string `yaml:"display_name,omitempty"`
}

// LoadLocalSchemaMeta reads a schema's meta.yaml sidecar. A missing file is
// not an error — callers get a zero-value LocalSchemaMeta so they can render
// a sensible default (usually the fingerprint short id).
func LoadLocalSchemaMeta(schemaDir string) (LocalSchemaMeta, error) {
	path := SchemaMetaPath(schemaDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return LocalSchemaMeta{}, nil
		}
		return LocalSchemaMeta{}, fmt.Errorf("reading %s: %v", path, err)
	}
	var meta LocalSchemaMeta
	if err := yaml.Unmarshal(data, &meta); err != nil {
		return LocalSchemaMeta{}, fmt.Errorf("parsing %s: %v", path, err)
	}
	return meta, nil
}

// SaveLocalSchemaMeta writes meta.yaml next to schema.json. When meta is the
// zero value, the sidecar is deleted so the on-disk layout stays tidy for
// users who clear a display name.
func SaveLocalSchemaMeta(schemaDir string, meta LocalSchemaMeta) error {
	path := SchemaMetaPath(schemaDir)
	if meta == (LocalSchemaMeta{}) {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("removing %s: %v", path, err)
		}
		return nil
	}
	data, err := yaml.Marshal(&meta)
	if err != nil {
		return fmt.Errorf("marshalling meta: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing %s: %v", path, err)
	}
	return nil
}

