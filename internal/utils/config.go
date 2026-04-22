package utils

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the shape of seedmancer.yaml (project or ~/.seedmancer/config.yaml).
//
// In the pure schema-first model a project no longer pins itself to a named
// schema — the schema is derived from the fingerprint of the dumped
// schema.json every time. All we persist is storage/API plumbing.
//
// Environments & legacy DatabaseURL:
//   - The modern shape stores one or more named targets under `environments:`,
//     with `default_env:` naming the one commands use when no --env is passed.
//   - Older configs wrote a single top-level `database_url:`. Those still
//     load — ResolveEnv injects a virtual env called "default" so every code
//     path can treat the map as authoritative. The legacy field is only
//     re-emitted when the file has no `environments:` at all so we don't
//     silently rewrite hand-edited configs.
//
// APIToken is kept on the struct for read-only backward compatibility: older
// versions wrote `api_token:` here, and we still accept it so existing
// installs don't get signed out. New writes go to ~/.seedmancer/credentials
// (see credentials.go); `seedmancer login` no longer touches config files.
//
// Unknown keys are ignored by yaml.Unmarshal (the default). That's on
// purpose so future keys (e.g. value_map, before_seed) can be hand-edited
// before older CLI versions understand them without causing errors.
type Config struct {
	StoragePath  string               `yaml:"storage_path"`
	DefaultEnv   string               `yaml:"default_env,omitempty"`
	Environments map[string]EnvConfig `yaml:"environments,omitempty"`

	// Legacy single-target fields — kept for read-compat.
	DatabaseURL string `yaml:"database_url,omitempty"`

	APIToken string `yaml:"api_token,omitempty"`
	APIURL   string `yaml:"api_url,omitempty"`
}

// EnvConfig is one named target inside `environments:`.
type EnvConfig struct {
	DatabaseURL string `yaml:"database_url"`
}

// NamedEnv pairs a resolved env with its name so callers can render banners
// like "→ seeding staging" without a second lookup.
type NamedEnv struct {
	Name string
	EnvConfig
}

// LegacyEnvName is the synthetic env name used when the config only has a
// top-level `database_url:`. Keeping it as a constant means every caller
// prints the same string and tests can assert on it.
const LegacyEnvName = "default"

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

// SaveConfig writes cfg to path atomically (temp file + rename) so a crashed
// write never leaves the project with a truncated seedmancer.yaml.
func SaveConfig(path string, cfg Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshalling config: %v", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("writing %s: %v", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("renaming %s -> %s: %v", tmp, path, err)
	}
	return nil
}

// EffectiveEnvs returns the environments map the CLI actually works against.
// When the file only has the legacy top-level database_url, that url is
// surfaced under the synthetic env name "default" so every lookup can go
// through the same code path.
//
// Callers must not mutate the returned map — it may be a view into the
// Config struct. Use Config.SetEnv / RemoveEnv for writes.
func (c Config) EffectiveEnvs() map[string]EnvConfig {
	if len(c.Environments) > 0 {
		return c.Environments
	}
	if strings.TrimSpace(c.DatabaseURL) != "" {
		return map[string]EnvConfig{
			LegacyEnvName: {DatabaseURL: strings.TrimSpace(c.DatabaseURL)},
		}
	}
	return map[string]EnvConfig{}
}

// ActiveEnvName returns the env name commands should use when no --env was
// passed. When the file has only the legacy top-level database_url and no
// default_env:, we synthesize LegacyEnvName so every command can render
// "using env: default" uniformly.
func (c Config) ActiveEnvName() string {
	if n := strings.TrimSpace(c.DefaultEnv); n != "" {
		return n
	}
	if len(c.Environments) == 1 {
		for name := range c.Environments {
			return name
		}
	}
	if strings.TrimSpace(c.DatabaseURL) != "" {
		return LegacyEnvName
	}
	return ""
}

// SortedEnvNames returns every known env name in alphabetical order. Used by
// `env list`, error messages ("available: local, prod, staging"), and shell
// completion so the output is deterministic across runs.
func (c Config) SortedEnvNames() []string {
	envs := c.EffectiveEnvs()
	names := make([]string, 0, len(envs))
	for n := range envs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ResolveEnv picks exactly one env. An empty name asks for the active
// default; otherwise the name must be a configured env. The returned
// NamedEnv always carries the resolved name so callers can print it.
func (c Config) ResolveEnv(name string) (NamedEnv, error) {
	name = strings.TrimSpace(name)
	envs := c.EffectiveEnvs()
	if len(envs) == 0 {
		return NamedEnv{}, fmt.Errorf(
			"no environments configured — add one with `seedmancer env add <name> --db-url <url>` or set `environments:` in seedmancer.yaml",
		)
	}
	if name == "" {
		name = c.ActiveEnvName()
		if name == "" {
			return NamedEnv{}, fmt.Errorf(
				"no default environment set — pass --env <name> or run `seedmancer env use <name>` (available: %s)",
				strings.Join(c.SortedEnvNames(), ", "),
			)
		}
	}
	env, ok := envs[name]
	if !ok {
		return NamedEnv{}, fmt.Errorf(
			"unknown environment %q (available: %s)",
			name, strings.Join(c.SortedEnvNames(), ", "),
		)
	}
	if strings.TrimSpace(env.DatabaseURL) == "" {
		return NamedEnv{}, fmt.Errorf("environment %q has no database_url set", name)
	}
	return NamedEnv{Name: name, EnvConfig: env}, nil
}

// ResolveEnvs parses a comma-separated list of env names and resolves each
// one. Order is preserved (so `--env local,staging` always seeds local
// before staging). Duplicates are dropped with a friendly error rather than
// silently collapsed — if someone types `--env local,local` that's almost
// certainly a mistake.
//
// An empty csv falls back to the active default env, so `seed` without
// --env does what a new user expects: "seed my default target".
func (c Config) ResolveEnvs(csv string) ([]NamedEnv, error) {
	raw := strings.Split(csv, ",")
	names := make([]string, 0, len(raw))
	seen := make(map[string]bool, len(raw))
	for _, r := range raw {
		n := strings.TrimSpace(r)
		if n == "" {
			continue
		}
		if seen[n] {
			return nil, fmt.Errorf("environment %q listed more than once in --env", n)
		}
		seen[n] = true
		names = append(names, n)
	}
	if len(names) == 0 {
		resolved, err := c.ResolveEnv("")
		if err != nil {
			return nil, err
		}
		return []NamedEnv{resolved}, nil
	}
	result := make([]NamedEnv, 0, len(names))
	for _, n := range names {
		resolved, err := c.ResolveEnv(n)
		if err != nil {
			return nil, err
		}
		result = append(result, resolved)
	}
	return result, nil
}

// SetEnv upserts an env into the map, materializing the map from the legacy
// top-level database_url the first time it's touched so callers don't end up
// with two sources of truth after `seedmancer env add`.
func (c *Config) SetEnv(name string, env EnvConfig) {
	name = strings.TrimSpace(name)
	c.materializeEnvs()
	if c.Environments == nil {
		c.Environments = map[string]EnvConfig{}
	}
	c.Environments[name] = env
}

// RemoveEnv deletes an env by name. Returns false when the env did not
// exist so callers can decide whether that's an error for them.
func (c *Config) RemoveEnv(name string) bool {
	c.materializeEnvs()
	if _, ok := c.Environments[name]; !ok {
		return false
	}
	delete(c.Environments, name)
	return true
}

// materializeEnvs promotes a legacy top-level database_url into the
// environments map so writes (add/remove/use) always operate on the modern
// shape. Called automatically by SetEnv / RemoveEnv — no-op when the map
// already exists or when no legacy value is present.
func (c *Config) materializeEnvs() {
	if c.Environments != nil {
		return
	}
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return
	}
	c.Environments = map[string]EnvConfig{
		LegacyEnvName: {DatabaseURL: strings.TrimSpace(c.DatabaseURL)},
	}
	if c.DefaultEnv == "" {
		c.DefaultEnv = LegacyEnvName
	}
	c.DatabaseURL = ""
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
