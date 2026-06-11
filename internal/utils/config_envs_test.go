package utils

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadConfig_modernShape(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seedmancer.yaml")
	writeFile(t, path, `storage_path: .seedmancer
default_env: staging
environments:
  local:
    database_url: postgres://localhost:5432/dev
  staging:
    database_url: postgres://stg/app
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.DefaultEnv != "staging" {
		t.Fatalf("DefaultEnv = %q, want staging", cfg.DefaultEnv)
	}
	if len(cfg.Environments) != 2 {
		t.Fatalf("want 2 envs, got %d", len(cfg.Environments))
	}
	if cfg.Environments["local"].DatabaseURL != "postgres://localhost:5432/dev" {
		t.Fatalf("local url = %q", cfg.Environments["local"].DatabaseURL)
	}
}

func TestLoadConfig_ignoresUnknownKeys(t *testing.T) {
	// Forward-compat: future keys like value_map: must not break older
	// CLI versions. yaml.Unmarshal silently drops unknown fields.
	dir := t.TempDir()
	path := filepath.Join(dir, "seedmancer.yaml")
	writeFile(t, path, `storage_path: .seedmancer
default_env: local
environments:
  local:
    database_url: postgres://x
    value_map:
      users.id:
        a: b
future_top_level_key: whatever
`)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if cfg.DefaultEnv != "local" {
		t.Fatalf("DefaultEnv = %q", cfg.DefaultEnv)
	}
}

func TestEffectiveEnvs_legacyFallback(t *testing.T) {
	cfg := Config{DatabaseURL: "postgres://legacy"}
	envs := cfg.EffectiveEnvs()
	if len(envs) != 1 {
		t.Fatalf("want 1 env, got %d", len(envs))
	}
	if envs[LegacyEnvName].DatabaseURL != "postgres://legacy" {
		t.Fatalf("legacy env url = %q", envs[LegacyEnvName].DatabaseURL)
	}
	if cfg.ActiveEnvName() != LegacyEnvName {
		t.Fatalf("ActiveEnvName = %q", cfg.ActiveEnvName())
	}
}

func TestActiveEnvName_precedence(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "default_env wins",
			cfg: Config{
				DefaultEnv:   "staging",
				Environments: map[string]EnvConfig{"local": {DatabaseURL: "a"}, "staging": {DatabaseURL: "b"}},
			},
			want: "staging",
		},
		{
			name: "single env auto-picked",
			cfg: Config{
				Environments: map[string]EnvConfig{"only": {DatabaseURL: "a"}},
			},
			want: "only",
		},
		{
			name: "legacy url only",
			cfg:  Config{DatabaseURL: "postgres://x"},
			want: LegacyEnvName,
		},
		{
			name: "multi env no default",
			cfg: Config{
				Environments: map[string]EnvConfig{"a": {DatabaseURL: "x"}, "b": {DatabaseURL: "y"}},
			},
			want: "",
		},
		{
			name: "empty config",
			cfg:  Config{},
			want: "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.cfg.ActiveEnvName(); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestResolveEnv(t *testing.T) {
	cfg := Config{
		DefaultEnv: "local",
		Environments: map[string]EnvConfig{
			"local":   {DatabaseURL: "postgres://local"},
			"staging": {DatabaseURL: "postgres://staging"},
			"empty":   {DatabaseURL: ""},
		},
	}
	t.Run("named", func(t *testing.T) {
		ne, err := cfg.ResolveEnv("staging")
		if err != nil || ne.Name != "staging" || ne.DatabaseURL != "postgres://staging" {
			t.Fatalf("got %+v err=%v", ne, err)
		}
	})
	t.Run("default", func(t *testing.T) {
		ne, err := cfg.ResolveEnv("")
		if err != nil || ne.Name != "local" {
			t.Fatalf("got %+v err=%v", ne, err)
		}
	})
	t.Run("unknown lists available", func(t *testing.T) {
		_, err := cfg.ResolveEnv("nope")
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "empty") || !strings.Contains(err.Error(), "staging") {
			t.Fatalf("error should list candidates, got: %v", err)
		}
	})
	t.Run("empty url is error", func(t *testing.T) {
		_, err := cfg.ResolveEnv("empty")
		if err == nil {
			t.Fatal("expected error for empty url env")
		}
	})
	t.Run("no envs configured", func(t *testing.T) {
		_, err := Config{}.ResolveEnv("")
		if err == nil {
			t.Fatal("expected error when no envs")
		}
	})
}

func TestResolveEnvs_csvOrderPreserved(t *testing.T) {
	cfg := Config{
		Environments: map[string]EnvConfig{
			"local":   {DatabaseURL: "postgres://local"},
			"staging": {DatabaseURL: "postgres://staging"},
			"prod":    {DatabaseURL: "postgres://prod"},
		},
	}
	got, err := cfg.ResolveEnvs("staging,local")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	gotNames := []string{got[0].Name, got[1].Name}
	if !reflect.DeepEqual(gotNames, []string{"staging", "local"}) {
		t.Fatalf("order not preserved: %v", gotNames)
	}
}

func TestResolveEnvs_dedupeErrors(t *testing.T) {
	cfg := Config{
		DefaultEnv:   "local",
		Environments: map[string]EnvConfig{"local": {DatabaseURL: "postgres://local"}},
	}
	if _, err := cfg.ResolveEnvs("local,local"); err == nil {
		t.Fatal("expected dupe error")
	}
}

func TestResolveEnvs_emptyCsvUsesDefault(t *testing.T) {
	cfg := Config{
		DefaultEnv:   "local",
		Environments: map[string]EnvConfig{"local": {DatabaseURL: "postgres://local"}},
	}
	got, err := cfg.ResolveEnvs("")
	if err != nil || len(got) != 1 || got[0].Name != "local" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestResolveEnvs_whitespaceTolerant(t *testing.T) {
	cfg := Config{
		Environments: map[string]EnvConfig{
			"local":   {DatabaseURL: "postgres://local"},
			"staging": {DatabaseURL: "postgres://staging"},
		},
	}
	got, err := cfg.ResolveEnvs("  staging ,  local  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[0].Name != "staging" || got[1].Name != "local" {
		t.Fatalf("got %v", got)
	}
}

func TestSetEnv_migratesLegacyURL(t *testing.T) {
	cfg := Config{DatabaseURL: "postgres://legacy"}
	cfg.SetEnv("staging", EnvConfig{DatabaseURL: "postgres://stg"})

	// After SetEnv the legacy url must have moved into environments.default
	// so there's exactly one source of truth.
	if cfg.DatabaseURL != "" {
		t.Fatalf("legacy DatabaseURL not cleared: %q", cfg.DatabaseURL)
	}
	if cfg.Environments[LegacyEnvName].DatabaseURL != "postgres://legacy" {
		t.Fatalf("legacy env not migrated: %+v", cfg.Environments)
	}
	if cfg.Environments["staging"].DatabaseURL != "postgres://stg" {
		t.Fatalf("staging not set: %+v", cfg.Environments)
	}
	if cfg.DefaultEnv != LegacyEnvName {
		t.Fatalf("DefaultEnv after migration = %q, want %q", cfg.DefaultEnv, LegacyEnvName)
	}
}

func TestRemoveEnv(t *testing.T) {
	cfg := Config{
		Environments: map[string]EnvConfig{
			"local":   {DatabaseURL: "postgres://local"},
			"staging": {DatabaseURL: "postgres://staging"},
		},
	}
	if !cfg.RemoveEnv("staging") {
		t.Fatal("expected remove to succeed")
	}
	if _, ok := cfg.Environments["staging"]; ok {
		t.Fatal("staging still present")
	}
	if cfg.RemoveEnv("staging") {
		t.Fatal("expected double-remove to return false")
	}
}

func TestSaveConfig_roundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seedmancer.yaml")
	cfg := Config{
		StoragePath: ".seedmancer",
		DefaultEnv:  "local",
		Environments: map[string]EnvConfig{
			"local": {DatabaseURL: "postgres://localhost/dev"},
		},
	}
	if err := SaveConfig(path, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !reflect.DeepEqual(got.Environments, cfg.Environments) {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got.Environments, cfg.Environments)
	}
	if got.DefaultEnv != "local" {
		t.Fatalf("default_env = %q", got.DefaultEnv)
	}
}

func TestSortedEnvNames(t *testing.T) {
	cfg := Config{
		Environments: map[string]EnvConfig{
			"zeta":  {DatabaseURL: "x"},
			"alpha": {DatabaseURL: "y"},
			"mid":   {DatabaseURL: "z"},
		},
	}
	got := cfg.SortedEnvNames()
	if !reflect.DeepEqual(got, []string{"alpha", "mid", "zeta"}) {
		t.Fatalf("got %v", got)
	}
}
