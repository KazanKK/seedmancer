package cmd

import (
	"flag"
	"strings"
	"testing"

	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// newTestContext wires a minimal urfave/cli Context with the flags the
// resolver cares about. Tests pass the pre-parsed argv so we can assert
// precedence between --db-url, --env, and env vars.
func newTestContext(t *testing.T, argv []string) *cli.Context {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.String("db-url", "", "")
	fs.String("env", "", "")
	if err := fs.Parse(argv); err != nil {
		t.Fatalf("flag parse: %v", err)
	}
	app := cli.NewApp()
	return cli.NewContext(app, fs, nil)
}

func TestResolveSingleDB_precedence(t *testing.T) {
	cfg := utils.Config{
		DefaultEnv: "local",
		Environments: map[string]utils.EnvConfig{
			"local":   {DatabaseURL: "postgres://local"},
			"staging": {DatabaseURL: "postgres://staging"},
		},
	}
	t.Run("flag db-url wins over env flag", func(t *testing.T) {
		c := newTestContext(t, []string{"--db-url", "postgres://adhoc", "--env", "staging"})
		ne, err := resolveSingleDB(c, cfg)
		if err != nil || ne.DatabaseURL != "postgres://adhoc" || ne.Name != adHocEnvName {
			t.Fatalf("got %+v err=%v", ne, err)
		}
	})
	t.Run("env var SEEDMANCER_DATABASE_URL wins when no flag", func(t *testing.T) {
		t.Setenv("SEEDMANCER_DATABASE_URL", "postgres://envvar")
		c := newTestContext(t, []string{"--env", "staging"})
		ne, err := resolveSingleDB(c, cfg)
		if err != nil || ne.DatabaseURL != "postgres://envvar" {
			t.Fatalf("got %+v err=%v", ne, err)
		}
	})
	t.Run("named env", func(t *testing.T) {
		t.Setenv("SEEDMANCER_DATABASE_URL", "")
		c := newTestContext(t, []string{"--env", "staging"})
		ne, err := resolveSingleDB(c, cfg)
		if err != nil || ne.Name != "staging" || ne.DatabaseURL != "postgres://staging" {
			t.Fatalf("got %+v err=%v", ne, err)
		}
	})
	t.Run("default env fallback", func(t *testing.T) {
		t.Setenv("SEEDMANCER_DATABASE_URL", "")
		c := newTestContext(t, nil)
		ne, err := resolveSingleDB(c, cfg)
		if err != nil || ne.Name != "local" {
			t.Fatalf("got %+v err=%v", ne, err)
		}
	})
}

func TestResolveSeedTargets_multi(t *testing.T) {
	cfg := utils.Config{
		Environments: map[string]utils.EnvConfig{
			"local":   {DatabaseURL: "postgres://local"},
			"staging": {DatabaseURL: "postgres://staging"},
		},
	}
	t.Setenv("SEEDMANCER_DATABASE_URL", "")
	c := newTestContext(t, []string{"--env", "local,staging"})
	got, err := resolveSeedTargets(c, cfg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 2 || got[0].Name != "local" || got[1].Name != "staging" {
		t.Fatalf("got %+v", got)
	}
}

func TestResolveSeedTargets_dbURLMutuallyExclusive(t *testing.T) {
	cfg := utils.Config{
		Environments: map[string]utils.EnvConfig{"local": {DatabaseURL: "postgres://local"}},
	}
	c := newTestContext(t, []string{"--db-url", "postgres://adhoc", "--env", "local"})
	_, err := resolveSeedTargets(c, cfg)
	if err == nil {
		t.Fatal("expected mutual-exclusion error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIsProdLike(t *testing.T) {
	hits := []string{"prod", "Production", "LIVE", " prod "}
	for _, n := range hits {
		if !isProdLike(n) {
			t.Errorf("expected %q to be prod-like", n)
		}
	}
	misses := []string{"local", "staging", "dev", "preprod", ""}
	for _, n := range misses {
		if isProdLike(n) {
			t.Errorf("expected %q to NOT be prod-like", n)
		}
	}
}

func TestNormalizePostgresDSN(t *testing.T) {
	cases := []struct {
		name, in          string
		wantScheme        string
		wantContains      []string
		wantNotContains   []string
		wantErr           bool
	}{
		{
			name:         "postgresql scheme rewritten to postgres",
			in:           "postgresql://user:pass@localhost:5432/db",
			wantScheme:   "postgres",
			wantContains: []string{"postgres://", "sslmode=disable"},
		},
		{
			name:         "postgres scheme kept, sslmode appended",
			in:           "postgres://user:pass@localhost:5432/db",
			wantScheme:   "postgres",
			wantContains: []string{"postgres://", "sslmode=disable"},
		},
		{
			name:            "existing sslmode preserved",
			in:              "postgres://u:p@h:5/db?sslmode=require",
			wantScheme:      "postgres",
			wantContains:    []string{"sslmode=require"},
			wantNotContains: []string{"sslmode=disable"},
		},
		{
			name:         "sslmode appended with ampersand when query already present",
			in:           "postgres://u:p@h:5/db?application_name=test",
			wantScheme:   "postgres",
			wantContains: []string{"application_name=test", "&sslmode=disable"},
		},
		{
			name:       "non-postgres scheme returned unchanged",
			in:         "mysql://u:p@h:3306/db",
			wantScheme: "mysql",
		},
		{
			name:    "unparseable URL returns error",
			in:      "://bad",
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, scheme, err := normalizePostgresDSN(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got dsn=%q scheme=%q", got, scheme)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if scheme != c.wantScheme {
				t.Fatalf("scheme = %q, want %q", scheme, c.wantScheme)
			}
			for _, s := range c.wantContains {
				if !strings.Contains(got, s) {
					t.Fatalf("dsn %q missing %q", got, s)
				}
			}
			for _, s := range c.wantNotContains {
				if strings.Contains(got, s) {
					t.Fatalf("dsn %q unexpectedly contains %q", got, s)
				}
			}
		})
	}
}
