package cmd

import (
	"strings"
	"testing"
)

func TestResolveDatabaseURL(t *testing.T) {
	cases := []struct {
		name, flag, cfg, want string
	}{
		{"flag wins", "postgres://a", "postgres://b", "postgres://a"},
		{"config fallback", "", "postgres://b", "postgres://b"},
		{"both empty", "", "", ""},
		{"trims whitespace", "  postgres://a  ", "", "postgres://a"},
		{"whitespace-only flag falls back to cfg", "   ", "postgres://b", "postgres://b"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := resolveDatabaseURL(c.flag, c.cfg); got != c.want {
				t.Fatalf("resolveDatabaseURL(%q,%q) = %q, want %q", c.flag, c.cfg, got, c.want)
			}
		})
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
