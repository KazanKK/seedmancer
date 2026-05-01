package db

import (
	"strings"
	"testing"
)

func TestNormalizeDSN_postgres(t *testing.T) {
	cases := []struct {
		name            string
		in              string
		wantScheme      string
		wantContains    []string
		wantNotContains []string
		wantErr         bool
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
			name:    "unparseable URL returns error",
			in:      "://bad",
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, scheme, err := normalizeDSN(c.in)
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
				t.Errorf("scheme = %q, want %q", scheme, c.wantScheme)
			}
			for _, sub := range c.wantContains {
				if !strings.Contains(got, sub) {
					t.Errorf("want %q in dsn %q", sub, got)
				}
			}
			for _, sub := range c.wantNotContains {
				if strings.Contains(got, sub) {
					t.Errorf("want %q NOT in dsn %q", sub, got)
				}
			}
		})
	}
}

func TestNormalizeDSN_mysql(t *testing.T) {
	cases := []struct {
		name            string
		in              string
		wantScheme      string
		wantContains    []string
		wantNotContains []string
	}{
		{
			name:         "mysql url converted to native DSN",
			in:           "mysql://user:pass@localhost:3306/mydb",
			wantScheme:   "mysql",
			wantContains: []string{"user:pass@tcp(localhost:3306)/mydb", "parseTime=true", "multiStatements=true"},
		},
		{
			name:         "default port 3306 added when missing",
			in:           "mysql://user:pass@localhost/mydb",
			wantScheme:   "mysql",
			wantContains: []string{"tcp(localhost:3306)"},
		},
		{
			name:            "mysql:// prefix stripped from output",
			in:              "mysql://user:pass@localhost:3306/mydb",
			wantScheme:      "mysql",
			wantNotContains: []string{"mysql://"},
		},
		{
			name:         "caller-supplied query params preserved",
			in:           "mysql://user:pass@localhost:3306/mydb?charset=utf8",
			wantScheme:   "mysql",
			wantContains: []string{"charset=utf8", "parseTime=true"},
		},
		{
			name:         "user without password",
			in:           "mysql://root@localhost:3306/testdb",
			wantScheme:   "mysql",
			wantContains: []string{"root:@tcp(localhost:3306)/testdb"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, scheme, err := normalizeDSN(c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if scheme != c.wantScheme {
				t.Errorf("scheme = %q, want %q", scheme, c.wantScheme)
			}
			for _, sub := range c.wantContains {
				if !strings.Contains(got, sub) {
					t.Errorf("want %q in dsn %q", sub, got)
				}
			}
			for _, sub := range c.wantNotContains {
				if strings.Contains(got, sub) {
					t.Errorf("want %q NOT in dsn %q", sub, got)
				}
			}
		})
	}
}

func TestNewManager_returnsCorrectType(t *testing.T) {
	pgManager, _, err := NewManager("postgres://u:p@localhost/db")
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	if _, ok := pgManager.(*PostgresManager); !ok {
		t.Errorf("expected *PostgresManager, got %T", pgManager)
	}

	myManager, _, err := NewManager("mysql://u:p@localhost/db")
	if err != nil {
		t.Fatalf("mysql: %v", err)
	}
	if _, ok := myManager.(*MySQLManager); !ok {
		t.Errorf("expected *MySQLManager, got %T", myManager)
	}
}

func TestNewManager_unknownSchemeErrors(t *testing.T) {
	_, _, err := NewManager("mongodb://localhost/db")
	if err == nil {
		t.Fatal("expected error for unsupported scheme")
	}
	if !strings.Contains(err.Error(), "unsupported database scheme") {
		t.Errorf("error message = %q, want 'unsupported database scheme'", err.Error())
	}
}
