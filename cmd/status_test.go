package cmd

import (
	"strings"
	"testing"
)

func TestMaskDatabaseURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "postgres with password",
			in:   "postgres://postgres:s3cret@127.0.0.1:54322/postgres",
			want: "postgres://postgres:****@127.0.0.1:54322/postgres",
		},
		{
			name: "no password leaves url alone",
			in:   "postgres://postgres@127.0.0.1:5432/postgres",
			want: "postgres://postgres@127.0.0.1:5432/postgres",
		},
		{
			name: "no userinfo at all",
			in:   "postgres://127.0.0.1:5432/postgres",
			want: "postgres://127.0.0.1:5432/postgres",
		},
		{
			name: "query string preserved",
			in:   "postgres://u:p@host:5432/db?sslmode=disable",
			want: "postgres://u:****@host:5432/db?sslmode=disable",
		},
		{
			name: "mysql dsn",
			in:   "mysql://root:hunter2@localhost:3306/app",
			want: "mysql://root:****@localhost:3306/app",
		},
		{
			name: "garbage returns input unchanged",
			in:   "not a url",
			want: "not a url",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := maskDatabaseURL(c.in)
			if got != c.want {
				t.Errorf("maskDatabaseURL(%q) = %q, want %q", c.in, got, c.want)
			}
			// Hard guarantee: the original password must never appear in
			// the masked output when one was provided.
			if strings.Contains(c.in, ":s3cret@") && strings.Contains(got, "s3cret") {
				t.Errorf("password leaked into masked output: %q", got)
			}
		})
	}
}

func TestMaskToken(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"1234567890abcdef", "1234…cdef (len 16)"},
		{"short", "***** (len 5)"},
		{"", "***** (len 0)"},
		{"12345678", "***** (len 8)"},
		{"123456789", "1234…6789 (len 9)"},
	}
	for _, c := range cases {
		got := maskToken(c.in)
		if got != c.want {
			t.Errorf("maskToken(%q) = %q, want %q", c.in, got, c.want)
		}
		// Never leak the full token.
		if len(c.in) > 8 && strings.Contains(got, c.in) {
			t.Errorf("maskToken leaked full token: %q", got)
		}
	}
}

func TestHumanizeTokenSource(t *testing.T) {
	cases := map[string]string{
		"env":     "SEEDMANCER_API_TOKEN (env var)",
		"unknown": "unknown",
	}
	for in, want := range cases {
		if got := humanizeTokenSource(in); got != want {
			t.Errorf("humanizeTokenSource(%q) = %q, want %q", in, got, want)
		}
	}
}
