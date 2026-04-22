package main

import (
	"reflect"
	"testing"

	"github.com/urfave/cli/v2"
)

// buildTestApp reproduces the subcommand shape that matters for flag
// reshuffling: a top-level `env` with `add` (taking a bool + string flag
// plus a positional name) and `seed` at the leaf with a string flag.
func buildTestApp() *cli.App {
	return &cli.App{
		Commands: []*cli.Command{
			{
				Name: "env",
				Subcommands: []*cli.Command{
					{
						Name: "add",
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "db-url"},
							&cli.BoolFlag{Name: "force"},
						},
					},
					{Name: "list"},
				},
			},
			{
				Name: "seed",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "dataset-id", Aliases: []string{"d"}},
					&cli.StringFlag{Name: "env"},
					&cli.BoolFlag{Name: "dry-run"},
				},
			},
		},
	}
}

func TestReorderArgs(t *testing.T) {
	app := buildTestApp()
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "positional after flag — untouched",
			in:   []string{"seedmancer", "env", "add", "--db-url", "postgres://x", "prod"},
			want: []string{"seedmancer", "env", "add", "--db-url", "postgres://x", "prod"},
		},
		{
			name: "positional before flag — moved to end",
			in:   []string{"seedmancer", "env", "add", "prod", "--db-url", "postgres://x"},
			want: []string{"seedmancer", "env", "add", "--db-url", "postgres://x", "prod"},
		},
		{
			name: "bool flag not misconsumed",
			in:   []string{"seedmancer", "env", "add", "prod", "--force", "--db-url", "postgres://x"},
			want: []string{"seedmancer", "env", "add", "--force", "--db-url", "postgres://x", "prod"},
		},
		{
			name: "flag=value form",
			in:   []string{"seedmancer", "env", "add", "prod", "--db-url=postgres://x"},
			want: []string{"seedmancer", "env", "add", "--db-url=postgres://x", "prod"},
		},
		{
			name: "short-form flag with value",
			in:   []string{"seedmancer", "seed", "snap1", "-d", "snap1-id", "--dry-run"},
			want: []string{"seedmancer", "seed", "-d", "snap1-id", "--dry-run", "snap1"},
		},
		{
			name: "double-dash preserves order",
			in:   []string{"seedmancer", "env", "add", "prod", "--", "--db-url", "postgres://x"},
			want: []string{"seedmancer", "env", "add", "--", "--db-url", "postgres://x", "prod"},
		},
		{
			name: "no subcommand leaf (just top-level)",
			in:   []string{"seedmancer"},
			want: []string{"seedmancer"},
		},
		{
			name: "unknown subcommand becomes positional",
			in:   []string{"seedmancer", "env", "unknown", "prod"},
			want: []string{"seedmancer", "env", "unknown", "prod"},
		},
		{
			name: "env list has no flags, no reshuffle needed",
			in:   []string{"seedmancer", "env", "list"},
			want: []string{"seedmancer", "env", "list"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := reorderArgs(c.in, app)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("\n got: %v\nwant: %v", got, c.want)
			}
		})
	}
}
