package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

// GenerateLocalCommand runs a user-supplied Go script locally to produce CSV
// files, without calling any cloud API or consuming any monthly quota.
//
// The script must be a self-contained Go program (package main, stdlib only)
// that writes <table>.csv files to the directory passed as os.Args[1].
// See `seedmancer://docs/local-generation` or the --help text for the contract.
//
// Typical agent workflow when MCP is unavailable:
//  1. Write the Go script to a temp file (e.g. /tmp/gen.go).
//  2. seedmancer generate-local --script-file /tmp/gen.go --schema-id <ref>
//  3. seedmancer seed --id <dataset-id>
func GenerateLocalCommand() *cli.Command {
	return &cli.Command{
		Name:  "generate-local",
		Usage: "Run a local Go script to generate CSV test data (no cloud, no quota)",
		Description: "Executes a user-supplied Go program locally via `go run` to\n" +
			"produce CSV files for one or more tables. The script receives the\n" +
			"output directory as os.Args[1] and must write <table>.csv files\n" +
			"there using only stdlib (encoding/csv, fmt, os, math/rand, …).\n\n" +
			"This is the offline / quota-free alternative to `seedmancer generate`.\n" +
			"Use it directly or let an AI agent write the script and call this\n" +
			"command when the Seedmancer MCP server is not available.\n\n" +
			"Script contract:\n" +
			"  package main\n\n" +
			"  import (\"encoding/csv\"; \"fmt\"; \"os\")\n\n" +
			"  func main() {\n" +
			"    out := os.Args[1]\n" +
			"    f, _ := os.Create(out + \"/users.csv\")\n" +
			"    w := csv.NewWriter(f)\n" +
			"    w.Write([]string{\"id\", \"name\", \"email\"})\n" +
			"    for i := 1; i <= 10; i++ {\n" +
			"      w.Write([]string{fmt.Sprintf(\"%d\",i), fmt.Sprintf(\"User %d\",i), fmt.Sprintf(\"u%d@example.com\",i)})\n" +
			"    }\n" +
			"    w.Flush(); f.Close()\n" +
			"  }",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "script-file",
				Aliases:  []string{"f"},
				Required: true,
				Usage:    "(required) Path to the Go source file to execute",
			},
			&cli.StringFlag{
				Name:    "schema-id",
				Aliases: []string{"s"},
				Usage:   "Schema fingerprint prefix or display name (defaults to the sole local schema)",
			},
			&cli.StringFlag{
				Name:    "id",
				Aliases: []string{"d", "dataset-id"},
				Usage:   "Dataset id for the result (auto-generated timestamp when empty)",
			},
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"y"},
				Usage:   "Overwrite an existing dataset folder with the same id",
			},
		},
		Action: func(c *cli.Context) error {
			return runGenerateLocal(c)
		},
	}
}

func runGenerateLocal(c *cli.Context) error {
	scriptFile := strings.TrimSpace(c.String("script-file"))
	if scriptFile == "" {
		return fmt.Errorf("--script-file is required")
	}

	script, err := os.ReadFile(scriptFile)
	if err != nil {
		return fmt.Errorf("reading script file %q: %w", scriptFile, err)
	}
	if strings.TrimSpace(string(script)) == "" {
		return fmt.Errorf("script file %q is empty", scriptFile)
	}

	out, err := RunGenerateLocal(context.Background(), GenerateLocalInput{
		Script:    string(script),
		SchemaRef: strings.TrimSpace(c.String("schema-id")),
		DatasetID: strings.TrimSpace(c.String("id")),
		Force:     c.Bool("force"),
	})
	if err != nil {
		return err
	}

	fmt.Printf("\nGenerated dataset → %s\n", out.Path)
	fmt.Printf("Tables: %s\n", strings.Join(out.Tables, ", "))
	fmt.Printf("Run: seedmancer seed --id %s\n", out.Dataset)
	return nil
}
