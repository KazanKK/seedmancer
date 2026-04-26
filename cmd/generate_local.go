package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

// GenerateLocalCommand runs a Go script to produce CSV files, without calling
// any cloud API or consuming any monthly quota.
//
// The script is interpreted by an embedded Go interpreter (yaegi) bundled
// inside the Seedmancer binary — no Go toolchain needs to be installed.
//
// Pass the script via stdin (recommended — no file written to disk):
//
//	seedmancer generate-local --schema-id <ref> --id <id> <<'EOF'
//	package main
//	...
//	EOF
//
// Or via a file path (use "-" for stdin explicitly):
//
//	seedmancer generate-local --script-file /path/to/gen.go --schema-id <ref>
func GenerateLocalCommand() *cli.Command {
	return &cli.Command{
		Name:  "generate-local",
		Usage: "Generate CSV test data from a Go script (no cloud, no quota, no Go toolchain needed)",
		Description: "Interprets a Go program via the embedded engine inside the binary.\n" +
			"The script receives the output directory as os.Args[1] and must\n" +
			"write <table>.csv files there using only stdlib.\n\n" +
			"Recommended: pipe the script via stdin so nothing is written to disk:\n\n" +
			"  seedmancer generate-local --schema-id <fp> --id mydata <<'EOF'\n" +
			"  package main\n" +
			"  import (\"encoding/csv\"; \"fmt\"; \"os\")\n" +
			"  func main() {\n" +
			"    out := os.Args[1]\n" +
			"    f, _ := os.Create(out + \"/users.csv\")\n" +
			"    w := csv.NewWriter(f)\n" +
			"    w.Write([]string{\"id\", \"name\"})\n" +
			"    for i := 1; i <= 5; i++ { w.Write([]string{fmt.Sprintf(\"%d\", i), fmt.Sprintf(\"User %d\", i)}) }\n" +
			"    w.Flush(); f.Close()\n" +
			"  }\n" +
			"  EOF\n\n" +
			"Pass --script-file /path/to/gen.go (or --script-file - for stdin) to read from a file.",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "script-file",
				Aliases: []string{"f"},
				Usage:   "Path to the Go source file (use \"-\" for stdin; omit to read stdin automatically)",
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

	var script []byte
	var err error

	switch {
	case scriptFile == "" || scriptFile == "-":
		// Read from stdin — nothing is written to disk. This is the
		// recommended path for agents: pipe the script via a heredoc.
		script, err = io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("reading script from stdin: %w", err)
		}
	default:
		script, err = os.ReadFile(scriptFile)
		if err != nil {
			return fmt.Errorf("reading script file %q: %w", scriptFile, err)
		}
	}

	if strings.TrimSpace(string(script)) == "" {
		return fmt.Errorf("script is empty (pass via stdin or --script-file)")
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
