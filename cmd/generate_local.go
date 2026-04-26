package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/KazanKK/seedmancer/internal/utils"
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
//	seedmancer generate-local --schema-id <ref> --id <id> --inherit baseline <<'EOF'
//	package main
//	...
//	EOF
//
// `--inherit baseline` pre-fills the new dataset with the baseline's CSVs and
// auto-clears descendant tables that FK to whatever the script overwrites.
// This is the recommended way to do partial updates (e.g. "regenerate only
// products") without manually copying CSVs around.
func GenerateLocalCommand() *cli.Command {
	return &cli.Command{
		Name:  "generate-local",
		Usage: "Generate CSV test data from a Go script (no cloud, no quota, no Go toolchain needed)",
		Description: "Interprets a Go program via the embedded engine inside the binary.\n" +
			"The script receives the output directory as os.Args[1] and must\n" +
			"write <table>.csv files there using only stdlib.\n\n" +
			"Recommended: pipe the script via stdin so nothing is written to disk:\n\n" +
			"  seedmancer generate-local --schema-id <fp> --id mydata --inherit baseline <<'EOF'\n" +
			"  package main\n" +
			"  import (\"encoding/csv\"; \"fmt\"; \"os\")\n" +
			"  func main() {\n" +
			"    out := os.Args[1]\n" +
			"    f, _ := os.Create(out + \"/products.csv\")\n" +
			"    w := csv.NewWriter(f)\n" +
			"    w.Write([]string{\"id\", \"name\"})\n" +
			"    for i := 1; i <= 5; i++ { w.Write([]string{fmt.Sprintf(\"%d\", i), fmt.Sprintf(\"P%d\", i)}) }\n" +
			"    w.Flush(); f.Close()\n" +
			"  }\n" +
			"  EOF\n\n" +
			"--inherit <base> copies the base dataset's CSVs in first, lets the\n" +
			"script overwrite the tables it cares about, and auto-clears any\n" +
			"descendant table that FKs to an overwritten table — so partial\n" +
			"datasets are always safe to seed.\n\n" +
			"--script-file paths must live outside the project directory; use\n" +
			"stdin (or --script-file -) so nothing is committed to the repo.",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "script-file",
				Aliases: []string{"f"},
				Usage:   "Path to the Go source file (use \"-\" for stdin; omit to read stdin automatically). Project-relative paths are rejected.",
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
			&cli.StringFlag{
				Name:    "inherit",
				Aliases: []string{"b"},
				Usage:   "Pre-fill from <base-dataset-id>; descendants of overwritten tables are auto-cleared",
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
		// Reject scripts that live inside the project directory. The whole
		// point of generate-local is that scripts are throwaway artifacts —
		// committing them to the repo defeats the design and creates noisy
		// diffs (e.g. scripts/seedmancer-go/...).
		if err := rejectProjectRelativeScriptPath(scriptFile); err != nil {
			return err
		}
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
		Inherit:   strings.TrimSpace(c.String("inherit")),
	})
	if err != nil {
		return err
	}

	fmt.Printf("\nGenerated dataset → %s\n", out.Path)
	fmt.Printf("Tables: %s\n", strings.Join(out.Tables, ", "))
	if out.InheritedFrom != "" {
		fmt.Printf("Inherited from: %s", out.InheritedFrom)
		if len(out.ClearedTables) > 0 {
			fmt.Printf(" (auto-cleared FK descendants: %s)", strings.Join(out.ClearedTables, ", "))
		}
		fmt.Println()
	}
	fmt.Printf("Run: seedmancer seed --id %s\n", out.Dataset)
	return nil
}

// rejectProjectRelativeScriptPath returns an error when scriptFile resolves
// to a path inside the project root (the directory containing seedmancer.yaml).
// This keeps generator scripts out of the repository — they should live in
// /tmp or be piped via stdin.
func rejectProjectRelativeScriptPath(scriptFile string) error {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		// No project root resolved → nothing to defend against.
		return nil
	}
	projectRoot, err := filepath.Abs(filepath.Dir(configPath))
	if err != nil {
		return nil
	}
	abs, err := filepath.Abs(scriptFile)
	if err != nil {
		return nil
	}
	rel, err := filepath.Rel(projectRoot, abs)
	if err != nil {
		return nil
	}
	if rel == "." || (!strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel)) {
		return fmt.Errorf(
			"refusing to read --script-file %q: path is inside the project (%s).\n"+
				"Generator scripts are throwaway — pipe via stdin (heredoc) or place the file under /tmp.",
			scriptFile, projectRoot,
		)
	}
	return nil
}
