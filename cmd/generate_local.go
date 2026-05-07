package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/KazanKK/seedmancer/internal/ui"
	"github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// GenerateLocalCommand runs a Go script that produces CSV files for a
// new revision of a scenario. The script is interpreted by an embedded
// Go interpreter (yaegi) — no Go toolchain needed.
//
// Recommended invocation (script piped via stdin so it never touches disk):
//
//	seedmancer generate-local <scenario> --inherit <base-scenario> <<'EOF'
//	package main
//	...
//	EOF
//
// `--inherit baseline-scenario` pre-fills the new revision with the
// inherit base's latest CSVs. The script overwrites whichever tables it
// cares about; descendant tables (FK descendants of the overwritten
// tables) are auto-cleared so the result is always safe to seed.
func GenerateLocalCommand() *cli.Command {
	return &cli.Command{
		Name:      "generate-local",
		Usage:     "Generate a scenario revision from a Go script (no cloud, no quota)",
		ArgsUsage: "<scenario>",
		Description: "Interprets a Go program via the embedded engine inside the binary.\n" +
			"The script receives the data directory as os.Args[1] and must\n" +
			"write <table>.csv files there using only stdlib.\n\n" +
			"Recommended: pipe the script via stdin so nothing is written to disk:\n\n" +
			"  seedmancer generate-local billing/pro --inherit basic <<'EOF'\n" +
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
			"--inherit <base-scenario> copies the latest revision of the base\n" +
			"scenario in first, lets the script overwrite the tables it cares\n" +
			"about, and auto-clears any descendant table that FKs to an\n" +
			"overwritten table — so partial datasets are always safe to seed.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "script-file",
				Aliases: []string{"f"},
				Usage:   "Path to the Go source file (use \"-\" for stdin; omit to read stdin automatically). Project-relative paths are rejected.",
			},
			&cli.StringFlag{
				Name:    "inherit",
				Aliases: []string{"b"},
				Usage:   "Base scenario whose latest revision pre-fills the new revision",
			},
			&cli.StringFlag{
				Name:  "description",
				Usage: "Optional description stored on the new revision manifest",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer generate-local <scenario>")
			}

			scriptFile := strings.TrimSpace(c.String("script-file"))
			var script []byte
			var err error
			switch {
			case scriptFile == "" || scriptFile == "-":
				script, err = io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("reading script from stdin: %w", err)
				}
			default:
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
				Script:      string(script),
				Scenario:    scenarioArg,
				Inherit:     strings.TrimSpace(c.String("inherit")),
				Description: strings.TrimSpace(c.String("description")),
			})
			if err != nil {
				return err
			}

			fmt.Println()
			ui.Success("Generated revision: %s @ %s", out.Scenario, out.Revision)
			ui.KeyValue("Schema: ", out.Schema)
			ui.KeyValue("Tables: ", strings.Join(out.Tables, ", "))
			if out.InheritedFrom != "" {
				ui.KeyValue("Inherited from: ", fmt.Sprintf("%s @ %s", out.InheritedFrom, out.InheritedRevision))
				if len(out.ClearedTables) > 0 {
					ui.KeyValue("Auto-cleared FK descendants: ", strings.Join(out.ClearedTables, ", "))
				}
			}
			ui.KeyValue("Run: ", fmt.Sprintf("seedmancer seed %s", out.Scenario))
			return nil
		},
	}
}

// rejectProjectRelativeScriptPath returns an error when scriptFile resolves
// to a path inside the project root (the directory containing seedmancer.yaml).
// This keeps generator scripts out of the repository — they should live in
// /tmp or be piped via stdin.
func rejectProjectRelativeScriptPath(scriptFile string) error {
	configPath, err := utils.FindConfigFile()
	if err != nil {
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
