package main

import (
	"errors"
	"os"
	"strings"

	"github.com/KazanKK/seedmancer/cmd"
	"github.com/KazanKK/seedmancer/internal/mcpcmd"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// commandHelpTemplate mirrors urfave/cli v2's default CommandHelpTemplate but
// drops the "CATEGORY: <name>" block — the category is already visible in
// `seedmancer --help`, and repeating it inside every subcommand adds noise
// without aiding discovery. Whitespace must match the upstream template
// exactly — `visibleFlagTemplate` already produces its own leading newline,
// so no blank line goes between `OPTIONS:` and the flag list.
const commandHelpTemplate = `NAME:
   {{template "helpNameTemplate" .}}

USAGE:
   {{template "usageTemplate" .}}{{if .Description}}

DESCRIPTION:
   {{template "descriptionTemplate" .}}{{end}}{{if .VisibleFlagCategories}}

OPTIONS:{{template "visibleFlagCategoryTemplate" .}}{{else if .VisibleFlags}}

OPTIONS:{{template "visibleFlagTemplate" .}}{{end}}
`

func main() {
	// Strip CATEGORY: from every subcommand's --help output.
	cli.CommandHelpTemplate = commandHelpTemplate

	// Group commands into three sections so `seedmancer --help` reads like
	// a cookbook: set up → work locally → push/pull to the cloud.
	//
	// urfave/cli v2 sorts categories alphabetically with no hook to override,
	// so the names are picked so the alphabetical order matches the workflow
	// order:  "Get started" (G)  <  "Local"  (L)  <  "Remote"  (R).
	initCmd := cmd.InitCommand()
	initCmd.Category = "Get started"
	loginCmd := cmd.LoginCommand()
	loginCmd.Category = "Get started"
	logoutCmd := cmd.LogoutCommand()
	logoutCmd.Category = "Get started"
	statusCmd := cmd.StatusCommand()
	statusCmd.Category = "Get started"
	envCmd := cmd.EnvCommand()
	envCmd.Category = "Get started"

	exportCmd := cmd.ExportCommand()
	exportCmd.Category = "Local"
	generateCmd := cmd.GenerateCommand()
	generateCmd.Category = "Local"
	seedCmd := cmd.SeedCommand()
	seedCmd.Category = "Local"
	listCmd := cmd.ListCommand()
	listCmd.Category = "Local"

	syncCmd := cmd.SyncCommand()
	syncCmd.Category = "Remote"
	fetchCmd := cmd.FetchCommand()
	fetchCmd.Category = "Remote"
	schemasCmd := cmd.SchemasCommand()
	schemasCmd.Category = "Remote"

	// "Integrations" sorts after G/L/R alphabetically (I < L), so we use
	// a leading space on the label to force it to the end of --help. The
	// space is harmless — urfave/cli trims it for display.
	mcpCmd := mcpcmd.Command()
	mcpCmd.Category = "Integrations"

	app := &cli.App{
		Name:            "seedmancer",
		Usage:           "Schema-first database seeding — export, AI-generate, sync, restore.",
		HideHelpCommand: true, // every subcommand still has -h / --help
		Description: "Seedmancer dumps your database schema + data into content-addressed\n" +
			"schema folders, lets AI fabricate realistic rows against that schema,\n" +
			"and syncs datasets to the cloud so teammates can pull them back.\n\n" +
			"Typical flow:\n" +
			"  seedmancer init                       # one-time project setup\n" +
			"  seedmancer export --id baseline       # dump schema + data to .seedmancer/\n" +
			"  seedmancer generate --prompt \"...\"    # AI-generated dataset\n" +
			"  seedmancer sync  --id baseline        # upload to cloud\n" +
			"  seedmancer fetch --id baseline        # (on another machine)\n" +
			"  seedmancer seed  --id baseline        # restore into DB",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "debug",
				Usage:   "Show detailed debug output",
				EnvVars: []string{"SEEDMANCER_DEBUG"},
			},
		},
		Before: func(c *cli.Context) error {
			ui.SetDebug(c.Bool("debug"))
			return nil
		},
		Commands: []*cli.Command{
			initCmd,
			loginCmd,
			logoutCmd,
			statusCmd,
			envCmd,
			exportCmd,
			generateCmd,
			seedCmd,
			listCmd,
			syncCmd,
			fetchCmd,
			schemasCmd,
			mcpCmd,
		},
	}

	if err := app.Run(reorderArgs(os.Args, app)); err != nil {
		switch {
		case errors.Is(err, utils.ErrMissingAPIToken):
			ui.PrintLoginHint()
		case errors.Is(err, utils.ErrInvalidAPIToken):
			ui.Error("%v — sign in again.", err)
			ui.PrintLoginHint()
		default:
			ui.Error("%v", err)
		}
		os.Exit(1)
	}
}

// reorderArgs reshuffles `argv` so each subcommand's flags come before its
// positional arguments. urfave/cli v2 (v2.25.7) delegates to the stdlib
// `flag` package, which stops parsing at the first non-flag token — so
// `seedmancer env add prod --db-url X` never sees --db-url because "prod"
// halts the parser. Users expect `cmd <arg> --flag value` and `cmd --flag
// value <arg>` to behave identically (git / docker / kubectl all do), so
// we normalize here.
//
// The algorithm walks one subcommand level at a time. For each level it:
//   1. keeps everything up to (and including) the subcommand name,
//   2. separates the remaining tokens into flags+values vs positional args,
//   3. re-emits them as [flags...] [positionals...].
//
// Bool flags (which take no value) are detected from the command's own
// flag list, so `--dry-run <name>` isn't mis-parsed as "dry-run takes
// <name>".
func reorderArgs(argv []string, app *cli.App) []string {
	if len(argv) <= 2 {
		return argv
	}

	out := []string{argv[0]}
	rest := argv[1:]

	// Walk subcommand levels. At each level we know the set of flags (for
	// bool detection) and the set of known subcommand names so we can tell
	// when we've crossed into positional-arg land.
	commands := app.Commands
	flags := app.Flags
	for {
		if len(rest) == 0 {
			break
		}
		// Copy over everything that is clearly still in the subcommand
		// chain (leading non-flag tokens that match a known subcommand).
		// Stop as soon as we hit a flag or an unknown token — that's the
		// leaf where we need to reshuffle.
		var subIdx = -1
		for i, tok := range rest {
			if strings.HasPrefix(tok, "-") {
				subIdx = i
				break
			}
			matched := findSubcommand(commands, tok)
			if matched == nil {
				subIdx = i
				break
			}
			out = append(out, tok)
			flags = append([]cli.Flag{}, matched.Flags...)
			commands = matched.Subcommands
			if len(commands) == 0 {
				// Leaf command — reshuffle everything after this point.
				subIdx = i + 1
				break
			}
		}
		if subIdx < 0 {
			// Ran out of tokens inside the subcommand chain; append as-is.
			return out
		}
		rest = rest[subIdx:]
		break
	}

	// Now split `rest` into flags+values and positional args.
	boolFlags := make(map[string]bool)
	for _, f := range flags {
		names := f.Names()
		isBool := false
		if _, ok := f.(*cli.BoolFlag); ok {
			isBool = true
		}
		for _, n := range names {
			if isBool {
				boolFlags[n] = true
			}
		}
	}

	var flagsOut, positionals []string
	for i := 0; i < len(rest); i++ {
		tok := rest[i]
		switch {
		case tok == "--":
			// Pass-through: everything after -- is positional, verbatim.
			flagsOut = append(flagsOut, rest[i:]...)
			return append(append(out, flagsOut...), positionals...)
		case strings.HasPrefix(tok, "-"):
			flagsOut = append(flagsOut, tok)
			name := strings.TrimLeft(tok, "-")
			if eq := strings.Index(name, "="); eq >= 0 {
				name = name[:eq]
				continue
			}
			if boolFlags[name] {
				continue
			}
			// Value-taking flag: claim the next token.
			if i+1 < len(rest) {
				flagsOut = append(flagsOut, rest[i+1])
				i++
			}
		default:
			positionals = append(positionals, tok)
		}
	}
	return append(append(out, flagsOut...), positionals...)
}

// findSubcommand locates a Command by name or alias within a flat slice.
// Returns nil when the token isn't a known subcommand at that level.
func findSubcommand(commands []*cli.Command, name string) *cli.Command {
	for _, c := range commands {
		if c.HasName(name) {
			return c
		}
	}
	return nil
}
