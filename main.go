package main

import (
	"errors"
	"os"

	"github.com/KazanKK/seedmancer/cmd"
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
			exportCmd,
			generateCmd,
			seedCmd,
			listCmd,
			syncCmd,
			fetchCmd,
			schemasCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
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
