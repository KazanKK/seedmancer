package main

import (
	"os"

	"github.com/KazanKK/seedmancer/cmd"
	"github.com/KazanKK/seedmancer/internal/ui"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "seedmancer",
		Usage: "A CLI tool to create and restore database seeding",
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
			cmd.InitCommand(),
			cmd.SeedCommand(),
			cmd.ExportCommand(),
			cmd.ListCommand(),
			cmd.FetchCommand(),
			cmd.GenerateCommand(),
			cmd.SyncCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		ui.Error("%v", err)
		os.Exit(1)
	}
}
