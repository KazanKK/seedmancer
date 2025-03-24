package main

import (
	"log"
	"os"

	"github.com/KazanKK/seedmancer/cmd"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:    "seedmancer",
		Usage:   "A CLI tool to create and restore database seeding",
		Commands: []*cli.Command{
			cmd.InitCommand(),
			cmd.SeedCommand(),
			cmd.ExportCommand(),
			cmd.ListCommand(),
			cmd.FetchCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
