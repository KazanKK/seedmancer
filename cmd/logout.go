package cmd

import (
	"os"
	"strings"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// LogoutCommand deletes the API token written by `seedmancer login`. Idempotent —
// a missing credentials file is treated as success so scripts can call logout
// unconditionally during teardown.
func LogoutCommand() *cli.Command {
	return &cli.Command{
		Name:  "logout",
		Usage: "Remove the saved API token from this machine",
		Description: "Deletes ~/.seedmancer/credentials so subsequent commands are anonymous.\n" +
			"Also warns when SEEDMANCER_API_TOKEN is still set in the shell — the\n" +
			"env var will pick up from there the moment the credentials file is gone.",
		ArgsUsage: " ",
		Action: func(c *cli.Context) error {
			if err := utils.ClearAPICredentials(); err != nil {
				return err
			}
			ui.Success("Signed out — removed ~/.seedmancer/credentials")

			if envTok := strings.TrimSpace(os.Getenv("SEEDMANCER_API_TOKEN")); envTok != "" {
				ui.Warn("SEEDMANCER_API_TOKEN is still set in your shell and will be used by future commands.")
				ui.Info("To clear it:  unset SEEDMANCER_API_TOKEN")
			}
			return nil
		},
	}
}
