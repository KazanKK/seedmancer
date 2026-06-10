package cmd

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

// usageError prints the command's full help text (name, usage, description,
// options) and returns a short error describing what was wrong. Use it for
// missing/invalid positional arguments so users see the complete usage
// instead of a bare one-line hint.
//
// The help is rendered directly from c.Command instead of going through
// cli.ShowCommandHelp: the framework appends an internal "help" subcommand
// to every command at setup time, which makes ShowCommandHelp search the
// wrong command list and silently print nothing.
func usageError(c *cli.Context, format string, args ...interface{}) error {
	if c != nil && c.Command != nil {
		cli.HelpPrinter(c.App.Writer, cli.CommandHelpTemplate, c.Command)
	}
	return fmt.Errorf(format, args...)
}
