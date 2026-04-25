package cmd

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
)

// EnvCommand exposes CRUD operations for the named environments in
// seedmancer.yaml. The shape of each subcommand mirrors how other CLI tools
// (gh, vercel, supabase) handle profile-like concepts: verb-noun, short,
// scriptable in CI via clear exit codes, and interactive in a TTY.
//
// We use a Before: hook on subcommands that accept one positional <name>
// so users can type the name and flags in any order. urfave/cli v2 stops
// flag parsing at the first non-flag token; reshuffleArgs pushes the
// positional to the end before the framework parses, matching the
// ergonomics of git / docker / kubectl.
func EnvCommand() *cli.Command {
	return &cli.Command{
		Name:            "env",
		Usage:           "Manage named environments (database URLs) in seedmancer.yaml",
		HideHelpCommand: true,
		Description: "An environment is a named database target. The same dataset can be\n" +
			"seeded into many of them via `seedmancer seed --env local,staging`.\n" +
			"The default environment is used when --env is omitted; change it with\n" +
			"`seedmancer env use <name>`.",
		Subcommands: []*cli.Command{
			envListCommand(),
			envCurrentCommand(),
			envUseCommand(),
			envAddCommand(),
			envRemoveCommand(),
			envShowCommand(),
		},
	}
}

func envListCommand() *cli.Command {
	return &cli.Command{
		Name:      "list",
		Usage:     "List all configured environments",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "show-secret",
				Usage: "Print database URLs with credentials (default masks the password)",
			},
		},
		Action: func(c *cli.Context) error {
			_, cfg, err := loadConfigForEnvCmd()
			if err != nil {
				return err
			}
			envs := cfg.EffectiveEnvs()
			if len(envs) == 0 {
				ui.Info("No environments configured. Add one with:")
				ui.Info("  seedmancer env add <name> --db-url <url>")
				return nil
			}
			active := cfg.ActiveEnvName()
			showSecret := c.Bool("show-secret")

			fmt.Fprintln(os.Stderr)
			for _, name := range cfg.SortedEnvNames() {
				env := envs[name]
				marker := "  "
				if name == active {
					marker = " *"
				}
				display := env.DatabaseURL
				if !showSecret {
					display = maskDatabaseURL(display)
				}
				if display == "" {
					display = "(unset)"
				}
				fmt.Fprintf(os.Stderr, "%s %-12s %s  %s\n",
					marker,
					name,
					envColorBadge(name),
					display,
				)
			}
			fmt.Fprintln(os.Stderr)
			if active != "" {
				ui.Info("Default: %s  (change with `seedmancer env use <name>`)", active)
			}
			return nil
		},
	}
}

func envCurrentCommand() *cli.Command {
	return &cli.Command{
		Name:      "current",
		Usage:     "Print the active (default) environment name",
		ArgsUsage: " ",
		Description: "Prints just the default env name on stdout — handy for shell prompts\n" +
			"and scripts that need to know which env the next command would hit.",
		Action: func(c *cli.Context) error {
			_, cfg, err := loadConfigForEnvCmd()
			if err != nil {
				return err
			}
			name := cfg.ActiveEnvName()
			if name == "" {
				return fmt.Errorf("no default environment set — run `seedmancer env use <name>`")
			}
			fmt.Println(name)
			return nil
		},
	}
}

func envUseCommand() *cli.Command {
	return &cli.Command{
		Name:      "use",
		Usage:     "Set the default environment",
		ArgsUsage: "<name>",
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("usage: seedmancer env use <name>")
			}
			name := strings.TrimSpace(c.Args().First())
			path, cfg, err := loadConfigForEnvCmd()
			if err != nil {
				return err
			}
			// Resolving validates the name exists (and has a URL) before we
			// write anything. This makes `env use typo` a loud no-op
			// instead of a silent config corruption.
			if _, err := cfg.ResolveEnv(name); err != nil {
				return err
			}
			cfg.DefaultEnv = name
			if err := utils.SaveConfig(path, cfg); err != nil {
				return err
			}
			ui.Success("Default environment set to %q", name)
			return nil
		},
	}
}

func envAddCommand() *cli.Command {
	return &cli.Command{
		Name:      "add",
		Usage:     "Add a new environment",
		ArgsUsage: "<name>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "db-url",
				Usage: "Database URL for the new environment (prompted interactively in a TTY)",
			},
			&cli.BoolFlag{
				Name:  "force",
				Usage: "Overwrite the environment if it already exists",
			},
			&cli.BoolFlag{
				Name:  "set-default",
				Usage: "Also make this the default environment",
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("usage: seedmancer env add <name> --db-url <url>")
			}
			name := strings.TrimSpace(c.Args().First())
			if err := validateEnvName(name); err != nil {
				return err
			}

			path, cfg, err := loadConfigForEnvCmd()
			if err != nil {
				return err
			}

			if _, exists := cfg.EffectiveEnvs()[name]; exists && !c.Bool("force") {
				return fmt.Errorf("environment %q already exists — use --force to overwrite", name)
			}

			dbURL := strings.TrimSpace(c.String("db-url"))
			if dbURL == "" && term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd())) {
				in := bufio.NewReader(os.Stdin)
				dbURL, err = prompt(in, fmt.Sprintf("Database URL for %q", name), "")
				if err != nil {
					return err
				}
				dbURL = strings.TrimSpace(dbURL)
			}
			if dbURL == "" {
				return fmt.Errorf("database URL required: pass --db-url <url>")
			}

			cfg.SetEnv(name, utils.EnvConfig{DatabaseURL: dbURL})
			if c.Bool("set-default") || cfg.DefaultEnv == "" {
				cfg.DefaultEnv = name
			}
			if err := utils.SaveConfig(path, cfg); err != nil {
				return err
			}
			ui.Success("Added environment %q", name)
			ui.KeyValue("database_url: ", maskDatabaseURL(dbURL))
			if cfg.DefaultEnv == name {
				ui.Info("Default environment is now %q.", name)
			}
			return nil
		},
	}
}

func envRemoveCommand() *cli.Command {
	return &cli.Command{
		Name:      "remove",
		Aliases:   []string{"rm"},
		Usage:     "Remove an environment",
		ArgsUsage: "<name>",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "yes",
				Aliases: []string{"y"},
				Usage:   "Skip confirmation",
			},
			&cli.BoolFlag{
				Name:  "force",
				Usage: "Allow removing the active default environment",
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("usage: seedmancer env remove <name>")
			}
			name := strings.TrimSpace(c.Args().First())
			path, cfg, err := loadConfigForEnvCmd()
			if err != nil {
				return err
			}
			if _, ok := cfg.EffectiveEnvs()[name]; !ok {
				return fmt.Errorf("unknown environment %q (available: %s)", name, strings.Join(cfg.SortedEnvNames(), ", "))
			}
			if name == cfg.ActiveEnvName() && !c.Bool("force") {
				return fmt.Errorf(
					"%q is the active default environment — pick a new default with `seedmancer env use <name>` first, or pass --force",
					name,
				)
			}
			if !c.Bool("yes") {
				if !ui.Confirm(fmt.Sprintf("Remove environment %q?", name), false) {
					ui.Info("Cancelled.")
					return nil
				}
			}
			if !cfg.RemoveEnv(name) {
				return fmt.Errorf("environment %q not found", name)
			}
			// If we just pulled the default out from under the user via
			// --force, leave DefaultEnv pointing at nothing so `seed`
			// errors loudly until they call `env use`. Better than
			// silently switching to an alphabetically arbitrary env.
			if cfg.DefaultEnv == name {
				cfg.DefaultEnv = ""
			}
			if err := utils.SaveConfig(path, cfg); err != nil {
				return err
			}
			ui.Success("Removed environment %q", name)
			return nil
		},
	}
}

func envShowCommand() *cli.Command {
	return &cli.Command{
		Name:      "show",
		Usage:     "Show one environment's configuration",
		ArgsUsage: "<name>",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "show-secret",
				Usage: "Print the database URL with credentials (default masks the password)",
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("usage: seedmancer env show <name>")
			}
			name := strings.TrimSpace(c.Args().First())
			_, cfg, err := loadConfigForEnvCmd()
			if err != nil {
				return err
			}
			ne, err := cfg.ResolveEnv(name)
			if err != nil {
				return err
			}
			ui.KeyValue("name:         ", ne.Name)
			u := ne.DatabaseURL
			if !c.Bool("show-secret") {
				u = maskDatabaseURL(u)
			}
			ui.KeyValue("database_url: ", u)
			if ne.Name == cfg.ActiveEnvName() {
				ui.KeyValue("default:      ", "yes")
			} else {
				ui.KeyValue("default:      ", "no")
			}
			return nil
		},
	}
}

// loadConfigForEnvCmd is the shared front door for every `env` subcommand:
// it errors with a friendly hint when the project hasn't been initialised
// yet (which is the most common first-run failure).
func loadConfigForEnvCmd() (string, utils.Config, error) {
	path, err := utils.FindConfigFile()
	if err != nil {
		return "", utils.Config{}, fmt.Errorf("%v — run `seedmancer init` first", err)
	}
	cfg, err := utils.LoadConfig(path)
	if err != nil {
		return "", utils.Config{}, err
	}
	return path, cfg, nil
}

// validateEnvName enforces a sane character set for env names so they can
// show up in filenames, shell arguments, and URLs without needing to be
// quoted. Matches what users type for git branches / docker tags today.
func validateEnvName(name string) error {
	if name == "" {
		return fmt.Errorf("environment name cannot be empty")
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return fmt.Errorf("environment name %q contains invalid character %q — use letters, digits, '-' or '_'", name, r)
		}
	}
	if _, err := url.Parse("https://example.com/" + name); err != nil {
		return fmt.Errorf("environment name %q is not URL-safe: %v", name, err)
	}
	return nil
}

// envColorBadge renders a one-character colored dot so `env list` reads at a
// glance. Color choices match the web dashboard: prod red, staging amber,
// local/dev emerald, anything else gray.
func envColorBadge(name string) string {
	lower := strings.ToLower(name)
	switch {
	case isProdLike(lower):
		return "\033[31m●\033[0m"
	case strings.Contains(lower, "stag"):
		return "\033[33m●\033[0m"
	case lower == "local" || lower == "dev" || strings.Contains(lower, "local") || strings.Contains(lower, "dev"):
		return "\033[32m●\033[0m"
	default:
		return "\033[90m●\033[0m"
	}
}
