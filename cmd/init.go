package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
)

// InitCommand writes a minimal seedmancer.yaml for a project.
//
// In the pure schema-first model seedmancer.yaml no longer pins the project
// to a named schema — the schema is derived from the fingerprint of the
// dumped schema.json every time. The config only stores the plumbing bits:
// where to keep local dumps and (optionally) the default database URL.
func InitCommand() *cli.Command {
	return &cli.Command{
		Name:  "init",
		Usage: "Initialize a seedmancer.yaml configuration file",
		Description: "Writes a minimal seedmancer.yaml in the current directory and creates\n" +
			"the local storage folder. When run in an interactive TTY it prompts\n" +
			"for missing values; in CI/CD pass --storage-path and --database-url.",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "storage-path",
				Usage: "Directory for local schema folders (default: .seedmancer)",
			},
			&cli.StringFlag{
				Name:  "env",
				Usage: "Name for the initial environment (default: local)",
			},
			&cli.StringFlag{
				Name:  "database-url",
				Usage: "Database connection URL for the initial environment",
			},
		},
		Action: func(c *cli.Context) error {
			storagePath := c.String("storage-path")
			envName := c.String("env")
			databaseURL := c.String("database-url")

			// Carry forward any values already in seedmancer.yaml so `init`
			// doubles as a safe way to re-run the flow without losing an
			// existing db url. We inspect the new env map first and fall
			// back to the legacy top-level field so old projects are
			// migrated into the modern shape on their next `init`.
			if existing, err := loadExistingConfig(); err == nil {
				if !c.IsSet("storage-path") && existing.StoragePath != "" {
					storagePath = existing.StoragePath
				}
				if !c.IsSet("env") && existing.ActiveEnvName() != "" {
					envName = existing.ActiveEnvName()
				}
				if !c.IsSet("database-url") {
					if ne, err := existing.ResolveEnv(""); err == nil {
						databaseURL = ne.DatabaseURL
					}
				}
			}

			if storagePath == "" {
				storagePath = ".seedmancer"
			}
			if strings.TrimSpace(envName) == "" {
				envName = "local"
			}

			if term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd())) {
				in := bufio.NewReader(os.Stdin)
				var err error

				if !c.IsSet("storage-path") {
					storagePath, err = prompt(in, "Storage path", storagePath)
					if err != nil {
						return err
					}
				}
				if !c.IsSet("env") {
					envName, err = prompt(in, "Environment name", envName)
					if err != nil {
						return err
					}
				}
				if !c.IsSet("database-url") {
					databaseURL, err = prompt(in, "Database URL", databaseURL)
					if err != nil {
						return err
					}
				}
			}

			storagePath = strings.TrimSpace(storagePath)
			envName = strings.TrimSpace(envName)
			databaseURL = strings.TrimSpace(databaseURL)

			if storagePath == "" {
				return fmt.Errorf("storage path cannot be empty")
			}
			if envName == "" {
				return fmt.Errorf("environment name cannot be empty")
			}

			cfg := utils.Config{
				StoragePath: storagePath,
				DefaultEnv:  envName,
				Environments: map[string]utils.EnvConfig{
					envName: {DatabaseURL: databaseURL},
				},
			}

			if err := utils.SaveConfig("seedmancer.yaml", cfg); err != nil {
				return err
			}

			if err := os.MkdirAll(storagePath, 0755); err != nil {
				return fmt.Errorf("creating storage directory: %v", err)
			}

			ui.Success("Created seedmancer.yaml")
			ui.KeyValue("storage_path: ", storagePath)
			ui.KeyValue("default_env:  ", envName)
			if databaseURL != "" {
				ui.KeyValue(fmt.Sprintf("environments.%s.database_url: ", envName), databaseURL)
			}
			fmt.Println()
			ui.Info("Add more environments with: seedmancer env add <name> --db-url <url>")
			ui.Info("Then push the same dataset to many: seedmancer seed -d <id> --env %s,<other>", envName)
			return nil
		},
	}
}

func loadExistingConfig() (utils.Config, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return utils.Config{}, err
	}
	return utils.LoadConfig(configPath)
}

func prompt(in *bufio.Reader, label, defaultVal string) (string, error) {
	if defaultVal != "" {
		fmt.Printf("  \033[36m?\033[0m %s \033[90m(%s)\033[0m: ", label, defaultVal)
	} else {
		fmt.Printf("  \033[36m?\033[0m %s: ", label)
	}

	line, err := in.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("reading input: %w", err)
	}

	s := strings.TrimSpace(line)
	if s == "" {
		return defaultVal, nil
	}
	return s, nil
}
