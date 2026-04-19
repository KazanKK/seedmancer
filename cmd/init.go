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
	"gopkg.in/yaml.v3"
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
				Name:  "database-url",
				Usage: "Default database connection URL saved into seedmancer.yaml",
			},
		},
		Action: func(c *cli.Context) error {
			storagePath := c.String("storage-path")
			databaseURL := c.String("database-url")

			if existing, err := loadExistingConfig(); err == nil {
				if !c.IsSet("storage-path") && existing.StoragePath != "" {
					storagePath = existing.StoragePath
				}
				if !c.IsSet("database-url") && existing.DatabaseURL != "" {
					databaseURL = existing.DatabaseURL
				}
			}

			if storagePath == "" {
				storagePath = ".seedmancer"
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
				if !c.IsSet("database-url") {
					databaseURL, err = prompt(in, "Database URL", databaseURL)
					if err != nil {
						return err
					}
				}
			}

			if strings.TrimSpace(storagePath) == "" {
				return fmt.Errorf("storage path cannot be empty")
			}

			cfg := utils.Config{
				StoragePath: storagePath,
				DatabaseURL: strings.TrimSpace(databaseURL),
			}

			yamlData, err := yaml.Marshal(cfg)
			if err != nil {
				return fmt.Errorf("creating yaml: %v", err)
			}

			if err := os.WriteFile("seedmancer.yaml", yamlData, 0644); err != nil {
				return fmt.Errorf("writing config file: %v", err)
			}

			if err := os.MkdirAll(storagePath, 0755); err != nil {
				return fmt.Errorf("creating storage directory: %v", err)
			}

			ui.Success("Created seedmancer.yaml")
			ui.KeyValue("storage_path: ", storagePath)
			if cfg.DatabaseURL != "" {
				ui.KeyValue("database_url: ", cfg.DatabaseURL)
			}
			fmt.Println()
			ui.Info("Next step: run `seedmancer export --id baseline` to dump your database.")
			ui.Info("The schema folder name is derived from the schema fingerprint — no setup needed.")
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
