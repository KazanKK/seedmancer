package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

func InitCommand() *cli.Command {
	return &cli.Command{
		Name:  "init",
		Usage: "Initialize seedmancer configuration file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "storage-path",
				Usage: "Path to store seedmancer files",
			},
			&cli.StringFlag{
				Name:  "database-name",
				Usage: "Default database name",
			},
			&cli.StringFlag{
				Name:  "database-url",
				Usage: "Default database connection URL",
			},
		},
		Action: func(c *cli.Context) error {
			// Seed values from existing config, then flags override.
			storagePath := c.String("storage-path")
			databaseName := c.String("database-name")
			databaseURL := c.String("database-url")

			if existing, err := loadExistingConfig(); err == nil {
				if !c.IsSet("storage-path") && existing.StoragePath != "" {
					storagePath = existing.StoragePath
				}
				if !c.IsSet("database-name") && existing.DatabaseName != "" {
					databaseName = existing.DatabaseName
				}
				if !c.IsSet("database-url") && existing.DatabaseURL != "" {
					databaseURL = existing.DatabaseURL
				}
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
				if !c.IsSet("database-name") {
					databaseName, err = prompt(in, "Database name", databaseName)
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
				StoragePath:  storagePath,
				DatabaseName: databaseName,
				DatabaseURL:  databaseURL,
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

			fmt.Println()
			fmt.Println("Created seedmancer.yaml")
			fmt.Printf("  storage_path:  %s\n", storagePath)
			if databaseName != "" {
				fmt.Printf("  database_name: %s\n", databaseName)
			}
			if databaseURL != "" {
				fmt.Printf("  database_url:  %s\n", databaseURL)
			}
			return nil
		},
	}
}

// loadExistingConfig returns the nearest config without failing if none exists.
func loadExistingConfig() (utils.Config, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return utils.Config{}, err
	}
	return utils.LoadConfig(configPath)
}

// prompt prints "? Label (default): " and reads one line.
// If the user presses Enter with no input the default is returned.
func prompt(in *bufio.Reader, label, defaultVal string) (string, error) {
	if defaultVal != "" {
		fmt.Printf("? %s (%s): ", label, defaultVal)
	} else {
		fmt.Printf("? %s: ", label)
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
