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
				Value: ".seedmancer",
			},
			&cli.StringFlag{
				Name:  "database-url",
				Usage: "Default database connection URL (written as database_url; optional)",
			},
		},
		Action: func(c *cli.Context) error {
			storagePath := c.String("storage-path")
			databaseURL := c.String("database-url")

			if !c.IsSet("storage-path") {
				if configPath, err := utils.FindConfigFile(); err == nil {
					if existing, err := utils.LoadConfig(configPath); err == nil {
						storagePath = existing.StoragePath
					}
				}
			}
			if !c.IsSet("database-url") {
				if configPath, err := utils.FindConfigFile(); err == nil {
					if existing, err := utils.LoadConfig(configPath); err == nil {
						databaseURL = existing.DatabaseURL
					}
				}
			}

			askStorage := !c.IsSet("storage-path")
			askDB := !c.IsSet("database-url")
			interactive := term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))

			if interactive && (askStorage || askDB) {
				totalSteps := 0
				if askStorage {
					totalSteps++
				}
				if askDB {
					totalSteps++
				}
				printInitIntro(storagePath, databaseURL, totalSteps)
				in := bufio.NewReader(os.Stdin)
				step := 0
				var err error
				if askStorage {
					step++
					storagePath, err = promptStep(in, step, totalSteps, "Storage path",
						"Directory for seed data (CSV, schema, etc.).", storagePath)
					if err != nil {
						return err
					}
					if strings.TrimSpace(storagePath) == "" {
						return fmt.Errorf("storage path cannot be empty")
					}
				}
				if askDB {
					step++
					databaseURL, err = promptStep(in, step, totalSteps, "Database URL",
						"Default connection for seed/export. Optional.", databaseURL)
					if err != nil {
						return err
					}
				}
			}

			config := utils.Config{
				StoragePath: storagePath,
				DatabaseURL: databaseURL,
			}

			yamlData, err := yaml.Marshal(config)
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
			fmt.Printf("Created seedmancer.yaml with storage path: %s\n", storagePath)
			if databaseURL != "" {
				fmt.Println("database_url is set in seedmancer.yaml")
			}
			return nil
		},
	}
}

func printInitIntro(storagePath, databaseURL string, totalSteps int) {
	fmt.Println()
	fmt.Println("  seedmancer init")
	fmt.Println("  ─────────────────")
	fmt.Println()
	fmt.Printf("  This wizard has %d step(s). Defaults you can accept with Enter:\n\n", totalSteps)
	fmt.Printf("    • storage_path  →  %s\n", storagePath)
	if databaseURL != "" {
		fmt.Printf("    • database_url  →  %s\n", databaseURL)
	} else {
		fmt.Printf("    • database_url  →  (not set; optional)\n")
	}
	fmt.Println()
	fmt.Println("  ─────────────────")
	fmt.Println()
}

func promptStep(in *bufio.Reader, step, total int, title, hint, defaultVal string) (string, error) {
	fmt.Printf("  Step %d of %d  %s\n", step, total, title)
	fmt.Printf("  %s\n", hint)
	if defaultVal == "" {
		fmt.Printf("  Default: (none — press Enter to skip)\n")
	} else {
		fmt.Printf("  Default: %s\n", defaultVal)
	}
	fmt.Print("  › ")
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
