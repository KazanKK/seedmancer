package cmd

import (
	"fmt"
	"os"

	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
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

			// If no --storage-path provided, try to get from existing config
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

			config := utils.Config{
				StoragePath: storagePath,
				DatabaseURL: databaseURL,
			}
			
			// Convert to YAML
			yamlData, err := yaml.Marshal(config)
			if err != nil {
				return fmt.Errorf("creating yaml: %v", err)
			}
			
			// Write to seedmancer.yaml in current directory
			if err := os.WriteFile("seedmancer.yaml", yamlData, 0644); err != nil {
				return fmt.Errorf("writing config file: %v", err)
			}
			
			// Create storage directory if it doesn't exist
			if err := os.MkdirAll(storagePath, 0755); err != nil {
				return fmt.Errorf("creating storage directory: %v", err)
			}
			
			fmt.Printf("Created seedmancer.yaml with storage path: %s\n", storagePath)
			if databaseURL != "" {
				fmt.Println("database_url is set in seedmancer.yaml")
			}
			return nil
		},
	}
}