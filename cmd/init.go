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
		},
		Action: func(c *cli.Context) error {
			storagePath := c.String("storage-path")
			
			// If no --storage-path provided, try to get from existing config
			if !c.IsSet("storage-path") {
				if configPath, err := utils.FindConfigFile(); err == nil {
					data, err := os.ReadFile(configPath)
					if err == nil {
						var existingConfig struct {
							StoragePath string `yaml:"storage_path"`
						}
						if err := yaml.Unmarshal(data, &existingConfig); err == nil {
							storagePath = existingConfig.StoragePath
						}
					}
				}
			}
			
			// Create config struct
			config := struct {
				StoragePath string `yaml:"storage_path"`
			}{
				StoragePath: storagePath,
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
			return nil
		},
	}
}