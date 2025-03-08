package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	db "github.com/KazanKK/seedmancer/database"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

func FetchCommand() *cli.Command {
	return &cli.Command{
		Name:  "fetch",
		Usage: "Fetch database schema and test data from API endpoint using database name",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database-name",
				Required: true,
				Usage:    "Database name",
			},
			&cli.StringFlag{
				Name:     "version",
				Required: true,
				Usage:    "Version name",
			},
			&cli.StringFlag{
				Name:     "token",
				Required: true,
				Usage:    "API token for authentication",
				EnvVars:  []string{"SEEDMANCER_API_TOKEN"},
			},
		},
		Action: func(c *cli.Context) error {
			// Find config file to get storage path and project root
			configPath, err := utils.FindConfigFile()
			if err != nil {
				return fmt.Errorf("finding config file: %v", err)
			}
			
			projectRoot := filepath.Dir(configPath)
			storagePath, err := utils.ReadConfig(configPath)
			if err != nil {
				return fmt.Errorf("reading config: %v", err)
			}

			databaseName := c.String("database-name")
			version := c.String("version")
			token := c.String("token")

			// Create output directory structure
			outputDir := filepath.Join(projectRoot, storagePath, "databases", databaseName, version)
			
			// Remove existing directory if it exists
			if _, err := os.Stat(outputDir); err == nil {
				if err := os.RemoveAll(outputDir); err != nil {
					return fmt.Errorf("removing existing directory: %v", err)
				}
			}

			// Create fresh directory
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				return fmt.Errorf("creating output directory: %v", err)
			}

			baseURL := utils.GetBaseURL()
			
			pg := &db.PostgresManager{}
			
			if err := pg.Fetch(baseURL, databaseName, version, outputDir, token); err != nil {
				if err.Error() == "unauthorized: please check your API token" {
					fmt.Println("\n❌ Authentication failed!")
					fmt.Println("\nPlease ensure you have:")
					fmt.Println("1. Set a valid API token using the --token flag")
					fmt.Println("   OR")
					fmt.Println("2. Set the SEEDMANCER_API_TOKEN environment variable")
					fmt.Println("\nExample:")
					fmt.Println("  seedmancer fetch --database-name <name> --version <version> --token <your-token>")
					fmt.Println("  # OR")
					fmt.Println("  export SEEDMANCER_API_TOKEN=<your-token>")
					fmt.Println("  seedmancer fetch --database-name <name> --version <version>")
				}
				return err
			}

			fmt.Printf("\n✅ Fetched test data version '%s' to: %s\n", version, outputDir)
			return nil
		},
	}
}