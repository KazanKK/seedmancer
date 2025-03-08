package cmd

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	db "github.com/KazanKK/seedmancer/database"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

func ExportCommand() *cli.Command {
	return &cli.Command{
		Name:  "export",
		Usage: "Export current database schema and data",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database-name",
				Required: true,
				Usage:    "Database name",
			},
			&cli.StringFlag{
				Name:     "version",
				Required: false,
				Usage:    "Version name (optional, defaults to unversioned)",
			},
			&cli.StringFlag{
				Name:     "db-url",
				Required: true,
				Usage:    "Database connection URL (e.g., postgres://user:pass@localhost:5432/dbname or mysql://user:pass@localhost:3306/dbname)",
			},
		},
		Action: func(c *cli.Context) error {
			// Find config file to get storage path and project root
			configPath, err := utils.FindConfigFile()
			if err != nil {
				return fmt.Errorf("finding config file: %v", err)
			}
			
			projectRoot := filepath.Dir(configPath)
			data, err := os.ReadFile(configPath)
			if err != nil {
				return fmt.Errorf("reading config file: %v", err)
			}
			
			var config struct {
				StoragePath string `yaml:"storage_path"`
			}
			if err := yaml.Unmarshal(data, &config); err != nil {
				return fmt.Errorf("parsing config file: %v", err)
			}

			dbURL := c.String("db-url")
			databaseName := c.String("database-name")
			version := c.String("version")
			
			// Add sslmode=disable to PostgreSQL connection if not present
			u, err := url.Parse(dbURL)
			if err != nil {
				return fmt.Errorf("parsing database URL: %v", err)
			}
			
			if u.Scheme == "postgres" && !strings.Contains(dbURL, "sslmode=") {
				if strings.Contains(dbURL, "?") {
					dbURL += "&sslmode=disable"
				} else {
					dbURL += "?sslmode=disable"
				}
			}
			
			// Create output directory based on version
			outputDir := utils.GetVersionPath(projectRoot, config.StoragePath, databaseName, version)
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				return fmt.Errorf("creating output directory: %v", err)
			}

			// Handle database connection and export
			switch u.Scheme {
			case "postgres":
				pg := &db.PostgresManager{}
				if err := pg.ConnectWithDSN(dbURL); err != nil {
					return fmt.Errorf("connecting to database: %v", err)
				}
				
				fmt.Println("Exporting database schema...")
				if err := pg.ExportSchema(filepath.Join(outputDir, "schema.json")); err != nil {
					return fmt.Errorf("exporting schema: %v", err)
				}
				
				fmt.Println("Exporting table data...")
				if err := pg.ExportToCSV(outputDir); err != nil {
					return fmt.Errorf("exporting data: %v", err)
				}
			
			case "mysql":
				my := &db.MySQLManager{}
				if err := my.ConnectWithDSN(dbURL); err != nil {
					return fmt.Errorf("connecting to database: %v", err)
				}
				
				fmt.Println("Exporting database schema...")
				if err := my.ExportSchema(filepath.Join(outputDir, "schema.json")); err != nil {
					return fmt.Errorf("exporting schema: %v", err)
				}
				
				fmt.Println("Exporting table data...")
				if err := my.ExportToCSV(outputDir); err != nil {
					return fmt.Errorf("exporting data: %v", err)
				}
				
			default:
				return fmt.Errorf("unsupported database type: %s", u.Scheme)
			}

			fmt.Printf("\n✅ Export successful! Data stored in %s\n", outputDir)
			
			if version == "" {
				fmt.Println("\nTo save this as a version:")
				fmt.Printf("  seedmancer save --database-name %s --version <version-name>\n", databaseName)
			}
			
			return nil
		},
	}
}