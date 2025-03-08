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

func SeedCommand() *cli.Command {
	return &cli.Command{
		Name:  "seed",
		Usage: "Import test data into database",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database-name",
				Required: true,
				Usage:    "Database name",
			},
			&cli.StringFlag{
				Name:     "version-name",
				Required: true,
				Usage:    "version name",
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

			databaseName := c.String("database-name")
			versionName := c.String("version-name")
			dbURL := c.String("db-url")

			// Check local test data directory
			versionPath := filepath.Join(projectRoot, config.StoragePath, "databases", databaseName, versionName)
			if _, err := os.Stat(versionPath); err != nil {
				if os.IsNotExist(err) {
					return fmt.Errorf("local test data not found for version '%s'", versionName)
				} else {
					return fmt.Errorf("checking version directory: %v", err)
				}
			}

			// Add sslmode=disable to the connection string if not present and it's postgres
			u, err := url.Parse(dbURL)
			if err != nil {
				return fmt.Errorf("parsing database URL: %v", err)
			}
			
			if u.Scheme == "postgres" && !strings.Contains(dbURL, "sslmode=") {
				dbURL += "?sslmode=disable"
			}

			// Create appropriate database manager based on URL scheme
			var dbManager db.DatabaseManager
			
			switch u.Scheme {
			case "postgres":
				pg := &db.PostgresManager{}
				if err := pg.ConnectWithDSN(dbURL); err != nil {
					return fmt.Errorf("connecting to database: %v", err)
				}
				dbManager = pg
				
			case "mysql":
				my := &db.MySQLManager{}
				if err := my.ConnectWithDSN(dbURL); err != nil {
					return fmt.Errorf("connecting to database: %v", err)
				}
				dbManager = my
				
			default:
				return fmt.Errorf("unsupported database type: %s", u.Scheme)
			}

			fmt.Printf("Importing test data from: %s\n", versionPath)
			if err := dbManager.RestoreFromCSV(versionPath); err != nil {
				return fmt.Errorf("importing test data: %v", err)
			}

			fmt.Printf("\n✅ Successfully imported version '%s'\n", versionName)
			return nil
		},
	}
}