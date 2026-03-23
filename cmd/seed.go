package cmd

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	db "github.com/KazanKK/seedmancer/database"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

func SeedCommand() *cli.Command {
	return &cli.Command{
		Name:  "seed",
		Usage: "Import test data into database",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database-name",
				Required: false,
				Usage:    "Database name (overrides database_name in seedmancer.yaml)",
			},
			&cli.StringFlag{
				Name:     "version-name",
				Required: false,
				Usage:    "Test data version directory (optional; if omitted, uses latest YYYYMMDDHHMMSS_(...) folder, else unversioned, else sole folder)",
			},
			&cli.StringFlag{
				Name:     "db-url",
				Required: false,
				Usage:    "Database connection URL (overrides database_url in seedmancer.yaml and SEEDMANCER_DATABASE_URL)",
				EnvVars:  []string{"SEEDMANCER_DATABASE_URL"},
			},
		},
		Action: func(c *cli.Context) error {
			// Find config file to get storage path and project root
			configPath, err := utils.FindConfigFile()
			if err != nil {
				return fmt.Errorf("finding config file: %v", err)
			}

			projectRoot := filepath.Dir(configPath)

			cfg, err := utils.LoadConfig(configPath)
			if err != nil {
				return err
			}

			databaseName := c.String("database-name")
			if databaseName == "" {
				databaseName = cfg.DatabaseName
			}
			if databaseName == "" {
				return fmt.Errorf("database name required: set database_name in seedmancer.yaml, or use --database-name")
			}
			dbURL := c.String("db-url")
			if dbURL == "" {
				dbURL = cfg.DatabaseURL
			}
			if dbURL == "" {
				return fmt.Errorf("database URL required: set database_url in seedmancer.yaml, or use --db-url / SEEDMANCER_DATABASE_URL")
			}

			versionName, versionPath, err := utils.ResolveSeedVersion(projectRoot, cfg.StoragePath, databaseName, c.String("version-name"))
			if err != nil {
				return err
			}
			fmt.Printf("Using test data version: %s\n", versionName)

			// Add sslmode=disable to the connection string if not present and it's postgres
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