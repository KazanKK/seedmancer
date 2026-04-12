package cmd

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/ui"
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
			ui.Step("Using version: %s", versionName)

			u, err := url.Parse(dbURL)
			if err != nil {
				return fmt.Errorf("parsing database URL: %v", err)
			}

			if u.Scheme == "postgresql" {
				dbURL = "postgres" + dbURL[len("postgresql"):]
				u.Scheme = "postgres"
			}

			if u.Scheme == "postgres" && !strings.Contains(dbURL, "sslmode=") {
				if strings.Contains(dbURL, "?") {
					dbURL += "&sslmode=disable"
				} else {
					dbURL += "?sslmode=disable"
				}
			}

			if u.Scheme != "postgres" {
				return fmt.Errorf("unsupported database type: %s (only postgres is supported)", u.Scheme)
			}

			pg := &db.PostgresManager{}
			if err := pg.ConnectWithDSN(dbURL); err != nil {
				return fmt.Errorf("connecting to database: %v", err)
			}
			var dbManager db.DatabaseManager = pg

			ui.Debug("Source path: %s", versionPath)
			sp := ui.StartSpinner("Importing test data...")
			if err := dbManager.RestoreFromCSV(versionPath); err != nil {
				sp.Stop(false, "Import failed")
				return fmt.Errorf("importing test data: %v", err)
			}
			sp.Stop(true, fmt.Sprintf("Imported version '%s'", versionName))
			return nil
		},
	}
}
