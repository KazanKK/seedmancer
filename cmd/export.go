package cmd

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	db "github.com/KazanKK/seedmancer/database"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

func ExportCommand() *cli.Command {
	return &cli.Command{
		Name:  "export",
		Usage: "Export current database schema and data",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "database-name",
				Required: false,
				Usage:    "Database name (overrides database_name in seedmancer.yaml)",
			},
			&cli.StringFlag{
				Name:     "version-name",
				Required: false,
				Usage:    "Version directory name (optional; if omitted, uses UTC YYYYMMDDHHMMSS_(database-name))",
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

			dbURL := c.String("db-url")
			if dbURL == "" {
				dbURL = cfg.DatabaseURL
			}
			if dbURL == "" {
				return fmt.Errorf("database URL required: set database_url in seedmancer.yaml, or use --db-url / SEEDMANCER_DATABASE_URL")
			}
			databaseName := c.String("database-name")
			if databaseName == "" {
				databaseName = cfg.DatabaseName
			}
			if databaseName == "" {
				return fmt.Errorf("database name required: set database_name in seedmancer.yaml, or use --database-name")
			}
			versionName := strings.TrimSpace(c.String("version-name"))
			if versionName == "" {
				versionName = utils.DefaultVersionName(databaseName)
				ui.Info("Auto-generated version: %s", versionName)
			}

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

			outputDir := utils.GetVersionPath(projectRoot, cfg.StoragePath, databaseName, versionName)
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				return fmt.Errorf("creating output directory: %v", err)
			}

			if u.Scheme != "postgres" {
				return fmt.Errorf("unsupported database type: %s (only postgres is supported)", u.Scheme)
			}

			pg := &db.PostgresManager{}
			if err := pg.ConnectWithDSN(dbURL); err != nil {
				return fmt.Errorf("connecting to database: %v", err)
			}

			sp := ui.StartSpinner("Exporting schema...")
			if err := pg.ExportSchema(outputDir); err != nil {
				sp.Stop(false, "Schema export failed")
				return fmt.Errorf("exporting schema: %v", err)
			}
			sp.Stop(true, "Schema exported")

			sp = ui.StartSpinner("Exporting table data...")
			if err := pg.ExportToCSV(outputDir); err != nil {
				sp.Stop(false, "Data export failed")
				return fmt.Errorf("exporting data: %v", err)
			}
			sp.Stop(true, "Data exported")

			ui.Success("Export complete → %s", outputDir)
			return nil
		},
	}
}
