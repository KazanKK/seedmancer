package main

import (
	"fmt"
	"log"
	"os"
	db "reseeder/database"

	"github.com/urfave/cli/v2"
)

var Version = "dev"

func main() {
	app := &cli.App{
		Name:    "reseeder",
		Usage:   "A CLI tool to create and restore database seeding",
		Version: Version,
		Commands: []*cli.Command{
			{
				Name:  "generate-fake-data",
				Usage: "Generate fake data based on schema",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "output-dir", Required: true, Usage: "Output directory for CSV files"},
					&cli.IntFlag{Name: "rows", Value: 10, Usage: "Number of rows to generate per table"},
					&cli.StringFlag{Name: "db", Required: true, Usage: "Database type (mysql or postgres)"},
					&cli.StringFlag{Name: "dsn", Required: true, Usage: "Database connection string (DSN)"},
				},
				Action: func(c *cli.Context) error {
					outputDir := c.String("output-dir")
					rowCount := c.Int("rows")
					dbType := c.String("db")
					dsn := c.String("dsn") + "?sslmode=disable"

					if err := os.MkdirAll(outputDir, 0755); err != nil {
						return fmt.Errorf("creating output directory: %v", err)
					}

					switch dbType {
					case "postgres":
						pg := &db.PostgresManager{}
						if err := pg.ConnectWithDSN(dsn); err != nil {
							return fmt.Errorf("connecting to database: %v", err)
						}
						if err := pg.GenerateFakeData(outputDir, rowCount); err != nil {
							return fmt.Errorf("generating fake data: %v", err)
						}
					default:
						return fmt.Errorf("unsupported database type: %s", dbType)
					}

					fmt.Printf("Fake data generated in directory: %s\n", outputDir)
					return nil
				},
			},
			{
				Name:  "restore-from-csv",
				Usage: "Restore database from CSV files",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "db", Required: true, Usage: "Database type (mysql or postgres)"},
					&cli.StringFlag{Name: "dsn", Required: true, Usage: "Database connection string (DSN)"},
					&cli.StringFlag{Name: "--csv-dir", Required: true, Usage: "Directory containing CSV files"},
					&cli.BoolFlag{Name: "debug", Value: false, Usage: "Enable debug logging"},
				},
				Action: func(c *cli.Context) error {
					dbType := c.String("db")
					dsn := c.String("dsn") + "?sslmode=disable"
					directory := c.String("--csv-dir")

					switch dbType {
					case "postgres":
						pg := &db.PostgresManager{}
						pg.SetDebug(c.Bool("debug"))
						if err := pg.ConnectWithDSN(dsn); err != nil {
							return err
						}
						if err := pg.RestoreFromCSV(directory); err != nil {
							return err
						}
					default:
						return fmt.Errorf("unsupported database type: %s", dbType)
					}

					fmt.Printf("Database restored from CSV files in: %s\n", directory)
					return nil
				},
			},
			{
				Name:  "export-to-csv",
				Usage: "Export current database content to CSV files",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "output-dir", Required: true, Usage: "Output directory for CSV files"},
					&cli.StringFlag{Name: "db", Required: true, Usage: "Database type (mysql or postgres)"},
					&cli.StringFlag{Name: "dsn", Required: true, Usage: "Database connection string (DSN)"},
					&cli.BoolFlag{Name: "debug", Value: false, Usage: "Enable debug logging"},
				},
				Action: func(c *cli.Context) error {
					outputDir := c.String("output-dir")
					dbType := c.String("db")
					dsn := c.String("dsn") + "?sslmode=disable"

					if err := os.MkdirAll(outputDir, 0755); err != nil {
						return fmt.Errorf("creating output directory: %v", err)
					}

					switch dbType {
					case "postgres":
						pg := &db.PostgresManager{}
						pg.SetDebug(c.Bool("debug"))
						if err := pg.ConnectWithDSN(dsn); err != nil {
							return fmt.Errorf("connecting to database: %v", err)
						}
						if err := pg.ExportToCSV(outputDir); err != nil {
							return fmt.Errorf("exporting to CSV: %v", err)
						}
					default:
						return fmt.Errorf("unsupported database type: %s", dbType)
					}

					fmt.Printf("Database exported to CSV files in: %s\n", outputDir)
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}