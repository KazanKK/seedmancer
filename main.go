package main

import (
	db "reseeder/database"
	snapshot "reseeder/snapshots"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"
)

var Version = "dev"

func main() {
	app := &cli.App{
		Name:    "reseeder",
		Usage:   "A CLI tool to create and restore database snapshots",
		Version: Version,
		Commands: []*cli.Command{
			{
				Name:  "snapshot-create",
				Usage: "Create a database snapshot",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "db", Required: true, Usage: "Database type (mysql or postgres)"},
					&cli.StringFlag{Name: "dsn", Required: true, Usage: "Database connection string (DSN)"},
					&cli.StringFlag{Name: "name", Required: true, Usage: "Snapshot name"},
				},
				Action: func(c *cli.Context) error {
					dbType := c.String("db")
					dsn := c.String("dsn") + "?sslmode=disable"
					name := c.String("name")

					if err := snapshot.CreateWithDSN(dbType, dsn, name); err != nil {
						return err
					}
					fmt.Println("Snapshot created successfully:", name)
					return nil
				},
			},
			{
				Name:  "snapshot-restore",
				Usage: "Restore a database snapshot",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "db", Required: true, Usage: "Database type (mysql or postgres)"},
					&cli.StringFlag{Name: "snapshot", Required: true, Usage: "Snapshot name"},
				},
				Action: func(c *cli.Context) error {
					dbType := c.String("db")
					snapshotName := c.String("snapshot")

					if err := snapshot.Restore(dbType, snapshotName); err != nil {
						return err
					}
					fmt.Println("Database restored to snapshot:", snapshotName)
					return nil
				},
			},
			{
				Name:  "schema-extract",
				Usage: "Extract database schema information",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "db", Required: true, Usage: "Database type (mysql or postgres)"},
					&cli.StringFlag{Name: "dsn", Required: true, Usage: "Database connection string (DSN)"},
					&cli.StringFlag{Name: "output-dir", Required: true, Usage: "Output directory for schema.json"},
				},
				Action: func(c *cli.Context) error {
					dbType := c.String("db")
					dsn := c.String("dsn") + "?sslmode=disable"
					outputDir := c.String("output-dir")

					if err := os.MkdirAll(outputDir, 0755); err != nil {
						return fmt.Errorf("creating output directory: %v", err)
					}

					var manager db.SchemaExtractor
					switch dbType {
					case "postgres":
						pg := &db.PostgresManager{}
						if err := pg.ConnectWithDSN(dsn); err != nil {
							return err
						}
						manager = pg
					default:
						return fmt.Errorf("unsupported database type: %s", dbType)
					}

					schema, err := manager.ExtractSchema()
					if err != nil {
						return err
					}

					outputPath := filepath.Join(outputDir, "schema.json")
					if err := manager.SaveSchemaToFile(schema, outputPath); err != nil {
						return err
					}

					fmt.Printf("Schema extracted successfully to: %s\n", outputPath)
					return nil
				},
			},
			{
				Name:  "generate-fake-data",
				Usage: "Generate fake data based on schema",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "schema", Required: true, Usage: "Path to schema.json file"},
					&cli.StringFlag{Name: "output-dir", Required: true, Usage: "Output directory for CSV files"},
					&cli.IntFlag{Name: "rows", Value: 10, Usage: "Number of rows to generate per table"},
				},
				Action: func(c *cli.Context) error {
					schemaPath := c.String("schema")
					outputDir := c.String("output-dir")
					rowCount := c.Int("rows")

					if err := os.MkdirAll(outputDir, 0755); err != nil {
						return fmt.Errorf("creating output directory: %v", err)
					}

					if err := db.GenerateFakeData(schemaPath, outputDir, rowCount); err != nil {
						return err
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
					&cli.StringFlag{Name: "dir", Required: true, Usage: "Directory containing CSV files"},
					&cli.BoolFlag{Name: "debug", Value: false, Usage: "Enable debug logging"},
				},
				Action: func(c *cli.Context) error {
					dbType := c.String("db")
					dsn := c.String("dsn") + "?sslmode=disable"
					directory := c.String("dir")

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
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}