package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var generateCmd = &cobra.Command{
    Use:   "generate [table] [count]",
    Short: "Generate fake data for a table",
    Args:  cobra.ExactArgs(2),
    RunE: func(cmd *cobra.Command, args []string) error {
        tableName := args[0]
        count := 0
        if _, err := fmt.Sscanf(args[1], "%d", &count); err != nil {
            return fmt.Errorf("invalid count: %s", args[1])
        }

        // Get schema
        schema, err := dbManager.ExtractSchema()
        if err != nil {
            return fmt.Errorf("failed to extract schema: %v", err)
        }

        // Find table
        var targetTable database.Table
        found := false
        for _, table := range schema.Tables {
            if table.Name == tableName {
                targetTable = table
                found = true
                break
            }
        }
        if !found {
            return fmt.Errorf("table not found: %s", tableName)
        }

        // Generate data
        if err := dbManager.GenerateFakeDataInMemory(targetTable, count); err != nil {
            return fmt.Errorf("failed to generate data: %v", err)
        }

        fmt.Printf("Successfully generated %d records for table %s\n", count, tableName)
        return nil
    },
}

func init() {
    rootCmd.AddCommand(generateCmd)
} 