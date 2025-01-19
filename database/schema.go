package db

type Column struct {
    Name        string
    Type        string
    Nullable    bool
    Default     interface{}
    IsPrimary   bool
    IsUnique    bool
    ForeignKey  *ForeignKey `json:",omitempty"`
    Values      []string    `json:",omitempty"`
}

type ForeignKey struct {
    Table     string // Referenced table
    Column    string // Referenced column
}

type Table struct {
    Name    string
    Columns []Column
}

type Schema struct {
    Tables []Table
}

type SchemaExtractor interface {
    ExtractSchema() (*Schema, error)
    SaveSchemaToFile(schema *Schema, filename string) error
} 