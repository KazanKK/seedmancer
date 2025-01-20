package db

type Column struct {
    Name       string      `json:"name"`
    Type       string      `json:"type"`        // Database type (e.g. text, integer, timestamp)
    SystemType string      `json:"systemType"`  // Standardized type (e.g. string, number, datetime)
    Nullable   bool        `json:"nullable"`
    Default    interface{} `json:"default,omitempty"`
    IsPrimary  bool        `json:"isPrimary"`
    IsUnique   bool        `json:"isUnique"`
    ForeignKey *ForeignKey `json:"foreignKey,omitempty"`
    Values     []string    `json:"values,omitempty"`
}

type ForeignKey struct {
    Table  string `json:"table"`  // Referenced table
    Column string `json:"column"` // Referenced column
}

type Table struct {
    Name    string   `json:"name"`
    Columns []Column `json:"columns"`
}

type Schema struct {
    Tables []Table `json:"tables"`
}

type SchemaExtractor interface {
    ExtractSchema() (*Schema, error)
} 