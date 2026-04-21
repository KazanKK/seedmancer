package utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// Canonical JSON form of a schema used for fingerprinting — MUST produce
// identical bytes as next/src/utils/schemaFingerprint.ts. Any drift and the
// server will treat schemas as different and spawn duplicate rows.
//
// Rules:
//   - Only the structural fields are considered (name, type, nullable,
//     isPrimary, isUnique, default, foreignKey, enum). Descriptions, UI
//     ordering, etc. are ignored.
//   - Enums are sorted by name; each enum's `values` is sorted ascending.
//   - Tables are sorted by name; each table's `columns` is sorted by name.
//   - Field order in the output JSON matches the JS object literal order so
//     that `JSON.stringify` and Go's `encoding/json` produce the same string.
//   - Missing fields become `null` (default, foreignKey, enum) or `false`
//     (nullable, isPrimary, isUnique) so both sides agree on canonical form.

type fpForeignKeyOut struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

type fpColumnOut struct {
	Name       string           `json:"name"`
	Type       string           `json:"type"`
	Nullable   bool             `json:"nullable"`
	IsPrimary  bool             `json:"isPrimary"`
	IsUnique   bool             `json:"isUnique"`
	Default    *string          `json:"default"`
	ForeignKey *fpForeignKeyOut `json:"foreignKey"`
	Enum       *string          `json:"enum"`
}

type fpTableOut struct {
	Name    string        `json:"name"`
	Columns []fpColumnOut `json:"columns"`
}

type fpEnumOut struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

type fpSchemaOut struct {
	Enums  []fpEnumOut  `json:"enums"`
	Tables []fpTableOut `json:"tables"`
}

// SchemaForeignKey is the pointer shape used inside SchemaColumn.
type SchemaForeignKey struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// SchemaColumn is the lenient shape we accept for fingerprinting.
// Optional fields use pointer/json.RawMessage types so absence vs present-null
// can be distinguished — but the canonical form collapses both to `null`.
type SchemaColumn struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Nullable   *bool             `json:"nullable"`
	IsPrimary  *bool             `json:"isPrimary"`
	IsUnique   *bool             `json:"isUnique"`
	Default    json.RawMessage   `json:"default"`
	ForeignKey *SchemaForeignKey `json:"foreignKey"`
	Enum       json.RawMessage   `json:"enum"`
}

type SchemaTable struct {
	Name    string         `json:"name"`
	Columns []SchemaColumn `json:"columns"`
}

type SchemaEnum struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// SchemaJSON is the lenient shape we accept for fingerprinting. It tolerates
// missing/extra fields so a user-hand-edited schema.json still works.
type SchemaJSON struct {
	Enums  []SchemaEnum  `json:"enums"`
	Tables []SchemaTable `json:"tables"`
}

// CanonicalSchemaJSON returns the bytes that feed the SHA-256 hasher. Kept
// exported so tests can inspect it.
func CanonicalSchemaJSON(schema SchemaJSON) ([]byte, error) {
	out := fpSchemaOut{
		Enums:  make([]fpEnumOut, len(schema.Enums)),
		Tables: make([]fpTableOut, len(schema.Tables)),
	}

	for i, e := range schema.Enums {
		values := append([]string(nil), e.Values...)
		sort.Strings(values)
		out.Enums[i] = fpEnumOut{Name: e.Name, Values: values}
	}
	sort.Slice(out.Enums, func(i, j int) bool { return out.Enums[i].Name < out.Enums[j].Name })

	for ti, t := range schema.Tables {
		cols := make([]fpColumnOut, len(t.Columns))
		for ci, c := range t.Columns {
			nullable := false
			if c.Nullable != nil {
				nullable = *c.Nullable
			}
			isPrimary := false
			if c.IsPrimary != nil {
				isPrimary = *c.IsPrimary
			}
			isUnique := false
			if c.IsUnique != nil {
				isUnique = *c.IsUnique
			}

			var def *string
			if s, ok := rawToNullableString(c.Default); ok {
				def = &s
			}
			var enm *string
			if s, ok := rawToNullableString(c.Enum); ok {
				enm = &s
			}
			var fk *fpForeignKeyOut
			if c.ForeignKey != nil {
				fk = &fpForeignKeyOut{Table: c.ForeignKey.Table, Column: c.ForeignKey.Column}
			}

			cols[ci] = fpColumnOut{
				Name:       c.Name,
				Type:       c.Type,
				Nullable:   nullable,
				IsPrimary:  isPrimary,
				IsUnique:   isUnique,
				Default:    def,
				ForeignKey: fk,
				Enum:       enm,
			}
		}
		sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })
		out.Tables[ti] = fpTableOut{Name: t.Name, Columns: cols}
	}
	sort.Slice(out.Tables, func(i, j int) bool { return out.Tables[i].Name < out.Tables[j].Name })

	// JS `JSON.stringify` does not escape `<`, `>`, `&`. Match that by disabling
	// Go's default HTML-safe escaping.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// rawToNullableString returns (value, true) if the raw JSON is a string,
// ("", false) if it's null/absent. TS `default?: string | null` collapses
// both cases to the JSON literal `null`, so we treat them identically.
func rawToNullableString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, true
	}
	return "", false
}

// FingerprintSchema returns the SHA-256 hex digest of the canonical form.
func FingerprintSchema(schema SchemaJSON) (string, error) {
	buf, err := CanonicalSchemaJSON(schema)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), nil
}

// FingerprintSchemaFile reads a schema.json from disk and returns its
// fingerprint. Returns a friendlier error on missing or malformed files.
func FingerprintSchemaFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading schema %q: %w", path, err)
	}
	var parsed SchemaJSON
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", fmt.Errorf("parsing %q: %w", path, err)
	}
	if len(parsed.Tables) == 0 {
		return "", fmt.Errorf("%q has no tables — cannot fingerprint", path)
	}
	return FingerprintSchema(parsed)
}

// FingerprintShortLen is the number of hex chars used as a short, human-facing
// id for a schema — folder names on disk, dashboard fallback label, and the
// value accepted by `--schema-id` in the CLI.
const FingerprintShortLen = 12

// FingerprintShort returns the first FingerprintShortLen hex characters of a
// fingerprint (or the whole string if shorter).
func FingerprintShort(fp string) string {
	if len(fp) < FingerprintShortLen {
		return fp
	}
	return fp[:FingerprintShortLen]
}
