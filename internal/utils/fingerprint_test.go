package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// The fingerprint MUST stay in lockstep with next/src/utils/schemaFingerprint.ts.
// If this golden ever flips, the TS side must be updated too — otherwise the
// same schema will produce different fingerprints on either side and the server
// will create duplicate rows.

func TestFingerprintSchema_goldenCanonical(t *testing.T) {
	schema := SchemaJSON{
		Tables: []SchemaTable{
			{
				Name: "users",
				Columns: []SchemaColumn{
					{Name: "id", Type: "uuid"},
					{Name: "email", Type: "text"},
				},
			},
		},
	}

	canonical, err := CanonicalSchemaJSON(schema)
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	expected := `{"enums":[],"tables":[{"name":"users","columns":[{"name":"email","type":"text","nullable":false,"isPrimary":false,"isUnique":false,"default":null,"foreignKey":null,"enum":null},{"name":"id","type":"uuid","nullable":false,"isPrimary":false,"isUnique":false,"default":null,"foreignKey":null,"enum":null}]}]}`
	if string(canonical) != expected {
		t.Fatalf("canonical mismatch\ngot:  %s\nwant: %s", canonical, expected)
	}

	sum := sha256.Sum256([]byte(expected))
	wantFP := hex.EncodeToString(sum[:])
	gotFP, err := FingerprintSchema(schema)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	if gotFP != wantFP {
		t.Fatalf("fingerprint mismatch:\ngot:  %s\nwant: %s", gotFP, wantFP)
	}
}

func TestFingerprintSchema_orderIndependence(t *testing.T) {
	a := SchemaJSON{Tables: []SchemaTable{
		{Name: "users", Columns: []SchemaColumn{{Name: "id", Type: "uuid"}}},
		{Name: "orders", Columns: []SchemaColumn{{Name: "id", Type: "uuid"}}},
	}}
	b := SchemaJSON{Tables: []SchemaTable{
		{Name: "orders", Columns: []SchemaColumn{{Name: "id", Type: "uuid"}}},
		{Name: "users", Columns: []SchemaColumn{{Name: "id", Type: "uuid"}}},
	}}

	fa, err := FingerprintSchema(a)
	if err != nil {
		t.Fatal(err)
	}
	fb, err := FingerprintSchema(b)
	if err != nil {
		t.Fatal(err)
	}
	if fa != fb {
		t.Fatalf("fingerprint should not depend on table order: a=%s b=%s", fa, fb)
	}
}

func TestFingerprintShort(t *testing.T) {
	if got := FingerprintShort("abcdef0123456789"); got != "abcdef012345" {
		t.Fatalf("got %q", got)
	}
	if got := FingerprintShort("short"); got != "short" {
		t.Fatalf("short input should pass through, got %q", got)
	}
}
