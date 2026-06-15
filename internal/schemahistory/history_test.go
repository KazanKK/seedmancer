package schemahistory_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KazanKK/seedmancer/internal/schemahistory"
)

// ─── helpers ────────────────────────────────────────────────────────────────

func tempHistoryPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "history.json")
}

// schemaJSON builds a minimal schema.json blob with the given table names.
func schemaJSON(tables ...string) []byte {
	type col struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	}
	type tbl struct {
		Name    string `json:"name"`
		Columns []col  `json:"columns"`
	}
	type schema struct {
		Tables []tbl `json:"tables"`
		Enums  []any `json:"enums"`
	}
	s := schema{}
	for _, name := range tables {
		s.Tables = append(s.Tables, tbl{
			Name:    name,
			Columns: []col{{Name: "id", Type: "int", Nullable: false}},
		})
	}
	b, _ := json.Marshal(s)
	return b
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
var t1 = time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
var t2 = time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

// noJSONPath always returns a path that does not exist (disables diff computation).
func noJSONPath(fpShort string) string { return "/nonexistent/" + fpShort + "/schema.json" }

// ─── test 1: creates history.json when missing ──────────────────────────────

func TestCreateHistoryWhenMissing(t *testing.T) {
	path := tempHistoryPath(t)
	entry, err := schemahistory.UpdateSchemaHistory(path, "aaabbbcccddd000111222333", noJSONPath, t0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry == nil {
		t.Fatal("expected non-nil entry")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("history.json not created: %v", err)
	}
}

// ─── test 2: first schema gets version 1 with empty PreviousFingerprint ─────

func TestFirstSchemaVersion1(t *testing.T) {
	path := tempHistoryPath(t)
	fp := "aaabbbcccddd000111222333"

	entry, err := schemahistory.UpdateSchemaHistory(path, fp, noJSONPath, t0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry.Version != 1 {
		t.Errorf("expected version 1, got %d", entry.Version)
	}
	if entry.PreviousFingerprint != "" {
		t.Errorf("expected empty PreviousFingerprint, got %q", entry.PreviousFingerprint)
	}
	if entry.Fingerprint != fp {
		t.Errorf("expected fingerprint %q, got %q", fp, entry.Fingerprint)
	}
}

// ─── test 3 & 4: no duplicate; lastSeenAt updated ───────────────────────────

func TestNoDuplicateAndLastSeenAtUpdated(t *testing.T) {
	path := tempHistoryPath(t)
	fp := "aaabbbcccddd000111222333"

	if _, err := schemahistory.UpdateSchemaHistory(path, fp, noJSONPath, t0); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if _, err := schemahistory.UpdateSchemaHistory(path, fp, noJSONPath, t1); err != nil {
		t.Fatalf("second call: %v", err)
	}

	h, err := schemahistory.LoadSchemaHistory(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(h.Schemas) != 1 {
		t.Errorf("expected 1 entry, got %d", len(h.Schemas))
	}
	if !h.Schemas[0].LastSeenAt.Equal(t1) {
		t.Errorf("expected LastSeenAt %v, got %v", t1, h.Schemas[0].LastSeenAt)
	}
	if h.Current != fp {
		t.Errorf("expected Current %q, got %q", fp, h.Current)
	}
}

// ─── test 5: new fingerprint gets next version and stores previousFingerprint ─

func TestNewFingerprintNextVersion(t *testing.T) {
	path := tempHistoryPath(t)
	fp1 := "aaabbbcccddd000111222333"
	fp2 := "zzzyyy000111222333444555"

	if _, err := schemahistory.UpdateSchemaHistory(path, fp1, noJSONPath, t0); err != nil {
		t.Fatalf("first: %v", err)
	}
	entry2, err := schemahistory.UpdateSchemaHistory(path, fp2, noJSONPath, t1)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if entry2.Version != 2 {
		t.Errorf("expected version 2, got %d", entry2.Version)
	}
	if entry2.PreviousFingerprint != fp1 {
		t.Errorf("expected PreviousFingerprint %q, got %q", fp1, entry2.PreviousFingerprint)
	}

	h, _ := schemahistory.LoadSchemaHistory(path)
	if h.Current != fp2 {
		t.Errorf("expected Current %q, got %q", fp2, h.Current)
	}
}

// ─── test 6: VersionsBehind v1→v3 returns (2, true) ─────────────────────────

func TestVersionsBehind_TwoVersions(t *testing.T) {
	fp1 := "aaa000000000000000000001"
	fp2 := "bbb000000000000000000002"
	fp3 := "ccc000000000000000000003"

	h := &schemahistory.SchemaHistory{
		Current: fp3,
		Schemas: []schemahistory.SchemaHistoryEntry{
			{Fingerprint: fp1, Version: 1},
			{Fingerprint: fp2, Version: 2},
			{Fingerprint: fp3, Version: 3},
		},
	}

	n, ok := schemahistory.VersionsBehind(h, fp1, fp3)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if n != 2 {
		t.Errorf("expected 2, got %d", n)
	}
}

// ─── test 7: VersionsBehind current→current returns (0, true) ───────────────

func TestVersionsBehind_SameFingerprint(t *testing.T) {
	fp := "aaa000000000000000000001"
	h := &schemahistory.SchemaHistory{
		Current: fp,
		Schemas: []schemahistory.SchemaHistoryEntry{
			{Fingerprint: fp, Version: 1},
		},
	}

	n, ok := schemahistory.VersionsBehind(h, fp, fp)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
}

// ─── test 8: VersionsBehind unknown fingerprint returns (0, false) ───────────

func TestVersionsBehind_UnknownFingerprint(t *testing.T) {
	fp := "aaa000000000000000000001"
	unknown := "zzz999999999999999999999"
	h := &schemahistory.SchemaHistory{
		Current: fp,
		Schemas: []schemahistory.SchemaHistoryEntry{
			{Fingerprint: fp, Version: 1},
		},
	}

	_, ok := schemahistory.VersionsBehind(h, unknown, fp)
	if ok {
		t.Error("expected ok=false for unknown fingerprint")
	}

	_, ok = schemahistory.VersionsBehind(h, fp, unknown)
	if ok {
		t.Error("expected ok=false for unknown target fingerprint")
	}
}

// ─── test 9: SummarizeSchemaDiff counts changes correctly ────────────────────

func TestSummarizeSchemaDiff(t *testing.T) {
	old := schemaJSON("users", "orders")
	newSchema := schemaJSON("users", "products") // orders removed, products added

	diff, err := schemahistory.SummarizeSchemaDiff(old, newSchema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff.AddedTables != 1 {
		t.Errorf("AddedTables: expected 1, got %d", diff.AddedTables)
	}
	if diff.RemovedTables != 1 {
		t.Errorf("RemovedTables: expected 1, got %d", diff.RemovedTables)
	}
}

func TestSummarizeSchemaDiff_Columns(t *testing.T) {
	// old: users(id, email), new: users(id, name) — email removed, name added
	type col struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	}
	type tbl struct {
		Name    string `json:"name"`
		Columns []col  `json:"columns"`
	}
	type schema struct {
		Tables []tbl `json:"tables"`
		Enums  []any `json:"enums"`
	}

	oldS, _ := json.Marshal(schema{Tables: []tbl{{
		Name:    "users",
		Columns: []col{{Name: "id", Type: "int"}, {Name: "email", Type: "text"}},
	}}})
	newS, _ := json.Marshal(schema{Tables: []tbl{{
		Name:    "users",
		Columns: []col{{Name: "id", Type: "int"}, {Name: "name", Type: "text"}},
	}}})

	diff, err := schemahistory.SummarizeSchemaDiff(oldS, newS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff.AddedColumns != 1 {
		t.Errorf("AddedColumns: expected 1, got %d", diff.AddedColumns)
	}
	if diff.RemovedColumns != 1 {
		t.Errorf("RemovedColumns: expected 1, got %d", diff.RemovedColumns)
	}
}

func TestSummarizeSchemaDiff_ChangedColumn(t *testing.T) {
	type col struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Nullable bool   `json:"nullable"`
	}
	type tbl struct {
		Name    string `json:"name"`
		Columns []col  `json:"columns"`
	}
	type schema struct {
		Tables []tbl `json:"tables"`
		Enums  []any `json:"enums"`
	}

	oldS, _ := json.Marshal(schema{Tables: []tbl{{
		Name:    "users",
		Columns: []col{{Name: "id", Type: "int"}},
	}}})
	newS, _ := json.Marshal(schema{Tables: []tbl{{
		Name:    "users",
		Columns: []col{{Name: "id", Type: "bigint"}},
	}}})

	diff, err := schemahistory.SummarizeSchemaDiff(oldS, newS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff.ChangedColumns != 1 {
		t.Errorf("ChangedColumns: expected 1, got %d", diff.ChangedColumns)
	}
}

// ─── test 10: SchemaDiffSummary.String formats correctly ─────────────────────

func TestSchemaDiffSummaryString(t *testing.T) {
	cases := []struct {
		d    schemahistory.SchemaDiffSummary
		want string
	}{
		{schemahistory.SchemaDiffSummary{}, "-"},
		{schemahistory.SchemaDiffSummary{AddedTables: 1}, "+1 tbl"},
		{schemahistory.SchemaDiffSummary{RemovedTables: 2}, "-2 tbl"},
		{schemahistory.SchemaDiffSummary{AddedColumns: 3}, "+3 col"},
		{schemahistory.SchemaDiffSummary{RemovedColumns: 1}, "-1 col"},
		{schemahistory.SchemaDiffSummary{ChangedColumns: 2}, "~2 col"},
		{schemahistory.SchemaDiffSummary{AddedTables: 1, AddedColumns: 3, RemovedColumns: 1}, "+1 tbl +3 col -1 col"},
	}

	for _, tc := range cases {
		got := tc.d.String()
		if got != tc.want {
			t.Errorf("SchemaDiffSummary%+v.String() = %q, want %q", tc.d, got, tc.want)
		}
	}
}

// ─── bonus: LoadSchemaHistory returns empty history for missing file ──────────

func TestLoadSchemaHistory_MissingFile(t *testing.T) {
	h, err := schemahistory.LoadSchemaHistory("/nonexistent/path/history.json")
	if err != nil {
		t.Fatalf("expected no error for missing file, got: %v", err)
	}
	if h == nil {
		t.Fatal("expected non-nil history")
	}
	if len(h.Schemas) != 0 {
		t.Errorf("expected empty Schemas, got %d entries", len(h.Schemas))
	}
}

// ─── diff with on-disk schema JSON files ─────────────────────────────────────

func TestUpdateSchemaHistory_ComputesDiff(t *testing.T) {
	dir := t.TempDir()
	histPath := filepath.Join(dir, "history.json")

	fp1 := "fp1000000000000000000001"
	fp2 := "fp2000000000000000000002"
	fp1Short := fp1[:12]
	fp2Short := fp2[:12]

	// Write schema JSON files so diff can be computed.
	old := schemaJSON("users")
	newS := schemaJSON("users", "orders")
	_ = os.MkdirAll(filepath.Join(dir, fp1Short), 0755)
	_ = os.MkdirAll(filepath.Join(dir, fp2Short), 0755)
	_ = os.WriteFile(filepath.Join(dir, fp1Short, "schema.json"), old, 0644)
	_ = os.WriteFile(filepath.Join(dir, fp2Short, "schema.json"), newS, 0644)

	schemaJSONFn := func(fpShort string) string {
		return filepath.Join(dir, fpShort, "schema.json")
	}

	if _, err := schemahistory.UpdateSchemaHistory(histPath, fp1, schemaJSONFn, t0); err != nil {
		t.Fatalf("first update: %v", err)
	}
	entry2, err := schemahistory.UpdateSchemaHistory(histPath, fp2, schemaJSONFn, t1)
	if err != nil {
		t.Fatalf("second update: %v", err)
	}

	if entry2.DiffFromPrevious == nil {
		t.Fatal("expected DiffFromPrevious to be set")
	}
	if entry2.DiffFromPrevious.AddedTables != 1 {
		t.Errorf("expected 1 added table, got %d", entry2.DiffFromPrevious.AddedTables)
	}
}
