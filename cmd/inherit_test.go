package cmd

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestFindFKDescendantsTransitive(t *testing.T) {
	idx := fkChildIndex{
		"products":       {"product_images", "inventory", "order_items"},
		"orders":         {"order_items", "order_status_history"},
		"order_items":    {"order_item_options"},
		"product_images": {"image_metadata"},
	}
	got := findFKDescendants(idx, map[string]bool{"products": true})
	want := map[string]bool{
		"products":           true,
		"product_images":     true,
		"inventory":          true,
		"order_items":        true,
		"order_item_options": true,
		"image_metadata":     true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("findFKDescendants(products):\n got=%v\nwant=%v", keys(got), keys(want))
	}
}

func TestFindFKDescendantsNoCycle(t *testing.T) {
	// A → B and B → A would loop; the BFS dedupes via the `out` set so this
	// must terminate.
	idx := fkChildIndex{
		"a": {"b"},
		"b": {"a"},
	}
	got := findFKDescendants(idx, map[string]bool{"a": true})
	if len(got) != 2 || !got["a"] || !got["b"] {
		t.Fatalf("expected {a, b}, got %v", keys(got))
	}
}

func TestBuildFKChildIndexSkipsSelfReferences(t *testing.T) {
	// Self-referencing table (e.g. categories.parent_id → categories.id).
	// The graph must not record a self-edge — clearing the table itself when
	// inheriting would wipe the parent rows the script just wrote.
	dir := t.TempDir()
	schemaJSON := filepath.Join(dir, "schema.json")
	const body = `{"tables":[
		{"name":"categories","columns":[
			{"name":"id","type":"uuid","isPrimary":true},
			{"name":"parent_id","type":"uuid","nullable":true,"foreignKey":{"table":"categories","column":"id"}}
		]},
		{"name":"products","columns":[
			{"name":"id","type":"uuid","isPrimary":true},
			{"name":"category_id","type":"uuid","foreignKey":{"table":"categories","column":"id"}}
		]}
	]}`
	if err := os.WriteFile(schemaJSON, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	idx, err := buildFKChildIndex(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	got := idx["categories"]
	if len(got) != 1 || got[0] != "products" {
		t.Fatalf("expected categories→[products], got %v", got)
	}
}

func TestTruncateCSVToHeader(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "products.csv")
	const before = "id,name,price\n1,foo,100\n2,bar,200\n"
	if err := os.WriteFile(p, []byte(before), 0644); err != nil {
		t.Fatal(err)
	}
	if err := truncateCSVToHeader(p); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "id,name,price\n" {
		t.Fatalf("expected header-only, got %q", string(got))
	}
}

func TestTrimCSVSuffix(t *testing.T) {
	cases := map[string]string{
		"products.csv":   "products",
		"products.CSV":   "products",
		"product_a.csv":  "product_a",
		"products.json":  "",
		"products":       "",
		"":               "",
	}
	for in, want := range cases {
		if got := trimCSVSuffix(in); got != want {
			t.Errorf("trimCSVSuffix(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestSingleFallbackDataset(t *testing.T) {
	cases := []struct {
		name      string
		available []string
		exclude   string
		want      string
	}{
		{"empty list", nil, "", ""},
		{"single match", []string{"baseline"}, "", "baseline"},
		{"single match exclude self", []string{"baseline", "v2"}, "v2", "baseline"},
		{"multiple → no fallback", []string{"a", "b"}, "", ""},
		{"only candidate is excluded", []string{"new"}, "new", ""},
		{"timestamp export", []string{"20260426231303"}, "", "20260426231303"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := singleFallbackDataset(tc.available, tc.exclude); got != tc.want {
				t.Errorf("singleFallbackDataset(%v, %q) = %q; want %q",
					tc.available, tc.exclude, got, tc.want)
			}
		})
	}
}

func TestListLocalDatasetIDs(t *testing.T) {
	dir := t.TempDir()
	datasetsDir := filepath.Join(dir, "datasets")
	if err := os.MkdirAll(filepath.Join(datasetsDir, "baseline"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(datasetsDir, "products-v2"), 0755); err != nil {
		t.Fatal(err)
	}
	// Files at the same level should be ignored.
	if err := os.WriteFile(filepath.Join(datasetsDir, "stray.csv"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	got := listLocalDatasetIDs(dir)
	want := []string{"baseline", "products-v2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("listLocalDatasetIDs = %v; want %v", got, want)
	}

	// Missing datasets dir → empty slice, no error.
	emptyDir := t.TempDir()
	if got := listLocalDatasetIDs(emptyDir); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
