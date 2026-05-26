package sqlcontract

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		populated []string
		// wantOK true means Validate returns nil. wantOK false means
		// the error message must mention every name in `wantTables`.
		wantOK     bool
		wantTables []string
	}{
		{
			name: "truncate then insert passes",
			sql: `TRUNCATE TABLE products RESTART IDENTITY CASCADE;
			      INSERT INTO products (id, name) VALUES (1, 'a');`,
			populated: []string{"products"},
			wantOK:    true,
		},
		{
			name: "delete from no where then insert passes",
			sql: `DELETE FROM products;
			      INSERT INTO products (id, name) VALUES (1, 'a');`,
			populated: []string{"products"},
			wantOK:    true,
		},
		{
			name:       "insert only fails and names the table",
			sql:        `INSERT INTO products (id, name) VALUES (1, 'a');`,
			populated:  []string{"products"},
			wantOK:     false,
			wantTables: []string{"products"},
		},
		{
			name: "delete with where is not a wipe",
			sql: `DELETE FROM products WHERE id < 100;
			      INSERT INTO products (id, name) VALUES (1, 'a');`,
			populated:  []string{"products"},
			wantOK:     false,
			wantTables: []string{"products"},
		},
		{
			name: "commented truncate does not satisfy",
			sql: `-- TRUNCATE products
			      INSERT INTO products (id, name) VALUES (1, 'a');`,
			populated:  []string{"products"},
			wantOK:     false,
			wantTables: []string{"products"},
		},
		{
			name: "block commented truncate does not satisfy",
			sql: `/* TRUNCATE products */
			      INSERT INTO products (id, name) VALUES (1, 'a');`,
			populated:  []string{"products"},
			wantOK:     false,
			wantTables: []string{"products"},
		},
		{
			name: "schema-qualified table in sql matches unqualified populated entry",
			sql: `TRUNCATE TABLE public.products RESTART IDENTITY CASCADE;
			      INSERT INTO public.products (id, name) VALUES (1, 'a');`,
			populated: []string{"products"},
			wantOK:    true,
		},
		{
			name: "case-insensitive table match",
			sql: `truncate table Products;
			      insert into PRODUCTS (id) values (1);`,
			populated: []string{"products"},
			wantOK:    true,
		},
		{
			name: "multi-table truncate covers all listed",
			sql: `TRUNCATE TABLE products, brands RESTART IDENTITY CASCADE;
			      INSERT INTO products (id) VALUES (1);
			      INSERT INTO brands (id) VALUES (1);`,
			populated: []string{"products", "brands"},
			wantOK:    true,
		},
		{
			name: "one of many tables missing wipe",
			sql: `TRUNCATE TABLE products RESTART IDENTITY CASCADE;
			      INSERT INTO products (id) VALUES (1);
			      INSERT INTO brands (id) VALUES (1);`,
			populated:  []string{"products", "brands"},
			wantOK:     false,
			wantTables: []string{"brands"},
		},
		{
			name: "populated table missing from sql entirely is flagged",
			sql: `TRUNCATE TABLE products;
			      INSERT INTO products (id) VALUES (1);`,
			populated:  []string{"products", "brands"},
			wantOK:     false,
			wantTables: []string{"brands"},
		},
		{
			name:      "empty populated set is a no-op",
			sql:       `INSERT INTO products (id) VALUES (1);`,
			populated: nil,
			wantOK:    true,
		},
		{
			name:      "empty sql with empty populated passes",
			sql:       ``,
			populated: nil,
			wantOK:    true,
		},
		{
			name: "TRUNCATE ONLY accepted",
			sql: `TRUNCATE ONLY products;
			      INSERT INTO products (id) VALUES (1);`,
			populated: []string{"products"},
			wantOK:    true,
		},
		{
			name: "double-quoted identifier matches",
			sql: `TRUNCATE TABLE "Products";
			      INSERT INTO "Products" (id) VALUES (1);`,
			populated: []string{"Products"},
			wantOK:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.sql, tc.populated)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected nil, got error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			for _, name := range tc.wantTables {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("error %q missing table name %q", err.Error(), name)
				}
			}
		})
	}
}

func TestStripComments(t *testing.T) {
	cases := map[string]string{
		"SELECT 1; -- trailing comment\nSELECT 2;": "SELECT 1; \nSELECT 2;",
		"SELECT /* mid */ 1;":                      "SELECT  1;",
		"-- only comment\nSELECT 1;":               "\nSELECT 1;",
		"no comments here":                         "no comments here",
	}
	for in, want := range cases {
		if got := stripComments(in); got != want {
			t.Errorf("stripComments(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"products":          "products",
		"  products  ":      "products",
		"Products":          "products",
		`"Products"`:        "products",
		"public.products":   "products",
		`"public"."Things"`: "things",
	}
	for in, want := range cases {
		if got := normalize(in); got != want {
			t.Errorf("normalize(%q) = %q, want %q", in, got, want)
		}
	}
}
