// Package sqlcontract validates that an agent-written SQL block satisfies
// Seedmancer's "full, self-contained, idempotent" contract for
// generate_dataset_local.
//
// The contract: every table that ends up with rows in the exported
// revision must appear in the SQL as a wipe (TRUNCATE or unconditional
// DELETE FROM) followed by INSERTs. That guarantees two properties at
// once:
//
//   - **Replay-safety**: running the saved dataset.sql twice produces the
//     same DB state (the wipe clears whatever the previous run wrote).
//   - **Self-contained**: the SQL alone, run against an empty migrated
//     schema, reproduces the entire dataset — no inherit step needed at
//     replay time.
//
// The check is intentionally a single-pass, regex-based scan rather than
// a full SQL parser. The contract only needs three statement shapes
// (TRUNCATE / DELETE FROM / INSERT INTO); anything else is opaque and
// passes through. Agents that hand-roll something exotic (CTE-driven
// inserts, dynamic SQL, COPY) bypass the check, which we accept — the
// validator is here to catch the most common failure mode, not to
// prove the SQL idempotent in general.
package sqlcontract

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Validate returns an error when sql does not satisfy the full +
// idempotent contract for the given populated tables.
//
// `populatedTables` is the set of unqualified table names that have
// at least one row in the exported revision (typically the keys of
// the runner's rowCounts map whose value is > 0). Tables that ended
// up empty are skipped — there's nothing to make idempotent.
//
// An empty populatedTables (or all-empty rowCounts) is a no-op: there
// is no data to protect from a non-idempotent INSERT.
func Validate(sql string, populatedTables []string) error {
	if len(populatedTables) == 0 {
		return nil
	}

	cleaned := stripComments(sql)
	wipes := findWipedTables(cleaned)
	inserts := findInsertedTables(cleaned)

	missing := make([]string, 0)
	for _, t := range populatedTables {
		key := normalize(t)
		// If the agent never inserted into this table at all, there
		// is no INSERT to gate. The data must have come from the
		// inherit base; replay won't reproduce it from this SQL,
		// which is a separate concern handled by docs ("write a full
		// script that covers every populated table"). We still flag
		// it so the agent knows the script is incomplete.
		if !inserts[key] {
			missing = append(missing, t)
			continue
		}
		if !wipes[key] {
			missing = append(missing, t)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	sort.Strings(missing)
	return fmt.Errorf(
		"tables missing a leading wipe (TRUNCATE or unconditional DELETE FROM) "+
			"before INSERT: %s — add `TRUNCATE TABLE <t> RESTART IDENTITY CASCADE;` "+
			"before each table's INSERTs so the script is replay-safe and self-contained",
		strings.Join(missing, ", "),
	)
}

// normalize maps a possibly schema-qualified, possibly quoted table
// identifier to a comparison key. We lowercase (Postgres is
// case-insensitive for unquoted identifiers) and drop the schema
// prefix so `public.products` matches `products`.
func normalize(name string) string {
	name = strings.TrimSpace(name)
	name = strings.Trim(name, `"`)
	name = strings.ToLower(name)
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
		name = strings.Trim(name, `"`)
	}
	return name
}

// stripComments removes `-- line` and `/* block */` comments from sql.
// Stays string-based so it stays cheap; doesn't try to be clever about
// comments inside string literals (rare in agent-written DML and the
// resulting false-positive can only make the validator more permissive,
// never more restrictive).
func stripComments(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	for i := 0; i < len(sql); {
		if i+1 < len(sql) && sql[i] == '-' && sql[i+1] == '-' {
			// Skip to end-of-line.
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				break
			}
			i += j
			continue
		}
		if i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*' {
			j := strings.Index(sql[i+2:], "*/")
			if j < 0 {
				break
			}
			i += j + 4
			continue
		}
		b.WriteByte(sql[i])
		i++
	}
	return b.String()
}

// Pre-compiled regexes. The identifier shape is intentionally
// permissive: an optional schema, an optional double-quote, then the
// usual ASCII letter+digit+underscore set Postgres allows. Anything
// fancier (Unicode, escaped quotes) is rare in agent-authored DML.
var (
	identifier = `(?:[A-Za-z_][A-Za-z0-9_]*|"[^"]+")`
	tableRef   = `(?:` + identifier + `\.)?` + identifier

	truncateRe = regexp.MustCompile(`(?i)\bTRUNCATE\s+(?:TABLE\s+)?(?:ONLY\s+)?(` + tableRef + `(?:\s*,\s*` + tableRef + `)*)`)
	deleteRe   = regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+(?:ONLY\s+)?(` + tableRef + `)([^;]*)`)
	insertRe   = regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+(?:ONLY\s+)?(` + tableRef + `)`)
	whereRe    = regexp.MustCompile(`(?i)\bWHERE\b`)
)

// findWipedTables returns the set of tables whose data is unconditionally
// cleared somewhere in sql. A `DELETE FROM t` with a `WHERE` clause does
// NOT count — it's a delta, not a wipe.
func findWipedTables(sql string) map[string]bool {
	out := map[string]bool{}

	for _, m := range truncateRe.FindAllStringSubmatch(sql, -1) {
		// TRUNCATE supports a comma-separated list: TRUNCATE a, b, c.
		for _, raw := range strings.Split(m[1], ",") {
			out[normalize(raw)] = true
		}
	}

	for _, m := range deleteRe.FindAllStringSubmatch(sql, -1) {
		tail := m[2]
		// Trim a trailing RETURNING clause / cast-style noise before
		// checking for WHERE. Cheap, safe, no parser required.
		if whereRe.MatchString(tail) {
			continue
		}
		out[normalize(m[1])] = true
	}

	return out
}

// findInsertedTables returns the set of tables that have at least one
// INSERT INTO statement in sql.
func findInsertedTables(sql string) map[string]bool {
	out := map[string]bool{}
	for _, m := range insertRe.FindAllStringSubmatch(sql, -1) {
		out[normalize(m[1])] = true
	}
	return out
}
