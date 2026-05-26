package mcp

// These constants back the seedmancer://docs/* resources. They live in
// Go source (not embed.FS) so they ship with the binary at zero cost
// and stay diff-reviewable alongside the tool definitions they describe.

const docQuickstart = `# Seedmancer quickstart (for agents)

Seedmancer is a Postgres seeding tool. Test data lives in **scenarios**
(slash-separated paths like ` + "`basic`" + ` or ` + "`billing/pro`" + `). Every export creates
a new immutable **revision** (` + "`r001`" + `, ` + "`r002`" + `, …) under the scenario.
Pointers ` + "`latest`" + ` and ` + "`stable`" + ` decide which revision a seed loads.

## First time in a new project

Run this sequence once to set everything up:

1. ` + "`init_project`" + ` — creates seedmancer.yaml, .seedmancer/, and writes agent
   rule files (.cursor/rules/seedmancer.mdc + CLAUDE.md) so future AI
   conversations in this project automatically use Seedmancer.
2. ` + "`export_database scenario=\"basic\"`" + ` — captures the current schema + data as
   ` + "`basic/r001`" + ` and advances pointers.latest.
3. ` + "`install_agent_rules`" + ` — if adopting Seedmancer in an existing project that
   was not created with init_project, run this to write the rule files manually.

## Typical loop (project already set up)

1. ` + "`list_datasets`" + ` — see existing scenarios + their pointers.
   - **If no scenarios exist**: call ` + "`export_database`" + ` first with a scenario
     name. The database is already running (configured in seedmancer.yaml).
2. ` + "`describe_schema`" + ` — get the exact table and column names.
3. ` + "`generate_dataset_local scenario=\"<new>\" inherit=\"<base>\" sql=\"...\"`" + ` —
   write SQL that mutates only the rows you want to change. Seedmancer seeds
   the inherit base into the configured local env first, runs your SQL in a
   transaction, then snapshots the result. Read seedmancer://docs/local-generation
   for the SQL contract.
4. ` + "`seed_database scenario=\"<new>\" yes=true`" + ` — loads the latest revision
   into the target env.
5. Run the actual test command outside MCP (e.g. Playwright, pytest).

## Pinning for CI

Once a revision is known-good, pin it as stable:

1. ` + "`pin_scenario scenario=\"basic\"`" + ` — points pointers.stable at the current latest.
2. CI runs ` + "`seed_database scenario=\"basic\" useStable=true`" + ` to lock onto
   that revision regardless of newer exports.

## Schema drift

If the live DB schema changes, ` + "`seed_database`" + ` refuses to load mismatched
revisions unless ` + "`force: true`" + ` is set. Use ` + "`check_scenario`" + ` to inspect
the diff and ` + "`export_database`" + ` again to create a new compatible revision.

## Need new data?

1. ` + "`list_history scenario=\"<scenario>\"`" + ` — see existing revisions. Rows
   with ` + "`hasSql: true`" + ` have a saved ` + "`dataset.sql`" + ` you can edit.
2. ` + "`get_dataset_sql scenario=\"<scenario>\"`" + ` — retrieve the SQL block from
   the latest revision (or pass ` + "`revision: \"rNNN\"`" + ` for a specific one) and
   **modify it** instead of writing new SQL from scratch.
3. ` + "`generate_dataset_local`" + ` with the modified SQL — creates a fresh
   revision under the same scenario.
4. ` + "`push_dataset scenario=\"<scenario>\"`" + ` — optionally publish the latest
   revision to your cloud account.

## Safety rails already enforced

- Tools never prompt. ` + "`seed_database`" + ` refuses to touch prod-like env
  names (prod, production, live, main, master) unless ` + "`yes: true`" + ` is set.
- All destructive tools carry the MCP ` + "`destructiveHint`" + `, so hosts can
  surface confirmation UI.
- Logs are written to --log-file, never stdout. stdio stays pristine.
`

const docPlaywrightRecipe = `# Reset DB before Playwright

Wire Seedmancer into Playwright's ` + "`globalSetup`" + ` so every run starts
from a known dataset.

### playwright.config.ts

` + "```ts" + `
import { defineConfig } from "@playwright/test";
export default defineConfig({
  globalSetup: "./tests/global-setup.ts",
  // …
});
` + "```" + `

### tests/global-setup.ts

` + "```ts" + `
import { spawnSync } from "node:child_process";

export default async function globalSetup() {
  if (process.env.SEEDMANCER_RESET_DATABASE === "false") return;
  const res = spawnSync(
    "seedmancer",
    ["seed", "api-test", "--stable", "--yes"],
    { stdio: "inherit" },
  );
  if (res.status !== 0) throw new Error("seedmancer seed failed");
}
` + "```" + `

### Agents calling this through MCP

Prefer the ` + "`seed_database`" + ` tool over shelling out. Pass:

- ` + "`scenario: \"api-test\"`" + ` (the scenario you keep for tests).
- ` + "`useStable: true`" + ` to load the pinned revision (CI should never seed an
  un-pinned scenario).
- ` + "`yes: true`" + ` so the prod-guard is acknowledged but not bypassed.

On success the tool returns ` + "`anyError: false`" + `; only then should the
agent kick off the test command.
`

const docLocalGeneration = `# Local dataset generation

## When to use local generation

Use ` + "`generate_dataset_local`" + ` when:
- You want deterministic, hand-crafted test data with precise values.
- You are offline or prefer not to use cloud APIs.
- You need any row count — generation runs entirely against your local DB.

## How it works

1. Seedmancer **seeds the inherit base** into the configured local env (CSV → COPY,
   same path as ` + "`seed_database`" + `). This puts the DB into a known starting state.
2. Your **SQL block is executed** against that state inside a single transaction.
   On failure the DB is rolled back to the inherit baseline so you can retry.
3. The resulting tables are **exported back to CSV** as a brand-new ` + "`rNNN`" + ` revision.
4. The raw SQL is saved as ` + "`dataset.sql`" + ` next to the CSVs so ` + "`get_dataset_sql`" + `
   can retrieve it later for incremental edits.

**Note:** this overwrites data in the configured local env (the SQL runs against
it). Fine for dev/test DBs; never use against a DB whose state you care about.

## Performance note for large datasets

` + "`generate_dataset_local`" + ` works for any row count. For very large tables
prefer set-based SQL (` + "`INSERT INTO ... SELECT FROM generate_series(...)`" + `)
so Postgres does the heavy lifting natively.

## Guidance on what Seedmancer should NOT be

- **Production-shape anonymisation pipelines.** Use a dedicated masking tool.
- **One-off "I just need to insert this row" hacks against a live DB.** Use plain SQL
  or your ORM seeders.

## Recommended workflow: ` + "`inherit`" + ` from a base scenario

` + "`inherit`" + ` is REQUIRED. It names the base scenario whose latest revision is
seeded into the local env before your SQL runs. Your SQL only has to express
the delta — anything you don't touch stays as the inherit base had it.

1. ` + "`list_datasets`" + ` — confirm a base scenario exists. If not,
   ` + "`export_database scenario=\"basic\"`" + ` to capture the live DB.
2. ` + "`describe_schema`" + ` — get exact table and column names.
3. ` + "`generate_dataset_local scenario=\"<new>\" inherit=\"basic\" sql=\"...\"`" + ` —
   write SQL that mutates only the rows you actually want to change.
4. ` + "`seed_database scenario=\"<new>\" yes=true`" + `.

## SQL contract

- DML only: ` + "`INSERT`" + ` / ` + "`UPDATE`" + ` / ` + "`DELETE`" + ` (plus ` + "`SELECT`" + ` if you want to
  build values dynamically). **No DDL** — the schema is owned by the inherit base.
- The whole block runs inside one transaction. Use semicolons to separate statements.
- Reference tables by their unqualified names; the configured local env's search_path
  is used as-is.
- Foreign keys are enforced. Delete child rows before deleting parents (or order
  ` + "`INSERT`" + ` so parent ids exist before child rows reference them).

## Minimal example (with inherit)

The SQL below replaces the ` + "`products`" + ` table while leaving the inherited
` + "`brands`" + ` and ` + "`categories`" + ` rows untouched.

` + "```sql" + `
-- inherit: basic (already seeded into the local env)
-- We're rewriting products, so clear out the rows that FK to it first.
DELETE FROM order_items WHERE product_id IN (SELECT id FROM products);
DELETE FROM product_images WHERE product_id IN (SELECT id FROM products);
DELETE FROM inventory WHERE product_id IN (SELECT id FROM products);
DELETE FROM products;

INSERT INTO products (id, brand_id, name, price) VALUES
  (1, 1, 'Product 1', 9.99),
  (2, 1, 'Product 2', 19.98),
  (3, 1, 'Product 3', 29.97);
` + "```" + `

Call:

` + "```" + `
generate_dataset_local
  scenario: products/v2
  inherit: basic
  sql: <the SQL above>
` + "```" + `

The exported revision contains every table the DB currently has — ` + "`brands`" + `
and ` + "`categories`" + ` inherited from basic, the new ` + "`products`" + `, and the now-empty
` + "`product_images`" + `/` + "`inventory`" + `/` + "`order_items`" + ` you cleared.

## Common pitfalls

- **Wrong column names**: copy names verbatim from ` + "`describe_schema`" + `.
- **Missing FK cleanup**: deleting parent rows fails (or orphans child rows) when
  child tables still reference them. Delete children first, or use ` + "`ON DELETE CASCADE`" + `.
- **Forgetting ` + "`inherit`" + `**: the tool refuses to run without it. Always pass
  ` + "`inherit: \"<base-scenario>\"`" + `.
- **DDL** (` + "`CREATE TABLE`" + `, ` + "`ALTER TABLE`" + `): not allowed. If the schema needs to
  change, update the live DB and run ` + "`export_database`" + ` to capture a new baseline.

## Incremental edits to existing generated data

Every time ` + "`generate_dataset_local`" + ` succeeds, the SQL is stored as ` + "`dataset.sql`" + `
inside the revision. Before writing a SQL block from scratch:

1. ` + "`list_history scenario=<scenario>`" + ` — see existing revisions; rows with
   ` + "`hasSql: true`" + ` were produced by ` + "`generate_dataset_local`" + ` and have a
   retrievable ` + "`dataset.sql`" + `.
2. ` + "`get_dataset_sql scenario=<scenario>`" + ` — returns the SQL block (defaults to
   the latest revision; pass ` + "`revision: \"rNNN\"`" + ` for a specific one).
3. Modify the SQL.
4. ` + "`generate_dataset_local scenario=<scenario> inherit=<base> sql=<modified>`" + ` —
   creates a new ` + "`rNNN`" + ` revision.
5. ` + "`seed_database scenario=<scenario> yes=true`" + `.
`
