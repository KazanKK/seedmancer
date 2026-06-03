package mcp

// These constants back the seedmancer://docs/* resources. They live in
// Go source (not embed.FS) so they ship with the binary at zero cost
// and stay diff-reviewable alongside the tool definitions they describe.

const docQuickstart = `# Seedmancer quickstart (for agents)

Seedmancer is a Postgres seeding tool. Test data lives in **scenarios**
(slash-separated paths like ` + "`basic`" + ` or ` + "`billing/pro`" + `). Every export creates
a new immutable **revision** (` + "`r001`" + `, ` + "`r002`" + `, …) under the scenario.
The ` + "`latest`" + ` revision is what gets seeded by default.

## First time in a new project

Run this sequence once to set everything up:

1. ` + "`init_project`" + ` — creates seedmancer.yaml, .seedmancer/, and writes agent
   rule files (.cursor/rules/seedmancer.mdc + CLAUDE.md) so future AI
   conversations in this project automatically use Seedmancer.
2. ` + "`export_database scenario=\"basic\"`" + ` — captures the current schema + data as
   ` + "`basic/r001`" + ` and advances manifest.latest.
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

Seedmancer always uses the latest revision. Use ` + "`--revision rNNN`" + ` to lock onto
a specific revision if needed.

## Schema drift

If the live DB schema changes, ` + "`seed_database`" + ` refuses to load mismatched
revisions unless ` + "`force: true`" + ` is set.

**Quick check:**
- ` + "`check_state_schema scenario=\"<name>\"`" + ` — returns a structured drift report
  with changes classified as auto / likely / decision / breaking.

**Fix drift with the AI refresh workflow:**
1. ` + "`check_state_schema`" + ` — inspect what changed (optional; apply_ai_refresh does this internally too).
2. ` + "`apply_ai_refresh`" + ` — sends the existing data + schema diff to the Seedmancer AI backend,
   which adapts every row to the new schema and commits a new ` + "`rNNN`" + ` revision.
3. ` + "`seed_database`" + ` — loads the fresh revision.

Read ` + "`seedmancer://docs/refresh`" + ` for the full workflow.

**dataset.sql note:** ` + "`dataset.sql`" + ` is NEVER deleted. It stays on every
revision as a permanent AI reference. Revisions created by ` + "`apply_ai_refresh`" + `
include a new ` + "`dataset.sql`" + ` with the AI-generated SQL.

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
    ["seed", "api-test", "--yes"],
    { stdio: "inherit" },
  );
  if (res.status !== 0) throw new Error("seedmancer seed failed");
}
` + "```" + `

### Agents calling this through MCP

Prefer the ` + "`seed_database`" + ` tool over shelling out. Pass:

- ` + "`scenario: \"api-test\"`" + ` (the scenario you keep for tests).
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

## Recommended workflow

` + "`inherit`" + ` is REQUIRED. It seeds a base scenario into the local env before
your SQL runs so the live DB starts in a known state. **The SQL itself must
still be a full, self-contained script** — see the contract below.

1. ` + "`list_datasets`" + ` — confirm a base scenario exists. If not,
   ` + "`export_database scenario=\"basic\"`" + ` to capture the live DB.
2. ` + "`describe_schema`" + ` — get exact table and column names for every table
   you intend to populate.
3. ` + "`generate_dataset_local scenario=\"<new>\" inherit=\"basic\" sql=\"...\"`" + ` —
   write a FULL SQL script (see contract below). The tool rejects partial /
   delta scripts.
4. ` + "`seed_database scenario=\"<new>\" yes=true`" + `.

## SQL contract — FULL, self-contained, idempotent

The SQL block stored in ` + "`dataset.sql`" + ` is the **source of truth** for the
revision. It must satisfy three properties:

1. **Self-contained.** Running the SQL alone against an empty migrated
   schema must reproduce the dataset. Do not assume any rows from the
   inherit base survive — every populated table must be re-populated
   here. The inherit step is just a runtime safety net, not a data
   source the SQL relies on.
2. **Idempotent.** Running the SQL twice must produce the same DB state.
   The validator enforces this by requiring every populated table to
   start with either ` + "`TRUNCATE TABLE <t> RESTART IDENTITY CASCADE`" + `
   or an unconditional ` + "`DELETE FROM <t>`" + ` (no ` + "`WHERE`" + `)
   **before** any ` + "`INSERT INTO <t>`" + `.
3. **DML only.** ` + "`TRUNCATE`" + ` / ` + "`DELETE`" + ` / ` + "`INSERT`" + ` /
   ` + "`UPDATE`" + ` (plus ` + "`SELECT`" + ` for value derivation). **No DDL** —
   the schema is owned by the inherit base / migrations.

Mechanics:
- The whole block runs inside one transaction. Use semicolons to separate
  statements. On failure the DB rolls back to the inherit baseline.
- Reference tables by their unqualified names; the configured local env's
  search_path is used as-is.
- Foreign keys are enforced. Truncate children before parents (or use
  ` + "`TRUNCATE a, b, c CASCADE`" + `), and INSERT parents before children.

## Minimal example (full + idempotent)

` + "```sql" + `
-- One TRUNCATE covers every populated table — CASCADE handles FK order.
TRUNCATE TABLE order_items, product_images, inventory, products, brands
    RESTART IDENTITY CASCADE;

INSERT INTO brands (id, name) VALUES
  (1, 'Acme');

INSERT INTO products (id, brand_id, name, price) VALUES
  (1, 1, 'Product 1', 9.99),
  (2, 1, 'Product 2', 19.98),
  (3, 1, 'Product 3', 29.97);
` + "```" + `

Run this script twice → same final state. Run it against an empty schema
with only your migrations applied → same final state. That's the contract.

Call:

` + "```" + `
generate_dataset_local
  scenario: products/v2
  inherit: basic
  sql: <the SQL above>
` + "```" + `

## Replay & idempotency — why the contract exists

Because ` + "`dataset.sql`" + ` is fully self-contained, you can:
- Hand it to a teammate who runs it directly via psql.
- Diff two revisions' SQL files to see what actually changed.
- Replay it after a schema migration without re-running ` + "`generate_dataset_local`" + `.
- Trust that a re-run of the same revision is a no-op (no PK conflicts).

If the validator rejects your SQL, it lists every populated table that's
missing a leading wipe — add ` + "`TRUNCATE TABLE <t> RESTART IDENTITY CASCADE`" + `
above that table's INSERTs and retry.

## Common pitfalls

- **Wrong column names**: copy names verbatim from ` + "`describe_schema`" + `.
- **INSERT without TRUNCATE/DELETE**: rejected. The script wouldn't be
  replay-safe — re-running it would PK-collide.
- **DELETE with a WHERE clause**: does NOT count as a wipe (it's a delta).
  Use ` + "`TRUNCATE`" + ` or ` + "`DELETE FROM <t>`" + ` with no WHERE.
- **Partial coverage**: if a table ends up with rows but isn't in the SQL,
  the revision relies on the inherit base — that's the old delta model
  and the validator flags it.
- **DDL** (` + "`CREATE TABLE`" + `, ` + "`ALTER TABLE`" + `): not allowed. If the schema needs to
  change, update the live DB and run ` + "`export_database`" + ` to capture a new baseline.

## Editing an existing dataset.sql — rewrite, never patch

Every successful ` + "`generate_dataset_local`" + ` call stores the SQL as
` + "`dataset.sql`" + `. To produce a new revision based on an existing one:

1. ` + "`list_history scenario=<scenario>`" + ` — see existing revisions; rows with
   ` + "`hasSql: true`" + ` were produced by ` + "`generate_dataset_local`" + ` and have a
   retrievable ` + "`dataset.sql`" + `.
2. ` + "`get_dataset_sql scenario=<scenario>`" + ` — returns the SQL block.
3. **Rewrite the SQL as a new full script** that reflects the desired end
   state. Do NOT append new statements to the old SQL hoping to patch the
   state — the result will not be replay-safe and the validator will
   reject it.
4. ` + "`generate_dataset_local scenario=<scenario> inherit=<base> sql=<rewritten>`" + ` —
   creates a new ` + "`rNNN`" + ` revision.
5. ` + "`seed_database scenario=<scenario> yes=true`" + `.

## dataset.sql and refresh-produced revisions

` + "`dataset.sql`" + ` is **never deleted**. It stays on every revision as a permanent
AI reference regardless of how the revision was created.

Revisions created by ` + "`apply_ai_refresh`" + ` include a new ` + "`dataset.sql`" + ` with
the AI-generated SQL that produced the revision. This can be retrieved and
edited for future ` + "`generate_dataset_local`" + ` calls.
`

const docRefresh = `# Schema drift refresh (for agents)

The refresh system updates an outdated scenario revision so its CSVs match
the current database schema. The AI backend adapts existing rows to the new
schema — preserving all data and only modifying what schema drift requires.

## The two-step MCP flow

` + "```" + `
check_state_schema (optional) → apply_ai_refresh
` + "```" + `

### 1. check_state_schema (optional)

Returns a structured drift report with every change classified as:

| Category | Meaning |
|---|---|
| **auto** | Safe — nullable column added, column removed, FK removed |
| **likely** | High-confidence (rename heuristic, type widening) |
| **decision** | Ambiguous — required column without default, FK added |
| **breaking** | PK changed, type incompatible narrowing, table removed |

You can call this first to show the user what changed, but it is not required
before apply_ai_refresh — that tool detects drift internally.

### 2. apply_ai_refresh

Sends the existing data and schema diff to the Seedmancer AI backend, which:
- Reads the existing data (dataset.sql if available, else CSVs converted to SQL)
- Receives the new schema and schema diff
- Produces modified SQL: same rows, new columns filled in, removed columns
  dropped, type-changed values converted
- Runs the SQL against the local database
- Exports updated tables as a new rNNN revision
- Saves the AI SQL as dataset.sql on the new revision
- Advances manifest.latest

**Required fields:** scenario (required), env or dbUrl (required — points at the
local database with the new schema already applied).

**Optional:** prompt — extra context for the AI (e.g. "keep email addresses realistic").

## dataset.sql role

dataset.sql is NEVER deleted and persists on every revision as a permanent
AI reference. Refresh-produced revisions include a new dataset.sql containing
the AI-generated SQL that created the revision.
`
