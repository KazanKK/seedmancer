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

## Environment markers (env-specific values)

If a CSV cell should contain a different value per environment (e.g. a
Supabase Auth user ID or org ID that differs between local and staging),
write ` + "`@env:KEY_NAME`" + ` as the value in your SQL INSERT:

` + "```sql" + `
INSERT INTO users (id, email) VALUES ('@env:FIXED_USER_ID', 'test@example.com');
` + "```" + `

Seedmancer saves ` + "`@env:FIXED_USER_ID`" + ` literally to CSV. At seed time it is
replaced with the value from ` + "`environments.<env>.values`" + ` in seedmancer.yaml,
or from the OS environment variable ` + "`FIXED_USER_ID`" + ` (fallback for CI).
The original CSV is never modified.

Configure in seedmancer.yaml:

` + "```yaml" + `
environments:
  local:
    database_url: postgres://localhost:5432/mydb
    values:
      FIXED_USER_ID: "11111111-1111-1111-1111-111111111111"
  staging:
    database_url: postgres://staging-host/mydb
    values:
      FIXED_USER_ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
` + "```" + `

Read ` + "`seedmancer://docs/env-markers`" + ` for the full reference.

## Pinning for CI

Seedmancer always uses the latest revision. Use ` + "`--revision rNNN`" + ` to lock onto
a specific revision if needed.

## Schema drift

If the live DB schema changes, ` + "`seed_database`" + ` refuses to load mismatched
revisions unless ` + "`force: true`" + ` is set.

**Quick check:**
- ` + "`check_state_schema scenario=\"<name>\"`" + ` — returns a structured drift report
  with changes classified as auto / likely / decision / breaking.

**Fix drift locally (MCP path):**
1. ` + "`check_state_schema`" + ` — inspect what changed.
2. ` + "`get_dataset_sql scenario=\"<name>\"`" + ` — retrieve the prior revision's data as a reference.
3. Rewrite the full SQL to match the new schema (add new required columns, drop removed ones, etc.).
4. ` + "`generate_dataset_local`" + ` with the rewritten SQL and the same inherit base.
5. ` + "`seed_database`" + ` — loads the fresh revision.

**Fix drift via CLI (no agent):** ` + "`seedmancer refresh <scenario>`" + ` — cloud AI adapts the data (Pro plan).

Read ` + "`seedmancer://docs/refresh`" + ` for the full local workflow.

**dataset.sql note:** ` + "`dataset.sql`" + ` is NEVER deleted. It stays on every
revision as a permanent reference for the next generation round.

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

Seed the database explicitly inside your test files using Playwright hooks.
Every run starts from the same deterministic snapshot — no hidden setup files.

## Pattern 1 — beforeAll: seed once for the file

Use when tests read but do not mutate the database.

` + "```ts" + `
// tests/api/users.spec.ts
import { spawnSync } from "node:child_process";
import { test, expect } from "@playwright/test";

test.beforeAll(() => {
  const res = spawnSync("seedmancer", ["seed", "myapp/api-test", "--yes"], {
    stdio: "inherit",
  });
  if (res.status !== 0) throw new Error("seedmancer seed failed");
});

test("GET /api/users returns seeded rows", async ({ request }) => {
  const res = await request.get("/api/users");
  expect(res.ok()).toBeTruthy();
});
` + "```" + `

## Pattern 2 — beforeEach: reset before every test

Use when tests write to the database and cannot share state.

` + "```ts" + `
// tests/api/signup.spec.ts
import { spawnSync } from "node:child_process";
import { test, expect } from "@playwright/test";

test.beforeEach(() => {
  const res = spawnSync("seedmancer", ["seed", "myapp/api-test", "--yes"], {
    stdio: "inherit",
  });
  if (res.status !== 0) throw new Error("seedmancer seed failed");
});

test("creates a new user", async ({ request }) => {
  const res = await request.post("/api/users", {
    data: { email: "new@example.com" },
  });
  expect(res.status()).toBe(201);
});
` + "```" + `

## Pattern 3 — different scenario per describe group

Use when different test groups need different starting states.

` + "```ts" + `
// tests/e2e/billing.spec.ts
import { spawnSync } from "node:child_process";
import { test, expect } from "@playwright/test";

test.describe("Pro plan", () => {
  test.beforeAll(() => {
    spawnSync("seedmancer", ["seed", "billing/pro", "--yes"], {
      stdio: "inherit",
    });
  });

  test("shows active subscription", async ({ page }) => {
    await page.goto("/billing");
    await expect(page.getByText("Pro — active")).toBeVisible();
  });
});

test.describe("Free plan", () => {
  test.beforeAll(() => {
    spawnSync("seedmancer", ["seed", "billing/free", "--yes"], {
      stdio: "inherit",
    });
  });

  test("shows upgrade prompt", async ({ page }) => {
    await page.goto("/billing");
    await expect(page.getByText("Upgrade to Pro")).toBeVisible();
  });
});
` + "```" + `

## Agents calling this through MCP

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

## Realistic, compact SQL

Generated data must look like it came from a real production system:
- Real-looking names, emails, companies — never 'Test User 1' or 'foo'.
- Emails matching names (alice.chen@example.com), plausible prices and quantities.
- Dates spread over realistic windows (signups over months, orders after signup).
- Status/enum values skewed like real data — mostly common values, fewer edge cases.

For any table needing more than ~20 rows, put the loop INSIDE the SQL instead
of writing literal VALUES rows — it is shorter, never truncates, and gives
exact row counts:

` + "```sql" + `
INSERT INTO "users" ("id", "name", "email", "status", "created_at")
SELECT
  i,
  (ARRAY['Alice Chen','Bob Patel','Carol Diaz','Dan Kim'])[1 + i % 4]
    || CASE WHEN i > 4 THEN ' ' || i ELSE '' END,
  'user' || i || '@example.com',
  (ARRAY['active','active','active','trial','churned'])[1 + i % 5],
  TIMESTAMP '2026-01-01 09:00:00' - (i || ' hours')::interval
FROM generate_series(1, 200) AS i;
` + "```" + `

Repeat ARRAY elements to skew distributions (the status pool above yields
~60% active). Postgres does the heavy lifting natively, so this works for
any row count — including millions of rows.

**Determinism requirement:** every expression must produce the same values on
every run, or the idempotency contract breaks. Derive values from the series
index ` + "`i`" + `; never use bare ` + "`random()`" + ` or ` + "`gen_random_uuid()`" + `. If pseudo-random
variety genuinely helps, run ` + "`SELECT setseed(0.42);`" + ` immediately after the
TRUNCATE — that makes ` + "`random()`" + ` reproducible. For uuid columns derive
deterministic literals: ` + "`('00000000-0000-0000-0000-' || lpad(i::text, 12, '0'))::uuid`" + `.

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
3. ` + "`generate_dataset_local scenario=\"<new>\" inherit=\"basic\" sql=\"...\" prompt=\"<purpose>\"`" + ` —
   write a FULL SQL script (see contract below). The tool rejects partial /
   delta scripts. Pass ` + "`prompt`" + ` with the user's natural-language purpose for
   this test data — it is saved on the scenario and reused by later refreshes
   and regenerations to keep the data's intent.
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

## dataset.sql

` + "`dataset.sql`" + ` is **never deleted**. It stays on every revision as a permanent
reference regardless of how the revision was created. Use ` + "`get_dataset_sql`" + ` to
retrieve it as the starting point for the next generation round.
`

const docRefresh = `# Schema drift — local fix workflow (for agents)

When the live database schema changes, existing scenario revisions become
mismatched. ` + "`seed_database`" + ` refuses to load them unless ` + "`force: true`" + ` is set.
This document explains how to fix drift entirely through local MCP tools.

## The local drift-fix flow

` + "```" + `
check_state_schema → get_dataset_sql → rewrite SQL → generate_dataset_local → seed_database
` + "```" + `

### 1. check_state_schema

Returns a structured drift report with every change classified as:

| Category | Meaning |
|---|---|
| **auto** | Safe — nullable column added, column removed, FK removed |
| **likely** | High-confidence (rename heuristic, type widening) |
| **decision** | Ambiguous — required column without default, FK added |
| **breaking** | PK changed, type incompatible narrowing, table removed |

Call this first to understand exactly what changed before rewriting the data.

### 2. get_dataset_sql

Retrieves the prior revision's SQL block as a reference, along with the
scenario's saved purpose. The schema has changed since this SQL was written,
so use it to understand the shape of the existing data — do NOT re-submit it
as-is.

### 3. Rewrite the SQL

Write a fresh, FULL, idempotent SQL script that conforms to the new schema:
- Add values for any new required columns.
- Drop any columns that no longer exist.
- Convert any type-changed values.
- Keep the data true to the scenario's saved purpose.
- Keep TRUNCATE + INSERT structure intact (see ` + "`seedmancer://docs/local-generation`" + `).

### 4. generate_dataset_local

Call with the rewritten SQL and the same ` + "`inherit`" + ` base as before.
Seedmancer seeds the base, runs your SQL, and creates a new ` + "`rNNN`" + ` revision.
The schema fingerprint on the new revision matches the live DB.

### 5. seed_database

Loads the fresh revision into the target environment.

## CLI alternative (no agent)

` + "`seedmancer refresh <scenario>`" + ` uses the Seedmancer cloud AI to adapt the data
automatically (requires a Pro plan). Use this when no local AI agent is
available. It requires a revision with a saved ` + "`dataset.sql`" + ` (` + "`hasSql: true`" + ` in
` + "`list_history`" + `) — plain exported snapshots cannot be refreshed and should be
re-exported after the migration instead.

## dataset.sql

` + "`dataset.sql`" + ` is never deleted and stays on every revision as a permanent
reference. The rewritten SQL from step 3 becomes the new ` + "`dataset.sql`" + ` after
` + "`generate_dataset_local`" + ` completes.
`

const docEnvMarkers = `# Environment markers

Environment markers let a single CSV dataset work across multiple environments
where certain IDs differ — for example, a Supabase Auth user ID that is
different between local and staging.

## Marker syntax

` + "```" + `
@env:KEY_NAME
` + "```" + `

Rules:
- Must be the **entire cell value** — partial interpolation is not supported.
- ` + "`KEY_NAME`" + ` must use uppercase letters, digits, and underscores only.
- Examples: ` + "`@env:FIXED_USER_ID`" + `, ` + "`@env:ADMIN_USER_ID`" + `, ` + "`@env:ORG_ID_1`" + `

## How to use markers in generated data

When writing SQL for ` + "`generate_dataset_local`" + `, use the marker as the value:

` + "```sql" + `
TRUNCATE TABLE users RESTART IDENTITY CASCADE;
INSERT INTO users (id, email, name)
VALUES ('@env:FIXED_USER_ID', 'test@example.com', 'Test User');
` + "```" + `

Seedmancer saves the literal string ` + "`@env:FIXED_USER_ID`" + ` to CSV. The marker
is replaced at seed time — not at generate time.

## Config

Add a ` + "`values:`" + ` map to each environment in seedmancer.yaml:

` + "```yaml" + `
environments:
  local:
    database_url: postgres://localhost:5432/mydb
    values:
      FIXED_USER_ID: "11111111-1111-1111-1111-111111111111"
      ADMIN_USER_ID: "22222222-2222-2222-2222-222222222222"

  staging:
    database_url: postgres://staging-host/mydb
    values:
      FIXED_USER_ID: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
      ADMIN_USER_ID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
` + "```" + `

## Value lookup order

For each ` + "`@env:KEY`" + ` marker, Seedmancer checks in this order:

1. ` + "`environments.<env>.values.KEY`" + ` in seedmancer.yaml
2. OS environment variable ` + "`KEY`" + ` — fallback for CI / values you do not want committed
3. Hard error if neither is set

The yaml value always wins when both are present.

## Seeding with markers

` + "```bash" + `
seedmancer seed base --env local    # resolves from local values
seedmancer seed base --env staging  # resolves from staging values
seedmancer seed base --env local,staging  # each env resolves its own values
` + "```" + `

Via MCP:
` + "```" + `
seed_database(scenario="base", env="staging", yes=true)
` + "```" + `

## Missing value error

If a marker key is missing from both the yaml config and OS env, seeding fails
with a clear error:

` + "```" + `
Missing environment value: FIXED_USER_ID

Environment: staging
Marker:      @env:FIXED_USER_ID
File:        users.csv
Column:      id

Add it to your seedmancer config:

  environments:
    staging:
      values:
        FIXED_USER_ID: "..."

Or export it as an environment variable before running seed:

  export FIXED_USER_ID="..."
` + "```" + `

## Safety

- The original revision CSVs are **never modified**.
- Replacement happens only in a temporary per-env directory that is deleted
  after seeding completes.
- Markers in column headers are ignored (replacement only applies to data rows).
`
