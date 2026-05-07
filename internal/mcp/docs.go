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
3. ` + "`generate_dataset_local scenario=\"<new>\" inherit=\"<base>\"`" + ` — write a Go
   script that produces CSVs locally. Inherit pre-fills the new revision from
   the base scenario; the script overwrites only the table(s) it cares about.
   Read seedmancer://docs/local-generation for the contract.
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

1. ` + "`list_history scenario=\"<scenario>\"`" + ` — see existing revisions.
2. ` + "`describe_dataset`" + ` — check if an existing dataset has ` + "`hasGeneratorScript: true`" + `.
   If so, use ` + "`get_dataset_script`" + ` to retrieve the source and **modify it** instead
   of writing a new script from scratch.
3. ` + "`generate_dataset_local`" + ` with the modified script — creates a fresh
   revision under the same scenario.
4. OR ` + "`generate_dataset`" + ` with a prompt — uses the Seedmancer cloud service.
5. ` + "`push_dataset scenario=\"<scenario>\"`" + ` — optionally publish the latest
   revision to your cloud account.

## Safety rails already enforced

- Tools never prompt. ` + "`seed_database`" + ` refuses to touch prod-like env
  names (prod, production, live, main, master) unless ` + "`yes: true`" + ` is set.
- All destructive tools carry the MCP ` + "`destructiveHint`" + `, so hosts can
  surface confirmation UI.
- Logs are written to --log-file, never stdout. stdio stays pristine.

## 3rd-party service connectors (Pro)

` + "`seed_database`" + ` and ` + "`export_database`" + ` also drive service connectors when they
are configured in ` + "`seedmancer.yaml`" + `. The available connector is:

- **Supabase Auth** — snapshots and restores ` + "`auth.users`" + ` via the Admin API,
  preserving original UUIDs so Postgres FK links stay valid.
  Read ` + "`seedmancer://docs/supabase-auth-connector`" + ` for the full reference.

Use ` + "`list_services`" + ` to see which connectors are active, and
` + "`export_service`" + ` / ` + "`seed_service`" + ` to operate them independently.
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

const docLocalGeneration = `# Local dataset generation (no cloud)

## When to use this instead of ` + "`generate_dataset`" + `

Use ` + "`generate_dataset_local`" + ` when:
- You have no Seedmancer account or have exhausted your monthly quota.
- You are offline or the cloud service is unreachable.
- You want deterministic, hand-crafted test data with precise values.

Use ` + "`generate_dataset`" + ` when you want the cloud service to fabricate a large,
realistic dataset from a short natural-language description.

## Performance note for large datasets

` + "`generate_dataset_local`" + ` works for any row count. For very large tables
(hundreds of thousands of rows) the Go generator runs in memory and writes CSV
files, which is efficient for insertion-later workflows but not streaming.
If the user asks for 1M+ rows and speed is the priority, mention that the seed
step will take time proportional to index rebuilding on the target DB.

## Guidance on what Seedmancer should NOT be

- **Production-shape anonymisation pipelines.** Use a dedicated masking tool.
- **One-off "I just need to insert this row" hacks.** Use plain SQL or your
  ORM seeders.

## Recommended workflow: ` + "`inherit`" + ` from a base scenario

The ` + "`inherit`" + ` parameter is the safe default for any partial generation.
It pre-fills the new revision with all CSVs from the base scenario's latest
revision, lets your script overwrite only the tables it cares about, and
**automatically clears any descendant table that FKs to an overwritten table**
so seeding can never produce orphan foreign keys.

1. ` + "`list_datasets`" + ` — confirm a base scenario exists. If not,
   ` + "`export_database scenario=\"basic\"`" + ` to capture the live DB.
2. ` + "`describe_schema`" + ` — get exact table and column names.
3. ` + "`generate_dataset_local scenario=\"<new>\" inherit=\"basic\"`" + ` — write a
   tiny script that only emits the table(s) you actually want to change.
4. ` + "`seed_database scenario=\"<new>\" yes=true`" + `.

**A single inherit call produces a complete, seedable revision** — no manual
merge step.

## Go script contract

- ` + "`package main`" + `, stdlib only (` + "`encoding/csv`" + `, ` + "`fmt`" + `, ` + "`os`" + `, ` + "`math/rand`" + `, ` + "`time`" + `, …).
- No ` + "`go.mod`" + ` needed — the script is interpreted by the embedded Go engine inside the Seedmancer binary. **No Go toolchain needs to be installed.**
- Output directory is ` + "`os.Args[1]`" + `. Write each CSV there as ` + "`<tableName>.csv`" + `.
- **First row must be the column header**, with names matching the schema exactly (case-sensitive).
- Subsequent rows are data. Use ` + "`encoding/csv`" + ` — it handles quoting automatically.
- When using ` + "`inherit`" + `, you only need to write the table(s) you're changing. Parent
  tables come from the base; descendant tables are auto-cleared. When **not** using
  ` + "`inherit`" + `, generate parent tables first and reuse their IDs in child rows.

## Minimal example (with inherit)

The script below replaces only ` + "`products.csv`" + `. ` + "`brands`" + ` and ` + "`categories`" + ` come
from the inherited baseline; ` + "`product_images`" + `, ` + "`inventory`" + `, ` + "`order_items`" + `
(any table that FKs to ` + "`products`" + `) are reduced to header-only automatically.

` + "```go" + `
package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

func main() {
	out := os.Args[1]

	pf, _ := os.Create(out + "/products.csv")
	pw := csv.NewWriter(pf)
	pw.Write([]string{"id", "brand_id", "name", "price"})
	for i := 1; i <= 10; i++ {
		pw.Write([]string{
			strconv.Itoa(i),
			"1", // any brand id present in the inherited brands.csv
			fmt.Sprintf("Product %d", i),
			fmt.Sprintf("%.2f", float64(i)*9.99),
		})
	}
	pw.Flush(); pf.Close()
}
` + "```" + `

Call:

` + "```" + `
generate_dataset_local
  scenario: products/v2
  inherit: basic
  script: <the Go source above>
` + "```" + `

The result has full ` + "`brands`" + `/` + "`categories`" + ` from the basic scenario, the new
` + "`products`" + `, and header-only ` + "`product_images`" + `/` + "`inventory`" + `/` + "`order_items`" + ` so
the seed never produces orphan FKs.

## Common pitfalls

- **Wrong column names**: copy names verbatim from ` + "`describe_schema`" + ` — a mismatch silently inserts NULL or causes restore errors.
- **Missing header row**: the first ` + "`Write`" + ` call must be the header, not a data row.
- **No ` + "`csv.Writer.Flush()`" + ` call**: buffered rows won't reach the file without ` + "`Flush()`" + `.
- **External imports**: the script runs without a module — only stdlib is available. Third-party packages are not supported.
- **Forgetting ` + "`inherit`" + `**: a partial generation without ` + "`inherit`" + ` produces a thin
  revision that, when seeded, wipes every table the script didn't write. Always pass
  ` + "`inherit: \"<base-scenario>\"`" + ` for partial updates.

## Incremental edits to existing generated data

Every time ` + "`generate_dataset_local`" + ` succeeds, the source script is stored privately
under the resulting ` + "`scenario@rNNN`" + `. Before writing a script from scratch:

1. ` + "`list_history scenario=<scenario>`" + ` — see existing revisions.
2. ` + "`describe_dataset datasetId=<scenario@rNNN>`" + ` — if ` + "`hasGeneratorScript: true`" + `,
   a saved script exists.
3. ` + "`get_dataset_script`" + ` — returns the full source as ` + "`script`" + `.
4. Modify the source.
5. ` + "`generate_dataset_local script=<modified> scenario=<scenario> inherit=<base>`" + ` —
   creates a new ` + "`rNNN`" + ` revision.
6. ` + "`seed_database scenario=<scenario> yes=true`" + `.
`

const docSupabaseAuthConnector = `# Supabase Auth service connector (Pro)

Seedmancer can snapshot and restore Supabase Auth users alongside your
Postgres database so that a single ` + "`seedmancer seed`" + ` command resets auth state.

> **Requires a Pro plan.** The connector is ignored (with a warning) when
> no API token is configured or the account is on the Free plan.

## seedmancer.yaml configuration

` + "```yaml" + `
services:
  auth:
    type: supabase-auth
    url_env: SUPABASE_URL            # env-var holding the project URL
    service_role_key_env: SUPABASE_SERVICE_ROLE_KEY  # env-var holding the service role JWT
    # OR supply values directly:
    # url: https://<project>.supabase.co
    # service_role_key: eyJ…
` + "```" + `

Both ` + "`*_env`" + ` fields accept an environment variable name **or** a direct value.
Seedmancer auto-detects direct values by their prefix (` + "`http://`" + `, ` + "`https://`" + `,
` + "`eyJ`" + ` for JWTs) so you can also write:

` + "```yaml" + `
services:
  auth:
    type: supabase-auth
    url_env: http://127.0.0.1:54321           # direct URL for local Supabase
    service_role_key_env: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9…  # direct JWT
` + "```" + `

## What is exported

All users in ` + "`auth.users`" + `:

| Field | Notes |
|---|---|
| id | Original UUID — preserved on restore to keep FK links intact |
| email | Used as the stable identity key for incremental sync |
| user_metadata | Arbitrary JSON stored against the user |
| password | Stored as a bcrypt hash; replayed during restore |

## What is seeded (restored)

Seeding is **incremental**:

1. Fetch all current auth users.
2. For each snapshot user:
   - Match by UUID first, then by e-mail as a fallback.
   - **Update** (PATCH) if a match is found — sets metadata and password.
   - **Create** (POST) otherwise, preserving the original UUID so any
     Postgres rows that reference ` + "`auth.users.id`" + ` remain valid.
3. Delete auth users not present in the snapshot.

### Mirror tables (trigger-based)

If your schema has a ` + "`public.user`" + ` (or similar) table that is populated by
a trigger on ` + "`auth.users`" + `, Seedmancer handles this automatically:

- Services are seeded **before** the Postgres database restore.
- When a new auth user is about to be created and a direct DB URL is
  available in the context, Seedmancer deletes any conflicting row in
  ` + "`public`" + ` schema tables that have an ` + "`email`" + ` column, preventing the
  ` + "`duplicate key`" + ` error the trigger would otherwise cause.
- After services are done, the DB restore replaces ` + "`public.user`" + ` with
  the correct CSV data, which already has the preserved UUIDs.

## MCP tools

| Tool | Description |
|---|---|
| ` + "`list_services`" + ` | List connectors from seedmancer.yaml |
| ` + "`export_service datasetId=<id> service=auth`" + ` | Snapshot auth users into ` + "`<dataset>/_supabase_auth.json`" + ` |
| ` + "`seed_service datasetId=<id> service=auth`" + ` | Restore auth users from the sidecar |
| ` + "`export_database datasetId=<id>`" + ` | Export Postgres + all services together |
| ` + "`seed_database datasetId=<id> yes=true`" + ` | Seed Postgres + all services together |

## Typical workflow

` + "```" + `
# 1. Set up (once)
#    Add the services block to seedmancer.yaml and set SUPABASE_URL +
#    SUPABASE_SERVICE_ROLE_KEY (use the service-role key, not the anon key).

# 2. Capture a known-good state
export_database datasetId=baseline

# 3. Reset everything (auth users + Postgres)
seed_database datasetId=baseline yes=true
` + "```" + `

## Common pitfalls

- **Anon key vs service-role key**: the Admin API requires the
  ` + "`service_role`" + ` JWT, not the ` + "`anon`" + ` JWT. Using the wrong key returns a
  403 ` + "`bad_jwt`" + ` error.
- **Local Supabase SSL**: the local Supabase Postgres instance typically has
  SSL disabled. Seedmancer appends ` + "`?sslmode=disable`" + ` automatically when
  connecting to a local DB for mirror-table cleanup.
- **Unique e-mail constraint**: if the snapshot was captured before UUID
  preservation was introduced, the seed falls back to matching by e-mail
  and issues a PATCH instead of a duplicate POST, so old snapshots still
  seed cleanly.
- **User quota**: Supabase free-tier projects have a limit on the number
  of auth users. Keep test datasets small (< 100 users) to stay within limits.
`
