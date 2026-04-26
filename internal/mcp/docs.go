package mcp

// These constants back the seedmancer://docs/* resources. They live in
// Go source (not embed.FS) so they ship with the binary at zero cost
// and stay diff-reviewable alongside the tool definitions they describe.

const docQuickstart = `# Seedmancer quickstart (for agents)

Seedmancer is a Postgres seeding tool. Its unit of reuse is a *dataset*:
a snapshot of CSVs + a JSON schema sidecar, content-addressed by the
schema fingerprint.

## First time in a new project

Run this sequence once to set everything up:

1. ` + "`init_project`" + ` — creates seedmancer.yaml, .seedmancer/, and writes agent
   rule files (.cursor/rules/seedmancer.mdc + CLAUDE.md) so future AI
   conversations in this project automatically use Seedmancer.
2. ` + "`export_database`" + ` — captures the current schema + data as a baseline dataset.
3. ` + "`install_agent_rules`" + ` — if adopting Seedmancer in an existing project that
   was not created with init_project, run this to write the rule files manually.

## Typical loop (project already set up)

1. ` + "`list_schemas`" + ` — check whether a schema has been exported.
   - **If no schemas exist**: call ` + "`export_database`" + ` first. The database is already
     running (it is in seedmancer.yaml). This creates the schema.json that all
     other tools depend on.
2. ` + "`describe_schema`" + ` — get the exact table and column names.
3. ` + "`generate_dataset_local`" + ` — write a Go script that produces CSVs locally.
   For partial updates (e.g. "regenerate only products"), pass
   ` + "`inherit: \"baseline\"`" + ` so the result is a complete, seedable dataset.
   Read seedmancer://docs/local-generation for the contract.
4. ` + "`seed_database`" + ` with ` + "`yes: true`" + ` — resets the env and loads the dataset.
5. Run the actual test command outside MCP (e.g. Playwright, pytest).

## Need new data?

1. ` + "`list_datasets`" + ` — check if an existing dataset has ` + "`hasGeneratorScript: true`" + `.
   If so, use ` + "`get_dataset_script`" + ` to retrieve the source and **modify it** instead
   of writing a new script from scratch. The saved script already has the correct
   column names, FK order, and enum values.
2. ` + "`generate_dataset_local`" + ` — pass the (modified) script and a ` + "`schemaRef`" + `; no cloud API
   or quota is consumed. Read seedmancer://docs/local-generation for the contract.
3. OR ` + "`generate_dataset`" + ` with a prompt — uses the Seedmancer cloud service
   (requires API token; consumes monthly quota).
4. ` + "`sync_dataset`" + ` — optionally publish the result to your cloud account.

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
    ["seed", "--dataset-id", "api-test", "--yes"],
    { stdio: "inherit" },
  );
  if (res.status !== 0) throw new Error("seedmancer seed failed");
}
` + "```" + `

### Agents calling this through MCP

Prefer the ` + "`seed_database`" + ` tool over shelling out. Pass:

- ` + "`datasetId: \"api-test\"`" + ` (or whichever dataset you keep for tests).
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

## When NOT to use Seedmancer at all

Seedmancer is for **curated, repeatable snapshots**. It is the wrong tool for:

- **Large-scale stress / load fixtures (≳ 100k rows per table).** CSV import is
  I/O-bound; for 1M+ rows prefer ` + "`INSERT … SELECT … FROM generate_series(...)`" + `
  or ` + "`COPY`" + ` from a streaming generator, run directly against the target
  database. Keep that script under ` + "`load-tests/`" + ` (or similar) — not as a
  Seedmancer dataset. Mixing scale loaders with reproducible fixtures makes
  both worse.
- **Production-shape anonymisation pipelines.** Use a dedicated masking tool.
- **One-off "I just need to insert this row" hacks.** Use plain SQL or your
  ORM seeders.

## Recommended workflow: ` + "`inherit`" + ` from baseline

The ` + "`inherit`" + ` parameter is the safe default for any partial generation.
It pre-fills the new dataset with all CSVs from a base dataset, lets your
script overwrite only the tables it cares about, and **automatically clears
any descendant table that FKs to an overwritten table** so seeding can never
produce orphan foreign keys.

1. ` + "`list_schemas`" + ` — confirm a schema exists. If not, ` + "`export_database`" + `
   first to capture the live DB as a ` + "`baseline`" + ` dataset.
2. ` + "`describe_schema`" + ` — get exact table and column names.
3. ` + "`generate_dataset_local`" + ` with ` + "`inherit: \"baseline\"`" + ` — write a tiny script that
   only emits the table(s) you actually want to change.
4. ` + "`seed_database`" + ` with ` + "`yes: true`" + `.

**No more "gen vs merged" datasets.** A single inherit call produces a complete,
seedable dataset.

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
  schemaRef: <fp>
  datasetId: products-v2
  inherit: baseline
  script: <the Go source above>
` + "```" + `

The result has full ` + "`brands`" + `/` + "`categories`" + ` from baseline, the new ` + "`products`" + `,
and header-only ` + "`product_images`" + `/` + "`inventory`" + `/` + "`order_items`" + ` so the seed never
produces orphan FKs.

## Common pitfalls

- **Wrong column names**: copy names verbatim from ` + "`describe_schema`" + ` — a mismatch silently inserts NULL or causes restore errors.
- **Missing header row**: the first ` + "`Write`" + ` call must be the header, not a data row.
- **No ` + "`csv.Writer.Flush()`" + ` call**: buffered rows won't reach the file without ` + "`Flush()`" + `.
- **External imports**: the script runs without a module — only stdlib is available. Third-party packages are not supported.
- **Forgetting ` + "`inherit`" + `**: a partial generation without ` + "`inherit`" + ` produces a thin
  dataset that, when seeded, wipes every table the script didn't write. Always pass
  ` + "`inherit: \"baseline\"`" + ` (or the most relevant existing dataset) for partial updates.

## Incremental edits to existing generated data

Every time ` + "`generate_dataset_local`" + ` succeeds, the source script is stored privately.
Before writing a script from scratch:

1. ` + "`describe_dataset datasetId=<id>`" + ` — if ` + "`hasGeneratorScript: true`" + `, a saved script exists.
2. ` + "`get_dataset_script datasetId=<id>`" + ` — returns the full source as ` + "`script`" + `.
3. Modify the source (bump row counts, change values, add columns, …).
4. ` + "`generate_dataset_local script=<modified> schemaRef=<fp> datasetId=<new-id> inherit=baseline`" + `.
5. ` + "`seed_database datasetId=<new-id> yes=true`" + `.
`
