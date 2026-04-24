package mcp

// These constants back the seedmancer://docs/* resources. They live in
// Go source (not embed.FS) so they ship with the binary at zero cost
// and stay diff-reviewable alongside the tool definitions they describe.

const docQuickstart = `# Seedmancer quickstart (for agents)

Seedmancer is a Postgres seeding tool. Its unit of reuse is a *dataset*:
a snapshot of CSVs + a JSON schema sidecar, content-addressed by the
schema fingerprint.

Typical loop agents run:

1. ` + "`get_status`" + ` — confirm seedmancer.yaml exists and ` + "`default_env`" + ` is set.
2. ` + "`list_datasets`" + ` — pick the dataset id you want to restore.
3. ` + "`seed_database`" + ` with ` + "`yes: true`" + ` — resets the env and loads the dataset.
4. Run the actual test command outside MCP (e.g. Playwright, pytest).

Need new data?

1. ` + "`export_database`" + ` to capture current state as a dataset.
2. ` + "`describe_schema`" + ` to understand the tables.
3. ` + "`generate_dataset`" + ` with a prompt to synthesize more rows.
4. ` + "`sync_dataset`" + ` to publish the result to your cloud account.

Safety rails already enforced:

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

## Workflow

1. ` + "`describe_schema`" + ` — get the exact table names and column names.
2. Write a Go script that produces one ` + "`<table>.csv`" + ` per table.
3. ` + "`generate_dataset_local`" + ` — pass the script and a ` + "`schemaRef`" + `.

## Go script contract

- ` + "`package main`" + `, stdlib only (` + "`encoding/csv`" + `, ` + "`fmt`" + `, ` + "`os`" + `, ` + "`math/rand`" + `, ` + "`time`" + `, …).
- No ` + "`go.mod`" + ` needed — the CLI runs it with ` + "`go run main.go <outDir>`" + `.
- Output directory is ` + "`os.Args[1]`" + `. Write each CSV there as ` + "`<tableName>.csv`" + `.
- **First row must be the column header**, with names matching the schema exactly (case-sensitive).
- Subsequent rows are data. Use ` + "`encoding/csv`" + ` — it handles quoting automatically.
- For tables with foreign keys, generate parent tables first and collect their IDs
  before generating child rows.

## Minimal example

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

	// --- brands (parent) ---
	bf, _ := os.Create(out + "/brands.csv")
	bw := csv.NewWriter(bf)
	bw.Write([]string{"id", "name"})
	for i := 1; i <= 3; i++ {
		bw.Write([]string{strconv.Itoa(i), fmt.Sprintf("Brand %d", i)})
	}
	bw.Flush(); bf.Close()

	// --- products (child, references brands.id) ---
	pf, _ := os.Create(out + "/products.csv")
	pw := csv.NewWriter(pf)
	pw.Write([]string{"id", "brand_id", "name", "price"})
	for i := 1; i <= 10; i++ {
		pw.Write([]string{
			strconv.Itoa(i),
			strconv.Itoa((i-1)%3 + 1), // brand_id cycles 1-3
			fmt.Sprintf("Product %d", i),
			fmt.Sprintf("%.2f", float64(i)*9.99),
		})
	}
	pw.Flush(); pf.Close()
}
` + "```" + `

## Common pitfalls

- **Wrong column names**: copy names verbatim from ` + "`describe_schema`" + ` — a mismatch silently inserts NULL or causes restore errors.
- **Missing header row**: the first ` + "`Write`" + ` call must be the header, not a data row.
- **No ` + "`csv.Writer.Flush()`" + ` call**: buffered rows won't reach the file without ` + "`Flush()`" + `.
- **External imports**: the script runs without a module — only stdlib is available.
`
