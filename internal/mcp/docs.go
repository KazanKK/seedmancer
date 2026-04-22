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
