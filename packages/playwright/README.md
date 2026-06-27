# @seedmancer/playwright

Playwright integration for [Seedmancer](https://seedmancer.dev). Automatically seeds your database with a named scenario before each test.

## Installation

```sh
npm install --save-dev @seedmancer/playwright
```

The [Seedmancer CLI](https://seedmancer.dev/docs/install) must be installed and available in `PATH`.

## Usage

Replace the standard Playwright `test` import with the one from this package:

```ts
import { test, expect } from "@seedmancer/playwright";

test.use({
  seedmancerScenario: "api-test",
});

test("user can open dashboard", async ({ page }) => {
  await page.goto("/dashboard");
  await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();
});
```

Before each test, `seedmancer seed <scenario> --yes` is executed automatically. If `seedmancerScenario` is not set, the fixture is a no-op and no seeding occurs.

## Options

Set options via `test.use()` at the file or describe level:

| Option | Type | Description |
|---|---|---|
| `seedmancerScenario` | `string` | Scenario name to seed. No seeding happens when omitted. |
| `seedmancerEnv` | `string` | Target environment (`--env` flag). Defaults to the project default. |
| `seedmancerCwd` | `string` | Working directory for the CLI. Defaults to `process.cwd()`. |

## Scoping

Options follow Playwright's standard scoping rules. You can apply them globally in `playwright.config.ts`, per file with `test.use()`, or per describe block:

```ts
// playwright.config.ts
import { defineConfig } from "@playwright/test";

export default defineConfig({
  use: {
    seedmancerScenario: "baseline",
  },
});
```

```ts
// override for one describe block
test.describe("admin flows", () => {
  test.use({ seedmancerScenario: "admin-data" });

  test("admin can manage users", async ({ page }) => {
    // ...
  });
});
```

## Error handling

| Situation | Error message |
|---|---|
| CLI not found | `Seedmancer CLI not found. Make sure it is installed and available in PATH.` |
| Non-zero exit | `Seedmancer exited with status <N>: <stderr/stdout>` |
| Killed by signal | `Seedmancer was terminated by signal: <signal>` |
