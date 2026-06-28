# @seedmancer/playwright

Playwright integration for [Seedmancer](https://seedmancer.dev). Connects Playwright tests to named Seedmancer **states** (test-data scenarios): seed a state before each test, read named data handles from the state's contract, and track which tests use which states.

## Installation

```sh
npm install --save-dev @seedmancer/playwright @seedmancer/cli
```

`@seedmancer/cli` makes the `seedmancer` command available in your project without a global install.

## Usage

Replace the standard Playwright `test` import with the one from this package and declare the state with `test.use()`:

```ts
import { test, expect } from "@seedmancer/playwright";

test.use({
  seedmancerState: "auth/login-success",
  seedmancerReset: "beforeEach",
});

test("user can login successfully", async ({ page, seedmancer }) => {
  const user = seedmancer.get("user:login");

  await page.goto("/login");
  await page.fill("[name=email]", user.email);
  await page.fill("[name=password]", user.password);
  await page.click("button[type=submit]");

  await expect(page.getByText("Dashboard")).toBeVisible();
});
```

The `seedmancer` fixture seeds `seedmancerState` before the test (per `seedmancerReset`) and exposes named data handles from the state's contract via `seedmancer.get()`.

## Options

Set options via `test.use()` at the file or describe level:

| Option | Type | Default | Description |
|---|---|---|---|
| `seedmancerState` | `string` | — | State (scenario) to seed. No seeding happens when omitted. |
| `seedmancerReset` | `"beforeEach" \| "beforeAll" \| "manual"` | `"beforeEach"` | When to seed. `beforeAll` seeds once per worker per state; `manual` only seeds via `seedmancer.seed()`. |
| `seedmancerEnv` | `string` | — | Target environment (`--env` flag). Defaults to the project default. |
| `seedmancerCwd` | `string` | `process.cwd()` | Working directory for the CLI. |

## The `seedmancer` fixture

| Member | Description |
|---|---|
| `seedmancer.state` | The state declared for this test, if any. |
| `seedmancer.get(name)` | Returns a named data handle from the state's contract `provides` block. |
| `seedmancer.seed(state?)` | Seeds a state on demand (for `reset: "manual"`). |

### Reading data with `get()`

`get(name)` reads from the state's contract at `.seedmancer/scenarios/<state>/contract.yaml`. Keys ending in `Env` are resolved from the environment:

```yaml
# .seedmancer/scenarios/auth/login-success/contract.yaml
state: auth/login-success
purpose: A valid verified user who can log in

provides:
  user:login:
    email: login@example.com
    passwordEnv: SEEDMANCER_TEST_PASSWORD
```

```ts
const user = seedmancer.get("user:login");
// => { email: "login@example.com", password: process.env.SEEDMANCER_TEST_PASSWORD }
```

### Manual seeding

```ts
test.use({ seedmancerState: "auth/login-success", seedmancerReset: "manual" });

test("...", async ({ seedmancer }) => {
  await seedmancer.seed(); // or seedmancer.seed("other/state")
});
```

## Usage tracking

When a test runs with a state declared, the fixture records a usage event under `.seedmancer/.usage-events/`. The CLI aggregates these so you can see which states are used by which tests:

```sh
seedmancer list --usage
seedmancer check auth/login-success
```

Tracking is best-effort and never fails a test. Set `SEEDMANCER_USAGE_STRICT=1` to surface write errors.

## Scoping

Options follow Playwright's standard scoping rules — global in `playwright.config.ts`, per file, or per describe block:

```ts
test.describe("admin flows", () => {
  test.use({ seedmancerState: "auth/admin-user" });

  test("admin can manage users", async ({ page, seedmancer }) => {
    // ...
  });
});
```

## Error handling

| Situation | Error message |
|---|---|
| CLI not found | `Seedmancer CLI not found. Make sure it is installed and available in PATH.` |
| Non-zero exit | `seedmancer seed failed for state "<state>" with status <N>: <output>` |
| Killed by signal | `Seedmancer was terminated by signal: <signal>` |

## Global setup — pull scenarios before the suite

Use `@seedmancer/playwright/global-setup` to pull seed data from Seedmancer Cloud once before the test suite starts. This is separate from the per-test seeding fixture.

### 1. Create a global setup file

```ts
// playwright.global-setup.ts
import { createSeedmancerGlobalSetup } from "@seedmancer/playwright/global-setup"

export default createSeedmancerGlobalSetup({
  scenarios: ["api-test"],
  // tokenEnv defaults to "SEEDMANCER_API_TOKEN"
  tokenEnv: "SEEDMANCER_API_TOKEN",
  // cwd defaults to process.cwd()
  cwd: "../",
})
```

### 2. Reference it from your Playwright config

```ts
// playwright.config.ts
import { defineConfig } from "@playwright/test"

export default defineConfig({
  globalSetup: "./playwright.global-setup.ts",
})
```

### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `scenarios` | `string[]` | required | Scenarios to pull from Seedmancer Cloud. |
| `token` | `string` | — | API token. Takes precedence over `tokenEnv`. |
| `tokenEnv` | `string` | `"SEEDMANCER_API_TOKEN"` | Environment variable name to read the token from. |
| `cwd` | `string` | `process.cwd()` | Working directory for the CLI (directory containing `seedmancer.yaml`). |

The token is never logged or included in error messages.
