# @seedmancer/cli

npm wrapper for the [Seedmancer](https://seedmancer.dev) CLI. Ships the pre-built Go binary so you can install it as a dev dependency without a separate install step.

## Install

```sh
npm install -D @seedmancer/cli
```

## Usage

```sh
npx seedmancer --help
npx seedmancer seed api-test --yes
npx seedmancer pull api-test --token $SEEDMANCER_API_TOKEN
```

The `seedmancer` command is available in any npm script or tool that runs inside the project (Playwright, Vitest, CI, etc.) without adding anything to `PATH`.

## Relationship with @seedmancer/playwright

Install both packages together. `@seedmancer/playwright` calls the `seedmancer` command before each test — this package makes that command available locally without a global install.

```sh
npm install -D @seedmancer/cli @seedmancer/playwright
```

## Local development

If you are building the Go CLI locally and want to test this wrapper against it:

```sh
# Build the Go binary
go build -o /tmp/seedmancer .

# Run the wrapper against the local binary
SEEDMANCER_BINARY_PATH=/tmp/seedmancer node packages/cli/bin/seedmancer.js --help

# Or, if the package is linked via npm workspaces
SEEDMANCER_BINARY_PATH=/tmp/seedmancer npx seedmancer --help
```

`SEEDMANCER_BINARY_PATH` overrides the bundled binary path entirely. It is only for local development and CI — do not set it in production.

## Supported platforms

| Platform | Architecture |
|---|---|
| macOS | arm64 (Apple Silicon) |
| macOS | x64 (Intel) |
| Linux | x64 |
| Linux | arm64 |
| Windows | x64 |

Binaries are placed in `dist/` at publish time. The wrapper selects the correct binary automatically based on `os.platform()` and `os.arch()`.
