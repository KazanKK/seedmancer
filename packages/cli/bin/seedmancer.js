#!/usr/bin/env node

const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

function getBinaryName() {
  const platform = os.platform();
  const arch = os.arch();
  const extension = platform === 'win32' ? '.exe' : '';
  const binaryName = `seedmancer-${platform}-${arch}${extension}`;

  const supported = new Set([
    'seedmancer-darwin-arm64',
    'seedmancer-darwin-x64',
    'seedmancer-linux-x64',
    'seedmancer-linux-arm64',
    'seedmancer-win32-x64.exe',
  ]);

  if (!supported.has(binaryName)) {
    throw new Error(
      `Unsupported platform for Seedmancer CLI. platform=${platform} arch=${arch}\n` +
        `Supported targets: darwin arm64, darwin x64, linux x64, linux arm64, win32 x64`,
    );
  }

  return binaryName;
}

function resolveBinaryPath() {
  if (process.env.SEEDMANCER_BINARY_PATH) {
    return process.env.SEEDMANCER_BINARY_PATH;
  }

  return path.resolve(__dirname, '..', 'dist', getBinaryName());
}

let binaryPath;

try {
  binaryPath = resolveBinaryPath();
} catch (err) {
  console.error(`Error: ${err.message}`);
  process.exit(1);
}

if (!fs.existsSync(binaryPath)) {
  console.error('Seedmancer CLI binary was not found.');
  console.error(`Expected binary path: ${binaryPath}`);
  console.error('');
  console.error('If you are developing locally, set SEEDMANCER_BINARY_PATH to point at the built binary:');
  console.error('  go build -o /tmp/seedmancer .');
  console.error('  SEEDMANCER_BINARY_PATH=/tmp/seedmancer npx seedmancer --help');
  process.exit(1);
}

const result = spawnSync(binaryPath, process.argv.slice(2), {
  stdio: 'inherit',
});

if (result.error) {
  console.error(`Failed to run Seedmancer CLI: ${result.error.message}`);
  process.exit(1);
}

if (result.signal) {
  console.error(`Seedmancer CLI was terminated by signal: ${result.signal}`);
  process.exit(1);
}

process.exit(result.status ?? 1);
