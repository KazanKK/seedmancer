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

// Search the system PATH for a 'seedmancer' executable that is NOT this shim
// (i.e., not inside any node_modules directory). This lets environments that
// have a pre-installed binary (devcontainers, go install, Homebrew, etc.) work
// even when the npm shim is the first hit on PATH.
function findOnSystemPath() {
  const sep = os.platform() === 'win32' ? ';' : ':';
  const pathDirs = (process.env.PATH ?? '').split(sep);
  const ext = os.platform() === 'win32' ? '.exe' : '';
  const name = `seedmancer${ext}`;

  for (const dir of pathDirs) {
    if (!dir) continue;
    // Skip any directory that lives inside node_modules to avoid recursion.
    if (dir.includes('node_modules')) continue;
    const candidate = path.join(dir, name);
    try {
      fs.accessSync(candidate, fs.constants.X_OK);
      return candidate;
    } catch (_) {
      // not found here, keep looking
    }
  }
  return null;
}

// Resolution order:
//   1. SEEDMANCER_BINARY_PATH env var (explicit override).
//   2. Bundled dist/<name> placed by the postinstall downloader.
//   3. A seedmancer binary found on the system PATH (outside node_modules).
//   4. Fall through → error below.
function resolveBinaryPath() {
  if (process.env.SEEDMANCER_BINARY_PATH) {
    return { path: process.env.SEEDMANCER_BINARY_PATH, mustExist: false };
  }

  let bundled;
  try {
    bundled = path.resolve(__dirname, '..', 'dist', getBinaryName());
  } catch (err) {
    // Unsupported platform — skip bundled check, try PATH.
    const onPath = findOnSystemPath();
    if (onPath) return { path: onPath, mustExist: false };
    throw err;
  }

  if (fs.existsSync(bundled)) {
    return { path: bundled, mustExist: false };
  }

  const onPath = findOnSystemPath();
  if (onPath) return { path: onPath, mustExist: false };

  // Return the bundled path so the error message below is informative.
  return { path: bundled, mustExist: true };
}

let resolved;
try {
  resolved = resolveBinaryPath();
} catch (err) {
  console.error(`Error: ${err.message}`);
  process.exit(1);
}

if (resolved.mustExist && !fs.existsSync(resolved.path)) {
  console.error('Seedmancer CLI binary was not found.');
  console.error(`Expected binary path: ${resolved.path}`);
  console.error('');
  console.error('If you are developing locally, set SEEDMANCER_BINARY_PATH to point at the built binary:');
  console.error('  go build -o /tmp/seedmancer .');
  console.error('  SEEDMANCER_BINARY_PATH=/tmp/seedmancer npx seedmancer --help');
  process.exit(1);
}

const result = spawnSync(resolved.path, process.argv.slice(2), {
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
