#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const https = require('node:https');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');

// Allow opting out entirely.
if (process.env.SEEDMANCER_BINARY_PATH || process.env.SEEDMANCER_SKIP_DOWNLOAD) {
  process.exit(0);
}

// Map Node.js platform/arch to the GoReleaser asset name and the local
// dist/ filename used by the bin shim.
function resolveNames() {
  const platform = os.platform();
  const arch = os.arch();

  const assetMap = {
    'linux-x64':   { asset: 'seedmancer_Linux_x86_64',         local: 'seedmancer-linux-x64' },
    'linux-arm64': { asset: 'seedmancer_Linux_arm64',           local: 'seedmancer-linux-arm64' },
    'darwin-x64':  { asset: 'seedmancer_Darwin_x86_64',         local: 'seedmancer-darwin-x64' },
    'darwin-arm64':{ asset: 'seedmancer_Darwin_arm64',          local: 'seedmancer-darwin-arm64' },
    'win32-x64':   { asset: 'seedmancer_Windows_x86_64.exe',    local: 'seedmancer-win32-x64.exe' },
  };

  const key = `${platform}-${arch}`;
  const entry = assetMap[key];
  if (!entry) {
    throw new Error(`Unsupported platform for binary download: ${key}`);
  }
  return entry;
}

// Follow up to maxRedirects HTTP/HTTPS redirects, then stream body to dest.
function downloadWithRedirects(url, destPath, maxRedirects) {
  return new Promise((resolve, reject) => {
    function follow(currentUrl, remaining) {
      const mod = currentUrl.startsWith('https:') ? https : http;
      const req = mod.get(currentUrl, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          if (remaining <= 0) {
            return reject(new Error('Too many redirects'));
          }
          return follow(res.headers.location, remaining - 1);
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode} downloading ${currentUrl}`));
        }
        const out = fs.createWriteStream(destPath);
        res.pipe(out);
        out.on('finish', () => out.close(resolve));
        out.on('error', reject);
        res.on('error', reject);
      });
      req.on('error', reject);
    }
    follow(url, maxRedirects);
  });
}

async function main() {
  let names;
  try {
    names = resolveNames();
  } catch (err) {
    console.warn(`[seedmancer] Skipping binary download: ${err.message}`);
    process.exit(0);
  }

  const distDir = path.resolve(__dirname, '..', 'dist');
  const destPath = path.join(distDir, names.local);

  // Skip if already present.
  if (fs.existsSync(destPath)) {
    process.exit(0);
  }

  const base =
    process.env.SEEDMANCER_DOWNLOAD_BASE ??
    'https://github.com/KazanKK/seedmancer/releases/latest/download';
  const url = `${base}/${names.asset}`;

  try {
    fs.mkdirSync(distDir, { recursive: true });
    console.log(`[seedmancer] Downloading CLI binary from ${url} ...`);
    await downloadWithRedirects(url, destPath, 10);
    fs.chmodSync(destPath, 0o755);
    console.log(`[seedmancer] Binary ready at ${destPath}`);
  } catch (err) {
    // Never break npm install — print a warning and let the shim explain.
    try { fs.unlinkSync(destPath); } catch (_) {}
    console.warn(`[seedmancer] Warning: failed to download binary (${err.message}).`);
    console.warn('[seedmancer] You can set SEEDMANCER_BINARY_PATH to point at a built binary.');
    process.exit(0);
  }
}

main();
