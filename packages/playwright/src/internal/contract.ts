import * as fs from 'node:fs';
import * as path from 'node:path';
import { parse as parseYaml } from 'yaml';
import { findProjectPaths } from './project';

type ProvideValue = string | number | boolean;
type ProvidesEntry = Record<string, ProvideValue>;

export type Contract = {
  state?: string;
  purpose?: string;
  provides?: Record<string, ProvidesEntry>;
  mustHave?: string[];
};

/** Resolved data handle returned by seedmancer.get(name). */
export type ResolvedProvides = Record<string, ProvideValue | undefined>;

function contractPath(storageRoot: string, state: string): string {
  const segments = state.split('/').filter((s) => s.length > 0);
  return path.join(storageRoot, 'scenarios', ...segments, 'contract.yaml');
}

/**
 * Load the `provides` block of a state's contract.yaml. Returns an empty
 * object when the project, contract, or provides block is absent — get()
 * then throws a helpful error only when a specific handle is requested.
 */
export function loadProvides(
  cwd: string,
  state: string,
): Record<string, ProvidesEntry> {
  const paths = findProjectPaths(cwd);
  if (paths === undefined) {
    return {};
  }
  const file = contractPath(paths.storageRoot, state);
  if (!fs.existsSync(file)) {
    return {};
  }
  try {
    const doc = parseYaml(fs.readFileSync(file, 'utf8')) as Contract | null;
    if (doc !== null && typeof doc === 'object' && doc.provides !== undefined) {
      return doc.provides;
    }
  } catch {
    // Malformed contract — treat as no provides.
  }
  return {};
}

/**
 * Resolve a provides entry into a flat data handle. Keys ending in "Env"
 * are treated as environment-variable references: `passwordEnv: FOO` becomes
 * `password: process.env.FOO`. All other values pass through unchanged.
 */
export function resolveProvides(entry: ProvidesEntry): ResolvedProvides {
  const out: ResolvedProvides = {};
  for (const [key, value] of Object.entries(entry)) {
    if (key.endsWith('Env') && key.length > 3 && typeof value === 'string') {
      const baseKey = key.slice(0, -3);
      out[baseKey] = process.env[value];
    } else {
      out[key] = value;
    }
  }
  return out;
}
