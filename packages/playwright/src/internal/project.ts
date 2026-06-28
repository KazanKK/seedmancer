import * as fs from 'node:fs';
import * as path from 'node:path';
import { parse as parseYaml } from 'yaml';

export type ProjectPaths = {
  projectRoot: string;
  storageRoot: string;
};

/**
 * Walk up from `startDir` looking for seedmancer.yaml and return the project
 * root plus the resolved storage directory (honouring `storage_path`, default
 * ".seedmancer"). Returns undefined when no config is found.
 */
export function findProjectPaths(startDir: string): ProjectPaths | undefined {
  let dir = path.resolve(startDir);

  for (;;) {
    const configPath = path.join(dir, 'seedmancer.yaml');
    if (fs.existsSync(configPath)) {
      let storage = '.seedmancer';
      try {
        const doc = parseYaml(fs.readFileSync(configPath, 'utf8'));
        if (
          doc !== null &&
          typeof doc === 'object' &&
          typeof (doc as { storage_path?: unknown }).storage_path === 'string'
        ) {
          const sp = (doc as { storage_path: string }).storage_path.trim();
          if (sp.length > 0) {
            storage = sp;
          }
        }
      } catch {
        // Malformed config — fall back to the default storage path.
      }
      return { projectRoot: dir, storageRoot: path.join(dir, storage) };
    }

    const parent = path.dirname(dir);
    if (parent === dir) {
      return undefined;
    }
    dir = parent;
  }
}
