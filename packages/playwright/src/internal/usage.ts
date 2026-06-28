import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { findProjectPaths } from './project';

export type RecordUsageInput = {
  state: string;
  file: string;
  title: string;
  project: string;
  resetMode: string;
  cwd: string;
};

function sha1(input: string | Buffer): string {
  return crypto.createHash('sha1').update(input).digest('hex');
}

/**
 * Write a single usage event for one test+state+project as a uniquely-named
 * file under <storage>/.usage-events/. Unique names mean parallel workers
 * never race on the same file, and re-runs overwrite their own event
 * (idempotent). The Go CLI aggregates these into state-usage.json on read.
 *
 * Best-effort: failures are swallowed unless SEEDMANCER_USAGE_STRICT=1, so a
 * read-only filesystem or permissions issue never fails a test.
 */
export function recordUsage(input: RecordUsageInput): void {
  try {
    const paths = findProjectPaths(input.cwd);
    if (paths === undefined) {
      return;
    }

    const eventsDir = path.join(paths.storageRoot, '.usage-events');
    fs.mkdirSync(eventsDir, { recursive: true });

    const relFile = path.relative(paths.projectRoot, input.file);

    let testHash = '';
    try {
      testHash = sha1(fs.readFileSync(input.file));
    } catch {
      // Source unreadable — skip the hash, usage is still useful.
    }

    const event = {
      state: input.state,
      file: relFile,
      title: input.title,
      project: input.project,
      resetMode: input.resetMode,
      lastSeenAt: new Date().toISOString(),
      testHash,
    };

    const key = [relFile, input.title, input.project, input.state].join('\u0000');
    const fileName = `${sha1(key)}.json`;
    fs.writeFileSync(
      path.join(eventsDir, fileName),
      JSON.stringify(event, null, 2),
    );
  } catch (err) {
    if (process.env.SEEDMANCER_USAGE_STRICT === '1') {
      throw err;
    }
  }
}
