import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { findProjectPaths } from './project';

export type RecordUsageInput = {
  /** Linked Seedmancer state, or undefined when the test declares none. */
  state?: string;
  file: string;
  title: string;
  project: string;
  resetMode: string;
  cwd: string;
  /** Names from the state's contract `provides` block (no values/secrets). */
  provides?: string[];
};

/** Shape persisted per event and uploaded to the cloud. */
export type UsageEvent = {
  testId: string;
  state: string | null;
  file: string;
  title: string;
  project: string;
  resetMode: string;
  lastSeenAt: string;
  testHash: string;
  provides?: string[];
};

function sha1(input: string | Buffer): string {
  return crypto.createHash('sha1').update(input).digest('hex');
}

/** Stable id for a test across runs: hash of file + title + project. */
export function testIdFor(relFile: string, title: string, project: string): string {
  return sha1([relFile, title, project].join('\u0000'));
}

/**
 * Write a single usage event for one test as a uniquely-named file under
 * <storage>/.usage-events/. The filename is keyed on file+title+project (not
 * state) so re-runs and link/unlink changes overwrite the same event
 * (idempotent) and parallel workers never race. The Go CLI and the cloud
 * uploader read these back.
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

    const event: UsageEvent = {
      testId: testIdFor(relFile, input.title, input.project),
      state: input.state ?? null,
      file: relFile,
      title: input.title,
      project: input.project,
      resetMode: input.resetMode,
      lastSeenAt: new Date().toISOString(),
      testHash,
      provides: input.provides,
    };

    const key = [relFile, input.title, input.project].join('\u0000');
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

/**
 * Read and parse every persisted usage event for the project containing `cwd`.
 * Returns an empty array when there is no project or no events directory.
 */
export function collectUsageEvents(cwd: string): UsageEvent[] {
  const paths = findProjectPaths(cwd);
  if (paths === undefined) return [];

  const eventsDir = path.join(paths.storageRoot, '.usage-events');
  let files: string[];
  try {
    files = fs.readdirSync(eventsDir).filter((f) => f.endsWith('.json'));
  } catch {
    return [];
  }

  const events: UsageEvent[] = [];
  for (const f of files) {
    try {
      const raw = fs.readFileSync(path.join(eventsDir, f), 'utf8');
      const parsed = JSON.parse(raw) as UsageEvent;
      if (parsed && typeof parsed.testId === 'string') {
        events.push(parsed);
      }
    } catch {
      // Skip unreadable/corrupt event files.
    }
  }
  return events;
}
