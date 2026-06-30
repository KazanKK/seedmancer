import { collectUsageEvents } from './usage';

export type UploadUsageOptions = {
  /** Cloud API origin. Defaults to SEEDMANCER_API_URL or production. */
  apiUrl?: string;
  /** Cloud API token. Defaults to the `tokenEnv` environment variable. */
  token?: string;
  /** Env var to read the token from. Defaults to SEEDMANCER_API_TOKEN. */
  tokenEnv?: string;
  /** Working directory used to locate the project. Defaults to process.cwd(). */
  cwd?: string;
};

const DEFAULT_API_URL = 'https://api.seedmancer.dev';

/**
 * Aggregate the persisted usage events and POST them to the cloud so the
 * Seedmancer dashboard can show which tests use which state.
 *
 * Best-effort and non-fatal: skips silently when uploads are disabled
 * (SEEDMANCER_USAGE_UPLOAD=0), no token is available, or there are no events.
 * Network/HTTP errors are swallowed unless SEEDMANCER_USAGE_STRICT=1.
 */
export async function uploadUsage(options: UploadUsageOptions = {}): Promise<void> {
  if (process.env.SEEDMANCER_USAGE_UPLOAD === '0') return;

  const cwd = options.cwd ?? process.cwd();
  const tokenEnv = options.tokenEnv ?? 'SEEDMANCER_API_TOKEN';
  const token = options.token ?? process.env[tokenEnv];
  // No token → can't authenticate the upload. Skip silently (local dev).
  if (token === undefined || token === '') return;

  const events = collectUsageEvents(cwd);
  if (events.length === 0) return;

  const baseUrl = (
    options.apiUrl ??
    process.env.SEEDMANCER_API_URL ??
    DEFAULT_API_URL
  ).replace(/\/$/, '');

  const payload = {
    events: events.map((e) => ({
      testId: e.testId,
      filePath: e.file,
      title: e.title,
      project: e.project,
      stateName: e.state ?? undefined,
      resetMode: e.resetMode,
      testHash: e.testHash,
      provides: e.provides,
      lastSeenAt: e.lastSeenAt,
    })),
  };

  try {
    const res = await fetch(`${baseUrl}/v1.0/test-usage`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(payload),
    });
    if (!res.ok && process.env.SEEDMANCER_USAGE_STRICT === '1') {
      const body = await res.text().catch(() => '');
      throw new Error(`Seedmancer usage upload failed: ${res.status} ${body}`);
    }
  } catch (err) {
    if (process.env.SEEDMANCER_USAGE_STRICT === '1') {
      throw err;
    }
  }
}
