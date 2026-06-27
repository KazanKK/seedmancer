import { spawnSync } from 'node:child_process';

export type SeedmancerGlobalSetupOptions = {
  scenarios: string[];
  token?: string;
  tokenEnv?: string;
  cwd?: string;
};

function runSeedmancerPull(
  scenario: string,
  token: string | undefined,
  cwd: string,
): void {
  const args = ['pull', scenario];
  if (token !== undefined && token !== '') {
    args.push('--token', token);
  }
  const result = spawnSync('seedmancer', args, {
    cwd,
    stdio: 'pipe',
    encoding: 'utf-8',
  });

  if (result.error !== undefined) {
    const err = result.error as NodeJS.ErrnoException;
    if (err.code === 'ENOENT') {
      throw new Error(
        'Seedmancer CLI not found. Make sure it is installed and available in PATH.\n' +
          'See https://seedmancer.dev/docs/install for installation instructions.',
      );
    }
    throw result.error;
  }

  if (result.signal !== null) {
    throw new Error(`Seedmancer was terminated by signal: ${result.signal}`);
  }

  if (result.status !== 0) {
    const output = [result.stderr?.trim(), result.stdout?.trim()]
      .filter(Boolean)
      .join('\n');
    throw new Error(
      `seedmancer pull failed for scenario "${scenario}" with status ${result.status}` +
        (output.length > 0 ? `:\n${output}` : ''),
    );
  }
}

export function createSeedmancerGlobalSetup(
  options: SeedmancerGlobalSetupOptions,
) {
  return async function seedmancerGlobalSetup(): Promise<void> {
    const cwd = options.cwd ?? process.cwd();
    const tokenEnv = options.tokenEnv ?? 'SEEDMANCER_API_TOKEN';
    const token = options.token ?? process.env[tokenEnv];
    // If no token is found, run without --token so the CLI falls back to
    // stored credentials from `seedmancer login`. This is the expected
    // behaviour for local development.

    for (const scenario of options.scenarios) {
      runSeedmancerPull(scenario, token, cwd);
    }
  };
}
