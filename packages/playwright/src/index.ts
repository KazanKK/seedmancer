import { test as base, expect } from '@playwright/test';
import { spawnSync } from 'node:child_process';

export type SeedmancerOptions = {
  seedmancerScenario: string | undefined;
  seedmancerEnv: string | undefined;
  seedmancerCwd: string | undefined;
};

type SeedmancerFixtures = {
  _seedmancerSeed: void;
};

export const test = base.extend<SeedmancerOptions & SeedmancerFixtures>({
  seedmancerScenario: [undefined, { option: true }],
  seedmancerEnv: [undefined, { option: true }],
  seedmancerCwd: [undefined, { option: true }],

  _seedmancerSeed: [
    async ({ seedmancerScenario, seedmancerEnv, seedmancerCwd }, use) => {
      if (seedmancerScenario !== undefined) {
        const args = ['seed', seedmancerScenario, '--yes'];

        if (seedmancerEnv !== undefined) {
          args.push('--env', seedmancerEnv);
        }

        const result = spawnSync('seedmancer', args, {
          cwd: seedmancerCwd ?? process.cwd(),
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
          throw new Error(
            `Seedmancer was terminated by signal: ${result.signal}`,
          );
        }

        if (result.status !== 0) {
          const output = [result.stderr?.trim(), result.stdout?.trim()]
            .filter(Boolean)
            .join('\n');
          throw new Error(
            `Seedmancer exited with status ${result.status}` +
              (output.length > 0 ? `:\n${output}` : ''),
          );
        }
      }

      await use();
    },
    { auto: true },
  ],
});

export { expect };
