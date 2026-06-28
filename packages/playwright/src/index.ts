import { test as base, expect } from '@playwright/test';
import { spawnSync } from 'node:child_process';
import { loadProvides, resolveProvides } from './internal/contract';
import type { ResolvedProvides } from './internal/contract';
import { recordUsage } from './internal/usage';

export type SeedmancerReset = 'beforeEach' | 'beforeAll' | 'manual';

export type SeedmancerOptions = {
  /** Seedmancer state (scenario) to seed for the test. */
  seedmancerState: string | undefined;
  /**
   * When to seed:
   *  - "beforeEach" (default): seed before every test.
   *  - "beforeAll": seed once per worker for a given state.
   *  - "manual": never seed automatically; call seedmancer.seed() yourself.
   */
  seedmancerReset: SeedmancerReset;
  /** Target environment passed as --env. Defaults to the project default. */
  seedmancerEnv: string | undefined;
  /** Working directory for the CLI. Defaults to process.cwd(). */
  seedmancerCwd: string | undefined;
};

/**
 * Data handle exposed to tests. Reads named values from the state's
 * contract.yaml and can trigger seeding manually.
 */
export type Seedmancer = {
  /** The state declared for this test, if any. */
  readonly state: string | undefined;
  /**
   * Return a named data handle from the state's contract `provides` block,
   * with `*Env` keys resolved from the environment. Throws if the name is
   * not defined in the contract.
   */
  get(name: string): ResolvedProvides;
  /** Seed a state on demand (used in manual reset mode). */
  seed(state?: string): Promise<void>;
};

type SeedmancerFixtures = {
  seedmancer: Seedmancer;
  _seedmancerAutoSeed: void;
};

type SeedmancerWorkerFixtures = {
  _seedmancerSeededStates: Set<string>;
};

function runSeed(
  state: string,
  env: string | undefined,
  cwd: string,
): void {
  const args = ['seed', state, '--yes'];
  if (env !== undefined) {
    args.push('--env', env);
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
          'Install it with: npm install --save-dev @seedmancer/cli',
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
      `seedmancer seed failed for state "${state}" with status ${result.status}` +
        (output.length > 0 ? `:\n${output}` : ''),
    );
  }
}

export const test = base.extend<
  SeedmancerOptions & SeedmancerFixtures,
  SeedmancerWorkerFixtures
>({
  seedmancerState: [undefined, { option: true }],
  seedmancerReset: ['beforeEach', { option: true }],
  seedmancerEnv: [undefined, { option: true }],
  seedmancerCwd: [undefined, { option: true }],

  // Tracks which (state|env) pairs have already been seeded in this worker so
  // reset: "beforeAll" seeds a state only once per worker process.
  _seedmancerSeededStates: [
    async ({}, use) => {
      await use(new Set<string>());
    },
    { scope: 'worker' },
  ],

  seedmancer: async (
    { seedmancerState, seedmancerEnv, seedmancerCwd },
    use,
    testInfo,
  ) => {
    const cwd = seedmancerCwd ?? process.cwd();
    let activeState = seedmancerState;
    let provides =
      activeState !== undefined ? loadProvides(cwd, activeState) : {};

    const handle: Seedmancer = {
      get state() {
        return activeState;
      },
      get(name: string): ResolvedProvides {
        if (activeState === undefined) {
          throw new Error(
            `seedmancer.get("${name}"): no state is set. ` +
              'Declare one with test.use({ seedmancerState: "..." }) or call seedmancer.seed("...") first.',
          );
        }
        const entry = provides[name];
        if (entry === undefined) {
          throw new Error(
            `seedmancer.get("${name}"): "${name}" is not defined in the contract for state "${activeState}". ` +
              `Add it under provides: in .seedmancer/scenarios/${activeState}/contract.yaml`,
          );
        }
        return resolveProvides(entry);
      },
      async seed(state?: string): Promise<void> {
        const target = state ?? activeState;
        if (target === undefined) {
          throw new Error(
            'seedmancer.seed(): no state provided and no seedmancerState option is set.',
          );
        }
        runSeed(target, seedmancerEnv, cwd);
        activeState = target;
        provides = loadProvides(cwd, target);
        recordUsage({
          state: target,
          file: testInfo.file,
          title: testInfo.title,
          project: testInfo.project.name,
          resetMode: 'manual',
          cwd,
        });
      },
    };

    await use(handle);
  },

  _seedmancerAutoSeed: [
    async (
      {
        seedmancerState,
        seedmancerReset,
        seedmancerEnv,
        seedmancerCwd,
        _seedmancerSeededStates,
      },
      use,
      testInfo,
    ) => {
      const cwd = seedmancerCwd ?? process.cwd();

      if (seedmancerState !== undefined && seedmancerReset !== 'manual') {
        const key = `${seedmancerState}|${seedmancerEnv ?? ''}`;
        const alreadySeeded = _seedmancerSeededStates.has(key);
        if (seedmancerReset === 'beforeEach' || !alreadySeeded) {
          runSeed(seedmancerState, seedmancerEnv, cwd);
          _seedmancerSeededStates.add(key);
        }
      }

      if (seedmancerState !== undefined) {
        recordUsage({
          state: seedmancerState,
          file: testInfo.file,
          title: testInfo.title,
          project: testInfo.project.name,
          resetMode: seedmancerReset,
          cwd,
        });
      }

      await use();
    },
    { auto: true },
  ],
});

export { expect };
