import { uploadUsage, type UploadUsageOptions } from './internal/upload';

export type SeedmancerGlobalTeardownOptions = UploadUsageOptions;

/**
 * Build a Playwright `globalTeardown` that uploads test→state usage to the
 * Seedmancer cloud once the suite finishes. Wire it up in playwright.config.ts:
 *
 *   import { createSeedmancerGlobalTeardown } from '@seedmancer/playwright/global-teardown';
 *   export default defineConfig({
 *     globalTeardown: require.resolve('./seedmancer.global-teardown.ts'),
 *   });
 *
 * or point `globalTeardown` at a file that default-exports the result of this
 * factory. Uploading is best-effort and never fails the run.
 */
export function createSeedmancerGlobalTeardown(
  options: SeedmancerGlobalTeardownOptions = {},
) {
  return async function seedmancerGlobalTeardown(): Promise<void> {
    await uploadUsage(options);
  };
}
