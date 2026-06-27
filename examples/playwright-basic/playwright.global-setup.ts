import { createSeedmancerGlobalSetup } from "@seedmancer/playwright/global-setup";

/**
 * Pulls the "api-test" scenario from Seedmancer Cloud once before the suite
 * starts. Requires SEEDMANCER_API_TOKEN to be set in the environment.
 *
 * Reference this file from playwright.config.ts:
 *
 *   export default defineConfig({
 *     globalSetup: "./playwright.global-setup.ts",
 *   })
 */
export default createSeedmancerGlobalSetup({
  scenarios: ["api-test"],
  // tokenEnv defaults to "SEEDMANCER_API_TOKEN" — shown here for clarity.
  tokenEnv: "SEEDMANCER_API_TOKEN",
  // Point at the directory that contains seedmancer.yaml.
  // Adjust the relative path to match your project layout.
  cwd: "../",
});
