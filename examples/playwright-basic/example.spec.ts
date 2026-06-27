import { test, expect } from "@seedmancer/playwright";

/**
 * Seed the "api-test" scenario before every test in this file.
 * Remove this line to skip seeding.
 */
test.use({
  seedmancerScenario: "api-test",
});

test("user can open dashboard", async ({ page }) => {
  await page.goto("/dashboard");
  await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();
});

test("user list is populated after seeding", async ({ page }) => {
  await page.goto("/users");
  await expect(page.getByRole("row")).toHaveCount(6); // 5 users + header
});

/**
 * Per-describe override: use a different scenario for admin tests.
 */
test.describe("admin section", () => {
  test.use({ seedmancerScenario: "admin-data" });

  test("admin can manage users", async ({ page }) => {
    await page.goto("/admin/users");
    await expect(page.getByRole("heading", { name: "User management" })).toBeVisible();
  });
});
