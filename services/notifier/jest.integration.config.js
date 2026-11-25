/** Integration tests: real Redpanda and PostgreSQL via Testcontainers (needs Docker). */
/** @type {import('jest').Config} */
module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  roots: ["<rootDir>/test/integration"],
  testMatch: ["**/*.test.ts"],
  testTimeout: 120_000,
};
