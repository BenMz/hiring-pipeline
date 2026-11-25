import { configFromEnv, parseDuration } from "../src/config";

describe("parseDuration", () => {
  it.each([
    ["500ms", 500],
    ["30s", 30_000],
    ["15m", 900_000],
    ["24h", 86_400_000],
    ["7d", 604_800_000],
    ["0", Number.POSITIVE_INFINITY],
  ])("%s", (input, want) => {
    expect(parseDuration(input)).toBe(want);
  });

  it.each(["", "1", "1y", "-1h", "1.5h"])("rejects %j", (input) => {
    expect(() => parseDuration(input)).toThrow("invalid duration");
  });
});

describe("configFromEnv", () => {
  it("has defaults for everything but the database", () => {
    const cfg = configFromEnv({ DATABASE_URL: "postgres://x" });
    expect(cfg).toMatchObject({
      brokers: ["localhost:19092"],
      topic: "applications.events",
      groupId: "pipeline-notifier",
      smtp: undefined,
      maxAgeMs: 86_400_000,
    });
  });

  it("requires DATABASE_URL", () => {
    expect(() => configFromEnv({})).toThrow("DATABASE_URL");
  });

  it("parses brokers and SMTP", () => {
    const cfg = configFromEnv({ DATABASE_URL: "x", KAFKA_BROKERS: "a:1, b:2", SMTP_HOST: "mail", SMTP_PORT: "2525" });
    expect(cfg.brokers).toEqual(["a:1", "b:2"]);
    expect(cfg.smtp).toEqual({ host: "mail", port: 2525 });
  });
});
