export interface Config {
  brokers: string[];
  topic: string;
  groupId: string;
  databaseUrl: string;
  smtp: { host: string; port: number } | undefined;
  mailFrom: string;
  maxAgeMs: number;
}

const units: Record<string, number> = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 };

/** parseDuration accepts "500ms", "30s", "15m", "24h", "7d", or "0" for no limit. */
export function parseDuration(s: string): number {
  if (s.trim() === "0") {
    return Number.POSITIVE_INFINITY;
  }
  const m = /^(\d+)(ms|s|m|h|d)$/.exec(s.trim());
  const n = m?.[1];
  const unit = m?.[2];
  if (n === undefined || unit === undefined) {
    throw new Error(`invalid duration ${JSON.stringify(s)}: want e.g. 30s, 15m, 24h, 7d, or 0`);
  }
  return Number(n) * (units[unit] ?? 1);
}

export function configFromEnv(env: NodeJS.ProcessEnv = process.env): Config {
  const brokers = (env.KAFKA_BROKERS ?? "localhost:19092")
    .split(",")
    .map((b) => b.trim())
    .filter(Boolean);
  const smtpHost = env.SMTP_HOST?.trim();
  // Required: without a durable delivery log, a restart would resend everything
  // that was redelivered.
  const databaseUrl = env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }
  return {
    brokers,
    topic: env.EVENTS_TOPIC ?? "applications.events",
    groupId: env.CONSUMER_GROUP ?? "pipeline-notifier",
    databaseUrl,
    smtp: smtpHost ? { host: smtpHost, port: Number(env.SMTP_PORT ?? "1025") } : undefined,
    mailFrom: env.MAIL_FROM ?? "Hiring Team <jobs@hiring-pipeline.local>",
    maxAgeMs: parseDuration(env.NOTIFY_MAX_AGE ?? "24h"),
  };
}
