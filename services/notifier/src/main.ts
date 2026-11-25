import { Pool } from "pg";
import { configFromEnv } from "./config";
import { NotifierConsumer } from "./consumer";
import { jsonLogger } from "./log";
import { Notifier } from "./notifier";
import { PgStore } from "./pg-store";
import type { Sender } from "./ports";
import { LogSender, SmtpSender } from "./senders";

async function main(): Promise<void> {
  const log = jsonLogger();
  const cfg = configFromEnv();

  const pool = new Pool({ connectionString: cfg.databaseUrl, max: 4 });
  const store = new PgStore(pool);
  await store.migrate();

  const smtp = cfg.smtp ? new SmtpSender(cfg.smtp, cfg.mailFrom) : undefined;
  const sender: Sender = smtp ?? new LogSender(log);
  const notifier = new Notifier(store, sender, { maxAgeMs: cfg.maxAgeMs, log });
  const consumer = new NotifierConsumer(notifier, {
    brokers: cfg.brokers,
    topic: cfg.topic,
    groupId: cfg.groupId,
    log,
  });

  let stopping = false;
  const shutdown = async (signal: string) => {
    if (stopping) return;
    stopping = true;
    log.info("shutting down", { signal });
    // disconnect waits for the in-flight record and commits its offset.
    await consumer.stop();
    smtp?.close();
    await pool.end();
    process.exit(0);
  };
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));

  await consumer.start();
  log.info("notifier running", {
    brokers: cfg.brokers,
    topic: cfg.topic,
    group: cfg.groupId,
    smtp: cfg.smtp ? `${cfg.smtp.host}:${cfg.smtp.port}` : "disabled (logging only)",
    maxAgeMs: cfg.maxAgeMs,
  });
}

main().catch((err: unknown) => {
  jsonLogger().error("exiting", { err: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
