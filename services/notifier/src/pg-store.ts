import type { Pool } from "pg";
import type { Contact, Notification } from "./notifications";
import type { Delivery, Store } from "./ports";

// The notifier owns its own schema. It never reads the projector's tables:
// everything it needs comes from the event stream.
export const schemaSQL = `
CREATE SCHEMA IF NOT EXISTS notifier;
CREATE TABLE IF NOT EXISTS notifier.jobs (
  job_id text PRIMARY KEY,
  title  text NOT NULL
);
CREATE TABLE IF NOT EXISTS notifier.contacts (
  application_id text PRIMARY KEY,
  job_id         text NOT NULL,
  name           text NOT NULL,
  email          text NOT NULL
);
-- One row per email that was handed to the mail server, keyed by the event
-- that caused it. This is the deduplication log.
CREATE TABLE IF NOT EXISTS notifier.deliveries (
  event_id       text PRIMARY KEY,
  application_id text NOT NULL,
  kind           text NOT NULL,
  recipient      text NOT NULL,
  sent_at        timestamptz NOT NULL DEFAULT now()
);
`;

export class PgStore implements Store {
  constructor(private readonly pool: Pool) {}

  async migrate(): Promise<void> {
    await this.pool.query(schemaSQL);
  }

  async saveJob(jobId: string, title: string): Promise<void> {
    await this.pool.query(
      "INSERT INTO notifier.jobs (job_id, title) VALUES ($1, $2) ON CONFLICT (job_id) DO NOTHING",
      [jobId, title],
    );
  }

  async jobTitle(jobId: string): Promise<string | undefined> {
    const res = await this.pool.query<{ title: string }>(
      "SELECT title FROM notifier.jobs WHERE job_id = $1",
      [jobId],
    );
    return res.rows[0]?.title;
  }

  async saveContact(c: Contact): Promise<void> {
    await this.pool.query(
      `INSERT INTO notifier.contacts (application_id, job_id, name, email) VALUES ($1, $2, $3, $4)
       ON CONFLICT (application_id) DO NOTHING`,
      [c.applicationId, c.jobId, c.name, c.email],
    );
  }

  async contact(applicationId: string): Promise<Contact | undefined> {
    const res = await this.pool.query<Contact>(
      `SELECT application_id AS "applicationId", job_id AS "jobId", name, email
       FROM notifier.contacts WHERE application_id = $1`,
      [applicationId],
    );
    return res.rows[0];
  }

  /**
   * The delivery row is inserted at the start of a transaction and committed
   * only after send succeeds. A second attempt for the same event (a
   * redelivery, or another instance during a rebalance) blocks on the row
   * lock until the first commits, then finds the row and skips. If send fails
   * or the process dies, the transaction rolls back and the next attempt
   * sends. The one gap: the mail server accepts the message and we crash
   * before COMMIT. Then the email goes out twice. See ADR 5.
   */
  async deliverOnce(n: Notification, send: () => Promise<void>): Promise<Delivery> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const res = await client.query(
        `INSERT INTO notifier.deliveries (event_id, application_id, kind, recipient)
         VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO NOTHING`,
        [n.eventId, n.applicationId, n.kind, n.to],
      );
      if (res.rowCount === 0) {
        await client.query("COMMIT");
        return "duplicate";
      }
      await send();
      await client.query("COMMIT");
      return "sent";
    } catch (err) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }
}
