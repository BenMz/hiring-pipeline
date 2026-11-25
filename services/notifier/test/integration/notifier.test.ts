// Real Redpanda and PostgreSQL via Testcontainers. Run with `npm run test:integration`.
import { toBinary } from "@bufbuild/protobuf";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { RedpandaContainer, type StartedRedpandaContainer } from "@testcontainers/redpanda";
import { type Admin, Kafka, logLevel } from "kafkajs";
import { Pool } from "pg";
import { NotifierConsumer } from "../../src/consumer";
import { type Event, EventSchema } from "../../src/gen/pipeline/v1/events_pb";
import { silentLogger } from "../../src/log";
import type { Notification } from "../../src/notifications";
import { Notifier, type Outcome } from "../../src/notifier";
import { PgStore } from "../../src/pg-store";
import type { Sender } from "../../src/ports";
import { moved, Stage, submitted } from "../events";

let redpanda: StartedRedpandaContainer;
let postgres: StartedPostgreSqlContainer;
let kafka: Kafka;
let admin: Admin;
let pool: Pool;

beforeAll(async () => {
  [redpanda, postgres] = await Promise.all([
    new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v24.2.7").start(),
    new PostgreSqlContainer("postgres:16-alpine").start(),
  ]);
  kafka = new Kafka({ brokers: [redpanda.getBootstrapServers()], logLevel: logLevel.NOTHING });
  admin = kafka.admin();
  await admin.connect();
  pool = new Pool({ connectionString: postgres.getConnectionUri() });
  await new PgStore(pool).migrate();
});

afterAll(async () => {
  await Promise.all([admin.disconnect(), pool.end()]);
  await Promise.all([redpanda.stop(), postgres.stop()]);
});

class RecordingSender implements Sender {
  sent: Notification[] = [];
  failures = 0;

  send(n: Notification): Promise<void> {
    if (this.failures > 0) {
      this.failures--;
      return Promise.reject(new Error("smtp unavailable"));
    }
    this.sent.push(n);
    return Promise.resolve();
  }
}

let topicSeq = 0;
async function newTopic(): Promise<string> {
  const topic = `events-${++topicSeq}`;
  await admin.createTopics({ topics: [{ topic, numPartitions: 6 }], waitForLeaders: true });
  return topic;
}

/**
 * produce writes records with rpk, which is built on franz-go like the
 * command service, so batches are snappy-compressed exactly as in
 * production. (kafkajs as a producer would write uncompressed batches and
 * hide a codec mismatch; that happened.)
 */
async function produceRaw(topic: string, records: { key: string; value: Uint8Array }[]): Promise<void> {
  const lines = records.map((r) => `${r.key} ${Buffer.from(r.value).toString("base64")}`).join("\n");
  const res = await redpanda.exec([
    "sh",
    "-c",
    `printf '%s\\n' "$0" | rpk topic produce ${topic} -z snappy -f '%k %v{base64}\\n'`,
    lines,
  ]);
  if (res.exitCode !== 0) throw new Error(`rpk produce: ${res.output}`);
}

async function produce(topic: string, events: Event[]): Promise<void> {
  await produceRaw(
    topic,
    events.map((e) => ({ key: e.metadata?.aggregateId ?? "", value: toBinary(EventSchema, e) })),
  );
}

/** Starts a consumer and resolves once it has handled `count` records. */
async function consume(topic: string, group: string, sender: Sender, count: number) {
  const outcomes: (Outcome | "invalid")[] = [];
  let done: () => void = () => undefined;
  const finished = new Promise<void>((resolve) => (done = resolve));
  const notifier = new Notifier(new PgStore(pool), sender, { maxAgeMs: 3_600_000, log: silentLogger });
  const consumer = new NotifierConsumer(notifier, {
    brokers: [redpanda.getBootstrapServers()],
    topic,
    groupId: group,
    log: silentLogger,
    onOutcome: (o) => {
      outcomes.push(o);
      if (outcomes.length === count) done();
    },
  });
  await consumer.start();
  await finished;
  await consumer.stop();
  return outcomes.sort();
}

function freshEvents(prefix: string): Event[] {
  const now = Date.now() - 1000;
  return [
    submitted(`${prefix}-a1`, "j1", { at: now }),
    moved(`${prefix}-a1`, Stage.APPLIED, Stage.SCREENING, { at: now }),
    submitted(`${prefix}-a2`, "j1", { at: now }),
  ];
}

it("emails once per event despite duplicate records and a full redelivery", async () => {
  const topic = await newTopic();
  const events = freshEvents("dup");
  const [first] = events;
  if (!first) throw new Error("no events");
  // The same event twice in the log, as a producer retry without
  // idempotence would leave it.
  await produce(topic, [...events, first]);

  const sender = new RecordingSender();
  expect(await consume(topic, "notifier-dup", sender, 4)).toEqual(["duplicate", "sent", "sent", "sent"]);

  // Lose the committed offsets: the group re-reads everything from 0.
  await admin.resetOffsets({ groupId: "notifier-dup", topic, earliest: true });
  expect(await consume(topic, "notifier-dup", sender, 4)).toEqual(["duplicate", "duplicate", "duplicate", "duplicate"]);

  expect(sender.sent.map((n) => `${n.applicationId}:${n.kind}`).sort()).toEqual([
    "dup-a1:received",
    "dup-a1:screening",
    "dup-a2:received",
  ]);
});

it("retries a failed send instead of skipping the record", async () => {
  const topic = await newTopic();
  const sender = new RecordingSender();
  sender.failures = 2;
  await produce(topic, freshEvents("retry").slice(0, 1));

  expect(await consume(topic, "notifier-retry", sender, 1)).toEqual(["sent"]);
  expect(sender.sent).toHaveLength(1);
  expect(sender.failures).toBe(0);
});

it("skips a record that is not an event and carries on", async () => {
  const topic = await newTopic();
  const sender = new RecordingSender();
  // Same key, so the poison record is on the event's partition, in front of it.
  await produceRaw(topic, [{ key: "poison-a1", value: Uint8Array.from([0xff, 0xff, 0xff]) }]);
  await produce(topic, [submitted("poison-a1", "j1", { at: Date.now() })]);

  expect(await consume(topic, "notifier-poison", sender, 2)).toEqual(["invalid", "sent"]);
  expect(sender.sent.map((n) => n.applicationId)).toEqual(["poison-a1"]);
});
