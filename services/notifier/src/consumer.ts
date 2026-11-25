import { Kafka, type Consumer, logLevel, type LogEntry } from "kafkajs";
import { decodeEvent } from "./decode";
import type { Logger } from "./log";
import { InvalidEventError, type Notifier, type Outcome } from "./notifier";
import { registerSnappy } from "./snappy";

export interface ConsumerOptions {
  brokers: string[];
  topic: string;
  groupId: string;
  log: Logger;
  /** Called after every record, for metrics and tests. */
  onOutcome?: (outcome: Outcome | "invalid", partition: number, offset: string) => void;
}

/**
 * NotifierConsumer reads the event topic in its own consumer group, so it
 * keeps its own offsets and never affects the projector.
 *
 * Delivery is at least once: an offset is committed only after its record was
 * handled, so a crash replays the uncommitted records and the Notifier's
 * deduplication absorbs the repeats. A record that fails with a transient
 * error (database or SMTP down) is retried, and its partition waits; it is
 * never skipped. A record that can never be decoded is logged and skipped,
 * because one bad record must not stop every notification behind it.
 */
export class NotifierConsumer {
  private readonly consumer: Consumer;

  constructor(
    private readonly notifier: Notifier,
    private readonly opts: ConsumerOptions,
  ) {
    registerSnappy();
    const kafka = new Kafka({
      clientId: "pipeline-notifier",
      brokers: opts.brokers,
      logLevel: logLevel.WARN,
      logCreator: () => (entry: LogEntry) => {
        const { message, ...rest } = entry.log;
        const fields = { namespace: entry.namespace, ...rest };
        if (entry.level === logLevel.ERROR) {
          opts.log.error(`kafkajs: ${message}`, fields);
        } else {
          opts.log.warn(`kafkajs: ${message}`, fields);
        }
      },
    });
    this.consumer = kafka.consumer({ groupId: opts.groupId });
  }

  async start(): Promise<void> {
    await this.consumer.connect();
    // A new group starts at the beginning, so the notifier learns every
    // candidate's contact details. Old events are not emailed: see maxAgeMs.
    await this.consumer.subscribe({ topic: this.opts.topic, fromBeginning: true });
    await this.consumer.run({
      eachMessage: async ({ partition, message }) => {
        let outcome: Outcome | "invalid";
        try {
          outcome = await this.notifier.handle(decodeEvent(message.value));
        } catch (err) {
          if (!(err instanceof InvalidEventError)) {
            throw err; // transient: kafkajs retries, the offset is not committed
          }
          this.opts.log.error("skipping invalid record", { partition, offset: message.offset, err });
          outcome = "invalid";
        }
        if (outcome !== "ignored") {
          this.opts.log.info("handled", { outcome, partition, offset: message.offset });
        }
        this.opts.onOutcome?.(outcome, partition, message.offset);
      },
    });
  }

  async stop(): Promise<void> {
    await this.consumer.disconnect();
  }
}
