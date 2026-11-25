// Builders for protobuf events, shared by unit and integration tests.
import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { type Event, EventSchema, Stage } from "../src/gen/pipeline/v1/events_pb";

export const NOW = Date.parse("2026-11-25T12:00:00Z");

let seq = 0;

interface Meta {
  aggregateId: string;
  version: bigint;
  eventId?: string;
  at?: number;
}

function metadata(m: Meta) {
  return {
    eventId: m.eventId ?? `evt-${m.aggregateId}-${m.version}-${++seq}`,
    aggregateId: m.aggregateId,
    version: m.version,
    idempotencyKey: `key-${m.aggregateId}-${m.version}`,
    occurredAt: timestampFromMs(m.at ?? NOW - 1000),
  };
}

export function jobOpened(jobId: string, title: string, m: Partial<Meta> = {}): Event {
  return create(EventSchema, {
    metadata: metadata({ aggregateId: jobId, version: 1n, ...m }),
    payload: { case: "jobOpened", value: { jobId, companyId: "acme", title } },
  });
}

export function submitted(appId: string, jobId: string, m: Partial<Meta> = {}): Event {
  return create(EventSchema, {
    metadata: metadata({ aggregateId: appId, version: 1n, ...m }),
    payload: {
      case: "applicationSubmitted",
      value: {
        applicationId: appId,
        jobId,
        candidateName: "Ada Lovelace",
        candidateEmail: `${appId}@example.com`,
      },
    },
  });
}

export function moved(appId: string, from: Stage, to: Stage, m: Partial<Meta> = {}): Event {
  return create(EventSchema, {
    metadata: metadata({ aggregateId: appId, version: BigInt(to), ...m }),
    payload: { case: "stageChanged", value: { applicationId: appId, fromStage: from, toStage: to } },
  });
}

export function rejected(appId: string, from: Stage, reason: string, m: Partial<Meta> = {}): Event {
  return create(EventSchema, {
    metadata: metadata({ aggregateId: appId, version: 9n, ...m }),
    payload: { case: "candidateRejected", value: { applicationId: appId, fromStage: from, reason } },
  });
}

export { Stage };
