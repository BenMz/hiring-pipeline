import { fromBinary } from "@bufbuild/protobuf";
import { type Event, EventSchema } from "./gen/pipeline/v1/events_pb";
import { InvalidEventError } from "./notifier";

/** decodeEvent parses a Kafka record value. Throws InvalidEventError on bad input. */
export function decodeEvent(value: Buffer | null): Event {
  if (!value || value.length === 0) {
    throw new InvalidEventError("empty record value");
  }
  try {
    return fromBinary(EventSchema, value);
  } catch (err) {
    throw new InvalidEventError(`not a pipeline.v1.Event: ${String(err)}`);
  }
}
