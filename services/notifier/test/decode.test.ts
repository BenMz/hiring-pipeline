import { toBinary } from "@bufbuild/protobuf";
import { decodeEvent } from "../src/decode";
import { EventSchema } from "../src/gen/pipeline/v1/events_pb";
import { InvalidEventError } from "../src/notifier";
import { submitted } from "./events";

describe("decodeEvent", () => {
  it("round-trips an event", () => {
    const event = submitted("a1", "j1");
    const got = decodeEvent(Buffer.from(toBinary(EventSchema, event)));
    expect(got.payload.case).toBe("applicationSubmitted");
    expect(got.metadata?.eventId).toBe(event.metadata?.eventId);
  });

  it.each([
    ["null", null],
    ["empty", Buffer.alloc(0)],
    ["garbage", Buffer.from([0xff, 0xff, 0xff, 0xff])],
  ])("rejects %s values", (_name, value) => {
    expect(() => decodeEvent(value)).toThrow(InvalidEventError);
  });
});
