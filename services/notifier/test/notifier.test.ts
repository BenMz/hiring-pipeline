import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { silentLogger } from "../src/log";
import { MemoryStore } from "../src/memory-store";
import type { Notification } from "../src/notifications";
import { InvalidEventError, Notifier } from "../src/notifier";
import type { Sender } from "../src/ports";
import { jobOpened, moved, NOW, rejected, Stage, submitted } from "./events";

class FakeSender implements Sender {
  sent: Notification[] = [];
  failNext = 0;

  send(n: Notification): Promise<void> {
    if (this.failNext > 0) {
      this.failNext--;
      return Promise.reject(new Error("smtp unavailable"));
    }
    this.sent.push(n);
    return Promise.resolve();
  }
}

const HOUR = 3_600_000;

function setup() {
  const store = new MemoryStore();
  const sender = new FakeSender();
  const notifier = new Notifier(store, sender, { maxAgeMs: 24 * HOUR, now: () => NOW, log: silentLogger });
  return { store, sender, notifier };
}

describe("Notifier", () => {
  it("emails the candidate on submit and on each stage that has a notification", async () => {
    const { sender, notifier } = setup();
    await notifier.handle(jobOpened("j1", "Backend Engineer"));
    await notifier.handle(submitted("a1", "j1"));
    await notifier.handle(moved("a1", Stage.APPLIED, Stage.SCREENING));
    await notifier.handle(moved("a1", Stage.SCREENING, Stage.INTERVIEW));

    expect(sender.sent.map((n) => [n.kind, n.to])).toEqual([
      ["received", "a1@example.com"],
      ["screening", "a1@example.com"],
      ["interview", "a1@example.com"],
    ]);
    expect(sender.sent[0]?.subject).toContain("Backend Engineer");
  });

  it("sends once when the same event is delivered twice", async () => {
    const { sender, notifier } = setup();
    const event = submitted("a1", "j1");
    expect(await notifier.handle(event)).toBe("sent");
    expect(await notifier.handle(event)).toBe("duplicate");
    expect(sender.sent).toHaveLength(1);
  });

  it("sends once when a redelivery races the first delivery", async () => {
    const { sender, notifier } = setup();
    const event = submitted("a1", "j1");
    const outcomes = await Promise.all([notifier.handle(event), notifier.handle(event), notifier.handle(event)]);
    expect(outcomes.sort()).toEqual(["duplicate", "duplicate", "sent"]);
    expect(sender.sent).toHaveLength(1);
  });

  it("deduplicates by event ID, not by content", async () => {
    // Identical emails from distinct events are both sent: only a redelivery
    // of the same event is a duplicate.
    const { sender, notifier } = setup();
    await notifier.handle(submitted("a1", "j1", { eventId: "e1" }));
    await notifier.handle(submitted("a1", "j1", { eventId: "e2" }));
    expect(sender.sent.map((n) => n.eventId)).toEqual(["e1", "e2"]);
  });

  it("does not record a delivery when sending fails, so a retry sends it", async () => {
    const { sender, notifier } = setup();
    const event = submitted("a1", "j1");
    sender.failNext = 1;
    await expect(notifier.handle(event)).rejects.toThrow("smtp unavailable");
    expect(await notifier.handle(event)).toBe("sent");
    expect(sender.sent).toHaveLength(1);
  });

  it("does not email about old events but still learns the contact", async () => {
    const { sender, notifier } = setup();
    expect(await notifier.handle(submitted("a1", "j1", { at: NOW - 48 * HOUR }))).toBe("stale");
    expect(await notifier.handle(moved("a1", Stage.APPLIED, Stage.SCREENING))).toBe("sent");
    expect(sender.sent.map((n) => n.kind)).toEqual(["screening"]);
  });

  it("never tells the candidate the internal rejection reason", async () => {
    const { sender, notifier } = setup();
    await notifier.handle(submitted("a1", "j1"));
    await notifier.handle(rejected("a1", Stage.INTERVIEW, "salary expectations too high"));
    const n = sender.sent.at(-1);
    expect(n?.kind).toBe("rejected");
    expect(`${n?.subject} ${n?.text}`).not.toContain("salary");
  });

  it("ignores events that notify nobody", async () => {
    const { sender, notifier } = setup();
    expect(await notifier.handle(jobOpened("j1", "Backend"))).toBe("ignored");
    expect(sender.sent).toHaveLength(0);
  });

  it("skips a stage change for an application it has never seen", async () => {
    const { sender, notifier } = setup();
    expect(await notifier.handle(moved("ghost", Stage.APPLIED, Stage.SCREENING))).toBe("unknown-contact");
    expect(sender.sent).toHaveLength(0);
  });

  it("uses the job title once it arrives, even if the job came after the application", async () => {
    const { sender, notifier } = setup();
    await notifier.handle(submitted("a1", "j1"));
    await notifier.handle(jobOpened("j1", "Designer"));
    await notifier.handle(moved("a1", Stage.APPLIED, Stage.SCREENING));
    expect(sender.sent[0]?.text).toContain("the role you applied for");
    expect(sender.sent[1]?.text).toContain("the Designer role");
  });

  it("rejects events without metadata as invalid", async () => {
    const { notifier } = setup();
    const event = submitted("a1", "j1");
    event.metadata = undefined;
    await expect(notifier.handle(event)).rejects.toBeInstanceOf(InvalidEventError);
    const noTime = submitted("a1", "j1");
    if (noTime.metadata) noTime.metadata.occurredAt = undefined;
    await expect(notifier.handle(noTime)).rejects.toBeInstanceOf(InvalidEventError);
  });

  it("treats an event exactly at the age limit as fresh", async () => {
    const { notifier } = setup();
    const event = submitted("a1", "j1");
    if (event.metadata) event.metadata.occurredAt = timestampFromMs(NOW - 24 * HOUR);
    expect(await notifier.handle(event)).toBe("sent");
  });
});
