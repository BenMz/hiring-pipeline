import { timestampMs } from "@bufbuild/protobuf/wkt";
import type { Event } from "./gen/pipeline/v1/events_pb";
import type { Logger } from "./log";
import { kindFor, render } from "./notifications";
import type { Sender, Store } from "./ports";

/** What handle did with an event. */
export type Outcome =
  | "sent" // an email went out
  | "duplicate" // already delivered for this event ID: nothing sent
  | "stale" // older than maxAgeMs: state updated, nothing sent
  | "ignored" // this event never notifies anyone
  | "unknown-contact"; // no ApplicationSubmitted seen for this application

export interface NotifierOptions {
  /**
   * Events older than this update state but send nothing. Stops a new
   * consumer group, which starts from the beginning of the topic, from
   * emailing every candidate about their whole history.
   */
  maxAgeMs: number;
  now?: () => number;
  log: Logger;
}

/** InvalidEventError marks an event that can never be processed. */
export class InvalidEventError extends Error {
  override name = "InvalidEventError";
}

export class Notifier {
  private readonly now: () => number;

  constructor(
    private readonly store: Store,
    private readonly sender: Sender,
    private readonly opts: NotifierOptions,
  ) {
    this.now = opts.now ?? Date.now;
  }

  /**
   * handle processes one event. It is safe to call any number of times with
   * the same event: state updates are upserts and delivery is deduplicated by
   * event ID. Errors from the store or the sender are thrown so the caller
   * can retry; InvalidEventError means retrying will not help.
   */
  async handle(event: Event): Promise<Outcome> {
    const meta = event.metadata;
    if (!meta?.eventId || !meta.occurredAt) {
      throw new InvalidEventError("event has no metadata");
    }

    // Update state first, so a stale or duplicate event still fills it in.
    const p = event.payload;
    if (p.case === "jobOpened") {
      await this.store.saveJob(p.value.jobId, p.value.title);
    } else if (p.case === "applicationSubmitted") {
      await this.store.saveContact({
        applicationId: p.value.applicationId,
        jobId: p.value.jobId,
        name: p.value.candidateName,
        email: p.value.candidateEmail,
      });
    }

    const kind = kindFor(event);
    if (kind === undefined || p.case === undefined || p.case === "jobOpened") {
      return "ignored";
    }
    if (this.now() - timestampMs(meta.occurredAt) > this.opts.maxAgeMs) {
      return "stale";
    }

    // Events of one application share a partition and arrive in order, so
    // its ApplicationSubmitted has always been handled before this.
    const contact = await this.store.contact(p.value.applicationId);
    if (!contact) {
      this.opts.log.warn("no contact for application", {
        applicationId: p.value.applicationId,
        eventId: meta.eventId,
      });
      return "unknown-contact";
    }
    const title = await this.store.jobTitle(contact.jobId);
    const notification = render(meta.eventId, kind, contact, title);
    return this.store.deliverOnce(notification, () => this.sender.send(notification));
  }
}
