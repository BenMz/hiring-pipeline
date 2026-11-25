import type { Contact, Notification } from "./notifications";

/** Outcome of one delivery attempt. */
export type Delivery = "sent" | "duplicate";

/** State the notifier keeps. It is derived from the event stream, plus a delivery log. */
export interface Store {
  saveJob(jobId: string, title: string): Promise<void>;
  jobTitle(jobId: string): Promise<string | undefined>;
  saveContact(contact: Contact): Promise<void>;
  contact(applicationId: string): Promise<Contact | undefined>;
  /**
   * deliverOnce calls send unless a notification with the same eventId was
   * already delivered. The delivery is recorded only if send succeeds, and
   * concurrent calls for the same eventId are serialised, so at most one of
   * them sends.
   */
  deliverOnce(notification: Notification, send: () => Promise<void>): Promise<Delivery>;
}

/** Sends one email. Must throw if the message was not accepted. */
export interface Sender {
  send(notification: Notification): Promise<void>;
}
