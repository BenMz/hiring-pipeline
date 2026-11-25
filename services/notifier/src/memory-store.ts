import type { Contact, Notification } from "./notifications";
import type { Delivery, Store } from "./ports";

/** In-memory Store for tests and for running without PostgreSQL. Not durable. */
export class MemoryStore implements Store {
  private readonly jobs = new Map<string, string>();
  private readonly contacts = new Map<string, Contact>();
  private readonly delivered = new Set<string>();
  private readonly inFlight = new Map<string, Promise<unknown>>();

  saveJob(jobId: string, title: string): Promise<void> {
    this.jobs.set(jobId, title);
    return Promise.resolve();
  }

  jobTitle(jobId: string): Promise<string | undefined> {
    return Promise.resolve(this.jobs.get(jobId));
  }

  saveContact(contact: Contact): Promise<void> {
    this.contacts.set(contact.applicationId, contact);
    return Promise.resolve();
  }

  contact(applicationId: string): Promise<Contact | undefined> {
    return Promise.resolve(this.contacts.get(applicationId));
  }

  async deliverOnce(n: Notification, send: () => Promise<void>): Promise<Delivery> {
    // Wait for any in-flight attempt for the same event, like a row lock would.
    for (let p = this.inFlight.get(n.eventId); p; p = this.inFlight.get(n.eventId)) {
      await p.catch(() => undefined);
    }
    if (this.delivered.has(n.eventId)) {
      return "duplicate";
    }
    const attempt = send();
    this.inFlight.set(n.eventId, attempt);
    try {
      await attempt;
      this.delivered.add(n.eventId);
      return "sent";
    } finally {
      this.inFlight.delete(n.eventId);
    }
  }
}
