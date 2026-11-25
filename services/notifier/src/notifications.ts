// What to tell a candidate, decided from one event. Pure: no I/O.

import { type Event, Stage } from "./gen/pipeline/v1/events_pb";

/** The candidate-facing notifications. Not every event produces one. */
export type Kind = "received" | "screening" | "interview" | "offer" | "hired" | "rejected";

/** Who an application belongs to. Built from ApplicationSubmitted events. */
export interface Contact {
  applicationId: string;
  jobId: string;
  name: string;
  email: string;
}

/** A rendered email, identified by the event that caused it. */
export interface Notification {
  /** ID of the triggering event. Also the deduplication key. */
  eventId: string;
  applicationId: string;
  kind: Kind;
  to: string;
  subject: string;
  text: string;
}

const stageKinds: Partial<Record<Stage, Kind>> = {
  [Stage.SCREENING]: "screening",
  [Stage.INTERVIEW]: "interview",
  [Stage.OFFER]: "offer",
  [Stage.HIRED]: "hired",
};

/** kindFor returns the notification an event should produce, or undefined if none. */
export function kindFor(event: Event): Kind | undefined {
  switch (event.payload.case) {
    case "applicationSubmitted":
      return "received";
    case "stageChanged":
      return stageKinds[event.payload.value.toStage];
    case "candidateRejected":
      return "rejected";
    default:
      return undefined;
  }
}

/**
 * render builds the email. The rejection reason is deliberately not included:
 * it is an internal note written for recruiters, not a message to the candidate.
 */
export function render(
  eventId: string,
  kind: Kind,
  contact: Contact,
  jobTitle: string | undefined,
): Notification {
  // The job's events live on a different partition from the application's, so
  // the title may not have been seen yet. Fall back rather than wait.
  const role = jobTitle ? `the ${jobTitle} role` : "the role you applied for";
  const hi = `Hi ${contact.name},\n\n`;
  const templates: Record<Kind, [string, string]> = {
    received: [
      `We received your application for ${role}`,
      `${hi}Thanks for applying for ${role}. We will be in touch about next steps.`,
    ],
    screening: [
      "Your application is being reviewed",
      `${hi}Your application for ${role} has moved to screening.`,
    ],
    interview: [
      "Interview invitation",
      `${hi}We would like to interview you for ${role}. We will contact you to find a time.`,
    ],
    offer: [`An offer for ${role}`, `${hi}We are happy to make you an offer for ${role}.`],
    hired: ["Welcome aboard", `${hi}Congratulations, and welcome. We look forward to working with you.`],
    rejected: [
      "An update on your application",
      `${hi}Thank you for your interest in ${role}. We have decided not to move forward with your application.`,
    ],
  };
  const [subject, text] = templates[kind];
  return { eventId, applicationId: contact.applicationId, kind, to: contact.email, subject, text };
}
