import { createTransport, type Transporter } from "nodemailer";
import type { Logger } from "./log";
import type { Notification } from "./notifications";
import type { Sender } from "./ports";

/** Sends through an SMTP server (Mailpit in docker-compose). */
export class SmtpSender implements Sender {
  private readonly transport: Transporter;

  constructor(
    opts: { host: string; port: number },
    private readonly from: string,
  ) {
    this.transport = createTransport({ host: opts.host, port: opts.port, secure: false });
  }

  async send(n: Notification): Promise<void> {
    await this.transport.sendMail({
      from: this.from,
      to: n.to,
      subject: n.subject,
      text: n.text,
      // Stable per event, so a duplicate that slips through is recognisable
      // (and dropped by mail systems that deduplicate on Message-ID).
      messageId: `<${n.eventId}@hiring-pipeline.local>`,
      headers: { "X-Pipeline-Event-Id": n.eventId },
    });
  }

  close(): void {
    this.transport.close();
  }
}

/** Logs instead of sending. Used when SMTP_HOST is not set. */
export class LogSender implements Sender {
  constructor(private readonly log: Logger) {}

  send(n: Notification): Promise<void> {
    this.log.info("email (not sent: SMTP_HOST unset)", {
      to: n.to,
      subject: n.subject,
      eventId: n.eventId,
    });
    return Promise.resolve();
  }
}
