import { kindFor, render } from "../src/notifications";
import { jobOpened, moved, rejected, Stage, submitted } from "./events";

describe("kindFor", () => {
  it.each([
    ["job opened", jobOpened("j1", "Backend"), undefined],
    ["submitted", submitted("a1", "j1"), "received"],
    ["to screening", moved("a1", Stage.APPLIED, Stage.SCREENING), "screening"],
    ["to interview", moved("a1", Stage.SCREENING, Stage.INTERVIEW), "interview"],
    ["to offer", moved("a1", Stage.INTERVIEW, Stage.OFFER), "offer"],
    ["to hired", moved("a1", Stage.OFFER, Stage.HIRED), "hired"],
    ["rejected", rejected("a1", Stage.SCREENING, "no"), "rejected"],
  ])("%s -> %s", (_name, event, want) => {
    expect(kindFor(event)).toBe(want);
  });
});

describe("render", () => {
  const contact = { applicationId: "a1", jobId: "j1", name: "Ada", email: "ada@example.com" };

  it("addresses the candidate and names the role", () => {
    const n = render("e1", "interview", contact, "Backend Engineer");
    expect(n).toMatchObject({ eventId: "e1", applicationId: "a1", kind: "interview", to: "ada@example.com" });
    expect(n.text).toContain("Hi Ada");
    expect(n.text).toContain("the Backend Engineer role");
  });

  it("falls back when the job title is not known yet", () => {
    const n = render("e1", "received", contact, undefined);
    expect(n.subject).toBe("We received your application for the role you applied for");
  });
});
