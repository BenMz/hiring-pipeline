/** Minimal structured logger: one JSON object per line, like the Go services. */
export interface Logger {
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
}

type Level = "INFO" | "WARN" | "ERROR";

export function jsonLogger(write: (line: string) => void = (l) => process.stdout.write(l)): Logger {
  const log = (level: Level, msg: string, fields: Record<string, unknown> = {}) => {
    const out: Record<string, unknown> = { time: new Date().toISOString(), level, msg };
    for (const [k, v] of Object.entries(fields)) {
      out[k] = v instanceof Error ? v.message : v;
    }
    write(JSON.stringify(out) + "\n");
  };
  return {
    info: (m, f) => { log("INFO", m, f); },
    warn: (m, f) => { log("WARN", m, f); },
    error: (m, f) => { log("ERROR", m, f); },
  };
}

export const silentLogger: Logger = { info() {}, warn() {}, error() {} };
