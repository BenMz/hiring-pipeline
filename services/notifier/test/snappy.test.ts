import { compressSync } from "snappy";
import { snappyDecompress } from "../src/snappy";

const header = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00, 0, 0, 0, 1, 0, 0, 0, 1]);

function block(s: string): Buffer {
  const c = compressSync(Buffer.from(s));
  const len = Buffer.alloc(4);
  len.writeUInt32BE(c.length);
  return Buffer.concat([len, c]);
}

describe("snappyDecompress", () => {
  it("reads plain snappy blocks (franz-go)", async () => {
    expect((await snappyDecompress(compressSync(Buffer.from("hello")))).toString()).toBe("hello");
  });

  it("reads xerial framing (Java clients)", async () => {
    const framed = Buffer.concat([header, block("hello "), block("world")]);
    expect((await snappyDecompress(framed)).toString()).toBe("hello world");
  });

  it("rejects a truncated xerial block", async () => {
    const framed = Buffer.concat([header, block("hello")]);
    await expect(snappyDecompress(framed.subarray(0, framed.length - 2))).rejects.toThrow("truncated");
  });
});
