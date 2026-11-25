// kafkajs only ships gzip. The Go producer (franz-go) compresses batches with
// snappy by default, so without this codec the notifier cannot read the topic.
import { CompressionCodecs, CompressionTypes } from "kafkajs";
import { compress, uncompress } from "snappy";

// Java clients frame snappy in the "xerial" format: a magic header, then
// length-prefixed blocks. franz-go writes plain blocks. Read both.
const XERIAL_MAGIC = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]);
const XERIAL_HEADER_LEN = 16; // magic + version + compatible version

export async function snappyDecompress(buf: Buffer): Promise<Buffer> {
  if (buf.length < XERIAL_HEADER_LEN || !buf.subarray(0, XERIAL_MAGIC.length).equals(XERIAL_MAGIC)) {
    return (await uncompress(buf)) as Buffer;
  }
  const blocks: Buffer[] = [];
  for (let pos = XERIAL_HEADER_LEN; pos < buf.length; ) {
    if (pos + 4 > buf.length) throw new Error("snappy: truncated xerial block length");
    const len = buf.readUInt32BE(pos);
    pos += 4;
    if (pos + len > buf.length) throw new Error("snappy: truncated xerial block");
    blocks.push((await uncompress(buf.subarray(pos, pos + len))) as Buffer);
    pos += len;
  }
  return Buffer.concat(blocks);
}

/** registerSnappy teaches kafkajs to read and write snappy-compressed batches. */
export function registerSnappy(): void {
  CompressionCodecs[CompressionTypes.Snappy] = () => ({
    compress: (encoder: { buffer: Buffer }) => compress(encoder.buffer),
    decompress: snappyDecompress,
  });
}
