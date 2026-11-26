#!/usr/bin/env node
/**
 * 本机 IPC 流量解析器：把抓到的原始字节流还原成可读的 RPC 报文。
 *
 * 为什么需要它：命名管道（NPFS）/ Unix socket 不走网卡，Wireshark 抓不到，
 * 实际能拿到的是「应用层 write 的字节」。本工具负责把字节还原成帧 + 报文。
 *
 * 线格式（与 MessageStream.js / message_stream.py 一致）：
 *   普通帧（默认启用 CRC）: [长度 u32LE][长度头 CRC32 u32LE][数据...][数据 CRC32 u32LE]
 *   普通帧（关闭 CRC）    : [长度 u32LE][数据...]
 *   重同步消息（无帧头）  : FF FF FF FF [长度 u32LE][数据]
 *   错误包（在普通帧内）  : EE EE EE EE [错误码 u8]
 *   报文                  : [type u8][contentType u8][UUID 16B][payload...]
 *
 * 支持的三种输入格式：
 *   1. framed（带方向/时间戳的抓包文件，推荐）
 *        Magic "IPCD" + version u8 + reserved u8 + reserved u16
 *        之后每条记录：dir u8 (1=A→B, 2=B→A, 0=未知) | len u32LE | tsMs f64LE | payload
 *        抓包方法：包住 socket.write / WriteFile，把每次写入的 chunk 原样记一条。
 *        ```js
 *        const orig = socket.write.bind(socket);
 *        socket.write = (chunk, ...rest) => {
 *          dump.record(1, chunk, Date.now());        // 例如 A→B 方向
 *          return orig(chunk, ...rest);
 *        };
 *        ```
 *   2. raw（单向裸流）：直接 dump 一个方向的所有字节（Frida / socat -x 转出的二进制）
 *   3. hex（文本十六进制）：每行 "aa bb cc" / "aabbcc" / "aa:bb:cc"，忽略 # 注释
 *
 * 用法：
 *   node tools/dump_parser.js capture.ipcd                # 自动识别格式
 *   node tools/dump_parser.js raw.bin --format raw --dir A→B
 *   node tools/dump_parser.js frida.txt --format hex
 *   node tools/dump_parser.js capture.ipcd --json         # NDJSON，便于二次处理
 *   node tools/dump_parser.js capture.ipcd --stats        # 结尾打印统计
 *   type dump.bin | node tools/dump_parser.js - --format raw
 *
 * 选项：
 *   --format auto|framed|raw|hex   默认 auto（看魔数 IPCD）
 *   --crc auto|on|off              默认 auto（逐帧用长度头 CRC 自检判定）
 *   --dir <label>                  raw 模式下方向标签，默认 "?"
 *   --json                         输出 NDJSON
 *   --stats                        输出统计汇总
 *   --max-body <n>                 单条报文最多打印的 payload 字符，默认 200
 *   --max-frame <n>                允许的最大帧长度，默认 268435456（256MiB）
 *   --limit <n>                    最多解析多少条报文
 */
'use strict';

import fs from 'node:fs';
import { fileURLToPath } from 'node:url';

const TYPES = { 0: 'REQUEST', 1: 'RESPONSE', 2: 'EVENT', 3: 'PING', 4: 'PONG', 5: 'HELLO', 6: 'HELLO_ACK' };
const CONTENT_TYPES = { 0: 'TEXT', 1: 'JSON', 2: 'BINARY' };
const ERROR_TYPES = { 1: 'LENGTH_CRC_FAILED', 2: 'MESSAGE_CRC_FAILED', 3: 'BUFFER_OVERFLOW' };
const MAGIC = Buffer.from('IPCD');
const PROTOCOL_HEADER = 0xffffffff;
const ERROR_HEADER = 0xeeeeeeee;
const MESSAGE_HEADER = 18; // type + contentType + uuid16

// ---------------- CRC32（多项式 0xEDB88320，等价 zlib.crc32） ----------------
const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[i] = c >>> 0;
  }
  return table;
})();

function crc32(buf) {
  let crc = 0xffffffff;
  for (let i = 0; i < buf.length; i++) crc = CRC_TABLE[(crc ^ buf[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

// ---------------- 基础工具 ----------------
function uuidFromBytes(buf, offset) {
  const hex = buf.subarray(offset, offset + 16).toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function preview(buf, max) {
  if (buf.length === 0) return '';
  const text = buf.toString('utf8');
  // 用「解码后有无替换字符 + 控制字符占比」判断是否为可读文本（对中文等非 ASCII 友好）
  if (!text.includes('\uFFFD')) {
    let control = 0;
    for (const ch of text) {
      const code = ch.codePointAt(0);
      if (code < 0x20 && !'\n\r\t'.includes(ch)) control++;
    }
    if (control / Math.max(1, text.length) < 0.05) {
      return text.length > max ? `${text.slice(0, max)}…(+${text.length - max}字符)` : text;
    }
  }
  const hex = buf.subarray(0, Math.min(buf.length, max)).toString('hex').replace(/(..)/g, '$1 ').trim();
  return buf.length > max ? `${hex}…(+${buf.length - max}B)` : hex;
}

function hexDumpHead(buf, n = 16) {
  const head = buf.subarray(0, Math.min(buf.length, n));
  const hex = [...head].map((b) => b.toString(16).padStart(2, '0')).join(' ');
  return `<${buf.length}B ${hex}${buf.length > n ? ' …' : ''}>`;
}

// ---------------- 帧扫描 ----------------
/**
 * 在 buf 的一段里逐帧切分。返回 frames 与 anomalies。
 * 帧结构：len u32LE [+ lenCrc u32LE][data][dataCrc u32LE]
 */
function scanFrames(buf, { crcMode = 'auto', maxFrame = 268435456 } = {}) {
  const frames = [];
  const anomalies = [];
  let forcedCrc = crcMode === 'on' ? true : crcMode === 'off' ? false : null;
  let offset = 0;

  while (offset < buf.length) {
    if (buf.length - offset < 4) {
      anomalies.push({ offset, reason: `尾部残留 ${buf.length - offset}B，不足长度头` });
      break;
    }
    // 重同步协议消息：裸字节，没有普通帧头
    if (buf.readUInt32LE(offset) === PROTOCOL_HEADER) {
      if (buf.length - offset < 8) {
        anomalies.push({ offset, reason: '重同步消息被截断' });
        break;
      }
      const len = buf.readUInt32LE(offset + 4);
      if (buf.length - offset < 8 + len) {
        anomalies.push({ offset, reason: `重同步消息声明 ${len}B 但数据不足` });
        break;
      }
      frames.push({
        offset,
        kind: 'RESYNC',
        data: buf.subarray(offset + 8, offset + 8 + len),
        rawLength: 8 + len,
        crcEnabled: false,
      });
      offset += 8 + len;
      continue;
    }

    const len = buf.readUInt32LE(offset);
    if (len > maxFrame) {
      anomalies.push({ offset, reason: `长度头异常（${len}B > 上限 ${maxFrame}B）` });
      offset = resync(buf, offset + 1, maxFrame);
      if (offset < 0) break;
      continue;
    }

    // 判定是否带 CRC：长度头 CRC 自校验
    const hasLenCrc = buf.length - offset >= 8
      && buf.readUInt32LE(offset + 4) === crc32(buf.subarray(offset, offset + 4));
    const crcEnabled = forcedCrc === null ? hasLenCrc : forcedCrc;
    const need = 4 + (crcEnabled ? 4 : 0) + len + (crcEnabled ? 4 : 0);

    if (buf.length - offset < need) {
      // 可能是抓包尾部被截断，也可能是错位：尝试向后找下一个可信帧
      const next = resync(buf, offset + 1, maxFrame);
      if (next > 0) {
        anomalies.push({ offset, reason: `帧声明 ${len}B 但剩余不足，跳过 ${next - offset}B 重新对齐` });
        offset = next;
        continue;
      }
      anomalies.push({ offset, reason: `帧声明 ${len}B 但剩余仅 ${buf.length - offset}B（抓包可能被截断）` });
      break;
    }

    const dataStart = offset + 4 + (crcEnabled ? 4 : 0);
    const data = buf.subarray(dataStart, dataStart + len);
    let dataCrcOk = null;
    if (crcEnabled) {
      const expect = buf.readUInt32LE(dataStart + len);
      dataCrcOk = expect === crc32(data);
      if (crcEnabled && forcedCrc === null && !dataCrcOk && hasLenCrc) {
        // 长度头对得上、数据 CRC 对不上 → 真损坏，如实记录
      }
    }

    frames.push({ offset, kind: 'FRAME', data, rawLength: need, crcEnabled, dataCrcOk });
    offset += need;
  }

  return { frames, anomalies };
}

/** 从 start 起寻找下一个可信帧起点；找不到返回 -1。 */
function resync(buf, start, maxFrame) {
  for (let i = start; i + 4 <= buf.length; i++) {
    if (buf.readUInt32LE(i) === PROTOCOL_HEADER) return i;
    const len = buf.readUInt32LE(i);
    if (len === 0 || len > maxFrame) continue;
    if (i + 8 <= buf.length && buf.readUInt32LE(i + 4) === crc32(buf.subarray(i, i + 4))) {
      if (i + 8 + len + 4 <= buf.length) return i;
    }
  }
  return -1;
}

// ---------------- 报文解析 ----------------
function parseBinaryRequest(payload) {
  // [methodLen u32BE][method][paramsLen u32BE][params JSON][binary...]
  if (payload.length < 8) return null;
  const methodLen = payload.readUInt32BE(0);
  if (payload.length < 4 + methodLen + 4) return null;
  const method = payload.subarray(4, 4 + methodLen).toString('utf8');
  const paramsLen = payload.readUInt32BE(4 + methodLen);
  const paramsStart = 4 + methodLen + 4;
  if (payload.length < paramsStart + paramsLen) return null;
  const paramsText = payload.subarray(paramsStart, paramsStart + paramsLen).toString('utf8');
  let params = paramsText;
  try { params = JSON.parse(paramsText); } catch (_) { /* 保留原文 */ }
  return { method, params, binary: payload.subarray(paramsStart + paramsLen) };
}

function parseMessage(data, maxBody) {
  if (data.length >= 5 && data.readUInt32LE(0) === ERROR_HEADER) {
    const code = data[4];
    return { kind: 'ERROR_PACKET', errorCode: code, errorName: ERROR_TYPES[code] || `UNKNOWN(${code})` };
  }
  if (data.length < MESSAGE_HEADER) {
    return { kind: 'SHORT', note: `报文不足 ${MESSAGE_HEADER}B（实际 ${data.length}B）` };
  }
  const typeCode = data[0];
  const contentTypeCode = data[1];
  const type = TYPES[typeCode] || `TYPE_${typeCode}`;
  const contentType = CONTENT_TYPES[contentTypeCode] || `CT_${contentTypeCode}`;
  const id = uuidFromBytes(data, 2);
  const payload = data.subarray(MESSAGE_HEADER);
  const msg = { kind: 'MESSAGE', type, typeCode, contentType, id };

  if (typeCode === 3 || typeCode === 4) { // PING / PONG 无业务载荷
    msg.summary = '';
    return msg;
  }

  if (contentTypeCode === 2) { // BINARY
    msg.payloadBytes = payload.length;
    msg.binaryHead = hexDumpHead(payload);
    if (typeCode === 0) {
      const parsed = parseBinaryRequest(payload);
      if (parsed) {
        msg.method = parsed.method;
        msg.params = parsed.params;
        msg.binaryBytes = parsed.binary.length;
        msg.binaryHead = hexDumpHead(parsed.binary);
        msg.summary = `${parsed.method} params=${preview(Buffer.from(JSON.stringify(parsed.params), 'utf8'), maxBody)}`
          + ` binary=${msg.binaryHead}`;
      } else {
        msg.note = '二进制请求体解析失败';
        msg.summary = `binary=${msg.binaryHead}`;
      }
      return msg;
    }
    msg.summary = `=> ${msg.binaryHead}`;
    return msg;
  }

  const text = payload.toString('utf8');
  if (contentTypeCode === 1) { // JSON
    let json;
    try { json = JSON.parse(text); } catch (err) { msg.summary = preview(payload, maxBody); msg.note = 'JSON 解析失败'; return msg; }
    if (typeof json === 'object' && json !== null && !Array.isArray(json)) {
      if (json.error !== undefined && typeCode === 1) {
        msg.error = json.error;
        msg.summary = `error=${preview(Buffer.from(JSON.stringify(json.error), 'utf8'), maxBody)}`;
        return msg;
      }
      if (json.method !== undefined) {
        msg.method = json.method;
        msg.params = json.params;
        if (json.to !== undefined) msg.to = json.to;
        msg.summary = `${json.method} params=${preview(Buffer.from(JSON.stringify(json.params), 'utf8'), maxBody)}`
          + (msg.to ? ` to=${preview(Buffer.from(String(json.to), 'utf8'), 40)}` : '');
        return msg;
      }
      if (typeCode === 5 || typeCode === 6) { // HELLO / HELLO_ACK
        const caps = Array.isArray(json.caps) ? json.caps.join('|') : '-';
        msg.summary = `impl=${json.impl ?? '-'} version=${json.version ?? '-'} caps=${caps}`
          + (json.token !== undefined ? ' token=<已隐去>' : '');
        return msg;
      }
    }
    msg.summary = (typeCode === 1 ? '=> ' : '') + preview(payload, maxBody);
    return msg;
  }

  // TEXT
  msg.summary = (typeCode === 1 ? '=> ' : '') + preview(payload, maxBody);
  return msg;
}

// ---------------- 输入读取 ----------------
function readInput(file) {
  if (file === '-') {
    return fs.readFileSync(0); // stdin
  }
  if (!fs.existsSync(file)) throw new Error(`文件不存在: ${file}`);
  return fs.readFileSync(file);
}

function parseHexText(buf) {
  const lines = buf.toString('utf8').split(/\r?\n/);
  const out = [];
  for (const line of lines) {
    const clean = line.replace(/#.*$/, '').trim();
    if (!clean) continue;
    const tokens = clean.match(/[0-9a-fA-F]{2}/g);
    if (tokens) for (const t of tokens) out.push(parseInt(t, 16));
  }
  return Buffer.from(out);
}

/** 解析 framed 抓包：返回 [{dir, tsMs, data}] */
function parseFramedDump(buf, { maxFrame }) {
  if (buf.length < 8) throw new Error('抓包文件过短，不是有效 IPCD 文件');
  if (!buf.subarray(0, 4).equals(MAGIC)) throw new Error('缺少 IPCD 魔数');
  let offset = 8;
  const records = [];
  while (offset + 13 <= buf.length) {
    const dir = buf[offset];
    const len = buf.readUInt32LE(offset + 1);
    const tsMs = buf.readDoubleLE(offset + 5);
    const start = offset + 13;
    if (len > maxFrame) throw new Error(`记录声明 ${len}B 超过上限，抓包已损坏（offset=${offset}）`);
    if (start + len > buf.length) {
      records.push({ dir, tsMs, data: buf.subarray(start), truncated: true });
      break;
    }
    records.push({ dir, tsMs, data: buf.subarray(start, start + len) });
    offset = start + len;
  }
  return records;
}

function dirLabel(dir, rawLabel) {
  if (dir === 1) return 'A→B';
  if (dir === 2) return 'B→A';
  if (dir === 0x53) return '发→';
  if (dir === 0x52) return '→收';
  return rawLabel || '?';
}

// ---------------- 主流程 ----------------
function main(argv) {
  const opts = {
    file: null, format: 'auto', crc: 'auto', dir: '?', json: false, stats: false,
    maxBody: 200, maxFrame: 256 * 1024 * 1024, limit: Infinity,
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const need = () => {
      if (i + 1 >= argv.length) throw new Error(`${arg} 缺少参数`);
      return argv[++i];
    };
    switch (arg) {
      case '--format': opts.format = need(); break;
      case '--crc': opts.crc = need(); break;
      case '--dir': opts.dir = need(); break;
      case '--max-body': opts.maxBody = Number(need()); break;
      case '--max-frame': opts.maxFrame = Number(need()); break;
      case '--limit': opts.limit = Number(need()); break;
      case '--json': opts.json = true; break;
      case '--stats': opts.stats = true; break;
      case '-h': case '--help': printHelp(); return 0;
      default:
        if (arg.startsWith('-') && arg !== '-') throw new Error(`未知选项 ${arg}`);
        opts.file = arg;
    }
  }
  if (!opts.file) { printHelp(); return 1; }
  if (!['auto', 'framed', 'raw', 'hex'].includes(opts.format)) throw new Error(`不支持的 --format ${opts.format}`);
  if (!['auto', 'on', 'off'].includes(opts.crc)) throw new Error(`不支持的 --crc ${opts.crc}`);

  let buf = readInput(opts.file);
  let format = opts.format;
  if (format === 'auto') format = buf.subarray(0, 4).equals(MAGIC) ? 'framed' : 'raw';

  const stats = { records: 0, frames: 0, messages: 0, bytes: 0, byType: {}, badCrc: 0, anomalies: 0 };
  const entries = [];

  const addParsed = (meta, frame) => {
    stats.frames++;
    if (frame.crcEnabled && frame.dataCrcOk === false) stats.badCrc++;
    const parsed = frame.kind === 'RESYNC'
      ? { kind: 'RESYNC', data: frame.data }
      : parseMessage(frame.data, opts.maxBody);
    if (parsed.kind === 'MESSAGE') stats.byType[parsed.type] = (stats.byType[parsed.type] || 0) + 1;
    entries.push({ tsMs: meta.tsMs, dir: meta.dir, dirLabel: meta.dirLabel, crcEnabled: frame.crcEnabled, dataCrcOk: frame.dataCrcOk, entry: parsed });
  };

  if (format === 'framed') {
    const records = parseFramedDump(buf, opts);
    // 抓包是按「一次 write 一条记录」写的，一帧可能被拆成多次 write，
    // 因此先按方向合并成连续流，再逐帧切分，帧的起始偏移回查时间戳。
    const streams = new Map();
    for (const record of records) {
      stats.records++;
      stats.bytes += record.data.length;
      let stream = streams.get(record.dir);
      if (!stream) { stream = { chunks: [], offset: 0, spans: [] }; streams.set(record.dir, stream); }
      stream.chunks.push(record.data);
      stream.spans.push({ start: stream.offset, end: stream.offset + record.data.length, tsMs: record.tsMs });
      stream.offset += record.data.length;
    }
    for (const [dir, stream] of streams) {
      const concat = Buffer.concat(stream.chunks);
      const tsAt = (offset) => {
        for (const span of stream.spans) if (offset < span.end) return span.tsMs;
        return stream.spans.length ? stream.spans[stream.spans.length - 1].tsMs : null;
      };
      const { frames, anomalies } = scanFrames(concat, { crcMode: opts.crc, maxFrame: opts.maxFrame });
      for (const anomaly of anomalies) {
        stats.anomalies++;
        entries.push({
          tsMs: tsAt(anomaly.offset), dir,
          entry: { kind: 'ANOMALY', offset: anomaly.offset, note: anomaly.reason },
        });
      }
      for (const frame of frames) addParsed({ tsMs: tsAt(frame.offset), dir }, frame);
    }
    if (stats.records === 0) { console.log('(没有可解析的记录)'); return finish(); }
  } else {
    const stream = format === 'hex' ? parseHexText(buf) : buf;
    stats.bytes = stream.length;
    const { frames, anomalies } = scanFrames(stream, { crcMode: opts.crc, maxFrame: opts.maxFrame });
    for (const anomaly of anomalies) {
      stats.anomalies++;
      entries.push({ tsMs: null, dir: 0, entry: { kind: 'ANOMALY', offset: anomaly.offset, note: anomaly.reason } });
    }
    for (const frame of frames) addParsed({ tsMs: null, dir: 0 }, frame);
    if (frames.length === 0 && anomalies.length === 0) { console.log('(没有可解析的帧)'); return finish(); }
  }

  // framed 抓包两个方向的记录交替写入，按时间戳回放才能还原真实先后
  if (format === 'framed') {
    entries.sort((a, b) => (a.tsMs ?? 0) - (b.tsMs ?? 0));
  }

  let seq = 0;
  let t0 = null;

  const emit = (meta, entry) => {
    const label = meta.dirLabel || dirLabel(meta.dir, opts.dir);
    const rel = t0 === null ? 0 : Math.max(0, meta.tsMs - t0);
    if (t0 === null) t0 = meta.tsMs || 0;
    const timeTag = meta.tsMs ? `+${(rel / 1000).toFixed(3)}s` : '';

    if (entry.kind === 'ANOMALY') {
      console.log(`! 偏移 ${entry.offset}: ${entry.note}`);
      return;
    }
    seq++;
    stats.messages++;
    if (opts.json) {
      console.log(JSON.stringify({ seq, dir: label, tMs: meta.tsMs ?? null, type: entry.type || entry.kind, ...entry }));
      return;
    }
    const head = `#${String(seq).padStart(3, '0')} ${timeTag.padStart(9)} ${label.padEnd(5)}`;
    if (entry.kind === 'ERROR_PACKET') {
      console.log(`${head} ERROR_PACKET  ${entry.errorName}`);
      return;
    }
    if (entry.kind === 'RESYNC') {
      console.log(`${head} RESYNC        ${preview(entry.data, opts.maxBody) || '(空)'}`);
      return;
    }
    if (entry.kind === 'SHORT') {
      console.log(`${head} MALFORMED     ${entry.note}`);
      return;
    }
    const crcTag = meta.crcEnabled ? (meta.dataCrcOk === false ? ' [CRC✗]' : '') : ' [no-crc]';
    const typeTag = entry.type.padEnd(9);
    const idTag = entry.typeCode === 3 || entry.typeCode === 4 ? '' : ` id=${String(entry.id).slice(0, 8)}`;
    console.log(`${head} ${typeTag} ${entry.summary ?? ''}${idTag}${crcTag}`);
  };

  for (const item of entries) {
    if (seq >= opts.limit) break;
    emit(item, item.entry);
  }

  return finish();

  function finish() {
    if (opts.stats) {
      const types = Object.entries(stats.byType).map(([k, v]) => `${k}=${v}`).join(', ') || '无';
      console.error(`\n统计: 输入格式=${format} 记录=${stats.records} 帧=${stats.frames} 报文=${stats.messages} `
        + `字节=${stats.bytes} 坏CRC=${stats.badCrc} 异常=${stats.anomalies} 类型分布={${types}}`);
    }
    return 0;
  }
}

function printHelp() {
  console.log(`本机 IPC 流量解析器

用法:
  node tools/dump_parser.js <file|-> [选项]

选项:
  --format auto|framed|raw|hex   输入格式，默认 auto（按 IPCD 魔数自动判定）
  --crc auto|on|off              帧是否带 CRC，默认 auto（用长度头 CRC 自检逐帧判定）
  --dir <label>                  raw 模式的方向标签，默认 "?"
  --json                         输出 NDJSON
  --stats                        结束后打印统计
  --max-body <n>                 单条报文最多打印的 payload 字符，默认 200
  --max-frame <n>                允许的最大帧长度，默认 256MiB
  --limit <n>                    最多输出多少条报文

输入格式:
  framed   IPCD 文件：魔数 + 版本(1B) + 保留(3B)，记录为 dir(1B)+len(u32LE)+tsMs(f64LE)+payload
  raw      单向裸字节流（Frida / 独立抓包脚本导出的二进制）
  hex      文本十六进制（"aa bb cc" / "aabbcc" / "aa:bb:cc"，# 起注释），typ. Frida 日志

示例:
  node tools/dump_parser.js capture.ipcd --stats
  node tools/dump_parser.js raw.bin --format raw --dir "Node→Py"
  node tools/dump_parser.js frida.txt --format hex --json
  type dump.bin | node tools/dump_parser.js - --format raw
`);
}

const isMain = process.argv[1]
  && fs.realpathSync(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  try {
    process.exitCode = main(process.argv.slice(2));
  } catch (err) {
    console.error(`解析失败: ${err.message}`);
    process.exitCode = 2;
  }
}

export { crc32, scanFrames, parseMessage, parseFramedDump };
