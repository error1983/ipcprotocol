// examples/ws-relay-gateway.js — 透明 WebSocket 中继网关（零依赖）
//
// 作用：把本仓库的 IPC 协议帧原样搬到 WebSocket 上，让「浏览器 ↔ 浏览器」「浏览器 ↔ 本机进程」
// 也能互相 RPC。网关只认路由信封，**从不解析 IPC 帧**——所以 PROTOCOL.md 不用改，
// tools/dump_parser.js 抓到的帧依然能直接解析。
//
// 路由信封（字节，上行下行同一格式）：
//   [srcLen u8][src utf8][dstLen u8][dst utf8][原始 IPC 字节流...]
//   · 上行：src 由网关用真实 peerId 覆盖（客户端填什么都不采信），dst 为目标 peerId；
//           dstLen=0（或 dst="*"）表示广播给房间内其他所有 peer。
//   · 下行：src=发送方 peerId，dst=接收方 peerId。
//   · 信封只负责寻址，里面的字节就是 MessageStream 的帧，一个字节都不动。
//
// 用法:
//   node examples/ws-relay-gateway.js --port 9100
//   ws://127.0.0.1:9100/?peer=alice        # 客户端用 ?peer= 声明自己的 id
//   curl http://127.0.0.1:9100/peers       # 查看在线 peer（调试用）
//
// 只实现了本仓库需要的那部分 WebSocket（RFC6455）：handshake、二进制帧、分片、ping/pong、close。
// 生产环境建议换成 `ws` 库；这里保持零依赖，方便直接跑。
import http from 'node:http';
import crypto from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const OPCODE = { CONT: 0x0, TEXT: 0x1, BINARY: 0x2, CLOSE: 0x8, PING: 0x9, PONG: 0xa };
const DEFAULT_MAX_BYTES = 32 * 1024 * 1024;

// ---------------- 单个 WebSocket 连接 ----------------
class WsPeer {
    constructor(socket, id, relay, maxBytes) {
        this.socket = socket;
        this.id = id;
        this.relay = relay;
        this.maxBytes = maxBytes;
        this.joinedAt = Date.now();
        this.framesIn = 0;
        this.framesOut = 0;
        this.bytesIn = 0;
        this._buf = Buffer.alloc(0);
        this._fragOpcode = null;
        this._frag = [];
        this._closed = false;

        socket.setNoDelay?.(true);
        socket.on('data', (chunk) => this._onData(chunk));
        socket.on('error', (err) => this.relay.log(`[${this.id}] socket error: ${err.message}`));
        socket.on('close', () => this._cleanup());
    }

    // ---- 帧解析 ----
    _onData(chunk) {
        if (this._closed) return;
        this._buf = this._buf.length ? Buffer.concat([this._buf, chunk]) : chunk;
        this.bytesIn += chunk.length;

        while (this._buf.length >= 2) {
            const b0 = this._buf[0];
            const b1 = this._buf[1];
            const fin = (b0 & 0x80) !== 0;
            const opcode = b0 & 0x0f;
            const masked = (b1 & 0x80) !== 0;
            let len = b1 & 0x7f;
            let offset = 2;

            if (len === 126) {
                if (this._buf.length < 4) return;
                len = this._buf.readUInt16BE(2);
                offset = 4;
            } else if (len === 127) {
                if (this._buf.length < 10) return;
                const big = this._buf.readBigUInt64BE(2);
                if (big > BigInt(this.maxBytes)) { this.close(1009, 'message too big'); return; }
                len = Number(big);
                offset = 10;
            }
            if (len > this.maxBytes) { this.close(1009, 'message too big'); return; }

            let mask = null;
            if (masked) {
                if (this._buf.length < offset + 4) return;
                mask = this._buf.subarray(offset, offset + 4);
                offset += 4;
            }
            if (this._buf.length < offset + len) return;   // 半包，等下一段

            let payload = this._buf.subarray(offset, offset + len);
            if (masked) {
                const out = Buffer.allocUnsafe(len);
                for (let i = 0; i < len; i++) out[i] = payload[i] ^ mask[i & 3];
                payload = out;
            }
            this._buf = this._buf.subarray(offset + len);
            this._handleFrame(fin, opcode, payload);
            if (this._closed) return;
        }
    }

    _handleFrame(fin, opcode, payload) {
        if (opcode === OPCODE.PING) { this._sendFrame(OPCODE.PONG, payload); return; }
        if (opcode === OPCODE.PONG) return;
        if (opcode === OPCODE.CLOSE) { this.close(1000, ''); return; }
        if (opcode === OPCODE.TEXT || (opcode === OPCODE.CONT && this._fragOpcode === OPCODE.TEXT)) {
            // 信封必须是二进制；文本帧说明对面不是我们的适配器
            this.close(1003, 'binary frames only');
            return;
        }

        if (opcode === OPCODE.BINARY) {
            this._fragOpcode = OPCODE.BINARY;
            this._frag = [payload];
        } else if (opcode === OPCODE.CONT) {
            if (this._fragOpcode !== OPCODE.BINARY) { this.close(1002, 'bad continuation'); return; }
            this._frag.push(payload);
        } else {
            this.close(1002, `bad opcode ${opcode}`);
            return;
        }

        if (!fin) return;   // 分片未结束
        const body = this._frag.length === 1 ? this._frag[0] : Buffer.concat(this._frag);
        this._frag = [];
        this._fragOpcode = null;
        this.framesIn++;
        this.relay.route(this, body);
    }

    // ---- 发送 ----
    _sendFrame(opcode, payload) {
        if (this._closed || this.socket.destroyed) return false;
        const len = payload.length;
        let header;
        if (len < 126) {
            header = Buffer.allocUnsafe(2);
            header[1] = len;
        } else if (len < 65536) {
            header = Buffer.allocUnsafe(4);
            header[1] = 126;
            header.writeUInt16BE(len, 2);
        } else {
            header = Buffer.allocUnsafe(10);
            header[1] = 127;
            header.writeBigUInt64BE(BigInt(len), 2);
        }
        header[0] = 0x80 | opcode;   // FIN + opcode，服务端不掩码
        this.framesOut++;
        const ok = this.socket.write(Buffer.concat([header, payload]));
        if (!ok) this.socket.once('drain', () => { });   // 让内核自己缓冲；小流量无需额外处理
        return ok;
    }

    sendBinary(buf) {
        return this._sendFrame(OPCODE.BINARY, buf);
    }

    close(code = 1000, reason = '') {
        if (this._closed) return;
        this._closed = true;
        const body = Buffer.allocUnsafe(2 + Buffer.byteLength(reason));
        body.writeUInt16BE(code, 0);
        body.write(reason, 2);
        try { this._sendFrameRaw(OPCODE.CLOSE, body); } catch { /* 已断开 */ }
        try { this.socket.end(); } catch { /* ignore */ }
        this._cleanup();
    }

    _sendFrameRaw(opcode, payload) {
        const len = payload.length;
        const header = len < 126 ? Buffer.allocUnsafe(2) : Buffer.allocUnsafe(4);
        if (len < 126) header[1] = len;
        else { header[1] = 126; header.writeUInt16BE(len, 2); }
        header[0] = 0x80 | opcode;
        if (!this.socket.destroyed) this.socket.write(Buffer.concat([header, payload]));
    }

    _cleanup() {
        if (this.relay.peers.get(this.id) === this) this.relay.peers.delete(this.id);
        this._buf = Buffer.alloc(0);
        this._frag = [];
    }

    status() {
        return { id: this.id, joinedAt: this.joinedAt, framesIn: this.framesIn, framesOut: this.framesOut, bytesIn: this.bytesIn };
    }
}

// ---------------- 中继本体 ----------------
export class Relay {
    constructor({ maxMessageBytes = DEFAULT_MAX_BYTES, quiet = false } = {}) {
        this.peers = new Map();
        this.maxMessageBytes = maxMessageBytes;
        this.quiet = quiet;
        this.dropped = 0;
        this.unroutable = 0;
        this.unroutedBytes = 0;
        this.server = null;
    }

    log(msg) {
        if (!this.quiet) console.log(`[relay] ${msg}`);
    }

    /** 解析信封并转发；此处只碰信封，不碰 IPC 帧内容 */
    route(from, body) {
        if (body.length < 2) { this._drop('信封过短', from); return; }
        const srcLen = body[0];
        if (body.length < 1 + srcLen + 1) { this._drop('信封 src 长度越界', from); return; }
        const dstLen = body[1 + srcLen];
        const dstStart = 1 + srcLen + 1;
        const rawStart = dstStart + dstLen;
        if (body.length < rawStart) { this._drop('信封 dst 长度越界', from); return; }

        const dst = body.toString('utf8', dstStart, rawStart);
        const raw = body.subarray(rawStart);
        if (raw.length === 0) { this._drop('空载荷', from); return; }

        const srcBuf = Buffer.from(from.id, 'utf8');

        if (dst === '' || dst === '*') {          // 广播
            for (const peer of this.peers.values()) {
                if (peer === from) continue;
                peer.sendBinary(this._wrap(srcBuf, dst.length ? Buffer.from(dst, 'utf8') : Buffer.alloc(0), raw));
            }
            return;
        }

        const target = this.peers.get(dst);
        if (!target) {
            // 目标不存在：网关不伪造响应（那会污染协议流），交给调用方自己的超时兜底。
            // 注意计数单位是「WS 消息块」——一个 IPC 帧可能被拆成 head/payload/tail 多块写入。
            this.unroutable++;
            this.unroutedBytes += raw.length;
            if (this.unroutable <= 3 || this.unroutable % 50 === 0) {
                this.log(`目标 ${JSON.stringify(dst)} 不在线，丢弃（累计 ${this.unroutable} 块 / ${this.unroutedBytes}B）`);
            }
            return;
        }
        target.sendBinary(this._wrap(srcBuf, Buffer.from(dst, 'utf8'), raw));
    }

    _wrap(srcBuf, dstBuf, raw) {
        const head = Buffer.allocUnsafe(1 + srcBuf.length + 1 + dstBuf.length);
        head[0] = srcBuf.length;
        srcBuf.copy(head, 1);
        head[1 + srcBuf.length] = dstBuf.length;
        dstBuf.copy(head, 2 + srcBuf.length);
        return Buffer.concat([head, raw]);
    }

    _drop(reason, from) {
        this.dropped++;
        if (this.dropped <= 3) this.log(`${from?.id || '?'} 的信封被丢弃：${reason}`);
    }
}

/**
 * 启动中继网关。
 * @param {Object} [options]
 * @param {string} [options.host='127.0.0.1'] 监听地址；要跨机才用 0.0.0.0（注意没有 TLS）
 * @param {number} [options.port=9100] 端口，0 表示自动分配
 * @param {number} [options.maxMessageBytes] 单条 WS 消息上限
 * @param {boolean} [options.quiet] 不打印日志（测试用）
 * @returns {Promise<{port:number, host:string, relay:Relay, close:Function}>}
 */
export function createRelay(options = {}) {
    const { host = '127.0.0.1', port = 9100, maxMessageBytes = DEFAULT_MAX_BYTES, quiet = false } = options;
    const relay = new Relay({ maxMessageBytes, quiet });

    const server = http.createServer((req, res) => {
        const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
        if (url.pathname === '/peers') {
            const list = [...relay.peers.values()].map((p) => p.status());
            res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
            res.end(JSON.stringify({ count: list.length, peers: list, unroutable: relay.unroutable, unroutedBytes: relay.unroutedBytes, dropped: relay.dropped }));
            return;
        }
        res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
        res.end('ipcprotocol ws relay\n用 ws://host:port/?peer=<id> 接入，GET /peers 查看在线节点\n');
    });

    server.on('upgrade', (req, socket) => {
        const reject = (code, text, body) => {
            socket.write(`HTTP/1.1 ${code} ${text}\r\nConnection: close\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
            socket.destroy();
        };

        let url;
        try { url = new URL(req.url, 'http://localhost'); } catch { reject(400, 'Bad Request', 'bad url'); return; }

        const peerId = (url.searchParams.get('peer') || '').trim();
        const key = req.headers['sec-websocket-key'];
        if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') {
            reject(400, 'Bad Request', 'expect websocket upgrade');
            return;
        }
        if (!peerId) { reject(400, 'Bad Request', 'missing ?peer=<id>'); return; }
        if (Buffer.byteLength(peerId) > 255) { reject(400, 'Bad Request', 'peer id too long'); return; }
        if (relay.peers.has(peerId)) { reject(409, 'Conflict', `peer id "${peerId}" already online`); return; }

        const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
        socket.write([
            'HTTP/1.1 101 Switching Protocols',
            'Upgrade: websocket',
            'Connection: Upgrade',
            `Sec-WebSocket-Accept: ${accept}`,
            '', '',
        ].join('\r\n'));

        const peer = new WsPeer(socket, peerId, relay, maxMessageBytes);
        relay.peers.set(peerId, peer);
        relay.log(`+ ${peerId}（在线 ${relay.peers.size}）`);
        socket.on('close', () => relay.log(`- ${peerId}（在线 ${relay.peers.size}）`));
    });

    relay.server = server;

    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, host, () => {
            const actual = server.address().port;
            relay.log(`listening ws://${host}:${actual}/?peer=<id>  （GET /peers 查看节点）`);
            resolve({
                port: actual,
                host,
                relay,
                close: () => new Promise((done) => {
                    for (const peer of [...relay.peers.values()]) peer.close(1001, 'gateway closing');
                    server.close(() => done());
                }),
            });
        });
    });
}

// ---------------- CLI ----------------
const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
    const argv = process.argv.slice(2);
    const opt = { host: '127.0.0.1', port: 9100 };
    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === '--port') opt.port = Number(argv[++i]);
        else if (argv[i] === '--host') opt.host = argv[++i];
        else if (argv[i] === '--help' || argv[i] === '-h') {
            console.log('用法: node examples/ws-relay-gateway.js [--host 127.0.0.1] [--port 9100]');
            process.exit(0);
        }
    }
    createRelay(opt).then(({ port, host }) => {
        console.log(JSON.stringify({ port, host }));
    }).catch((err) => {
        console.error('网关启动失败:', err.message);
        process.exit(1);
    });
}
