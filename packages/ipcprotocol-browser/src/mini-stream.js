// examples/browser/mini-stream.js — 浏览器侧帧层（纯 Uint8Array，无 Node 依赖）
//
// 与 MessageStream.js 线格式完全一致，可与之互通：
//   CRC 开:  [len u32LE][lenCrc u32LE][payload][payloadCrc u32LE]
//   CRC 关:  [len u32LE][payload]
//   错误包:  EE EE EE EE [code u8]      （作为普通帧的载荷发送）
//   重同步:  FF FF FF FF [len u32LE][utf8]（如 RESET_CONFIRM，裸帧无 CRC）
//
// 和 Node 版的差异（都是有意的）：
//   1. 不做异步 IO，数据由 link 的 'data' 事件推进；
//   2. 整帧合并成一次 write（Node 版是 head/payload/tail 三次），减少 WS 消息数；
//   3. 缓冲区用 Uint8Array 拼接，不做零拷贝队列——浏览器场景数据量小，优先可读性。
//
// 用法：
//   const stream = new MiniMessageStream(link);          // link 需有 on('data') / write() / destroy()
//   stream.on('message', (bytes) => { ... });            // 一整帧（未解析）
//   stream.send(frameBytes);                             // 自动加长度头与 CRC

const ERROR_CODES = { LENGTH_CRC_FAILED: 1, MESSAGE_CRC_FAILED: 2, BUFFER_OVERFLOW: 3 };
const ERROR_NAMES = { 1: 'LENGTH_CRC_FAILED', 2: 'MESSAGE_CRC_FAILED', 3: 'BUFFER_OVERFLOW' };
const PROTOCOL_HEADER = [0xff, 0xff, 0xff, 0xff];
const ERROR_HEADER = [0xee, 0xee, 0xee, 0xee];
const MAX_PROTOCOL_MSG = 50;

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

export function crc32(bytes) {
    let crc = 0xffffffff;
    for (let i = 0; i < bytes.length; i++) crc = CRC_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
    return (crc ^ 0xffffffff) >>> 0;
}

// ---------------- 小工具 ----------------
function concat(a, b) {
    if (!a.length) return b;
    if (!b.length) return a;
    const out = new Uint8Array(a.length + b.length);
    out.set(a, 0);
    out.set(b, a.length);
    return out;
}

function readU32(bytes, offset) {
    return new DataView(bytes.buffer, bytes.byteOffset + offset, 4).getUint32(0, true);
}

function writeU32(bytes, offset, value) {
    new DataView(bytes.buffer, bytes.byteOffset + offset, 4).setUint32(0, value, true);
}

function startsWith(bytes, pattern) {
    if (bytes.length < pattern.length) return false;
    for (let i = 0; i < pattern.length; i++) if (bytes[i] !== pattern[i]) return false;
    return true;
}

class Emitter {
    constructor() { this._map = new Map(); }
    on(event, fn) {
        if (!this._map.has(event)) this._map.set(event, []);
        this._map.get(event).push(fn);
        return this;
    }
    off(event, fn) {
        const list = this._map.get(event);
        if (!list) return this;
        const idx = list.indexOf(fn);
        if (idx >= 0) list.splice(idx, 1);
        return this;
    }
    listenerCount(event) { return (this._map.get(event) || []).length; }
    emit(event, ...args) {
        for (const fn of [...(this._map.get(event) || [])]) {
            try { fn(...args); } catch (err) { console.error(`[MiniMessageStream] '${event}' 监听器异常:`, err); }
        }
    }
}

function ipcError(message, code) {
    const err = new Error(message);
    err.code = code;
    return err;
}

export class MiniMessageStream extends Emitter {
    /**
     * @param {Object} link 具备 on(event, fn) / write(Uint8Array) / destroy() 的传输对象（如 WsTransport）
     * @param {Object} [options]
     * @param {boolean} [options.enableCrc=true] 必须与对端一致，协议内没有协商
     * @param {number} [options.maxBufferSize=33554432] 单帧/缓冲上限，默认 32MiB
     * @param {number} [options.syncTimeoutMs=3000] 重同步握手超时，<=0 关闭
     */
    constructor(link, options = {}) {
        super();
        if (!link) throw new Error('MiniMessageStream: link 必填');
        const { enableCrc = true, maxBufferSize = 32 * 1024 * 1024, syncTimeoutMs = 3000 } = options;

        this.link = link;
        this.enableCrc = enableCrc;
        this.maxBufferSize = maxBufferSize;
        this.syncTimeoutMs = syncTimeoutMs;

        this._buf = new Uint8Array(0);
        this._expectedLength = null;
        this._resyncing = false;
        this._syncTimer = null;
        this._closed = false;
        this._chain = Promise.resolve();

        this._onData = (chunk) => this._handleData(chunk);
        this._onClose = () => this._failClosed('link closed');
        link.on('data', this._onData);
        link.on('close', this._onClose);
        link.on('error', (err) => this.emit('error', err));
    }

    // ---------------- 发送 ----------------
    /** 发送一整帧（自动加长度头与 CRC），返回 Promise */
    send(payload) {
        const data = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
        if (data.length > this.maxBufferSize) return Promise.reject(ipcError('载荷超过 maxBufferSize', 'ETOOBIG'));
        const next = this._chain.then(() => this._writeFrame(data)).catch((err) => { throw err; });
        this._chain = next.then(() => { }, () => { });
        return next;
    }

    _encode(data) {
        const n = data.length;
        const frame = new Uint8Array(this.enableCrc ? 12 + n : 4 + n);
        writeU32(frame, 0, n);
        if (!this.enableCrc) {
            frame.set(data, 4);
            return frame;
        }
        writeU32(frame, 4, crc32(frame.subarray(0, 4)));
        frame.set(data, 8);
        writeU32(frame, 8 + n, crc32(data));
        return frame;
    }

    async _writeFrame(data) {
        if (this._closed) throw ipcError('stream closed', 'ECLOSED');
        const frame = this._encode(data);
        if (!this.link.write(frame)) {
            await new Promise((resolve) => {
                const done = () => { this.link.off('drain', done); resolve(); };
                this.link.on('drain', done);
            });
        }
    }

    /** 发送错误包（绕过重同步门控） */
    _sendError(code) {
        const body = new Uint8Array([0xee, 0xee, 0xee, 0xee, code]);
        return this.send(body).catch(() => { });
    }

    /** 发送裸协议消息：FF FF FF FF [len][utf8] */
    _sendProtocolMessage(text) {
        const body = new TextEncoder().encode(text);
        const raw = new Uint8Array(8 + body.length);
        raw.set(PROTOCOL_HEADER, 0);
        writeU32(raw, 4, body.length);
        raw.set(body, 8);
        if (this._closed) return Promise.resolve();
        this.link.write(raw);
        return Promise.resolve();
    }

    // ---------------- 接收 ----------------
    _handleData(chunk) {
        if (this._closed) return;
        const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
        this._buf = concat(this._buf, bytes);

        if (this._buf.length > this.maxBufferSize + 16) {
            this._sendError(ERROR_CODES.BUFFER_OVERFLOW);
            this._emitError(ipcError(`缓冲 ${this._buf.length}B 超限`, 'BUFFER_OVERFLOW'));
            this._enterResync();
            return;
        }
        try { this._process(); } catch (err) { this._emitError(ipcError(`解析失败: ${err.message}`, 'EPARSE')); }
    }

    _process() {
        while (true) {
            if (this._resyncing) {
                const idx = this._indexOfProtocolHeader();
                if (idx < 0) {
                    if (this._buf.length > 3) this._buf = this._buf.subarray(this._buf.length - 3);
                    return;
                }
                if (idx > 0) this._buf = this._buf.subarray(idx);
                if (this._buf.length < 8) return;
                const len = readU32(this._buf, 4);
                if (len > MAX_PROTOCOL_MSG || len > this.maxBufferSize) { this._buf = this._buf.subarray(8); continue; }
                if (this._buf.length < 8 + len) return;
                const text = new TextDecoder().decode(this._buf.subarray(8, 8 + len)).trim();
                this._buf = this._buf.subarray(8 + len);
                if (text === 'RESET_CONFIRM') {
                    this._resyncing = false;
                    this._clearSyncTimer();
                    this.emit('syncComplete');
                }
                continue;
            }

            if (this._expectedLength === null) {
                // 裸协议消息（FF FF FF FF）：只在收尾阶段出现，直接消费掉，避免被当成帧长度
                if (startsWith(this._buf, PROTOCOL_HEADER)) {
                    if (this._buf.length < 8) return;
                    const msgLen = readU32(this._buf, 4);
                    if (msgLen > MAX_PROTOCOL_MSG || msgLen > this.maxBufferSize) { this._buf = this._buf.subarray(8); continue; }
                    if (this._buf.length < 8 + msgLen) return;
                    const text = new TextDecoder().decode(this._buf.subarray(8, 8 + msgLen)).trim();
                    this._buf = this._buf.subarray(8 + msgLen);
                    if (text === 'RESET_CONFIRM') {
                        this._resyncing = false;
                        this._clearSyncTimer();
                        this.emit('syncComplete');
                    }
                    continue;
                }

                const headerSize = this.enableCrc ? 8 : 4;
                if (this._buf.length < headerSize) return;
                const length = readU32(this._buf, 0);
                if (length === 0 || length > this.maxBufferSize) {
                    this._sendError(length > this.maxBufferSize ? ERROR_CODES.BUFFER_OVERFLOW : ERROR_CODES.LENGTH_CRC_FAILED);
                    this._emitError(ipcError(`非法帧长度 ${length}`, 'BAD_LENGTH'));
                    this._enterResync();
                    return;
                }
                if (this.enableCrc && crc32(this._buf.subarray(0, 4)) !== readU32(this._buf, 4)) {
                    this._sendError(ERROR_CODES.LENGTH_CRC_FAILED);
                    this._emitError(ipcError('长度头 CRC 校验失败', 'LENGTH_CRC_FAILED'));
                    this._enterResync();
                    return;
                }
                this._expectedLength = length;
                this._buf = this._buf.subarray(headerSize);
            }

            const need = this._expectedLength + (this.enableCrc ? 4 : 0);
            if (this._buf.length < need) return;

            const payload = this._buf.subarray(0, this._expectedLength);
            if (this.enableCrc) {
                const expected = readU32(this._buf, this._expectedLength);
                if (crc32(payload) !== expected) {
                    this._expectedLength = null;
                    this._sendError(ERROR_CODES.MESSAGE_CRC_FAILED);
                    this._emitError(ipcError('载荷 CRC 校验失败', 'MESSAGE_CRC_FAILED'));
                    this._enterResync();
                    return;
                }
            }
            this._expectedLength = null;
            this._buf = this._buf.subarray(need);

            if (startsWith(payload, ERROR_HEADER) && payload.length === 5) {
                this._onPeerError(payload[4]);
                continue;
            }
            this.emit('message', payload.slice());   // 切一份独立副本，避免后续缓冲移动影响使用者
        }
    }

    _indexOfProtocolHeader() {
        const window = Math.min(this._buf.length, 64 * 1024);
        for (let i = 0; i + 4 <= window; i++) {
            if (this._buf[i] === 0xff && this._buf[i + 1] === 0xff && this._buf[i + 2] === 0xff && this._buf[i + 3] === 0xff) return i;
        }
        return -1;
    }

    _enterResync() {
        this._buf = new Uint8Array(0);
        this._expectedLength = null;
        if (this._resyncing) return;
        this._resyncing = true;
        // 这里只发错误包（调用方已发），等对端回 RESET_CONFIRM 才能恢复；
        // 主动发 RESET_CONFIRM 会把「没在重同步」的对端带偏。
        if (this.syncTimeoutMs > 0) {
            this._syncTimer = setTimeout(() => {
                this._syncTimer = null;
                this._emitError(ipcError('重同步超时', 'ERESYNC_TIMEOUT'));
            }, this.syncTimeoutMs);
        }
    }

    _onPeerError(code) {
        this._emitError(ipcError(`对端报告 ${ERROR_NAMES[code] || code}`, 'ERESYNC'));
        this._buf = new Uint8Array(0);
        this._expectedLength = null;
        this._sendProtocolMessage('RESET_CONFIRM');
        this._resyncing = false;
        this._clearSyncTimer();
    }

    _clearSyncTimer() {
        if (this._syncTimer) { clearTimeout(this._syncTimer); this._syncTimer = null; }
    }

    _failClosed(reason) {
        if (this._closed) return;
        this._closed = true;
        this._clearSyncTimer();
        this._emitError(ipcError(reason, 'ECLOSED'));
        this.emit('close');
    }

    _emitError(err) {
        if (this.listenerCount('error') > 0) this.emit('error', err);
        else console.error('[MiniMessageStream] 未处理的错误:', err.code, err.message);
    }

    destroy() {
        if (this._closed) return;
        this._closed = true;
        this._clearSyncTimer();
        try { this.link.off('data', this._onData); } catch { }
        this._buf = new Uint8Array(0);
        this.emit('close');
    }
}

export default MiniMessageStream;
