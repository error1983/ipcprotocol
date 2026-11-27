// examples/browser/ws-transport.js — WebSocket 传输适配器（浏览器 / Node 通用）
//
// 把「一条 WebSocket 连接」伪装成 net.Socket 的样子（on/write/destroy/destroyed/drain），
// 于是可以直接塞给本仓库的 MessageStream：帧格式一个字节都不改，dump_parser.js 照样能解析。
//
// 一个适配器 = 一条「本端 ↔ 某个对端」的逻辑链路：
//   · 发出的每个 chunk 自动套上路由信封（src=自己，dst=this.to），交给中继网关寻址；
//   · 收到的消息剥掉信封，只把原始 IPC 字节交给上层。
// 要同时和多个对端通信，就开多个适配器（各自配一个 RpcProtocol）。
//
// 浏览器里没有 Buffer，emit 出去的是 Uint8Array；Node 里会转成 Buffer，
// 所以 Node 端可以直接套用现有的 MessageStream + RpcProtocol（见 examples/ws-rpc-demo.js）。
//
// 用法（浏览器）：
//   import { WsTransport } from './ws-transport.js';
//   const link = new WsTransport('ws://127.0.0.1:9100', { peer: 'alice', to: 'bob' });
//   link.on('data', (bytes) => { /* 喂给你的前端帧解析器 */ });
//   link.write(frameBytes);

const OPEN = 1;
const HAS_BUFFER = typeof Buffer !== 'undefined' && typeof Buffer.from === 'function';
const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** 极简事件发射器：只实现 MessageStream 需要的那几个方法 */
class Emitter {
    constructor() { this._map = new Map(); }

    on(event, fn) {
        if (!this._map.has(event)) this._map.set(event, []);
        this._map.get(event).push(fn);
        return this;
    }

    once(event, fn) {
        const wrap = (...args) => { this.off(event, wrap); fn(...args); };
        wrap._origin = fn;
        return this.on(event, wrap);
    }

    off(event, fn) {
        const list = this._map.get(event);
        if (!list) return this;
        if (!fn) { this._map.delete(event); return this; }
        const idx = list.findIndex((f) => f === fn || f._origin === fn);
        if (idx >= 0) list.splice(idx, 1);
        return this;
    }

    listenerCount(event) { return (this._map.get(event) || []).length; }

    emit(event, ...args) {
        for (const fn of [...(this._map.get(event) || [])]) {
            try { fn(...args); } catch (err) { console.error(`[WsTransport] '${event}' 监听器异常:`, err); }
        }
    }
}

function makeErr(message, code) {
    const err = new Error(message);
    err.code = code;
    return err;
}

export class WsTransport extends Emitter {
    /**
     * @param {string} url 网关地址，如 ws://127.0.0.1:9100
     * @param {Object} options
     * @param {string} options.peer 本端 peerId（网关用它寻址，重复会被拒）
     * @param {string} [options.to] 对端 peerId；空串表示广播
     * @param {boolean} [options.autoReconnect=false] 断线自动重连。
     *        ⚠️ 重连后旧的 MessageStream/RpcProtocol 状态已失效，必须重建（见 README）。
     * @param {number} [options.reconnectMinDelayMs=300]
     * @param {number} [options.reconnectMaxDelayMs=15000]
     * @param {number} [options.maxPendingBytes=8388608] 未连接时允许缓存的写入字节上限
     * @param {number} [options.highWaterMark=4194304] WS 发送缓冲超过它就返回 false，让上层等待 drain
     * @param {string} [options.protocols] 子协议（一般不用）
     */
    constructor(url, options = {}) {
        super();
        const {
            peer, to = '',
            autoReconnect = false,
            reconnectMinDelayMs = 300,
            reconnectMaxDelayMs = 15000,
            maxPendingBytes = 8 * 1024 * 1024,
            highWaterMark = 4 * 1024 * 1024,
        } = options;

        if (!peer) throw new Error('WsTransport: 必须指定 peer');

        this.url = url;
        this.peer = peer;
        this.to = to;
        this.autoReconnect = autoReconnect;
        this.reconnectMinDelayMs = reconnectMinDelayMs;
        this.reconnectMaxDelayMs = reconnectMaxDelayMs;
        this.maxPendingBytes = maxPendingBytes;
        this.highWaterMark = highWaterMark;

        this._ws = null;
        this._pending = [];
        this._pendingBytes = 0;
        this._delay = reconnectMinDelayMs;
        this._reconnectTimer = null;
        this._manualClose = false;
        this._drainTimer = null;
        this.destroyed = false;
        this.readyState = 0;
        this._open();
    }

    /** 运行中切换目标 peer（同一连接复用） */
    setTarget(peerId) { this.to = peerId; }

    // ---------------- 连接管理 ----------------
    _open() {
        if (this.destroyed) return;
        const sep = this.url.includes('?') ? '&' : '?';
        const url = `${this.url}${sep}peer=${encodeURIComponent(this.peer)}`;

        let ws;
        try {
            ws = new WebSocket(url);
        } catch (err) {
            this._emitError(makeErr(`连接失败: ${err.message}`, 'ECONNECT'));
            this._scheduleReconnect();
            return;
        }
        ws.binaryType = 'arraybuffer';
        this._ws = ws;
        this.readyState = 0;

        ws.onopen = () => {
            this.readyState = OPEN;
            this._delay = this.reconnectMinDelayMs;
            this._flushPending();
            this.emit('open');
            this.emit('connect');
        };

        ws.onmessage = (ev) => this._onMessage(ev.data);

        ws.onerror = () => {
            // 浏览器不暴露细节，只给一个可判定的错误码，避免上层拿到 undefined
            this._emitError(makeErr('websocket error', 'ESOCKET'));
        };

        ws.onclose = (ev) => {
            this.readyState = 3;
            this._ws = null;
            this.emit('offline', { code: ev?.code });
            this.emit('end');
            this.emit('close');
            if (!this._manualClose && this.autoReconnect) this._scheduleReconnect();
        };
    }

    _scheduleReconnect() {
        if (this.destroyed || this._manualClose || this._reconnectTimer) return;
        const delay = Math.min(this._delay, this.reconnectMaxDelayMs);
        this._delay = Math.min(this._delay * 2, this.reconnectMaxDelayMs);
        this._reconnectTimer = setTimeout(() => {
            this._reconnectTimer = null;
            if (!this.destroyed && !this._manualClose) this._open();
        }, delay);
        this._reconnectTimer.unref?.();
        this.emit('reconnecting', { delayMs: delay });
    }

    // ---------------- 收发 ----------------
    _onMessage(data) {
        if (typeof data === 'string') {
            this._emitError(makeErr('收到文本帧，本协议只接受二进制帧', 'EPROTO'));
            return;
        }
        if (typeof Blob !== 'undefined' && data instanceof Blob) {
            data.arrayBuffer().then((ab) => this._onMessage(ab)).catch(() => { });
            return;
        }

        const bytes = data instanceof ArrayBuffer
            ? new Uint8Array(data)
            : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);

        const env = this._decode(bytes);
        if (!env) { this._emitError(makeErr('路由信封格式非法', 'EPROTO')); return; }
        if (env.dst && env.dst !== this.peer) return;   // 不是发给我的（网关正常不会出现）
        if (env.payload.length === 0) return;

        this.emit('data', HAS_BUFFER ? Buffer.from(env.payload) : env.payload);
    }

    _decode(bytes) {
        if (bytes.length < 2) return null;
        const srcLen = bytes[0];
        if (bytes.length < 1 + srcLen + 1) return null;
        const dstLen = bytes[1 + srcLen];
        const dstStart = 1 + srcLen + 1;
        const start = dstStart + dstLen;
        if (bytes.length < start) return null;
        return {
            src: decoder.decode(bytes.subarray(1, 1 + srcLen)),
            dst: decoder.decode(bytes.subarray(dstStart, start)),
            payload: bytes.subarray(start),
        };
    }

    _encode(payload) {
        const src = encoder.encode(this.peer);
        const dst = encoder.encode(this.to || '');
        const out = new Uint8Array(1 + src.length + 1 + dst.length + payload.length);
        let o = 0;
        out[o++] = src.length;
        out.set(src, o); o += src.length;
        out[o++] = dst.length;
        out.set(dst, o); o += dst.length;
        out.set(payload, o);
        return out;
    }

    /**
     * 写入原始 IPC 字节（MessageStream 会按 64KB 分块调用）。
     * @param {Uint8Array|Buffer} chunk
     * @returns {boolean} false 表示上层应等待 'drain'
     */
    write(chunk) {
        if (this.destroyed) return false;
        const payload = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
        const framed = this._encode(payload);

        if (!this._ws || this.readyState !== OPEN) {
            if (this._pendingBytes + framed.length > this.maxPendingBytes) {
                this._emitError(makeErr('未连接时写入队列已满', 'EQUEUE'));
                return false;
            }
            this._pending.push(framed);
            this._pendingBytes += framed.length;
            return false;   // 触发上层等 drain（连上后会 emit）
        }

        try {
            this._ws.send(framed);
        } catch (err) {
            this._emitError(makeErr(`发送失败: ${err.message}`, 'ESEND'));
            return false;
        }
        const buffered = this._ws.bufferedAmount || 0;
        if (buffered >= this.highWaterMark) {
            this._scheduleDrain();
            return false;
        }
        return true;
    }

    _scheduleDrain() {
        if (this._drainTimer) return;
        let attempts = 0;
        const tick = () => {
            this._drainTimer = null;
            if (this.destroyed || !this._ws) return;
            const buffered = this._ws.bufferedAmount || 0;
            if (buffered < this.highWaterMark) { this.emit('drain'); return; }
            if (++attempts > 2000) { this.emit('drain'); return; }   // 兜底：别让上层永久挂住
            this._drainTimer = setTimeout(tick, 8);
            this._drainTimer.unref?.();
        };
        this._drainTimer = setTimeout(tick, 8);
        this._drainTimer.unref?.();
    }

    _flushPending() {
        if (!this._pending.length) return;
        const queue = this._pending;
        this._pending = [];
        this._pendingBytes = 0;
        for (const framed of queue) {
            try { this._ws.send(framed); } catch { break; }
        }
        this.emit('drain');
    }

    // ---------------- net.Socket 兼容面 ----------------
    setNoDelay() { }
    setTimeout() { }
    end() { this.destroy(); }

    destroy() {
        if (this.destroyed) return;
        this.destroyed = true;
        this._manualClose = true;
        if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
        if (this._drainTimer) { clearTimeout(this._drainTimer); this._drainTimer = null; }
        this._pending = [];
        this._pendingBytes = 0;
        try { this._ws?.close(); } catch { /* ignore */ }
        this._ws = null;
        this.readyState = 3;
        this.emit('close');
    }

    _emitError(err) {
        if (this.listenerCount('error') > 0) this.emit('error', err);
        else console.error('[WsTransport] 未处理的错误:', err.code, err.message);
    }
}

export default WsTransport;
