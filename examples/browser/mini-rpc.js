// examples/browser/mini-rpc.js — 浏览器侧 RPC 层（纯 Uint8Array，无 Node 依赖）
//
// 报文格式与 RpcProtocol.js 完全一致：
//   [type u8][contentType u8][UUID 16B][payload]
//   type: 0=REQUEST 1=RESPONSE 2=EVENT 3=PING 4=PONG 5=HELLO 6=HELLO_ACK
//   contentType: 0=TEXT(utf8) 1=JSON(utf8) 2=BINARY
//   JSON 请求体: {"method":..., "params":[...]}     错误响应: {"error":"..."}
//   二进制请求: [methodLen u32BE][method][paramsLen u32BE][params JSON][binary...]
//
// 与 Node 版一致的能力：HELLO 鉴权 + 能力协商、请求/响应匹配、超时、事件、心跳 PING/PONG。
// 裁掉的部分：多客户端路由（本端一条链路只对端一个 peer）、进程守护、token 热轮换（预留 setRequiredTokens）。
//
// 用法：
//   const rpc = new MiniRpc(stream, { authToken, implName: 'browser' });
//   rpc.registerMethod('add', (a, b) => a + b);
//   rpc.on('event', (msg) => console.log(msg));
//   await rpc.hello();
//   const sum = await rpc.sendRequest('add', [1, 2]);

const TYPES = { REQUEST: 0, RESPONSE: 1, EVENT: 2, PING: 3, PONG: 4, HELLO: 5, HELLO_ACK: 6 };
const CONTENT_TYPES = { TEXT: 0, JSON: 1, BINARY: 2 };
const PROTOCOL_VERSION = 1;
const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_CAPABILITIES = ['ping', 'binary', 'event'];

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export class MiniRpcError extends Error {
    constructor(message, { code = 'RPC_ERROR', remote = false } = {}) {
        super(message);
        this.name = 'MiniRpcError';
        this.code = code;
        this.remote = remote;
    }
}

// ---------------- UUID ----------------
function uuidToBytes(uuid) {
    const hex = uuid.replace(/-/g, '');
    const out = new Uint8Array(16);
    for (let i = 0; i < 16; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    return out;
}

function bytesToUuid(bytes, offset) {
    let hex = '';
    for (let i = 0; i < 16; i++) hex += bytes[offset + i].toString(16).padStart(2, '0');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function newUuid() {
    if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
    const b = new Uint8Array(16);
    if (globalThis.crypto?.getRandomValues) globalThis.crypto.getRandomValues(b);
    else for (let i = 0; i < 16; i++) b[i] = Math.floor(Math.random() * 256);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    return bytesToUuid(b, 0);
}

// ---------------- 小工具 ----------------
function concat(list) {
    let total = 0;
    for (const item of list) total += item.length;
    const out = new Uint8Array(total);
    let offset = 0;
    for (const item of list) { out.set(item, offset); offset += item.length; }
    return out;
}

function u32be(value) {
    return new Uint8Array([(value >>> 24) & 0xff, (value >>> 16) & 0xff, (value >>> 8) & 0xff, value & 0xff]);
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
            try { fn(...args); } catch (err) { console.error(`[MiniRpc] '${event}' 监听器异常:`, err); }
        }
    }
}

export class MiniRpc extends Emitter {
    /**
     * @param {import('./mini-stream.js').MiniMessageStream} stream 已创建的帧层
     * @param {Object} [options]
     * @param {string} [options.authToken] 客户端凭据；服务端开了鉴权时必须提供
     * @param {string} [options.requiredToken] 服务端要求客户端出示的 token
     * @param {string[]} [options.requiredTokens] 服务端接受的 token 集合（优先于 requiredToken）
     * @param {string[]} [options.capabilities] HELLO 里声明的能力，默认 ['ping','binary','event']
     * @param {string} [options.implName='browser'] 实现名，出现在 HELLO 里
     * @param {number} [options.heartbeatIntervalMs=5000] 空闲多久发 PING；0 关闭心跳
     * @param {number} [options.heartbeatTimeoutMs=15000] 多久收不到任何入站消息判定断链；0 关闭
     * @param {boolean} [options.heartbeatRequireCapability=false] 仅当对端声明 ping 才发心跳
     * @param {number} [options.defaultTimeoutMs=15000] sendRequest 默认超时
     */
    constructor(stream, options = {}) {
        super();
        if (!stream) throw new Error('MiniRpc: stream 必填');
        const {
            authToken = null,
            requiredToken = null,
            requiredTokens,
            capabilities = DEFAULT_CAPABILITIES,
            implName = 'browser',
            heartbeatIntervalMs = 5000,
            heartbeatTimeoutMs = 15000,
            heartbeatRequireCapability = false,
            defaultTimeoutMs = DEFAULT_TIMEOUT_MS,
        } = options;

        this.stream = stream;
        this.handlers = new Map();
        this.pending = new Map();
        this.authToken = authToken;
        this.requiredTokens = new Set(requiredTokens ?? (requiredToken ? [requiredToken] : []));
        this._authenticated = this.requiredTokens.size === 0;
        this.capabilities = [...capabilities];
        this.implName = implName;
        this.peerCapabilities = null;
        this.serverCapabilities = null;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.heartbeatRequireCapability = heartbeatRequireCapability;
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.heartbeatDisabledReason = null;
        this._lastReceived = Date.now();
        this._destroyed = false;
        this._unauthorizedHits = 0;

        this.stream.on('message', (bytes) => this._handleMessage(bytes));
        this.stream.on('error', (err) => {
            this._rejectAll(err);
            this._emitError(err);
        });
        this.stream.on('close', () => {
            this._rejectAll(new MiniRpcError('stream closed', { code: 'ECLOSED' }));
            this._stopHeartbeat();
            this._emitClose();
        });

        if (heartbeatIntervalMs > 0) {
            this._heartbeatTimer = setInterval(() => this._heartbeatTick(), heartbeatIntervalMs);
        }
    }

    // ---------------- 收发 ----------------
    _encodeMessage(type, contentType, uuid, payload) {
        const head = new Uint8Array(18);
        head[0] = type;
        head[1] = contentType;
        head.set(uuidToBytes(uuid), 2);
        return concat([head, payload]);
    }

    _sendRaw(type, contentType, uuid, data) {
        let payload;
        if (contentType === CONTENT_TYPES.TEXT) payload = encoder.encode(String(data));
        else if (contentType === CONTENT_TYPES.JSON) payload = encoder.encode(JSON.stringify(data));
        else payload = data instanceof Uint8Array ? data : new Uint8Array(data);
        return this.stream.send(this._encodeMessage(type, contentType, uuid, payload));
    }

    // ---------------- 握手 ----------------
    /** 显式握手：启用鉴权时必须在业务请求前调用；同时完成能力协商 */
    hello(timeoutMs = this.defaultTimeoutMs) {
        const uuid = newUuid();
        const payload = { version: PROTOCOL_VERSION, caps: this.capabilities, impl: this.implName };
        if (this.authToken) payload.token = this.authToken;
        return this._request(uuid, 'hello', () => this._sendRaw(TYPES.HELLO, CONTENT_TYPES.JSON, uuid, payload), timeoutMs);
    }

    // ---------------- 请求 ----------------
    sendRequest(method, params = [], timeoutMs = this.defaultTimeoutMs) {
        const uuid = newUuid();
        const body = { method, params: Array.isArray(params) ? params : [params] };
        return this._request(uuid, method, () => this._sendRaw(TYPES.REQUEST, CONTENT_TYPES.JSON, uuid, body), timeoutMs);
    }

    /** 二进制请求：第一个参数是二进制体，其余作为 JSON 参数，对齐其它语言实现 */
    sendRequestBinary(method, buffer, ...args) {
        const timeoutMs = typeof args[args.length - 1] === 'number' ? args.pop() : this.defaultTimeoutMs;
        const uuid = newUuid();
        const methodBytes = encoder.encode(method);
        const paramsBytes = encoder.encode(JSON.stringify(args));
        const body = concat([u32be(methodBytes.length), methodBytes, u32be(paramsBytes.length), paramsBytes,
            buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer)]);
        return this._request(uuid, method, () => this._sendRaw(TYPES.REQUEST, CONTENT_TYPES.BINARY, uuid, body), timeoutMs);
    }

    _request(uuid, method, sendFn, timeoutMs) {
        if (this._destroyed) return Promise.reject(new MiniRpcError('rpc destroyed', { code: 'ECLOSED' }));

        return new Promise((resolve, reject) => {
            const timer = timeoutMs > 0
                ? setTimeout(() => {
                    this.pending.delete(uuid);
                    reject(new MiniRpcError(`request timeout: ${method} (${timeoutMs}ms)`, { code: 'ETIMEDOUT' }));
                }, timeoutMs)
                : null;
            this.pending.set(uuid, { resolve, reject, timer, method });
            Promise.resolve()
                .then(sendFn)
                .catch((err) => {
                    const entry = this.pending.get(uuid);
                    if (!entry) return;
                    this.pending.delete(uuid);
                    if (entry.timer) clearTimeout(entry.timer);
                    reject(err);
                });
        });
    }

    registerMethod(name, handler) {
        if (typeof handler !== 'function') throw new Error(`MiniRpc: ${name} 的 handler 不是函数`);
        this.handlers.set(name, handler);
    }

    // ---------------- 事件 ----------------
    sendText(text) { return this._sendRaw(TYPES.EVENT, CONTENT_TYPES.TEXT, newUuid(), text); }
    sendJson(data) { return this._sendRaw(TYPES.EVENT, CONTENT_TYPES.JSON, newUuid(), data); }
    sendBinary(bytes) { return this._sendRaw(TYPES.EVENT, CONTENT_TYPES.BINARY, newUuid(), bytes); }

    // ---------------- 入站处理 ----------------
    _handleMessage(bytes) {
        this._lastReceived = Date.now();
        if (bytes.length < 18) { this._emitError(new MiniRpcError('报文过短', { code: 'EMALFORMED' })); return; }
        const type = bytes[0];
        const contentType = bytes[1];
        const uuid = bytesToUuid(bytes, 2);
        const payload = bytes.subarray(18);

        let content;
        if (contentType === CONTENT_TYPES.TEXT) content = decoder.decode(payload);
        else if (contentType === CONTENT_TYPES.JSON) {
            try { content = JSON.parse(decoder.decode(payload)); }
            catch (err) { this._emitError(new MiniRpcError(`JSON 解析失败: ${err.message}`, { code: 'EMALFORMED' })); return; }
        } else content = payload.slice();

        switch (type) {
            case TYPES.REQUEST: return void this._handleRequest(uuid, contentType, content, payload);
            case TYPES.RESPONSE: return void this._handleResponse(uuid, content);
            case TYPES.EVENT:
                if (!this._authenticated) return;
                this.emit('event', { contentType, content });
                return;
            case TYPES.PING:
                this._sendRaw(TYPES.PONG, CONTENT_TYPES.TEXT, uuid, '').catch(() => { });
                return;
            case TYPES.PONG: return;
            case TYPES.HELLO: return void this._handleHello(uuid, content);
            case TYPES.HELLO_ACK: return void this._handleHelloAck(uuid, content);
            default:
                this._emitError(new MiniRpcError(`未知报文类型 ${type}`, { code: 'EMALFORMED' }));
        }
    }

    async _handleRequest(uuid, contentType, content, payload) {
        if (!this._authenticated) {
            this._unauthorizedHits++;
            this._sendRaw(TYPES.RESPONSE, CONTENT_TYPES.JSON, uuid, { error: 'Unauthorized' }).catch(() => { });
            if (this._unauthorizedHits >= 5) this.destroy();
            return;
        }

        let method;
        let args = [];
        try {
            if (contentType === CONTENT_TYPES.BINARY) {
                // [methodLen u32BE][method][paramsLen u32BE][params JSON][binary]
                const methodLen = (payload[0] << 24 | payload[1] << 16 | payload[2] << 8 | payload[3]) >>> 0;
                method = decoder.decode(payload.subarray(4, 4 + methodLen));
                const paramsStart = 4 + methodLen;
                const paramsLen = (payload[paramsStart] << 24 | payload[paramsStart + 1] << 16
                    | payload[paramsStart + 2] << 8 | payload[paramsStart + 3]) >>> 0;
                const jsonStart = paramsStart + 4;
                const params = JSON.parse(decoder.decode(payload.subarray(jsonStart, jsonStart + paramsLen)));
                args = [payload.subarray(jsonStart + paramsLen), ...(Array.isArray(params) ? params : [params])];
            } else {
                method = content?.method;
                const params = content?.params;
                args = Array.isArray(params) ? params : (params === undefined || params === null ? [] : [params]);
            }
            if (!method) throw new Error('缺少 method');
        } catch (err) {
            this._sendRaw(TYPES.RESPONSE, CONTENT_TYPES.JSON, uuid, { error: `Invalid request payload: ${err.message}` }).catch(() => { });
            return;
        }

        const handler = this.handlers.get(method);
        if (!handler) {
            this._sendRaw(TYPES.RESPONSE, CONTENT_TYPES.JSON, uuid, { error: `Method not found: ${method}` }).catch(() => { });
            return;
        }

        try {
            const result = await handler(...args);
            await this._sendResult(uuid, result);
        } catch (err) {
            await this._sendRaw(TYPES.RESPONSE, CONTENT_TYPES.JSON, uuid, { error: String(err?.message || err) }).catch(() => { });
        }
    }

    _sendResult(uuid, result) {
        if (result === undefined || result === null) return this._sendRaw(TYPES.RESPONSE, CONTENT_TYPES.JSON, uuid, null);
        if (typeof result === 'string') return this._sendRaw(TYPES.RESPONSE, CONTENT_TYPES.TEXT, uuid, result);
        if (result instanceof Uint8Array) return this._sendRaw(TYPES.RESPONSE, CONTENT_TYPES.BINARY, uuid, result);
        return this._sendRaw(TYPES.RESPONSE, CONTENT_TYPES.JSON, uuid, result);
    }

    _handleResponse(uuid, content) {
        const entry = this.pending.get(uuid);
        if (!entry) return;
        this.pending.delete(uuid);
        if (entry.timer) clearTimeout(entry.timer);
        if (content && typeof content === 'object' && !Array.isArray(content) && content.error !== undefined
            && !(content instanceof Uint8Array)) {
            entry.reject(new MiniRpcError(String(content.error), { code: 'EREMOTE', remote: true }));
            return;
        }
        entry.resolve(content);
    }

    _handleHello(uuid, content) {
        if (content?.version !== PROTOCOL_VERSION) {
            this._sendRaw(TYPES.HELLO_ACK, CONTENT_TYPES.JSON, uuid, { error: `unsupported protocol version: ${content?.version}` })
                .catch(() => { });
            this._destroySoon();
            return;
        }
        if (this.requiredTokens.size > 0 && !this.requiredTokens.has(content?.token)) {
            this._sendRaw(TYPES.HELLO_ACK, CONTENT_TYPES.JSON, uuid, { error: 'unauthorized' }).catch(() => { });
            this._destroySoon();
            return;
        }
        this._authenticated = true;
        this.peerCapabilities = Array.isArray(content?.caps) ? content.caps : null;
        if (this.peerCapabilities && !this.peerCapabilities.includes('ping')) {
            this._disableHeartbeat('对端未声明 ping 能力');
        }
        this._sendRaw(TYPES.HELLO_ACK, CONTENT_TYPES.JSON, uuid, {
            version: PROTOCOL_VERSION, ok: true, caps: this.capabilities, impl: this.implName,
        }).catch(() => { });
    }

    _handleHelloAck(uuid, content) {
        const entry = this.pending.get(uuid);
        if (!entry) return;
        this.pending.delete(uuid);
        if (entry.timer) clearTimeout(entry.timer);
        if (content?.error) {
            entry.reject(new MiniRpcError(String(content.error), { code: 'EAUTH', remote: true }));
            return;
        }
        if (content?.version !== PROTOCOL_VERSION) {
            entry.reject(new MiniRpcError(`unsupported protocol version: ${content?.version}`, { code: 'EVERSION', remote: true }));
            return;
        }
        this.serverCapabilities = Array.isArray(content.caps) ? content.caps : null;
        if (this.serverCapabilities && !this.serverCapabilities.includes('ping')) {
            this._disableHeartbeat('服务端未声明 ping 能力');
        }
        entry.resolve(content);
    }

    // ---------------- 心跳 ----------------
    _disableHeartbeat(reason) {
        if (this.heartbeatDisabledReason) return;
        this.heartbeatDisabledReason = reason;
        this._stopHeartbeat();
        this.emit('heartbeatDisabled', reason);
    }

    _stopHeartbeat() {
        if (this._heartbeatTimer) { clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }
    }

    _heartbeatTick() {
        if (this._destroyed || this.heartbeatDisabledReason) return;
        if (this.heartbeatRequireCapability && !(this.peerCapabilities || []).includes('ping')) return;
        const silent = Date.now() - this._lastReceived;
        const busy = this.pending.size > 0;
        if (!busy && this.heartbeatTimeoutMs > 0 && silent >= this.heartbeatTimeoutMs) {
            const err = new MiniRpcError(`heartbeat timeout: ${silent}ms 无入站数据`, { code: 'EHEARTBEAT' });
            this._rejectAll(err);
            this._emitError(err);
            this.destroy();
            return;
        }
        if (silent >= this.heartbeatIntervalMs) {
            this._sendRaw(TYPES.PING, CONTENT_TYPES.TEXT, newUuid(), '').catch(() => { });
        }
    }

    _rejectAll(err) {
        for (const [, entry] of this.pending) {
            if (entry.timer) clearTimeout(entry.timer);
            entry.reject(err);
        }
        this.pending.clear();
    }

    _emitError(err) {
        if (this.listenerCount('error') > 0) this.emit('error', err);
        else console.error('[MiniRpc] 未处理的错误:', err.code, err.message);
    }

    /** 握手失败：先让 ack 刷出去，再断开（立刻 destroy 会把未发出的帧丢掉） */
    _destroySoon(delayMs = 300) {
        setTimeout(() => this.destroy(), delayMs);
    }

    destroy() {
        if (this._destroyed) return;
        this._destroyed = true;
        this._stopHeartbeat();
        this._rejectAll(new MiniRpcError('rpc destroyed', { code: 'ECLOSED' }));
        try { this.stream.destroy(); } catch { }
        this._emitClose();
    }

    _emitClose() {
        if (this._closeEmitted) return;
        this._closeEmitted = true;
        this.emit('close');
    }
}

export default MiniRpc;
