// examples/browser/rpc-protocol-compat.js — 把浏览器精简栈伪装成 Node 版 RpcProtocol 的接口形状
//
// 目的：同一段业务代码（注册方法、hello、sendRequest、发事件、错误处理）在 Node 端和浏览器端
// 可以一字不改地复用，只需要换 import 路径：
//
//   Node 端：  import { RpcProtocol, RpcError } from '../../RpcProtocol.js';
//   浏览器端：import { RpcProtocol, RpcError } from './rpc-protocol-compat.js';
//
// 对齐的范围（方法名 / 参数顺序 / 默认值 / 事件名 / 属性名）：
//   new RpcProtocol(stream, options)     options: heartbeatIntervalMs, heartbeatTimeoutMs,
//                                                 authToken, requiredToken(s), capabilities,
//                                                 implName, heartbeatRequireCapability
//   hello(timeoutMs)                     sendRequest(method, params, to?, timeoutMs?)
//   sendRequestBinary(method, buffer…)   sendResponse(id, response, contentType?)
//   sendRaw(type, contentType, id, data) onEvent(listener) / registerMethod(name, fn)
//   sendText / sendJson / sendBinary     destroy() / setRequiredTokens(tokens) / handleMessage(bytes)
//   事件：'event' 'error' 'heartbeatDisabled'（额外转发 'close'、'syncComplete'）
//   静态：TYPES、CONTENT_TYPES、serialize、deserialize、encodeBinaryRequest、decodeBinaryRequest
//
// 已知差异（无法对齐的，都是浏览器环境的固有限制，不是 bug）：
//   1. 二进制载荷在浏览器里是 Uint8Array（没有 Buffer）；
//   2. sendRequest 的 `to` 参数被忽略 —— IPC 帧里没有目标地址，路由由 WS 网关的传输层信封负责；
//   3. 一条链路只对应一个对端（Node 版可以做多客户端路由）；
//   4. heartbeatSkipWhilePending 恒为 true（有在途请求时不做超时判定），无法关闭；
//   5. 没有 token 热轮换的宽限期语义，setRequiredTokens 立即整体替换。

import { MiniRpc, MiniRpcError } from './mini-rpc.js';

const PROTOCOL_VERSION = 1;
const DEFAULT_TIMEOUT = 15000;

export const RpcProtocolTypes = Object.freeze({
    REQUEST: 0, RESPONSE: 1, EVENT: 2, PING: 3, PONG: 4, HELLO: 5, HELLO_ACK: 6,
});
export const RpcContentTypes = Object.freeze({ TEXT: 0, JSON: 1, BINARY: 2 });

/**
 * 与 Node 版 RpcError 同名同形。实际就是 mini-rpc 的 MiniRpcError：
 * 保留 code / remote 字段，`instanceof RpcError` 判定成立。
 */
export const RpcError = MiniRpcError;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// ---------------- 内部小工具 ----------------
function uuidToBytes(uuid) {
    const hex = uuid.replace(/-/g, '');
    const out = new Uint8Array(16);
    for (let i = 0; i < 16; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    return out;
}

function bytesToUuid(bytes, offset = 0) {
    let hex = '';
    for (let i = 0; i < 16; i++) hex += bytes[offset + i].toString(16).padStart(2, '0');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function concat(parts) {
    let total = 0;
    for (const p of parts) total += p.length;
    const out = new Uint8Array(total);
    let offset = 0;
    for (const p of parts) { out.set(p, offset); offset += p.length; }
    return out;
}

function u32be(v) {
    return new Uint8Array([(v >>> 24) & 0xff, (v >>> 16) & 0xff, (v >>> 8) & 0xff, v & 0xff]);
}

function readU32be(bytes, offset) {
    return ((bytes[offset] << 24) | (bytes[offset + 1] << 16) | (bytes[offset + 2] << 8) | bytes[offset + 3]) >>> 0;
}

/** 最小事件发射器：提供 Node EventEmitter 里被 RpcProtocol 用到的那几个方法 */
class Emitter {
    constructor() { this._listeners = new Map(); }

    on(event, fn) {
        if (typeof fn !== 'function') throw new TypeError('listener must be a function');
        if (!this._listeners.has(event)) this._listeners.set(event, []);
        this._listeners.get(event).push({ fn, once: false });
        return this;
    }

    once(event, fn) {
        if (typeof fn !== 'function') throw new TypeError('listener must be a function');
        if (!this._listeners.has(event)) this._listeners.set(event, []);
        this._listeners.get(event).push({ fn, once: true });
        return this;
    }

    off(event, fn) {
        const list = this._listeners.get(event);
        if (!list) return this;
        if (!fn) { this._listeners.delete(event); return this; }
        const idx = list.findIndex((l) => l.fn === fn);
        if (idx >= 0) list.splice(idx, 1);
        return this;
    }

    removeAllListeners(event) {
        if (event === undefined) this._listeners.clear();
        else this._listeners.delete(event);
        return this;
    }

    listenerCount(event) { return (this._listeners.get(event) || []).length; }

    emit(event, ...args) {
        const list = this._listeners.get(event);
        if (!list) return false;
        for (const entry of [...list]) {
            if (entry.once) this.off(event, entry.fn);
            try { entry.fn(...args); } catch (err) { console.error(`[RpcProtocolCompat] '${event}' 监听器异常:`, err); }
        }
        return true;
    }
}

export class RpcProtocol extends Emitter {
    /**
     * 浏览器侧的 RpcProtocol —— 接口形状与 Node 版一致，内部委托给 MiniRpc。
     *
     * @param {import('./mini-stream.js').MiniMessageStream} messageStream 帧层（MiniMessageStream）
     * @param {Object} [options={}] 与 Node 版同名同义，见文件头部说明
     *
     * @example
     * const rpc = new RpcProtocol(new MiniMessageStream(link), { implName: 'browser/alice' });
     * rpc.registerMethod('add', (a, b) => a + b);
     * rpc.on('event', (msg) => console.log(msg.content));
     * await rpc.hello();
     * console.log(await rpc.sendRequest('add', [1, 2]));   // 3
     */
    constructor(messageStream, options = {}) {
        if (!messageStream) throw new Error('messageStream is undefined');
        super();

        this.messageStream = messageStream;
        this._rpc = new MiniRpc(messageStream, {
            authToken: options.authToken ?? null,
            requiredToken: options.requiredToken ?? null,
            requiredTokens: options.requiredTokens,
            capabilities: options.capabilities,
            implName: options.implName ?? 'browser',
            heartbeatIntervalMs: options.heartbeatIntervalMs ?? 5000,
            heartbeatTimeoutMs: options.heartbeatTimeoutMs ?? 15000,
            heartbeatRequireCapability: options.heartbeatRequireCapability ?? false,
            defaultTimeoutMs: options.defaultTimeoutMs ?? DEFAULT_TIMEOUT,
        });

        // handlers 暴露成普通对象（Node 版就是这个形状），写入时同步给 MiniRpc 的 Map
        const self = this;
        this.handlers = new Proxy({}, {
            set(target, key, value) {
                target[key] = value;
                try { self._rpc.registerMethod(String(key), value); } catch { /* 非函数由 MiniRpc 侧抛 */ }
                return true;
            },
            deleteProperty(target, key) {
                delete target[key];
                try { self._rpc.handlers.delete(String(key)); } catch { }
                return true;
            },
        });

        // 事件透传：MiniRpc 负责协议，Compat 只做形状转换
        this._rpc.on('event', (msg) => this.emit('event', { contentType: msg.contentType, content: msg.content }));
        this._rpc.on('error', (err) => this.emit('error', err));
        this._rpc.on('heartbeatDisabled', (reason) => this.emit('heartbeatDisabled', reason));
        this._rpc.on('close', () => this.emit('close'));
        if (typeof messageStream.on === 'function') {
            messageStream.on('syncComplete', () => this.emit('syncComplete'));
        }

        this._warnedSkipWhilePending = false;
    }

    // ========== 静态工具（与 Node 版同名，返回 Uint8Array） ==========
    static TYPES = RpcProtocolTypes;
    static CONTENT_TYPES = RpcContentTypes;

    /** [type u8][contentType u8][uuid 16B][payload] */
    static serialize(type, contentType, id, data) {
        const header = new Uint8Array(18);
        header[0] = type;
        header[1] = contentType;
        header.set(uuidToBytes(id), 2);

        let payload;
        if (contentType === RpcContentTypes.TEXT) payload = encoder.encode(String(data));
        else if (contentType === RpcContentTypes.JSON) payload = encoder.encode(JSON.stringify(data));
        else if (contentType === RpcContentTypes.BINARY) payload = data instanceof Uint8Array ? data : new Uint8Array(data);
        else throw new Error(`Unsupported content type: ${contentType}`);

        return concat([header, payload]);
    }

    static deserialize(bytes) {
        if (!bytes || bytes.length < 18) {
            throw new RpcError(`message too short (${bytes?.length})`, { code: 'EMALFORMED' });
        }
        const type = bytes[0];
        const contentType = bytes[1];
        const id = bytesToUuid(bytes, 2);
        const payload = bytes.subarray(18);

        let content;
        if (contentType === RpcContentTypes.TEXT) content = decoder.decode(payload);
        else if (contentType === RpcContentTypes.JSON) content = JSON.parse(decoder.decode(payload));
        else if (contentType === RpcContentTypes.BINARY) content = payload;
        else throw new RpcError(`invalid contentType ${contentType}`, { code: 'EMALFORMED' });

        return { type, contentType, id, content };
    }

    /** [methodLen u32BE][method][paramsLen u32BE][params JSON][binary] */
    static encodeBinaryRequest(method, params, binaryData) {
        const methodBytes = encoder.encode(method);
        const paramsBytes = encoder.encode(JSON.stringify(params ?? []));
        const body = binaryData instanceof Uint8Array ? binaryData : new Uint8Array(binaryData ?? []);
        return concat([u32be(methodBytes.length), methodBytes, u32be(paramsBytes.length), paramsBytes, body]);
    }

    static decodeBinaryRequest(bytes) {
        if (!bytes || bytes.length < 8) throw new RpcError('binary request too short', { code: 'EMALFORMED' });
        let offset = 0;
        const methodLength = readU32be(bytes, offset); offset += 4;
        if (offset + methodLength + 4 > bytes.length) throw new RpcError('binary request truncated', { code: 'EMALFORMED' });
        const method = decoder.decode(bytes.subarray(offset, offset + methodLength)); offset += methodLength;
        const paramsLength = readU32be(bytes, offset); offset += 4;
        if (offset + paramsLength > bytes.length) throw new RpcError('binary request params truncated', { code: 'EMALFORMED' });
        const params = JSON.parse(decoder.decode(bytes.subarray(offset, offset + paramsLength)));
        offset += paramsLength;
        return { method, params, binaryData: bytes.subarray(offset) };
    }

    // ========== 属性（镜像 Node 版字段名） ==========
    get pendingRequests() { return this._rpc.pending; }
    get capabilities() { return this._rpc.capabilities; }
    get implName() { return this._rpc.implName; }
    get peerCapabilities() { return this._rpc.peerCapabilities; }
    get serverCapabilities() { return this._rpc.serverCapabilities; }
    get heartbeatDisabledReason() { return this._rpc.heartbeatDisabledReason; }
    get heartbeatIntervalMs() { return this._rpc.heartbeatIntervalMs; }
    get heartbeatTimeoutMs() { return this._rpc.heartbeatTimeoutMs; }
    get heartbeatRequireCapability() { return this._rpc.heartbeatRequireCapability; }
    get authToken() { return this._rpc.authToken; }
    get requiredToken() { return [...this._rpc.requiredTokens][0] ?? null; }
    get requiredTokens() { return this._rpc.requiredTokens; }
    get defaultTimeoutMs() { return this._rpc.defaultTimeoutMs; }

    /** Node 版可关；浏览器精简栈恒为「有在途请求就不判超时」 */
    get heartbeatSkipWhilePending() { return true; }
    set heartbeatSkipWhilePending(value) {
        if (value === false && !this._warnedSkipWhilePending) {
            this._warnedSkipWhilePending = true;
            console.warn('[RpcProtocolCompat] heartbeatSkipWhilePending 恒为 true，该设置被忽略');
        }
    }

    // ========== 握手 / 请求 ==========
    /** 显式握手；启用鉴权时必须在业务请求前 await */
    hello(timeoutMs = this.defaultTimeoutMs) { return this._rpc.hello(timeoutMs); }

    /**
     * 与 Node 版签名一致：sendRequest(method, params, to, timeoutMs)。
     * `to` 在浏览器里没有意义（IPC 帧没有目标地址，路由由 WS 网关的信封负责），会被忽略；
     * 第三个参数若传数字，按出口版本的兼容习惯当作超时处理。
     */
    sendRequest(method, params = [], to = undefined, timeoutMs = this.defaultTimeoutMs) {
        if (typeof to === 'number') return this._rpc.sendRequest(method, params, to);
        return this._rpc.sendRequest(method, params, timeoutMs);
    }

    /** 与 Node 版一致的参数解析规则：末尾数字当超时，数组当附加参数 */
    sendRequestBinary(method, buffer, ...args) {
        let timeoutMs = this.defaultTimeoutMs;
        let extraArgs = args;
        if (args.length === 1 && typeof args[0] === 'number') { timeoutMs = args[0]; extraArgs = []; }
        else if (args.length === 2 && Array.isArray(args[0]) && typeof args[1] === 'number') { extraArgs = args[0]; timeoutMs = args[1]; }
        return this._rpc.sendRequestBinary(method, buffer, ...extraArgs, timeoutMs);
    }

    sendResponse(id, response, contentType = RpcContentTypes.JSON) {
        return this._rpc._sendRaw(RpcProtocolTypes.RESPONSE, contentType, id, response);
    }

    sendRaw(type, contentType, id, data) {
        return this._rpc._sendRaw(type, contentType, id, data);
    }

    registerMethod(name, handler) {
        this._rpc.registerMethod(name, handler);
        this.handlers[name] = handler;   // Proxy 的 set 会再写一次，幂等
    }

    onEvent(listener) { this.on('event', listener); }

    sendText(text) { return this._rpc.sendText(text); }
    sendJson(data) { return this._rpc.sendJson(data); }
    sendBinary(bytes) { return this._rpc.sendBinary(bytes); }

    setRequiredTokens(tokens) {
        this._rpc.requiredTokens = new Set(Array.isArray(tokens) ? tokens.filter(Boolean) : []);
        this._rpc._authenticated = this._rpc.requiredTokens.size === 0;
    }

    /** 帧层已经把完整报文交上来时可直接投喂（测试 / 自定义传输用） */
    handleMessage(bytes) { return this._rpc._handleMessage(bytes); }

    destroy() {
        this._rpc.destroy();
        this.removeAllListeners();
    }
}

export default RpcProtocol;
