// modules/ipc/RpcProtocol.js — 修复版（线格式不变）
// payload 内部结构: type(1) | contentType(1) | uuid(16 raw) | business payload
import { v4 as uuidv4 } from 'uuid';
import { EventEmitter } from 'events';

export { uuidv4 }

const DEFAULT_TIMEOUT = 15000;
const PROTOCOL_VERSION = 1;

export class RpcError extends Error {
    constructor(message, { code = 'RPC_ERROR', remote = false, data = undefined } = {}) {
        super(message);
        this.name = 'RpcError';
        this.code = code;
        this.remote = remote;
        this.data = data;
    }
}

export class RpcProtocol extends EventEmitter {
    constructor(messageStream, options = {}) {
        if (!messageStream) throw new Error('messageStream is undefined');
        super();
        this.messageStream = messageStream;
        this.handlers = {};
        this.pendingRequests = new Map();
        this._destroyed = false;

        // v1.1：心跳与可选 HELLO 握手（PROTOCOL.md §8.1/§8.2）
        this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 5000;
        this.heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? 15000;
        this.authToken = options.authToken ?? null;
        this.requiredToken = options.requiredToken ?? null;
        this._authenticated = false;
        this._lastReceived = Date.now();
        this._heartbeatTimer = null;
        if (this.heartbeatIntervalMs > 0) {
            this._heartbeatTimer = setInterval(() => this._heartbeatTick(), this.heartbeatIntervalMs);
            this._heartbeatTimer.unref?.();
        }

        this._onMessage = (buffer) => this.handleMessage(buffer);
        this._onStreamError = (error) => {
            this.handleStreamError(error);
            this._safeEmit('error', error);
        };
        this._onSync = () => this.emit('syncComplete');   // 信息类事件，无监听器不报错

        this.messageStream.on('message', this._onMessage);
        this.messageStream.on('error', this._onStreamError);
        this.messageStream.on('syncComplete', this._onSync);
    }

    _safeEmit(event, payload) {
        if (this.listenerCount(event) > 0) this.emit(event, payload);
        else console.error(`[RpcProtocol] unhandled '${event}':`, payload?.code || payload?.message || payload);
    }

    // ========== 协议定义 ==========
    static TYPES = { REQUEST: 0, RESPONSE: 1, EVENT: 2, PING: 3, PONG: 4, HELLO: 5, HELLO_ACK: 6 };
    static CONTENT_TYPES = { TEXT: 0, JSON: 1, BINARY: 2 };

    static serialize(type, contentType, id, data) {
        const header = Buffer.alloc(1 + 1 + 16);
        header.writeUInt8(type, 0);
        header.writeUInt8(contentType, 1);
        const idBuffer = Buffer.from(id.replace(/-/g, ''), 'hex');
        idBuffer.copy(header, 2);

        let payload;
        if (contentType === RpcProtocol.CONTENT_TYPES.TEXT) payload = Buffer.from(data, 'utf8');
        else if (contentType === RpcProtocol.CONTENT_TYPES.JSON) payload = Buffer.from(JSON.stringify(data), 'utf8');
        else if (contentType === RpcProtocol.CONTENT_TYPES.BINARY) payload = Buffer.from(data);
        else throw new Error(`Unsupported content type: ${contentType}`);

        return Buffer.concat([header, payload]);
    }

    static deserialize(buffer) {
        if (!Buffer.isBuffer(buffer) || buffer.length < 18) {
            throw new RpcError(`message too short (${buffer?.length})`, { code: 'EMALFORMED' });
        }
        const type = buffer.readUInt8(0);
        const contentType = buffer.readUInt8(1);
        // ★ 不再用正则格式化 uuid：直接按 8-4-4-4-12 切分，长度固定可控
        const hex = buffer.subarray(2, 18).toString('hex');
        const id = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;

        const payload = buffer.subarray(18);
        let content;
        if (contentType === RpcProtocol.CONTENT_TYPES.TEXT) content = payload.toString('utf8');
        else if (contentType === RpcProtocol.CONTENT_TYPES.JSON) content = JSON.parse(payload.toString('utf8'));
        else if (contentType === RpcProtocol.CONTENT_TYPES.BINARY) content = payload;
        else throw new RpcError(`invalid contentType ${contentType}`, { code: 'EMALFORMED' });

        return { type, contentType, id, content };
    }

    static encodeBinaryRequest(method, params, binaryData) {
        const methodBuffer = Buffer.from(method, 'utf8');
        const methodLength = methodBuffer.length;
        const paramsJson = JSON.stringify(params);
        const paramsBuffer = Buffer.from(paramsJson, 'utf8');
        const paramsLength = paramsBuffer.length;

        const totalLength = 4 + methodLength + 4 + paramsLength + binaryData.length;
        const result = Buffer.alloc(totalLength);
        let offset = 0;
        result.writeUInt32BE(methodLength, offset); offset += 4;
        methodBuffer.copy(result, offset); offset += methodLength;
        result.writeUInt32BE(paramsLength, offset); offset += 4;
        paramsBuffer.copy(result, offset); offset += paramsLength;
        Buffer.from(binaryData).copy(result, offset);
        return result;
    }

    static decodeBinaryRequest(buffer) {
        let offset = 0;
        if (buffer.length < 8) throw new RpcError('binary request too short', { code: 'EMALFORMED' });
        const methodLength = buffer.readUInt32BE(offset); offset += 4;
        if (offset + methodLength + 4 > buffer.length) throw new RpcError('binary request truncated', { code: 'EMALFORMED' });
        const method = buffer.toString('utf8', offset, offset + methodLength); offset += methodLength;
        const paramsLength = buffer.readUInt32BE(offset); offset += 4;
        if (offset + paramsLength > buffer.length) throw new RpcError('binary request params truncated', { code: 'EMALFORMED' });
        const paramsJson = buffer.toString('utf8', offset, offset + paramsLength);
        const params = JSON.parse(paramsJson);
        offset += paramsLength;
        return { method, params, binaryData: buffer.subarray(offset) };
    }

    // ========== 请求处理 ==========
    async handleRequest(id, request) {
        let method, params, args;

        if (Buffer.isBuffer(request)) {
            const decoded = RpcProtocol.decodeBinaryRequest(request);
            method = decoded.method;
            params = decoded.params;
            args = [decoded.binaryData, ...params];
        } else {
            method = request?.method;
            params = request?.params;
            if (Array.isArray(params)) args = [...params];
            else if (params === null || params === undefined) args = [];
            else args = [params];
        }

        const handler = this.handlers?.[method];
        if (!handler) {
            this.sendResponse(id, { error: `Method not found: ${method}` }, RpcProtocol.CONTENT_TYPES.JSON).catch(() => { });
            return;
        }

        try {
            const result = await handler(...args);
            let contentType = RpcProtocol.CONTENT_TYPES.JSON;
            let responseData = result;

            if (result === undefined || result === null) {
                responseData = null;
                contentType = RpcProtocol.CONTENT_TYPES.JSON;
            } else if (typeof result === 'string') {
                contentType = RpcProtocol.CONTENT_TYPES.TEXT;
            } else if (Buffer.isBuffer(result)) {
                contentType = RpcProtocol.CONTENT_TYPES.BINARY;
            }
            await this.sendResponse(id, responseData, contentType);
        } catch (err) {
            await this.sendResponse(id, { error: err?.message || String(err) }, RpcProtocol.CONTENT_TYPES.JSON).catch(() => { });
        }
    }

    handleMessage(buffer) {
        if (this._destroyed) return;
        this._lastReceived = Date.now();   // 任何入站消息都刷新心跳活动时刻
        let parsed;
        try {
            parsed = RpcProtocol.deserialize(buffer);
        } catch (err) {
            this._safeEmit('error', new RpcError(`failed to deserialize: ${err.message}`, { code: 'EMALFORMED' }));
            // 尽量回一个错误响应，避免对方永久等待
            try {
                if (buffer.length >= 18 && buffer.readUInt8(0) === RpcProtocol.TYPES.REQUEST) {
                    const hex = buffer.subarray(2, 18).toString('hex');
                    const id = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
                    this.sendResponse(id, { error: 'Invalid request payload' }, RpcProtocol.CONTENT_TYPES.JSON).catch(() => { });
                }
            } catch { }
            return;
        }

        const { type, id, content, contentType } = parsed;

        if (!Object.values(RpcProtocol.TYPES).includes(type) || !Object.values(RpcProtocol.CONTENT_TYPES).includes(contentType)) {
            this._safeEmit('error', new RpcError(`invalid type/contentType ${type}/${contentType}`, { code: 'EMALFORMED' }));
            return;
        }

        if (type === RpcProtocol.TYPES.REQUEST) {
            if (this.requiredToken && !this._authenticated) {
                this.sendResponse(id, { error: 'Unauthorized' }, RpcProtocol.CONTENT_TYPES.JSON)
                    .then(() => this.destroy()).catch(() => this.destroy());
                return;
            }
            this.handleRequest(id, content).catch(err => {
                this._safeEmit('error', new RpcError(`handler failure: ${err.message}`, { code: 'EHANDLER' }));
            });
        } else if (type === RpcProtocol.TYPES.RESPONSE) {
            this.handleResponse(id, content);
        } else if (type === RpcProtocol.TYPES.EVENT) {
            if (this.requiredToken && !this._authenticated) return;   // 未鉴权事件丢弃
            this._safeEmit('event', { contentType, content });
        } else if (type === RpcProtocol.TYPES.PING) {
            this.sendRaw(RpcProtocol.TYPES.PONG, RpcProtocol.CONTENT_TYPES.TEXT, id, '').catch(() => { });
        } else if (type === RpcProtocol.TYPES.PONG) {
            // 仅刷新 lastReceived，不交付应用层
        } else if (type === RpcProtocol.TYPES.HELLO) {
            this.handleHello(id, content);
        } else if (type === RpcProtocol.TYPES.HELLO_ACK) {
            this.handleHelloAck(id, content);
        }
    }

    // ========== 心跳（§8.1） ==========
    _heartbeatTick() {
        if (this._destroyed) return;
        const silent = Date.now() - this._lastReceived;
        if (this.heartbeatTimeoutMs > 0 && silent >= this.heartbeatTimeoutMs) {
            const err = new RpcError(`heartbeat timeout: no data for ${silent}ms`, { code: 'EHEARTBEAT' });
            this._rejectAll(err);
            this._safeEmit('error', err);
            this.destroy();
            return;
        }
        if (silent >= this.heartbeatIntervalMs) {
            this.sendRaw(RpcProtocol.TYPES.PING, RpcProtocol.CONTENT_TYPES.TEXT, uuidv4(), '').catch(() => { });
        }
    }

    // ========== HELLO 握手（§8.2） ==========
    handleHello(id, content) {
        const version = content?.version;
        let ack, fail = false;
        if (version !== PROTOCOL_VERSION) {
            ack = { error: `unsupported protocol version: ${version}` }; fail = true;
        } else if (this.requiredToken && content?.token !== this.requiredToken) {
            ack = { error: 'unauthorized' }; fail = true;
        } else {
            this._authenticated = true;
            ack = { version: PROTOCOL_VERSION, ok: true };
        }
        this.sendRaw(RpcProtocol.TYPES.HELLO_ACK, RpcProtocol.CONTENT_TYPES.JSON, id, ack)
            .then(() => { if (fail) this.destroy(); })
            .catch(() => { if (fail) this.destroy(); });
    }

    handleHelloAck(id, content) {
        const pending = this.pendingRequests.get(id);
        if (!pending) return;
        this.pendingRequests.delete(id);
        if (pending.timer) clearTimeout(pending.timer);
        if (content && typeof content === 'object' && content.error) {
            pending.reject(new RpcError(String(content.error), { code: 'EAUTH', remote: true, data: content.error }));
        } else if (!content || content.version !== PROTOCOL_VERSION) {
            pending.reject(new RpcError(`unsupported protocol version: ${content?.version}`, { code: 'EVERSION', remote: true }));
        } else {
            pending.resolve(content);
        }
    }

    /** 显式握手：启用鉴权时调用方必须先 await hello() 再发业务请求 */
    hello(timeoutMs = DEFAULT_TIMEOUT) {
        const id = uuidv4();
        const payload = { version: PROTOCOL_VERSION };
        if (this.authToken) payload.token = this.authToken;
        return this._sendAndTrack(id, RpcProtocol.TYPES.HELLO, RpcProtocol.CONTENT_TYPES.JSON, payload, timeoutMs, 'hello');
    }

    handleResponse(id, response) {
        const pending = this.pendingRequests.get(id);
        if (!pending) return;
        this.pendingRequests.delete(id);
        if (pending.timer) clearTimeout(pending.timer);

        if (response && typeof response === 'object' && 'error' in response && response.error) {
            // ★ 统一用 Error 对象，保留远程错误信息
            const message = typeof response.error === 'string' ? response.error : JSON.stringify(response.error);
            pending.reject(new RpcError(message, { code: 'EREMOTE', remote: true, data: response.error }));
        } else {
            pending.resolve(response);
        }
    }

    handleStreamError(error) {
        // 连接级错误：所有在途请求必须结算，否则永久泄漏
        this._rejectAll(new RpcError(`stream error: ${error?.message || error}`, { code: error?.code || 'ESTREAM' }));
    }

    _rejectAll(err) {
        if (!this.pendingRequests) return;
        for (const [, pending] of this.pendingRequests) {
            if (pending.timer) clearTimeout(pending.timer);
            try { pending.reject(err); } catch { }
        }
        this.pendingRequests.clear();
    }

    // ========== 发送 ==========
    sendRaw(type, contentType, id, data) {
        if (!this.messageStream) return Promise.reject(new RpcError('messageStream is not initialized', { code: 'ECLOSED' }));
        const message = RpcProtocol.serialize(type, contentType, id, data);
        return this.messageStream.send(message);
    }

    _track(id, timeoutMs, method) {
        return new Promise((resolve, reject) => {
            const pending = {
                resolve: (v) => { if (pending.timer) clearTimeout(pending.timer); resolve(v); },
                reject: (e) => { if (pending.timer) clearTimeout(pending.timer); reject(e); },
                timer: null,
            };
            if (timeoutMs && timeoutMs > 0) {
                pending.timer = setTimeout(() => {
                    if (this.pendingRequests?.delete(id)) {
                        reject(new RpcError(`RPC timeout after ${timeoutMs}ms: ${method}`, { code: 'ETIMEDOUT' }));
                    }
                }, timeoutMs);
                pending.timer.unref?.();
            }
            this.pendingRequests.set(id, pending);
        });
    }

    _sendAndTrack(id, type, contentType, data, timeoutMs, method) {
        const p = this._track(id, timeoutMs, method);
        Promise.resolve(this.sendRaw(type, contentType, id, data)).catch(err => {
            const pending = this.pendingRequests?.get(id);
            if (pending) {
                this.pendingRequests.delete(id);
                if (pending.timer) clearTimeout(pending.timer);
                pending.reject(new RpcError(`send failed: ${err?.message || err}`, { code: err?.code || 'ESEND' }));
            }
        });
        return p;
    }

    sendRequest(method, params, to = undefined, timeoutMs = DEFAULT_TIMEOUT) {
        const id = uuidv4();
        return this._sendAndTrack(id, RpcProtocol.TYPES.REQUEST, RpcProtocol.CONTENT_TYPES.JSON,
            { method, params, to }, timeoutMs, method);
    }

    sendRequestBinary(method, buffer, ...args) {
        const timeoutMs = DEFAULT_TIMEOUT;
        const id = uuidv4();
        const binaryRequest = RpcProtocol.encodeBinaryRequest(method, args, buffer);
        return this._sendAndTrack(id, RpcProtocol.TYPES.REQUEST, RpcProtocol.CONTENT_TYPES.BINARY,
            binaryRequest, timeoutMs, method);
    }

    sendResponse(id, response, contentType = RpcProtocol.CONTENT_TYPES.JSON) {
        return this.sendRaw(RpcProtocol.TYPES.RESPONSE, contentType, id, response);
    }

    onEvent(listener) { this.on('event', listener); }

    registerMethod(name, handler) { this.handlers[name] = handler; }

    sendText(text) { return this.sendRaw(RpcProtocol.TYPES.EVENT, RpcProtocol.CONTENT_TYPES.TEXT, uuidv4(), text); }
    sendBinary(data) { return this.sendRaw(RpcProtocol.TYPES.EVENT, RpcProtocol.CONTENT_TYPES.BINARY, uuidv4(), data); }
    sendJson(data) { return this.sendRaw(RpcProtocol.TYPES.EVENT, RpcProtocol.CONTENT_TYPES.JSON, uuidv4(), data); }

    destroy() {
        if (this._destroyed) return;
        this._destroyed = true;
        if (this._heartbeatTimer) { clearInterval(this._heartbeatTimer); this._heartbeatTimer = null; }
        this._rejectAll(new RpcError('RPC Protocol destroyed', { code: 'ECLOSED' }));
        this.pendingRequests = null;
        try { this.messageStream?.off('message', this._onMessage); } catch { }
        try { this.messageStream?.off('error', this._onStreamError); } catch { }
        try { this.messageStream?.off('syncComplete', this._onSync); } catch { }
        try { this.messageStream?.destroy(); } catch { }
        this.messageStream = null;
        this.handlers = null;
        this.removeAllListeners();
    }
}
