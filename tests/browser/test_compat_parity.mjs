// 接口形状一致性回归：rpc-protocol-compat（子包 ipcprotocol-browser）vs Node 版 RpcProtocol
// 同一段 scenario 代码分别跑在「Node 版」与「浏览器 compat」上，比较可观察结果是否一致。
import { RpcProtocol as NodeRpc, RpcError as NodeRpcError } from '../../RpcProtocol.js';
import { RpcProtocol as CompatRpc, RpcError as CompatRpcError } from 'ipcprotocol-browser';

let pass = 0, fail = 0;
function check(name, cond, detail = '') {
    if (cond) { pass++; console.log('  PASS', name); }
    else { fail++; console.log('  FAIL', name, detail); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------- 假流：一对互连的全双工，形状同时满足 MessageStream / MiniMessageStream ----------------
class FakeStream {
    constructor(name) {
        this.name = name;
        this.peer = null;
        this.destroyed = false;
        this._listeners = new Map();
        this.bytesSent = 0;
        this.messagesSent = 0;
    }

    on(event, fn) {
        if (!this._listeners.has(event)) this._listeners.set(event, []);
        this._listeners.get(event).push(fn);
        return this;
    }

    once(event, fn) {
        const wrap = (...args) => { this.off(event, wrap); fn(...args); };
        return this.on(event, wrap);
    }

    off(event, fn) {
        const list = this._listeners.get(event);
        if (!list) return this;
        const i = list.indexOf(fn);
        if (i >= 0) list.splice(i, 1);
        return this;
    }

    _emit(event, ...args) {
        for (const fn of [...(this._listeners.get(event) || [])]) fn(...args);
    }

    send(bytes) {
        if (this.destroyed) return Promise.reject(Object.assign(new Error('closed'), { code: 'ECLOSED' }));
        this.bytesSent += bytes.length;
        this.messagesSent++;
        const copy = Buffer.from(bytes);           // 模拟真实传输：线上不共享内存
        this.peer && setTimeout(() => this.peer._emit('message', copy), 0);
        return Promise.resolve();
    }

    destroy() {
        if (this.destroyed) return;
        this.destroyed = true;
        this._emit('close');
    }
}

function makePair() {
    const a = new FakeStream('a');
    const b = new FakeStream('b');
    a.peer = b; b.peer = a;
    return [a, b];
}

const arr = (x) => Array.from(x instanceof Uint8Array ? x : Buffer.from(x));

// ---------------- 同一个 scenario，两边各跑一遍 ----------------
async function scenario(ctor, env) {
    const log = [];
    // Node 版内部定时器都是 unref 的，纯假流场景下事件循环会提前空转；
    // 这里挂一个 ref 的定时器把循环撑住，否则进程会在等待超时前退出。
    const keepAlive = setInterval(() => { }, 100);
    const [sa, sb] = makePair();
    const opts = { implName: env.label, heartbeatIntervalMs: 80, heartbeatTimeoutMs: 600 };

    const A = new ctor(sa, opts);
    const B = new ctor(sb, opts);

    const errors = [];
    A.on('error', (e) => errors.push(e.code));
    B.on('error', (e) => errors.push(e.code));

    let eventOnceCount = 0;
    A.once('event', () => eventOnceCount++);

    // 用 registerMethod 注册（Node / compat 同一条路径）
    B.registerMethod('add', (x, y) => x + y);
    B.registerMethod('greet', (name) => `hello ${name}`);
    B.registerMethod('echo', (o) => o);
    B.registerMethod('blob', () => env.bin([0, 1, 2, 255]));
    B.registerMethod('upload', (data, tag) => ({ size: data.length, tag, head: arr(data).slice(0, 3) }));
    B.registerMethod('boom', () => { throw new Error('handler exploded'); });
    B.registerMethod('slow', () => new Promise(() => { }));

    // 用 handlers 对象直接塞（Node 原生就支持，compat 靠 Proxy 对齐）
    B.handlers.extra = (x) => x * 2;

    A.handlers.revAdd = (x, y) => x + y;

    // handlers 形状
    log.push(['handlers-keys', Object.keys(B.handlers).sort().join(',')]);

    const ack = await A.hello();
    log.push(['hello', ack.ok, ack.version, (ack.caps || []).slice().sort().join(',')]);
    log.push(['peerCaps', (A.peerCapabilities || []).slice().sort().join(',')]);
    log.push(['serverCaps', (A.serverCapabilities || []).slice().sort().join(',')]);

    log.push(['add', await A.sendRequest('add', [1, 2])]);
    log.push(['add-with-to', await A.sendRequest('add', [4, 5], 'somepeer')]);
    log.push(['greet', await A.sendRequest('greet', ['world'])]);
    log.push(['echo', await A.sendRequest('echo', [{ a: 1, b: '中文' }])]);
    log.push(['extra-via-handlers', await A.sendRequest('extra', [21])]);

    const blob = await A.sendRequest('blob');
    log.push(['blob', env.isBinary(blob), arr(blob)]);

    const up = await A.sendRequestBinary('upload', env.bin([9, 8, 7, 6, 5]), 'tag');
    log.push(['upload', up.size, up.tag, up.head]);

    // 反向调用：B → A
    log.push(['revAdd', await B.sendRequest('revAdd', [3, 4])]);

    // 错误：方法不存在 / handler 抛错
    try { await A.sendRequest('nope', []); } catch (e) { log.push(['no-method', e.code, e.remote === true, e instanceof (env.errCtor)]); }
    try { await A.sendRequest('boom', []); } catch (e) { log.push(['handler-throw', e.code, e.remote === true]); }

    // 超时
    try { await A.sendRequest('slow', [], undefined, 250); } catch (e) { log.push(['timeout', e.code]); }

    // 事件三类，双向
    const got = [];
    const onEv = (m) => got.push([m.contentType, env.contentOf(m.content)]);
    B.on('event', onEv);
    await A.sendJson({ k: 1 });
    await A.sendText('plain');
    await A.sendBinary(env.bin([7, 7]));
    await sleep(80);
    log.push(['events-A->B', JSON.stringify(got.sort((x, y) => x[0] - y[0]))]);

    const back = [];
    A.on('event', (m) => back.push(m.contentType));
    await B.sendJson([1, 2, 3]);
    await sleep(60);
    log.push(['events-B->A', back.join(',')]);
    log.push(['once-only-fired-once', eventOnceCount === 1]);

    // off 生效
    B.off('event', onEv);
    got.length = 0;
    await A.sendJson({ k: 2 });
    await sleep(50);
    log.push(['after-off', got.length]);

    // 心跳：跨过多个心跳周期仍能调用
    await sleep(350);
    log.push(['alive-after-heartbeat', await A.sendRequest('add', [5, 6])]);

    // 静态工具
    const id = '123e4567-e89b-12d3-a456-426614174000';
    const frame = ctor.serialize(0, 1, id, { method: 'x', params: [1] });
    const parsed = ctor.deserialize(env.toBytes(frame));
    log.push(['static-roundtrip', parsed.type, parsed.contentType, parsed.id, JSON.stringify(parsed.content)]);
    const binBody = ctor.encodeBinaryRequest('m', [1, 2], env.bin([5, 6]));
    const binParsed = ctor.decodeBinaryRequest(env.toBytes(binBody));
    log.push(['static-binary', binParsed.method, JSON.stringify(binParsed.params), arr(binParsed.binaryData)]);
    log.push(['TYPES', JSON.stringify(ctor.TYPES), JSON.stringify(ctor.CONTENT_TYPES)]);

    // 销毁后：在途请求被结算
    const [sc, sd] = makePair();
    const C = new ctor(sc, { heartbeatIntervalMs: 0 });
    const D = new ctor(sd, { heartbeatIntervalMs: 0 });
    D.registerMethod('never', () => new Promise(() => { }));
    const blow = C.sendRequest('never', []);
    await sleep(30);
    C.destroy();
    try { await blow; log.push(['destroy-pending', 'resolved?!']); }
    catch (e) { log.push(['destroy-pending', e.code]); }
    D.destroy();

    log.push(['errors-seen', errors.join(',') || 'none']);
    log.push(['peerErrors', A.peerCapabilities ? 'ok' : 'null']);

    A.destroy();
    B.destroy();
    clearInterval(keepAlive);
    return log;
}

// ---------------- 两套环境 ----------------
const nodeEnv = {
    label: 'node',
    bin: (a) => Buffer.from(a),
    isBinary: (x) => Buffer.isBuffer(x),
    contentOf: (c) => (Buffer.isBuffer(c) ? arr(c) : c),
    toBytes: (x) => (Buffer.isBuffer(x) ? x : Buffer.from(x)),
    errCtor: NodeRpcError,
};

const browserEnv = {
    label: 'browser',
    bin: (a) => new Uint8Array(a),
    isBinary: (x) => x instanceof Uint8Array,
    contentOf: (c) => (c instanceof Uint8Array ? arr(c) : c),
    toBytes: (x) => (x instanceof Uint8Array ? x : new Uint8Array(x)),
    errCtor: CompatRpcError,
};

console.log('[1] Node 版 RpcProtocol');
const nodeLog = await scenario(NodeRpc, nodeEnv);
console.log('[2] 浏览器 compat');
const compatLog = await scenario(CompatRpc, browserEnv);

console.log('[3] 逐项比对');
let allSame = true;
const n = Math.max(nodeLog.length, compatLog.length);
for (let i = 0; i < n; i++) {
    const a = JSON.stringify(nodeLog[i]);
    const b = JSON.stringify(compatLog[i]);
    const same = a === b;
    if (!same) allSame = false;
    check(`#${i + 1} ${nodeLog[i]?.[0] ?? compatLog[i]?.[0]}`, same, `\n    node  : ${a}\n    compat: ${b}`);
}
check('两边观察点数量一致', nodeLog.length === compatLog.length, `${nodeLog.length} vs ${compatLog.length}`);
check('compat 错误也 instanceof RpcError', compatLog.some((e) => e[0] === 'no-method' && e[3] === true));
check('Node 错误也 instanceof RpcError', nodeLog.some((e) => e[0] === 'no-method' && e[3] === true));

console.log(`\n结果: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);

