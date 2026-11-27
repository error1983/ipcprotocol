// 真实 WS 链路回归：ipcprotocol-browser 的 compat 层 ↔ Node 版 RpcProtocol
import { createRelay } from '../../examples/ws-relay-gateway.js';
import { WsTransport, MiniMessageStream, RpcProtocol as BrowserRpc, RpcError as BrowserRpcError } from 'ipcprotocol-browser';
import { MessageStream } from '../../MessageStream.js';
import { RpcProtocol as NodeRpc } from '../../RpcProtocol.js';

let pass = 0, fail = 0;
const check = (name, cond, extra = '') => {
    if (cond) { pass++; console.log(`  PASS ${name}`); }
    else { fail++; console.log(`  FAIL ${name} ${extra}`); }
};
const open = (t) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('连接超时')), 5000);
    t.once('open', () => { clearTimeout(timer); resolve(); });
    t.once('error', (e) => { clearTimeout(timer); reject(e); });
});

async function main() {
    const relay = await createRelay({ port: 0, quiet: true });
    const url = `ws://127.0.0.1:${relay.port}`;

    // bob：Node 原版
    const bLink = new WsTransport(url, { peer: 'bob', to: 'alice' });
    await open(bLink);
    const bob = new NodeRpc(new MessageStream({ socket: bLink }), { heartbeatIntervalMs: 300, heartbeatTimeoutMs: 5000 });
    bob.on('error', (e) => console.log('  [bob] error', e.code, e.message));
    bob.registerMethod('add', (a, b) => a + b);
    bob.registerMethod('echo', (o) => o);
    bob.registerMethod('blob', () => Buffer.from([0, 1, 2, 255]));
    bob.registerMethod('upload', (data, tag) => ({ size: data.length, tag, head: [...data.subarray(0, 3)] }));
    bob.registerMethod('slow', () => new Promise(() => { }));

    // alice：compat 层，写法与 Node 完全一致
    const aLink = new WsTransport(url, { peer: 'alice', to: 'bob' });
    await open(aLink);
    const alice = new BrowserRpc(new MiniMessageStream(aLink), {
        implName: 'browser/compat', heartbeatIntervalMs: 300, heartbeatTimeoutMs: 5000,
    });
    alice.on('error', (e) => console.log('  [alice] error', e.code, e.message));

    const events = [];
    alice.on('event', (m) => events.push(m.contentType));
    alice.registerMethod('greet', (name) => `hi ${name}`);
    alice.handlers.double = (x) => x * 2;      // handlers 对象写法也要能用

    console.log('[1] 握手');
    const ack = await alice.hello(5000);
    check('hello ok', ack.ok === true);
    check('serverCapabilities 可见', Array.isArray(alice.serverCapabilities) && alice.serverCapabilities.includes('ping'));

    console.log('[2] 用 Node 写法调用');
    check('add', (await alice.sendRequest('add', [1, 2])) === 3);
    check('echo 中文', (await alice.sendRequest('echo', [{ t: '中文' }])).t === '中文');
    const blob = await alice.sendRequest('blob');
    check('二进制响应', blob instanceof Uint8Array && blob[3] === 255);
    const up = await alice.sendRequestBinary('upload', new Uint8Array([9, 8, 7]), 'compat');
    check('二进制请求体', up.size === 3 && up.tag === 'compat' && up.head[0] === 9, JSON.stringify(up));
    check('handlers 里的方法可被调用', (await bob.sendRequest('double', [21])) === 42);

    console.log('[3] 错误与超时');
    try { await alice.sendRequest('nope', []); check('方法不存在', false); }
    catch (e) { check('方法不存在 → EREMOTE', e.code === 'EREMOTE' && e instanceof BrowserRpcError); }
    try { await alice.sendRequest('slow', [], undefined, 300); check('超时', false); }
    catch (e) { check('超时 → ETIMEDOUT', e.code === 'ETIMEDOUT'); }

    console.log('[4] 事件');
    await bob.sendJson({ from: 'bob' });
    await bob.sendText('hi');
    await new Promise((r) => setTimeout(r, 200));
    check('收到两类事件', events.length === 2 && events.includes(1) && events.includes(0), JSON.stringify(events));

    console.log('[5] 跨心跳周期保活');
    await new Promise((r) => setTimeout(r, 700));
    check('仍可调用', (await alice.sendRequest('add', [20, 22])) === 42);

    console.log('[6] 静态工具与销毁');
    const frame = BrowserRpc.serialize(0, 1, '123e4567-e89b-12d3-a456-426614174000', { m: 1 });
    check('serialize → Uint8Array', frame instanceof Uint8Array && frame.length === 18 + JSON.stringify({ m: 1 }).length);
    const parsed = BrowserRpc.deserialize(frame);
    check('deserialize 还原', parsed.type === 0 && parsed.id === '123e4567-e89b-12d3-a456-426614174000' && parsed.content.m === 1);

    alice.destroy();
    bob.destroy();
    await relay.close();
    console.log(`\n结果: ${pass} passed, ${fail} failed`);
    process.exit(fail ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });

