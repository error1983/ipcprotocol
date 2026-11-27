// examples/ws-rpc-demo.js — 两个 peer 经中继网关互调 RPC（最小可运行示例 + 自检）
//
// 链路：alice ⇄ WsTransport ⇄ 网关 ⇄ WsTransport ⇄ bob
// 关键点：中间的字节就是 MessageStream 的帧，网关只认路由信封、不解析协议内容，
// 所以两端用的还是仓库里那套 MessageStream + RpcProtocol，PROTOCOL.md 一个字节没改。
//
// 运行: node examples/ws-rpc-demo.js
// 依赖: Node ≥ 22（用内置 WebSocket 客户端）；浏览器端同理，把 WsTransport 换成 <script type=module>
import { createRelay } from './ws-relay-gateway.js';
import { WsTransport } from './browser/ws-transport.js';
import { MessageStream } from '../MessageStream.js';
import { RpcProtocol } from '../RpcProtocol.js';

let passed = 0;
let failed = 0;

function check(name, cond, extra = '') {
    if (cond) { passed++; console.log(`  PASS ${name}`); }
    else { failed++; console.log(`  FAIL ${name} ${extra}`); }
}

/** 建立一个 peer 到网关的连接，并套上仓库原版 MessageStream + RpcProtocol */
async function connect(url, peer, to) {
    const transport = new WsTransport(url, { peer, to });
    transport.on('error', (err) => console.log(`  [${peer}] transport: ${err.code} ${err.message}`));

    await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`${peer} 连接超时`)), 5000);
        transport.once('open', () => { clearTimeout(timer); resolve(); });
        transport.once('error', (err) => { clearTimeout(timer); reject(err); });
    });

    const stream = new MessageStream({ socket: transport });
    const rpc = new RpcProtocol(stream);
    rpc.on('error', (err) => console.log(`  [${peer}] stream: ${err.code} ${err.message}`));
    return { transport, stream, rpc };
}

async function main() {
    if (typeof WebSocket === 'undefined') {
        console.error('需要 Node ≥ 22（内置 WebSocket）。低版本请 npm i ws 并替换 globalThis.WebSocket。');
        process.exit(2);
    }

    const relay = await createRelay({ port: 0, quiet: true });
    const url = `ws://127.0.0.1:${relay.port}`;
    console.log(`网关: ${url}（在线节点见 ${url.replace('ws://', 'http://')}/peers）\n`);

    const bob = await connect(url, 'bob', 'alice');
    bob.rpc.registerMethod('add', (a, b) => a + b);
    bob.rpc.registerMethod('echo', (obj) => obj);
    bob.rpc.registerMethod('blob', () => Buffer.from([0x00, 0x01, 0x02, 0xff]));
    bob.rpc.registerMethod('upload', (data, tag) => ({ size: data.length, tag, head: [...data.subarray(0, 4)] }));
    bob.rpc.registerMethod('pushEvents', async () => {
        await bob.rpc.sendJson({ push: 1 });
        await bob.rpc.sendText('bob-text');
        return 'sent';
    });

    const alice = await connect(url, 'alice', 'bob');
    const events = [];
    alice.rpc.on('event', (msg) => events.push(msg));

    console.log('[1] 握手与单向调用（alice → bob）');
    const ack = await alice.rpc.hello();
    check('hello 成功', !!ack && Array.isArray(ack.caps), JSON.stringify(ack));
    check('add(1,2) = 3', (await alice.rpc.sendRequest('add', [1, 2])) === 3);
    const echoed = await alice.rpc.sendRequest('echo', ['中文 ✓']);
    check('echo 保留 Unicode', echoed === '中文 ✓', String(echoed));
    const blob = await alice.rpc.sendRequest('blob', []);
    check('二进制响应', Buffer.isBuffer(blob) && blob.equals(Buffer.from([0, 1, 2, 0xff])), String(blob));

    console.log('[2] 二进制请求体（alice → bob）');
    const uploaded = await alice.rpc.sendRequestBinary('upload', Buffer.from([9, 8, 7, 6, 5]), 'raw-tag');
    check('upload 收到 5B + 参数', uploaded.size === 5 && uploaded.tag === 'raw-tag' && uploaded.head[0] === 9,
        JSON.stringify(uploaded));

    console.log('[3] 反向调用（bob → alice），验证 RPC 是双向的');
    alice.rpc.registerMethod('greet', (name) => `hi ${name}`);
    check('bob 调 alice.greet', (await bob.rpc.sendRequest('greet', ['bob'])) === 'hi bob');

    console.log('[4] 事件推送（bob → alice）');
    await alice.rpc.sendRequest('pushEvents', []);
    await new Promise((r) => setTimeout(r, 150));
    check('收到 2 条事件', events.length === 2, `实际 ${events.length}`);
    check('事件内容正确', JSON.stringify(events[0]?.content) === '{"push":1}', JSON.stringify(events[0]));

    console.log('[5] 目标不在线时由调用方超时兜底');
    const carol = await connect(url, 'carol', 'nobody');   // 指向不存在的 peer
    carol.rpc.on('error', () => { });
    const t0 = Date.now();
    let timedOut = false;
    try {
        await carol.rpc.sendRequest('add', [1, 2], undefined, 600);
    } catch (err) {
        timedOut = err.code === 'ETIMEDOUT';
    }
    check('未路由时 600ms 超时', timedOut, `耗时 ${Date.now() - t0}ms`);

    console.log('[6] 网关旁路查询 /peers');
    const res = await fetch(`${url.replace('ws://', 'http://')}/peers`);
    const info = await res.json();
    check('在线 3 个节点', info.count === 3, JSON.stringify(info.peers.map((p) => p.id)));
    // 计数单位是 WS 消息块：一个 IPC 帧会被拆成 head/payload/tail 多次写入
    check('未路由的字节被丢弃', info.unroutable >= 1 && info.unroutedBytes > 0,
        `chunks=${info.unroutable} bytes=${info.unroutedBytes}`);

    console.log('\n清理连接…');
    for (const peer of [alice, bob, carol]) { try { peer.rpc.destroy(); } catch { } }
    await new Promise((r) => setTimeout(r, 100));
    await relay.close();

    console.log(`\n结果: ${passed} passed, ${failed} failed`);
    process.exit(failed === 0 ? 0 : 1);
}

main().catch((err) => {
    console.error('demo 失败:', err);
    process.exit(1);
});
