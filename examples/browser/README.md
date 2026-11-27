# 浏览器 / WebSocket 互通（可选组件）

把本仓库的 IPC 帧原样搬到 WebSocket 上，让「浏览器 ↔ 浏览器」「浏览器 ↔ 本机进程」也能互相 RPC。
**协议帧一个字节都没改**，`PROTOCOL.md` 与 `tools/dump_parser.js` 依然适用。

```text
浏览器 A ── WsTransport ──┐
                          ├─ ws-relay-gateway.js（只认路由信封，不解析 IPC 帧）──┐
浏览器 B ── WsTransport ──┘                                                │
本机进程（Node/Python/C#…）──────────────────────────────────────────────────┘
```

## 路由信封

网关需要在字节流前面加一层寻址头，格式是固定的 4 段（上下行一致）：

```text
[srcLen u8][src utf8][dstLen u8][dst utf8][原始 IPC 字节...]
```

- 上行时 `src` 由网关用真实 peerId 覆盖（客户端填什么都不采信）；`dst` 是目标 peerId，
  `dstLen=0` 或 `dst="*"` 表示广播给其他所有在线节点。
- 下行时 `src` / `dst` 都由网关填好。
- 信封之外的内容完全不碰，所以一个 IPC 帧被拆成多次写入也不会破坏协议。

> ⚠️ 寻址信息放在传输层，不要塞进 IPC 帧里——那会破坏已发布的多语言实现和抓包工具。

## 文件

| 文件 | 说明 |
|---|---|
| `../ws-relay-gateway.js` | 零依赖 WebSocket 中继网关（自带最小 RFC6455 实现，生产建议换 `ws` 库） |
| `../ws-rpc-demo.js` | 端到端自检：两个 peer 经网关互调 RPC、推事件、传二进制、验证超时 |
| `ws-transport.js` | 传输适配器：把一条 WS 连接伪装成 `net.Socket`（`on`/`write`/`destroy`/`drain`） |

## 跑起来

```powershell
node examples/ws-relay-gateway.js --port 9100     # 启动网关
node examples/ws-rpc-demo.js                      # 端到端自检（会自己起网关，11 项断言）
curl http://127.0.0.1:9100/peers                  # 查看在线节点
```

## Node 侧直接复用现有实现

适配器暴露的是 `net.Socket` 的形状，所以 Node 里可以原样套 `MessageStream + RpcProtocol`：

```js
import { WsTransport } from './browser/ws-transport.js';
import { MessageStream } from '../MessageStream.js';
import { RpcProtocol } from '../RpcProtocol.js';

const link = new WsTransport('ws://127.0.0.1:9100', { peer: 'alice', to: 'bob' });
await new Promise((r) => link.once('open', r));
const rpc = new RpcProtocol(new MessageStream({ socket: link }), { authToken: token });
await rpc.hello();
console.log(await rpc.sendRequest('add', [1, 2]));
```

## 浏览器侧需要注意

浏览器没有 `Buffer` / `net`，**不能直接用 Node 版 `MessageStream`**（它依赖 `Buffer.copy`）。
适配器在浏览器里 emit 的是 `Uint8Array`，需要配一套前端精简栈（帧编解码 + CRC32 + requestId 匹配），
帧格式与本仓库完全一致：

```js
import { WsTransport } from './ws-transport.js';

const link = new WsTransport('ws://127.0.0.1:9100', { peer: 'alice', to: 'bob' });
link.on('data', (bytes) => stack.feed(bytes));      // stack = 前端精简 MessageStream
stack.on('frame', (frame) => rpc.handleFrame(frame));
link.write(stack.encode(requestFrame));
```

## 已知边界

1. **一个适配器 = 一条「本端 ↔ 某对端」链路**。要和 N 个对端通信，就开 N 个适配器（各自一个 `RpcProtocol`）。
   IPC 帧里没有目标地址，只能在传输层定向。
2. **目标不在线时网关直接丢弃**，不伪造错误响应（伪造会污染协议流），由调用方的请求超时兜底。
3. **`autoReconnect` 默认关闭**。浏览器要开的话，重连后旧的 `MessageStream` / `RpcProtocol` 状态已失效，
   必须整条重建再重新 `hello()`。
4. **没有 TLS**：`ws://` 明文，只适合本机或受信内网；跨机请用 `wss://` 或 SSH 隧道（同 `SECURITY.md`）。
5. 一个小帧会被拆成 head / payload / tail 三次写入，即 3 条 WS 消息、3 个信封头；正确性无影响，
   带宽敏感场景可自行在适配器里做同 tick 合并。
