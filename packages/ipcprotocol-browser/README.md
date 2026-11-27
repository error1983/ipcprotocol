# ipcprotocol-browser

把 [ipcprotocol](https://github.com/error1983/ipcprotocol) 的**二进制 IPC 帧原样搬到 WebSocket 上**，让
「浏览器 ↔ 浏览器」「浏览器 ↔ 本机进程」互相 RPC。纯 ESM、零运行时依赖、无 Node 内置模块，
帧格式与 Node / Python / C# / Java / Go / Rust / C++ / C 各端逐字节一致（`PROTOCOL.md` 不用改）。

```text
浏览器 A ── WsTransport ──┐
                          ├─ ws-relay-gateway（只认路由信封，不解析 IPC 帧）
浏览器 B ── WsTransport ──┘
本机进程（Node/Python/C#…）── 命名管道 / TCP ──┘
```

## 安装

```bash
npm install ipcprotocol-browser
```

> 仓库内开发时用 workspace：`npm install` 后 `import ... from 'ipcprotocol-browser'` 直接可用。

## 快速开始（浏览器）

需要一个中继网关（在主仓库里，零依赖）：

```bash
node examples/ws-relay-gateway.js --port 9100
```

```js
import { WsTransport, MiniMessageStream, RpcProtocol } from 'ipcprotocol-browser';

const link = new WsTransport('ws://127.0.0.1:9100', { peer: 'alice', to: 'bob' });
const stream = new MiniMessageStream(link);              // 帧层：长度头 + CRC32
const rpc = new RpcProtocol(stream, { implName: 'browser/alice' }); // 报文层

rpc.registerMethod('add', (a, b) => a + b);              // 本端可被对方调用
rpc.on('event', (msg) => console.log(msg.contentType, msg.content));
rpc.on('error', (err) => console.warn(err.code, err.message));

await rpc.hello();                                       // HELLO / HELLO_ACK 能力协商
console.log(await rpc.sendRequest('add', [1, 2]));       // 3
await rpc.sendRequestBinary('upload', new Uint8Array([1, 2, 3]), 'tag');
```

## 三层结构

| 出口 | 类 | 职责 |
|---|---|---|
| `ipcprotocol-browser` | `WsTransport` | 一条 WebSocket 伪装成 `net.Socket`，自动加/剥路由信封 |
| | `MiniMessageStream` | 帧层：`[len u32LE][lenCrc][payload][payloadCrc]`，错误包与重同步 |
| | `MiniRpc` | 报文层：REQUEST / RESPONSE / EVENT / PING / PONG / HELLO / HELLO_ACK |
| | `RpcProtocol` | 把上面两层包成 **Node 版 `RpcProtocol` 的接口形状** |
| `ipcprotocol-browser/transport` | `WsTransport` | 只想要传输层时用 |
| `ipcprotocol-browser/stream` | `MiniMessageStream` | 只想要帧层时用 |
| `ipcprotocol-browser/rpc` | `MiniRpc` | 低层 RPC API |
| `ipcprotocol-browser/compat` | `RpcProtocol` | 同形适配层 |

## 与 Node 端共用一份业务代码

```js
// Node 端（本机进程 / 子进程，路径按你仓库实际位置）
import { RpcProtocol, RpcError } from '../RpcProtocol.js';
// 浏览器端
import { RpcProtocol, RpcError } from 'ipcprotocol-browser';
```

两端之后完全一样：`registerMethod / handlers / hello / sendRequest / sendRequestBinary / sendResponse /
sendText / sendJson / sendBinary / on('event') / RpcError.code`。

浏览器环境的固有限制（不是 bug）：

| 差异 | 说明 |
|---|---|
| 二进制容器 | 浏览器是 `Uint8Array`，Node 端是 `Buffer` |
| `sendRequest` 的 `to` | 被忽略；IPC 帧没有目标地址，路由靠传输层信封 |
| 对端数量 | 一条链路一个对端；Node 版可多客户端路由 |
| `heartbeatSkipWhilePending` | 恒为 `true` |
| token 轮换 | `setRequiredTokens` 立即整体替换，无宽限期 |
| `RpcError.data` | 远程错误只保留 `code` 与 message |

## 安全提醒

网关是**明文 `ws://`，没有 TLS**。只在 `127.0.0.1` / 受信内网使用；跨机请套 `wss://`
或 SSH 隧道，并给网关加准入控制（任何人都能连上就等于任何人都能调你的方法）。
详见主仓库 `SECURITY.md`。

## 路由信封

网关在字节流前加一层寻址头，上下行一致：

```text
[srcLen u8][src utf8][dstLen u8][dst utf8][原始 IPC 字节...]
```

上行的 `src` 由网关用真实 peerId 覆盖（客户端填什么都不采信）；`dstLen=0` 或 `dst="*"` 为广播。
信封之外一个字节都不碰，所以 `tools/dump_parser.js` 两侧通吃。

## 许可

MIT，与主仓库一致。
