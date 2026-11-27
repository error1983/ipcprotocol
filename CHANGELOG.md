# Changelog

所有值得注意的变更都记录在这里。
版本号对应仓库发布版本；线格式版本（HELLO 中声明的 `version`）自 1.0 起未变。

## 未发布

**浏览器 / WebSocket 互通（新增）**

- 新增 `examples/ws-relay-gateway.js`：零依赖 WebSocket 中继网关（最小 RFC6455 实现），
  路由信封 `[srcLen][src][dstLen][dst][IPC 字节]` 放在传输层，**协议帧零改动**；
- 新增 `packages/ipcprotocol-browser`（npm 包 `ipcprotocol-browser`）：浏览器侧精简栈
  `WsTransport` / `MiniMessageStream` / `MiniRpc` / `RpcProtocol`（compat 层），纯 `Uint8Array`、无 Node 依赖；
- compat 层与 Node 版 `RpcProtocol` 接口同形：前端与 Node 端业务代码只差一行 `import`，
  已用同一段 scenario 比对 29 个观察点全部一致；
- 子包提供类型声明（`.d.ts`）与子路径导出 `ipcprotocol-browser/{transport,stream,rpc,compat}`；
- 新增 `examples/browser/rpc-demo.html` 双浏览器互调演示与 `examples/browser/serve.js` 零依赖静态服务；
- `/peers` 增加 CORS 头，浏览器跨源可查在线节点；
- CI 新增 `browser` 作业：包解析、接口一致性、真实 WS 链路、网关自检与发布内容检查。

**工具与文档**

- 新增 `tools/dump_parser.js`：解析抓包（framed / raw / hex 三种输入、CRC 自检、二进制请求体拆解、断帧重对齐）；
- `MessageStream` / `RpcProtocol` / `NativeService` 构造器补齐 JSDoc 接入注解（零逻辑变更）；
- 新增 `examples/reconnect_client_grace.py`：常驻子进程保活 + `system.shutdown` 优雅退出。

## 1.2.0

**C#：Native AOT 支持**

- 新增 `JsonPayload` 手写序列化层，协议报文（HELLO / HELLO_ACK / REQUEST / 错误）不再经过 `System.Text.Json` 反射；
- 业务载荷支持 `JsonElement`、基础类型、`byte[]`、集合/字典等 AOT 可见类型；
- 新增 `JsonTypeInfo<T>` 重载：`SendRequestAsync<T>(..., JsonTypeInfo<T>)`、`SendJsonAsync<T>(...)`、
  `RegisterMethod<T>(...)`、`Convert<T>(..., JsonTypeInfo<T>)`、`RpcProtocol.Json(...)`、`RpcProtocol.Args(...)`；
- `IpcProtocol.csproj` 开启 `IsAotCompatible`，构建期检查 IL2026 / IL3050；
- 新增 `IpcProtocol.AotSmoke` 冒烟工程与 `csharp/NATIVE_AOT.md`；
- 运行时任意 POCO 在 AOT 下由 `IsReflectionEnabledByDefault` 守卫，抛出明确异常。

**修复**

- `tests/v112/csharp/AuthClient.csproj` 未排除同目录的 `SupervisorTest.cs`，导致 CS8802、C# 鉴权回归无法编译。

## 1.1.4

**P2 收口：自动握手、宿主守护、跨平台**

- Python / C# / Java / Go / Rust / C++ / C 在首个业务请求前自动 `hello()`，并发首请求只握手一次；
- 修复"客户端构造即自认已鉴权"导致的 `ensureAuth()` 短路缺陷（拆分 `authenticated` 与 `authSent`）；
- 新增 `python/supervisor.py`、`csharp/IpcProtocol/Supervisor.cs`、`java/src/ipc/Supervisor.java`：
  500ms 起退避、翻倍至 30s、稳定 30s 清零、超限放弃、`stop()` 不触发 crash；
- C / C++ 补齐 POSIX socket 分支，C 补 `_POSIX_C_SOURCE`，Windows / Linux 同一份源码编译；
- 新增 `tests/v112/` 回归与 `run_all.ps1` 一键执行。

## 1.1.3

**P1：多客户端、token 轮换、能力协商全覆盖**

- `NativeService` 支持 `maxClients`、`callTo()`、`broadcastText()`，主会话断开自动切换；
- `rotateToken({ graceMs })` / `revokeToken()`：新旧 token 并存，宽限期后自动吊销；
- 五语言补齐 caps 声明与心跳降级（对端声明无 `ping` 时停心跳、停静默超时）；
- 修复 C 版"延迟断链"造成的 use-after-free。

## 1.1.2

**P0：鉴权、能力协商、进程守护**

- `NativeService` 默认 `requireAuth=true`，随机 token 经 `IPC_TOKEN` 下发；
- 未鉴权请求回 `Unauthorized` 并保持连接，连续 5 次才断链；握手失败在 ack 落地后 300ms 断链；
- HELLO / HELLO_ACK 携带 `caps` + `impl`；
- 崩溃指数退避重启（500ms→30s）+ `crash` / `restarting` / `restarted` / `give-up` 事件。

## 1.1.1

- 修复心跳与 HELLO 握手在连接初期互相干扰的问题；
- 重同步期间发送门控，避免协议帧混入业务帧。

## 1.1.0

- MessageStream 增加 PING/PONG 心跳与半开连接检测；
- RpcProtocol 增加 HELLO / HELLO_ACK 握手；
- 错误包 + `RESET_CONFIRM` 重同步增加超时上限。

## 1.0.0

- 首个完整版本：MessageStream（18 字节头 + CRC32 + 重同步）与 RpcProtocol（REQUEST / RESPONSE / EVENT）；
- 八语言实现：Node.js / Python / C# / Java / Go / Rust / C++ / C，线格式互通；
- Windows 命名管道与 Linux Unix domain socket 实测。
