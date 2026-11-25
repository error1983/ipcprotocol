# IPC 跨语言协议规格（PROTOCOL.md）

本文档定义 `MessageStream.js` / `RpcProtocol.js` 所用协议的**线格式（wire format）与行为语义**，
作为 Node.js / Python / C# / Java / Go / Rust / C++ / C 各实现的唯一互操作依据。

- 规格版本：1.1（v1.0 线格式不变；v1.1 新增消息类型 3–6、心跳、可选 HELLO 握手、重同步超时断链，见 §4.5、§8.1、§8.2）
- 字节序约定：除特别注明外，长度字段为**小端（LE）**；二进制请求体内的长度字段为**大端（BE）**
- 文本编码：一律 UTF-8
- 传输层：协议与传输无关，可运行于任意双向字节流（TCP、命名管道、WebSocket 二进制帧等）。
  仓库示例统一使用 TCP。

## 目录

1. [分层结构](#1-分层结构)
2. [MessageStream：帧层](#2-messagestream帧层)
3. [CRC32 算法](#3-crc32-算法)
4. [错误包与重同步协议](#4-错误包与重同步协议)
5. [RpcProtocol：消息层](#5-rpcprotocol消息层)
6. [请求/响应语义](#6-请求响应语义)
7. [二进制请求](#7-二进制请求)
8. [事件](#8-事件)
9. [跨语言实现约定](#9-跨语言实现约定)
10. [一致性检查清单](#10-一致性检查清单)
11. [实现与测试索引](#11-实现与测试索引)
12. [JS 原版已知行为与注意事项](#12-js-原版已知行为与注意事项)

---

## 1. 分层结构

```
┌─────────────────────────────────────────────┐
│ RpcProtocol  请求/响应/事件、方法分发、UUID 关联 │
├─────────────────────────────────────────────┤
│ MessageStream  分帧、长度头、CRC32、错误重同步   │
├─────────────────────────────────────────────┤
│ 传输层  TCP / 管道 / WebSocket / ...           │
└─────────────────────────────────────────────┘
```

- MessageStream 向上交付**完整消息字节块**（`message` 事件），不关心内容。
- RpcProtocol 消费消息字节块，解析 18 字节消息头后按类型分发。
- 两层各自独立可测试；RpcProtocol 依赖 MessageStream 的 `message` / `error` 事件与 `send()`。

---

## 2. MessageStream：帧层

### 2.1 正常帧格式（启用 CRC，默认）

```
┌──────────────┬───────────────────┬──────────────────┬───────────────────┐
│ length (4B)  │ lengthCRC (4B)    │ payload (length) │ payloadCRC (4B)   │
│ u32 LE       │ u32 LE            │ 原始字节          │ u32 LE            │
└──────────────┴───────────────────┴──────────────────┴───────────────────┘
```

- `length`：payload 的字节数。
- `lengthCRC`：对 **length 的 4 字节 LE 编码本身**计算的 CRC32。
- `payloadCRC`：对 payload 计算的 CRC32。

关闭 CRC 时（`enableCRC = false`，两端必须一致）：

```
┌──────────────┬──────────────────┐
│ length (4B)  │ payload (length) │
└──────────────┴──────────────────┘
```

### 2.2 发送规则

- 每次 `send(data)` 产出一个完整帧；并发调用必须**串行化**（队列/锁），保证帧不交错。
- `data.length > maxBufferSize` 时拒绝发送（默认 `maxBufferSize = 256 MiB`）。
- 分片写入（chunk）只是 IO 策略，对线格式透明；接收端按字节流累积解析。

### 2.3 接收状态机（正常模式）

1. 累积字节到接收缓冲区；缓冲区超过 `maxBufferSize` → 触发 `BUFFER_OVERFLOW` 流程（见 §4）。
2. 等待帧头凑齐（启用 CRC 为 8 字节，否则 4 字节）。
3. 读取 `length`；启用 CRC 时校验 `lengthCRC`：
   - 失败 → 发送 `LENGTH_CRC_FAILED` 错误包、向上抛 `error`、进入重同步（见 §4）。
4. 记录 `expectedLength = length`，消费帧头。
5. 等待 `payload + payloadCRC`（关闭 CRC 时仅 payload）凑齐。
6. 校验 `payloadCRC`：
   - 失败 → 发送 `MESSAGE_CRC_FAILED` 错误包、向上抛 `error`、进入重同步。
7. 提取 payload，消费整帧；**先检查是否为错误包**（§4.1），是则内部处理，不交付上层；
   否则触发 `message(payload)`。
8. 回到步骤 2 继续处理缓冲区剩余数据（一次 IO 可能包含多帧）。

---

## 3. CRC32 算法

标准 CRC-32（与 zlib / `java.util.zip.CRC32` / IEEE 802.3 一致）：

| 参数 | 值 |
|---|---|
| 多项式（反射形式） | `0xEDB88320` |
| 初始值 | `0xFFFFFFFF` |
| 输入/输出反射 | 是 / 是 |
| 结果异或 | `0xFFFFFFFF` |
| 输出 | 无符号 32 位，按 **u32 LE** 写入线路 |

参考实现（与 JS 版查找表法等价）：

```python
import zlib
crc = zlib.crc32(data)          # Python
```
```csharp
// C#：查表法，见 csharp/IpcProtocol/MessageStream.cs 的 Crc32()
```
```java
java.util.zip.CRC32 crc = new java.util.zip.CRC32();
crc.update(data);
long value = crc.getValue();    // Java
```

校验向量：`crc32("hello") = 0x3610A686`（十进制 907060870）。

---

## 4. 错误包与重同步协议

### 4.1 错误包（Error Packet）

错误包是一个**按正常帧格式封装**的 5 字节 payload：

```
payload = [0xEE 0xEE 0xEE 0xEE][errorCode (1B)]
```

| errorCode | 名称 | 触发条件 |
|---|---|---|
| 1 | `LENGTH_CRC_FAILED` | 长度头 CRC 校验失败 |
| 2 | `MESSAGE_CRC_FAILED` | 消息体 CRC 校验失败 |
| 3 | `BUFFER_OVERFLOW` | 接收缓冲区超过 maxBufferSize |
| 0 | （未知类型兜底） | 发送未知错误名时使用 |

识别规则：提取出的 payload **长度恰为 5** 且前 4 字节为 `EE EE EE EE`。
错误包不交付上层应用。

### 4.2 协议消息（Protocol Message）

重同步用的带外消息，**裸帧、不带 CRC**：

```
┌──────────────────────┬──────────────┬─────────────────────┐
│ 0xFF 0xFF 0xFF 0xFF  │ length (4B)  │ data (length 字节)  │
│ 协议标识头            │ u32 LE       │ UTF-8 文本          │
└──────────────────────┴──────────────┴─────────────────────┘
```

目前仅定义一种消息：`RESET_CONFIRM`（payload 为 UTF-8 文本，解析时 trim，长度上限 50 字节）。

### 4.3 协议模式（protocolMode）状态机

每端维护 `protocolMode` 标志（初始 false）：

- **进入协议模式**（`reset()`）：清空接收缓冲区与 `expectedLength`，`protocolMode = true`。
- 协议模式下：
  - 缓冲区 ≥ 8 字节且前 4 字节为 `FF FF FF FF` → 按 §4.2 解析协议消息；
    收到 `RESET_CONFIRM` → `protocolMode = false`、清空缓冲区、触发 `syncComplete`。
  - 缓冲区 ≥ 8 字节但**没有**协议标识头 → 视为残留脏数据，**丢弃整个缓冲区**。
  - 数据不足 → 等待，不处理任何正常帧。

### 4.4 完整重同步握手

以 A 端检测到 CRC 失败为例：

```
A（检测端）                          B（对端）
─────────────────────────────────   ─────────────────────────────────
1. CRC 校验失败
2. sendError(错误类型)  ──────────►  3. 收到错误包（正常帧封装）
   （错误包按正常帧发送）              4. reset()：清缓冲、进协议模式
5. reset()：清缓冲、进协议模式         6. sendProtocolMessage('RESET_CONFIRM')
   向上抛 error                      7. 立即 protocolMode = false（回正常模式）
8. 收到 RESET_CONFIRM  ◄──────────
9. protocolMode = false
   触发 syncComplete
─────────────────────────────────   ─────────────────────────────────
双方回到正常模式，通信继续
```

关键细节（必须与 JS 原版一致）：

- **错误包的接收方（B）发完 `RESET_CONFIRM` 后立即退出协议模式**，不等待任何确认。
- 只有**检测端（A）**停留在协议模式等待 `RESET_CONFIRM`。
- A 在协议模式下会丢弃一切非协议数据，因此 B 在握手期间不应发送正常业务帧。
- `BUFFER_OVERFLOW` 同样走 `sendError + reset()` 流程。

### 4.5 重同步超时断链与发送门控（v1.1，全语言必须实现）

- `syncTimeout` 默认 **3000ms**，可配置；`<=0` 关闭超时（保持 v1.0 无限等待行为）。
- 进入协议模式时启动计时；收到 `RESET_CONFIRM` 或流关闭时取消。
- 超时触发：上报错误码 **`ERESYNC_TIMEOUT`**，随后**关闭流**（半开/恶意对端不再无限占用资源）；
  RPC 层随之结算全部在途请求。
- **发送门控**：协议模式期间，普通业务帧的 `send()` 必须等待重同步完成后再写；
  超时/关闭时以 `ERESYNC`/`ECLOSED` 失败。错误包与协议消息（RESET_CONFIRM）不受门控。
- **流错误结算在途请求**：RPC 层收到帧层任何 `error`（含 `ERESYNC`、CRC 失败、
  `ERESYNC_TIMEOUT`、`EHEARTBEAT`）时，必须拒绝全部 pending（错误码透传），与 JS 版
  `handleStreamError → _rejectAll` 一致。

---

## 5. RpcProtocol：消息层

### 5.1 消息头（18 字节，固定）

```
偏移  大小   字段           说明
0     1B    type           消息类型
1     1B    contentType    载荷类型
2     16B   id             UUID 原始字节
18    ...   payload        载荷
```

**type**

| 值 | 名称 | 方向语义 |
|---|---|---|
| 0 | REQUEST | 调用方 → 服务方 |
| 1 | RESPONSE | 服务方 → 调用方（id 与请求一致） |
| 2 | EVENT | 单向通知，无响应 |
| 3 | PING | 双向，心跳探测（§8.1），收到即回 PONG |
| 4 | PONG | 双向，心跳应答（§8.1），id 回显 PING 的 id |
| 5 | HELLO | 客户端 → 服务端，握手（§8.2） |
| 6 | HELLO_ACK | 服务端 → 客户端，握手应答（§8.2），id 回显 HELLO 的 id |

**contentType**

| 值 | 名称 | payload 编码 |
|---|---|---|
| 0 | TEXT | UTF-8 字符串 |
| 1 | JSON | UTF-8 的 JSON 文本（紧凑格式） |
| 2 | BINARY | 原始字节 |

**id（UUID）字节序 —— 重要**

16 字节 = UUID 标准十六进制字符串去掉连字符后按**从左到右**顺序解码的字节
（即 RFC 4122 网络字节序，等价于 `uuid.UUID(hex).bytes`）。

```
UUID "01234567-89ab-cdef-0123-456789abcdef"
→ 字节 01 23 45 67 89 ab cd ef 01 23 45 67 89 ab cd ef
```

各语言注意：

- Python：`uuid.UUID(s).bytes` / `str(uuid.UUID(bytes=b))`，天然一致。
- Java：`ByteBuffer.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits())`，一致。
- **C#：`Guid.ToByteArray()` 是混合小端，不一致！** 必须用
  `Convert.FromHexString(guid.ToString("N"))`（或 .NET 8+ `TryWriteBytes(span, bigEndian: true, ...)`）。
- Node：`Buffer.from(id.replace(/-/g,''), 'hex')`。

字符串形式统一为小写带连字符（`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`），
作为 pending 请求表的键。

### 5.2 反序列化校验

- 消息长度 < 18 → 错误。
- `type` ∉ {0,1,2,3,4,5,6} 或 `contentType` ∉ {0,1,2} → 错误。
- JSON 解析失败 → 错误。
- 处理失败的消息**记录日志后丢弃**，不影响后续消息（不中断连接）。

---

## 6. 请求/响应语义

### 6.1 JSON 请求体（type=REQUEST, contentType=JSON）

```json
{ "method": "add", "params": [3, 4], "to": "optional-target" }
```

- `method`：字符串，方法名。
- `params`：任意 JSON 值；`to`：可选路由字段，**为 null/undefined 时整个键省略**。

### 6.2 服务端参数展开规则

| params 形态 | handler 实参 |
|---|---|
| 数组 `[a, b, ...]` | 展开为 `a, b, ...` |
| `null` / 缺省 | 无参数 |
| 其他标量/对象 `x` | 单参数 `x` |

### 6.3 方法分发与响应

- 方法未注册 → 响应 JSON：`{"error": "Method not found: <method>"}`。
- handler 抛异常 → 响应 JSON：`{"error": "<异常消息>"}`。
- handler 正常返回，按结果类型选择 contentType：

| 返回值 | contentType | payload |
|---|---|---|
| `undefined` / `null` / `void` | JSON | `null` |
| 字符串 | TEXT | UTF-8 原文 |
| 字节数组（Buffer/byte[]/bytes） | BINARY | 原始字节 |
| 其他（对象/数组/数字/布尔） | JSON | JSON 序列化 |

- 响应消息 type=RESPONSE，**id 与请求完全一致**。

### 6.4 客户端响应处理

- 按 id 匹配 pending 请求表；无匹配则忽略。
- 若响应是 JSON 对象且 `error` 字段为 **JS 真值** → 以 `error` 的**原始值**拒绝该请求
  （不是包装后的字符串；各语言用携带原始值的异常类型表达，如 `RpcError` / `RpcException`）。
- 否则以**整个 content** 兑现请求（TEXT→字符串，BINARY→字节数组，JSON→解析值）。

**JS 真值判定**（`error` 字段为以下值时视为假，不触发拒绝）：
`null`、`undefined`、`false`、`0`、`""`（空字符串）、`NaN`。其余（含 `{}`、`[]`）为真。

### 6.5 pending 请求生命周期

- 发送失败（流不可写等）→ 立即移除并以发送异常拒绝。
- 底层流 `error` → **拒绝所有** pending，错误码透传（含 `ERESYNC`/`ERESYNC_TIMEOUT`/`EHEARTBEAT`，见 §4.5）。
- `destroy()/close()` → 拒绝所有 pending，错误消息 `RPC Protocol destroyed`，并清理流与处理器。

---

## 7. 二进制请求

用于携带大块二进制数据的 RPC（type=REQUEST, **contentType=BINARY**），payload 格式：

```
┌──────────────────┬─────────────────┬──────────────────┬─────────────────┬──────────────┐
│ methodLen (4B)   │ method          │ paramsLen (4B)   │ params          │ binaryData   │
│ u32 BE           │ UTF-8           │ u32 BE           │ UTF-8 JSON 数组 │ 剩余全部字节 │
└──────────────────┴─────────────────┴──────────────────┴─────────────────┴──────────────┘
```

- 注意此处两个长度字段是**大端**（与帧层的小端不同）。
- `params` 是**附加参数数组**的 JSON（不含二进制数据本身）。
- 服务端 handler 实参 = `[binaryData, ...params]`，即**二进制数据作为第一个参数**。
- 响应规则与 §6.3 完全相同（响应可以是 JSON/TEXT/BINARY 任意一种）。

调用示例（语义）：

```
sendRequestBinary("upload", <75KB bytes>, "tag-1")
→ handler("upload") 收到 (bytes, "tag-1")
```

---

## 8. 事件

- type=EVENT，id 为**新生成的随机 UUID**（与任何请求无关），contentType 任意。
- 辅助发送：`sendText(str)` → TEXT；`sendJson(obj)` → JSON；`sendBinary(bytes)` → BINARY。
- 接收方向上层触发 `event`，携带 `{contentType, content}`；**不回响应**。
- 事件与请求/响应共用同一条流与帧层，天然保序。

### 8.1 心跳 PING/PONG（v1.1，半开连接检测）

线格式：18 字节标准头，`type=3(PING)/4(PONG)`，`contentType=0(TEXT)`，**payload 为空**。
PING 的 id 为随机 UUID；PONG 的 id **回显所应答 PING 的 id**。

行为语义（RPC 层实现）：

- 配置：`heartbeatIntervalMs` 默认 **5000**，`heartbeatTimeoutMs` 默认 **15000**；
  `interval<=0` 关闭心跳（v1.0 行为）。建议 `timeout >= 2×interval`。
- 维护 `lastReceived` = 最近一次收到**任何消息**的时刻（初始为构造时刻）。
- 后台定时检查（周期≈interval）：
  - 静默时长 ≥ `heartbeatTimeoutMs` → 上报错误码 **`EHEARTBEAT`**、结算全部在途请求、关闭连接；
  - 否则静默时长 ≥ `heartbeatIntervalMs` → 发送 PING。
- 收到 PING → 立即回 PONG（回显 id）；收到 PONG → 仅刷新 `lastReceived`，不交付应用层。
- PING/PONG **不受 HELLO 鉴权门禁**（存活探测不携带业务信息）。
- 发送的 PING 不算接收活动；只有入站数据刷新 `lastReceived`。

兼容性：旧实现收到未知类型 3/4 会按 §5.2 记录 `EMALFORMED` 后丢弃（不崩溃、不断链），
但不会回 PONG；与旧实现互通时应关闭心跳（`interval<=0`）。

### 8.2 可选 HELLO 握手：版本协商 + 预共享 token 鉴权（v1.1）

线格式：`type=5(HELLO)/6(HELLO_ACK)`，`contentType=1(JSON)`，id 规则同请求/响应
（HELLO 随机 UUID，HELLO_ACK 回显）。

```
HELLO     payload = {"version": 1, "token": "<可选字符串>"}
HELLO_ACK payload = {"version": 1, "ok": true}          // 成功
HELLO_ACK payload = {"error": "<原因>"}                  // 失败，服务端随后关闭连接
```

服务端：

- 配置 `requiredToken`（默认 null/空 = 不启用鉴权门禁）。
- 收到 HELLO：`version != 1` → ACK `{"error":"unsupported protocol version: <v>"}` + 关闭；
  `requiredToken` 已配置且 token 不匹配（含缺失）→ ACK `{"error":"unauthorized"}` + 关闭；
  否则标记本连接已鉴权，ACK `{"version":1,"ok":true}`。
- `requiredToken` 已配置时，未鉴权连接的 **REQUEST** → 错误响应 `{"error":"Unauthorized"}` + 关闭；
  EVENT 丢弃。PING/HELLO 始终允许。
- 未配置 `requiredToken` 时不门禁任何请求（向后兼容），但仍应答 HELLO（含版本校验）。

客户端：

- 配置 `authToken`（可选）；显式调用 `hello(timeoutMs)` 发送 HELLO 并等待 ACK
  （pending 跟踪，超时走 `ETIMEDOUT`）。
- ACK 含 `error` → 抛 `RpcError`，错误码 **`EAUTH`**；ACK 的 `version != 1` → **`EVERSION`**。
- 库**不自动**拦截未握手请求；启用鉴权时调用方必须先 `await hello()` 再发业务请求。

兼容性：默认不启用鉴权、不自动握手，v1.0 调用方零改动；旧服务端收到 HELLO 会回
`Method not found` 类错误（按未知类型丢弃），启用鉴权的客户端不应连旧服务端。

---

## 9. 跨语言实现约定

### 9.1 API 对照

| 概念 | Node.js | Python | C# | Java |
|---|---|---|---|---|
| 构造消息流 | `new MessageStream({socket})` | `MessageStream(reader, writer)` | `new MessageStream(stream)` | `new MessageStream(in, out)` |
| 启动读循环 | 构造即可 | 构造即可（asyncio） | `RpcProtocol` 自动 `Start()` | `RpcProtocol` 自动 `start()` |
| 发送消息 | `send(buf)` | `await send(data)` | `SendAsync(data)` | `send(data)` |
| 注册方法 | `registerMethod(name, fn)` | `register_method(name, fn)` | `RegisterMethod(name, fn)` | `registerMethod(name, h)` |
| 发请求 | `sendRequest(m, params)` | `await send_request(m, params)` | `SendRequestAsync(m, params)` | `sendRequest(m, params)` → `CompletableFuture` |
| 二进制请求 | `sendRequestBinary(m, buf, ...args)` | `await send_request_binary(m, buf, *args)` | `SendRequestBinaryAsync(m, data, args)` | `sendRequestBinary(m, data, args)` |
| 事件监听 | `onEvent(cb)` | `on_event(cb)` | `Event += cb` | `onEvent(cb)` |
| 销毁 | `destroy()` | `await destroy()` | `Dispose()` | `close()` |

新增四语言（均为同步阻塞式请求 API）：

| 概念 | Go | Rust | C++ | C |
|---|---|---|---|---|
| 构造消息流 | `NewMessageStream(r, w)` | `MessageStream::new(r, w)` | `MessageStream(stream)` | `ms_create(io, crc, max)` |
| 启动读循环 | `NewRpcProtocol` 内自动 `Start()` | `RpcProtocol::new` 内自动 `start()` | `RpcProtocol` 构造内自动 `start()` | `rpc_create` 内自动 `ms_start()` |
| 发送消息 | `Send(data)` | `send(&data)` | `send(data)` | `ms_send(ms, data, len)` |
| 注册方法 | `RegisterMethod(name, h)` | `register_method(name, h)` | `registerMethod(name, h)` | `rpc_register(rpc, name, h, user)` |
| 发请求 | `SendRequest(m, params)` | `send_request(m, params)` | `sendRequest(m, params)` | `rpc_send_request(rpc, m, params, to)` |
| 二进制请求 | `SendRequestBinary(m, data, args...)` | `send_request_binary(m, data, args)` | `sendRequestBinary(m, data, args)` | `rpc_send_request_binary(rpc, m, data, len, args)` |
| 事件监听 | `rpc.OnEvent = cb`（字段回调） | `on_event(cb)` | `onEvent(cb)` | `rpc_on_event(rpc, cb, user)` |
| 销毁 | `Close()` | `close()` | `close()` | `rpc_destroy(rpc)` |

> Go/Rust/C++/C 的 `send_request` 系列为**同步阻塞**调用（内部用 channel / mpsc /
> condvar 等待响应），可在任意线程/协程并发调用；`to` 路由字段分别经
> `SendRequestTo` / `send_request_to` / `sendRequest(..., to)` / 第 4 参数传入。

### 9.2 JSON 值映射

| JSON | Node | Python | C# | Java（内置 Json） |
|---|---|---|---|---|
| object | Object | dict | `JsonElement`(Object) | `Map<String,Object>` |
| array | Array | list | `JsonElement`(Array) | `List<Object>` |
| string | string | str | `JsonElement`(String) | `String` |
| number | number(double) | int/float | `JsonElement`(Number) | `Long`（整）/ `Double`（浮点） |
| true/false | boolean | bool | `JsonElement` | `Boolean` |
| null | null | None | `JsonElement`(Null) | `null` |

新增四语言的 JSON 值表示：

| 语言 | 值类型 | 字节数组 |
|---|---|---|
| Go | `any`（`map[string]any` / `[]any` / `string` / `float64` / `bool` / `nil`） | `[]byte`，序列化前经 `normalizeJSON` 转数字数组 |
| Rust | 内置 `Json` 枚举（`Null/Bool/Num/Str/Arr/Obj/Bytes`，零依赖） | `Json::Bytes`，stringify 输出数字数组 |
| C++ | 内置 `ipc::Json` 类（同上七种类型，仅标准库） | `Json::Bytes`，dump 输出数字数组 |
| C | `JsonValue`（`JSON_NULL/BOOL/NUM/STR/ARR/OBJ/BYTES`，手写解析器） | `json_bytes()`，dump 输出数字数组 |

### 9.3 必须对齐的序列化细节

1. **字节数组进 JSON 必须是数字数组**（`[0,1,2,255]`），对齐 `JSON.stringify(Buffer)`。
   - C# `System.Text.Json` 默认输出 Base64，**必须加转换器**（仓库已实现）。
   - Java 内置 `Json.stringify(byte[])` 已按数字数组输出。
   - Rust/C++/C 的内置 JSON 均实现了 `Bytes → 数字数组`；Go 在发送前递归归一化 `[]byte`。
2. JSON 输出紧凑（无多余空白）；非 ASCII 字符可直接输出 UTF-8 或 `\uXXXX` 转义，两者等价。
3. 浮点整数值（如 `2.0`）JS 输出 `2`；接收端按数值比较即可，不必逐字节比对 JSON 文本。
4. UUID 字节序见 §5.1（C# 特别小心）。

### 9.4 并发模型与启动时序（重要）

| 语言 | 读循环模型 | 启动约定 |
|---|---|---|
| Node | 单线程事件循环 | 构造即安全（数据事件是宏任务，注册必先于分发） |
| Python | asyncio 单线程 | 构造即安全（读任务在同步注册完成前不会运行） |
| C# | 后台 `Task` | **构造不自动读**；`RpcProtocol` 挂好事件后调用 `Start()` |
| Java | 守护读线程 | **构造不自动读**；`RpcProtocol` 挂好监听器后调用 `start()` |
| Go | goroutine + channel | `NewRpcProtocol` 先挂 `OnMessage/OnError` 再 `ms.Start()`（`sync.Once`） |
| Rust | 独立读线程 + `Arc<Mutex>` | `RpcProtocol::new` 先挂回调再 `ms.start()` |
| C++ | detached `std::thread` + condvar | `RpcProtocol` 构造先挂 `onMessage/onError` 再 `start()` |
| C | detached pthread + mutex/cond | `rpc_create` 先 `ms_on_message` 再 `ms_start()` |

> 教训：多线程实现若在构造函数里立即启动读循环，对端"连接即发数据"时，
> 早到消息会在监听器注册前被分发（listeners=0）而丢失，客户端首请求永久挂起。
> 直接使用 `MessageStream`（不经 `RpcProtocol`）时，务必**先挂监听器再 `start()`**。

### 9.5 错误表达

| 场景 | Node | Python | C# | Java |
|---|---|---|---|---|
| 对端错误响应 | `reject(response.error)` | `RpcError(error)` | `RpcException(Error)` | `RpcException(getError())` |
| 流错误拒绝 pending | `Error("Request failed: ...")` | 同左 | 同左 | 同左 |
| 销毁拒绝 pending | `Error("RPC Protocol destroyed")` | 同左 | 同左 | 同左 |

新增四语言：

| 场景 | Go | Rust | C++ | C |
|---|---|---|---|---|
| 对端错误响应 | `RpcError{Value any}` | `RpcError::Remote(Json)` | `RpcException(remote=true)` | `RpcResult{remote=1, error=原始值}` |
| 本地/流错误 | 普通 `error` | `RpcError::Message(String)` | `RpcException(remote=false)` | `RpcResult{remote=0, error=JSON_STR}` |

> C 的所有权约定：handler 的 `args` 为借用指针；返回值由 rpc 层接管；
> `RpcResult` 用完须 `rpc_result_free()`；事件回调中的 `content` 为借用，
> 需留存时按类型深拷贝（`JSON_BYTES` 不能经 dump+parse 往返，会退化为普通数组）。

---

## 10. 一致性检查清单

新语言实现逐项自检：

**帧层**
- [ ] 长度头 u32 LE；CRC 可选且默认开启；两端开关必须一致
- [ ] lengthCRC 对 4 字节 LE 长度头计算；payloadCRC 对 payload 计算
- [ ] CRC32 参数与 §3 一致（用 `"hello"` 向量自检）
- [ ] 粘包/半包：一次 IO 多帧、一帧跨多次 IO 均正确
- [ ] 发送串行化，帧不交错
- [ ] maxBufferSize 超限：发送拒绝、接收触发 BUFFER_OVERFLOW

**错误与重同步**
- [ ] 错误包 = 正常帧封装的 `EEEEEEEE+code`，不交付上层
- [ ] 检测端：发错误包 → 抛 error → 进协议模式等 RESET_CONFIRM
- [ ] 接收端：清状态 → 发 RESET_CONFIRM（裸帧 FFFFFFFF）→ 立即回正常模式
- [ ] 协议模式下丢弃非协议数据；RESET_CONFIRM 后触发 syncComplete
- [ ] 重同步后业务通信可继续
- [ ] syncTimeout（默认 3s）超时 → ERESYNC_TIMEOUT + 断链；协议模式期间发送门控（§4.5）
- [ ] 流错误（含 ERESYNC）结算全部 pending（§4.5）

**心跳与握手（v1.1）**
- [ ] PING/PONG：type 3/4、TEXT 空载荷、PONG 回显 id；空闲发 PING、静默超限 EHEARTBEAT 断链（§8.1）
- [ ] 心跳可配置可关闭；只有入站数据刷新 lastReceived
- [ ] HELLO/HELLO_ACK：type 5/6、JSON；版本校验、可选 token 门禁、失败后断链（§8.2）
- [ ] 未配置 requiredToken 时不门禁请求；客户端 hello() 显式调用，EAUTH/EVERSION 错误码

**消息层**
- [ ] 18 字节头；UUID 字节序正确（C# 不用 Guid.ToByteArray 默认序）
- [ ] 请求参数展开规则（数组展开 / null 无参 / 标量单参）
- [ ] 响应类型映射（null→JSON null、str→TEXT、bytes→BINARY、其他→JSON）
- [ ] 错误响应按 JS 真值判定，拒绝时携带原始 error 值
- [ ] 方法不存在 / handler 异常 → `{"error": ...}`
- [ ] 二进制请求：u32 BE 长度、handler 首参为二进制数据
- [ ] 事件：随机 UUID、无响应、`{contentType, content}` 交付
- [ ] 流错误/销毁拒绝全部 pending
- [ ] 字节数组 JSON 序列化为数字数组（非 Base64）
- [ ] 读循环在监听器注册后启动（多线程语言）

---

## 11. 实现与测试索引

### 11.1 文件布局

```
ipcprotocol/
├── MessageStream.js / RpcProtocol.js      # Node 原版（规格事实来源）
├── python/
│   ├── message_stream.py / rpc_protocol.py / __init__.py
│   ├── test_loopback.py                   # Python 回环测试（12 项）
│   └── test_interop_node.py               # Python ↔ Node 互操作（12 项）
├── csharp/
│   ├── IpcProtocol/                       # 类库（net8.0）
│   │   ├── MessageStream.cs / RpcProtocol.cs
│   └── IpcProtocol.Tests/                 # 回环测试（12 项）
├── java/src/ipc/                          # Java 11+，零依赖
│   ├── MessageStream.java / RpcProtocol.java / Json.java
│   ├── LoopbackTest.java                  # 回环测试（13 项）
│   └── Example.java                       # server/client 双模式示例
├── go/                                    # Go 1.21+，仅标准库
│   ├── message_stream.go / rpc_protocol.go
│   ├── rpc_test.go                        # 回环测试（9 项，go test -race）
│   └── example/main.go                    # server/client 双模式示例
├── rust/                                  # Rust，零依赖（cargo --offline 可构建）
│   ├── src/{json,message_stream,rpc_protocol,lib}.rs
│   ├── tests/loopback.rs                  # 回环测试（7 项）
│   └── src/bin/example.rs                 # server/client 双模式示例
├── cpp/                                   # C++17，仅标准库（header-only）
│   ├── include/ipc/{json,message_stream,rpc_protocol,tcp}.hpp
│   ├── tests/loopback_test.cpp            # 回环测试（13 项）
│   └── examples/example.cpp               # server/client 双模式示例
├── c/                                     # C11 + pthreads（Windows 下 Winsock）
│   ├── json.{h,c} / message_stream.{h,c} / rpc.{h,c} / tcp.{h,c}
│   ├── loopback_test.c                    # 回环测试（12 项）
│   └── example.c                          # server/client 双模式示例
└── examples/
    ├── node_server.js / node_client.js    # Node 原版互操作端
    ├── python_server.py                   # Python 示例服务端
    └── csharp/                            # C# server/client 双模式示例
```

### 11.2 运行测试

```powershell
# Python 回环 / Python↔Node
python python/test_loopback.py
python python/test_interop_node.py

# C# 回环
dotnet run --project csharp/IpcProtocol.Tests

# Java 回环（Java 11+，无需 Maven）
javac -encoding UTF-8 -d java/out java/src/ipc/*.java
java -cp java/out ipc.LoopbackTest

# Go 回环（含竞态检测）
cd go; go test -count=1 -race ./...

# Rust 回环（零依赖，离线可构建）
cd rust; cargo test --offline

# C++ 回环（MinGW g++ 15 / 任意 C++17 编译器）
g++ -std=c++17 -O2 -Wall -Icpp/include cpp/tests/loopback_test.cpp -o cpp/out/loopback_test.exe -lws2_32
cpp/out/loopback_test.exe

# C 回环（MinGW gcc，C11 + pthreads）
gcc -std=c11 -O2 -Ic c/loopback_test.c c/json.c c/message_stream.c c/rpc.c c/tcp.c -o c/out/loopback_test.exe -lws2_32 -lpthread
c/out/loopback_test.exe

# 跨语言互操作（先起服务端，再跑客户端；端口可自定）
node examples/node_server.js 9000          # Node 原版服务端
python examples/python_server.py 9001      # Python 服务端
dotnet run --project examples/csharp -- server 9002
java -cp java/out ipc.Example server 9003
go run ./example server 9004               # Go 服务端（在 go/ 目录下）
cargo run --offline --bin example -- server 9005   # Rust 服务端（在 rust/ 目录下）
g++ -std=c++17 -O2 -Icpp/include cpp/examples/example.cpp -o cpp/out/example.exe -lws2_32
cpp/out/example.exe server 9006            # C++ 服务端
gcc -std=c11 -O2 -Ic c/example.c c/json.c c/message_stream.c c/rpc.c c/tcp.c -o c/out/example.exe -lws2_32 -lpthread
c/out/example.exe server 9007              # C 服务端

node examples/node_client.js <port>        # Node 客户端（10 项）
dotnet run --project examples/csharp -- client <port>   # C# 客户端（11 项）
java -cp java/out ipc.Example client <port>             # Java 客户端（11 项）
go run ./example client <port>             # Go 客户端（11 项，在 go/ 目录下）
cargo run --offline --bin example -- client <port>     # Rust 客户端（11 项）
cpp/out/example.exe client <port>          # C++ 客户端（11 项）
c/out/example.exe client <port>            # C 客户端（11 项）
```

> Windows/MinGW 注意：C/C++ 源码已定义 `WIN32_LEAN_AND_MEAN` 避免 winsock2.h 与
> windows.h 的 RPC 头冲突；链接需 `-lws2_32`，C 还需 `-lpthread`（winpthreads）。

### 11.3 已验证的互操作矩阵

| 客户端 \ 服务端 | Node | Python | C# | Java | Go | Rust | C++ | C |
|---|---|---|---|---|---|---|---|---|
| Node | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Python | ✅ | ✅(回环) | — | — | — | — | — | — |
| C# | ✅ | ✅ | ✅(回环) | ✅ | — | — | — | — |
| Java | ✅ | ✅ | ✅ | ✅(回环) | — | — | — | — |
| Go | ✅ | — | — | — | ✅(回环) | — | — | — |
| Rust | ✅ | — | — | — | — | ✅(回环) | — | — |
| C++ | ✅ | — | — | — | — | — | ✅(回环) | — |
| C | ✅ | — | — | — | — | — | — | ✅(回环) |

✅ = 已实测通过；✅(回环) = 同语言 TCP 回环通过；— = 未测（线格式一致，理论可互操作）。

覆盖：JSON/TEXT/BINARY 响应、Unicode、null、错误响应、方法不存在、
75KB 二进制请求、三类事件、50 并发请求、CRC 损坏注入后的重同步恢复。

---

## 12. JS 原版已知行为与注意事项

1. **必须注册 error 监听器**：`RpcProtocol` 会把流错误重新 `emit('error')`；
   Node EventEmitter 在无监听器时 emit error 会**直接崩溃进程**。
   业务侧务必 `rpc.on('error', ...)`（示例已包含）。
2. 错误包接收方发完 `RESET_CONFIRM` 后**立即**退出协议模式（§4.4），
   这是原版行为，移植时不要"改进"为等待确认，否则破坏互操作。
3. `send()` 的分片大小（默认 64KB）与 cork/uncork 是 Node IO 策略，对线格式无影响。
4. `destroy()` 时 JS 原版会向 socket 写入 `'EOF'` 文本再销毁——这是传输层残留行为，
   不属于协议帧；其他语言实现直接关闭流即可，互操作不受影响。
5. 请求体中的 `to` 字段原版仅透传，服务端不做路由处理；移植版保持一致。
6. CRC 开关、maxBufferSize 属于**连接两端必须协商一致**的配置，协议内无协商机制。