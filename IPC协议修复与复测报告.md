# IPC 协议修复与复测报告

修复对象：`MessageStream.js`、`RpcProtocol.js`、`nativeService.js`
复测环境：Linux / 2 核 / 4GB，Node v22.23.4，Unix Domain Socket
复测脚本：`ipc-stress/tests/`（原版备份在 `src/orig/`）

---

## 一、结论

上一轮 5 个 P0、6 个 P1 全部修完，复测 **17 项全部 PASS**。关键指标：

- 2 万并发请求：15.0s → **318ms**（1334 → 62933 req/s）
- 32MB 二进制往返：16.07s → **553ms**（2.0 → 57.9 MB/s）
- 断开连接：300 个悬空请求 → **300/300 全部拒绝**
- 单字节损坏注入：10/12 自愈 → **12/12 自愈，0 个残留请求**

**线格式一个字节都没改**，CRC 校验向量 `crc32("hello")=0x3610A686` 通过，原版与修复版双向互操作（500/500、2MB 二进制、200/200 反方向）。

---

## 二、修复前后对比

| 场景 | 修复前 | 修复后 | 变化 |
|---|---|---|---|
| 顺序 RPC 2000 次 | 216ms / 9245 req/s | 183ms / 10951 req/s | 略快 |
| 并发 1000 | 62ms / 16187 req/s | 31ms / 32489 req/s | **2×** |
| 并发 5000 | 462ms / 10834 req/s | 135ms / 36949 req/s | **3.4×** |
| 并发 20000 | 15000ms / 1334 req/s | 318ms / 62933 req/s | **47×** |
| 1MB 往返 | 60ms / 16.6 MB/s | 9.6ms / 51.9 MB/s | **3×** |
| 8MB 往返 | 567ms / 14.1 MB/s | 127ms / 63.1 MB/s | **4.5×** |
| 32MB 往返 | 16067ms / 2.0 MB/s / RSS 385MB | **553ms / 57.9 MB/s / RSS 301MB** | **29×** |
| 双向同时 6000 次 | 512ms | 138ms | 3.7× |
| 16000 次长跑 | Δheap 0.18MB | Δheap 0.03MB | — |
| 断开时有 300 在途 | 拒绝 0/300，永久悬空 | **拒绝 300/300，残留 0** | 修复 |
| 超大长度头（CRC 合法） | 静默假死，后续全超时 | 报 `BAD_LENGTH` + 重同步，**后续调用正常** | 修复 |
| 未注册 `error` 监听 | 进程崩溃 exit 2 | 仅日志，`NO_CRASH` | 修复 |
| 单字节损坏 12 处 | 自愈 10/12，每次漏 1 个请求 | **自愈 12/12，残留 pending 0** | 修复 |
| 方法不存在 / handler 抛错 | 抛出字符串 | `RpcError(code='EREMOTE', remote=true)` | 修复 |
| `destroy()` 后对端 | 残留 3 字节 `EOF` 垃圾 | 缓冲 0，pending 0 | 修复 |
| NativeService 第二客户端 | 原链路被顶掉，调用 3s 超时 | **拒绝新连接，原链路正常** | 修复 |
| NativeService stop→restart | PASS | PASS | — |

---

## 三、修复要点（详细见 `ipc-stress/FIXES.md`）

**MessageStream（收发内核重写）**

- 接收缓冲从"整段 concat"改为**队列 + 头游标**，消除 O(n²)；单块内零拷贝，跨块只复制一次
- 补 `close`/`end`/`error` 监听，断开即 `emit('error', code='ECLOSED')`
- 长度头强制校验上限，杜绝"等 4GB"假死
- 发送单一串行队列 + 分片 `await drain` + 去掉嵌套 `cork/uncork`
- 所有写出（数据/错误包/协议消息）统一排队，错误包不再与在途帧竞争
- 重同步加 3s 超时兜底，失败直接断链交给上层重连；协议模式改为**扫描 `0xFFFFFFFF`** 而不是整段清空，避免误丢 `RESET_CONFIRM`
- 删掉 `socket.end('EOF')`；`error` 统一 Error 对象 + 无监听时不崩

**RpcProtocol**

- `sendRequest` 加 15s 默认超时（可传 `0` 关闭），发送失败立即清理 pending
- 统一 `RpcError`（`code/remote/data`），不再 reject 字符串
- `deserialize` 先校验长度 ≥18，uuid 去掉正则；畸形请求尽量回错误响应，避免调用方永久等待
- `destroy` 幂等，精确解绑监听器，避免泄漏

**NativeService**

- 单客户端门禁、断开清理、未连接时返回 rejected Promise、stop 加锁、Unix socket 残留清理、`stdio` 可配、默认 error 兜底

---

## 四、行为变更（对接 Python / C# 要注意）

1. 重同步时两端在途请求都会以 `ERESYNC` 拒绝，业务需要重试（这是故意的：宁可失败也不能永久挂起）
2. 重同步 3s 未完成会断链（`syncTimeout` 可调）
3. 默认 RPC 超时 15s；错误一律按 `code` 分支（`ETIMEDOUT`/`ERESYNC`/`ECLOSED`/`EREMOTE`/`ESEND`/`ETOOBIG`）
4. 线格式不变，`PROTOCOL.md` 无需修订

---

## 五、剩余风险

- **Windows 命名管道未实测**（本地只有 Linux）。协议与传输无关，但管道断开事件、权限、`PIPE_TYPE_MESSAGE` 模式建议在目标机器上跑一遍 `nativeService.test.js`。
- ~~Python / C# 实现仍是零~~ 已补齐八语言实现（见第七节），并已按本报告修复要点完成同类 bug 排查与修复；跨语言损坏注入建议后续按 `tests/chaos2.js` 思路常态化。
- **多路并发上限**：单连接吞吐已到 6 万 req/s，双核环境下继续加压会先撞 CPU 而不是协议。

---

## 六、复现命令

```bash
cd ipc-stress
node --expose-gc tests/stress.js          # 主压测（全项 PASS）
node --expose-gc tests/big.js            # 1/4/8/16/32MB 分级
node tests/chaos2.js                     # 损坏注入 12 处，自愈 12/12
node tests/edge.js                       # 边界/异常帧
node tests/destroy.js                    # destroy 后对端状态
node tests/crash_no_listener.js          # 未注册 error → NO_CRASH
node tests/nativeService.test.js         # 端到端 / 双客户端 / 重启
node tests/interop.js                    # 原版↔修复版互操作
```

---

## 七、跨语言复核与同类 bug 修复（本轮新增）

### 7.1 JS 修复版复核结论

逐项核对工作区 `MessageStream.js` / `RpcProtocol.js` / `nativeService.js`，与本报告描述一致：
队列+头游标（`_q/_qHead/_buffered`）、close/end/error 监听、长度头 0/上限校验（`BAD_LENGTH`）、
单一发送串行队列、重同步扫描 `0xFFFFFFFF`（64KB 窗口、保留尾部 3 字节）、15s 默认超时、
统一 `RpcError(code/remote/data)`、畸形 REQUEST 回错误响应、destroy 幂等且不写 `EOF`、
NativeService 单客户端门禁。**复核通过，报告可信。**

### 7.2 其它语言审计矩阵（修复前）

| Bug | Python | C# | Java | Go | Rust | C++ | C |
|---|---|---|---|---|---|---|---|
| B1 长度头无 0/上限校验（CRC 合法的超大/零长度→静默假死，Java/C# 另有整型溢出风险） | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 |
| B2 断连拒绝全部 pending | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 |
| B3 接收缓冲 O(n²) 头部删除/全量复制 | 命中 | 命中 | 命中 | 无（append+reslice 摊还 OK） | 命中 | 命中 | 命中 |
| B4 发送串行化 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 |
| B5 畸形请求不回错误响应（调用方永久等待） | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 |
| B7 destroy 幂等守卫 | 缺失 | 已有 | 已有 | 已有 | 已有 | 已有 | 已有 |
| B8 协议模式整段清空（误丢 RESET_CONFIRM）/伪头卡死 | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 |

### 7.3 修复内容（与 JS 修复版行为对齐，线格式不变）

- **B1**：七语言均在 CRC 校验后、消费帧头前强制校验 `length==0 || length>maxBufferSize`，
  错误码选择与 JS 一致（超限→`BUFFER_OVERFLOW`，零长→`LENGTH_CRC_FAILED`），发错误包 + 上报错误 + 进协议模式；
  Java/C#/Go 先用无符号宽类型比较再 cast，消除溢出为负的风险。
- **B3**：Python/C#/Java/Rust/C++/C 接收缓冲改为**头游标 + 条件 compact**（游标 ≥1MiB 或 ≥剩余量时一次性前移），
  消费 O(1) 摊还；Go 原有 append+reslice 已是摊还 O(1)，未改。
- **B5**：七语言 RPC 层对反序列化失败/非对象请求体/缺失 method/二进制请求截断均回
  `{"error":"Invalid request payload..."}` 错误响应；Python/C#/Java 同时补齐二进制请求解码边界检查。
- **B7**：Python `destroy()` 补幂等守卫。
- **B8**：七语言协议模式统一改为**扫描 `0xFFFFFFFF`**（64KB 窗口）：命中后按裸帧解析，
  `length>50` 的伪头跳过 8 字节继续扫描（不再卡死）；窗口内无头时仅丢弃已扫描部分、保留尾部 3 字节（半截头不丢）。
- 附带：Rust CRC32 表改为 `OnceLock` 一次性初始化（原先每次调用重建 256 项表）；
  `examples/node_client.js` 错误断言适配 JS 修复版 `RpcError.data`（原 `err.error` 已不存在）。

### 7.4 未移植项（行为变更，非 bug）

报告§4 的 15s 默认 RPC 超时、3s 重同步超时断链、`ERESYNC` 拒绝在途请求属**行为变更**，
本轮未移植到其它语言（各语言保持无默认超时，由调用方自行控制）；如需对齐可另行追加。

### 7.5 复测结果（Windows / 回环 TCP）

| 语言 | 回环测试 | Node→该语言服务端 | 该语言客户端→Node |
|---|---|---|---|
| Python | 12/12 | —（test_interop_node 12/12） | 12/12 |
| C# | 12/12 | 10/10 | 11/11 |
| Java | 13/13 | 10/10 | 11/11 |
| Go | `go test -race` 全部 ok | 10/10 | 11/11 |
| Rust | 7/7 | 10/10 | 11/11 |
| C++ | 13/13 | 10/10 | 11/11 |
| C | 12/12 | 10/10 | 11/11 |

各语言回环均含 CRC 损坏注入→错误包→RESET_CONFIRM→重同步后 RPC 恢复用例；
互操作覆盖 JSON/TEXT/BINARY/null/错误响应/方法不存在/75KB 二进制请求/事件/50 并发。
