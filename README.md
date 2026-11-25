# ipcprotocol

一套跨语言的轻量 IPC/RPC 协议实现，线格式完全兼容，可在 **Node.js / Python / C# / Java / Go / Rust / C++ / C** 之间任意互操作。

协议分两层：

- **MessageStream（帧层）**：长度头分帧 + 可选 CRC32 校验 + 错误包与自动重同步；
- **RpcProtocol（消息层）**：请求/响应/事件三种消息，支持 JSON、文本、二进制载荷，UUID 关联并发请求。

完整的线格式与行为语义见 **[PROTOCOL.md](./PROTOCOL.md)**（新语言移植请对照其中的一致性检查清单）。

## 特性

- 请求/响应 RPC：方法注册、参数展开、并发请求（UUID 关联）、错误传播
- 三种载荷：TEXT（UTF-8 字符串）、JSON、BINARY（原始字节）
- 二进制请求：大块字节数据 + JSON 附加参数混合传输
- 单向事件通知（文本/JSON/二进制）
- 帧级 CRC32 校验，损坏后通过错误包 + `RESET_CONFIRM` 握手自动重同步
- 传输无关：TCP、命名管道、WebSocket 二进制等任意双向字节流均可承载（示例使用 TCP）
- Python / Java / Go / Rust / C++ / C 零第三方依赖（C/C++ 仅标准库 + 系统 socket/pthread）；C# 仅用 BCL；Node 原版仅依赖 `uuid`

## 目录结构

```
├── MessageStream.js / RpcProtocol.js   # Node.js 原版（协议事实来源）
├── PROTOCOL.md                         # 跨语言协议规格文档
├── python/                             # Python 实现（asyncio，3.10+）
│   ├── message_stream.py / rpc_protocol.py
│   ├── test_loopback.py                # 回环测试
│   └── test_interop_node.py            # 与 Node 原版互操作测试
├── csharp/
│   ├── IpcProtocol/                    # C# 类库（net8.0）
│   └── IpcProtocol.Tests/              # 回环测试
├── java/src/ipc/                       # Java 实现（11+，零依赖，含内置 JSON）
│   ├── MessageStream.java / RpcProtocol.java / Json.java
│   ├── LoopbackTest.java               # 回环测试
│   └── Example.java                    # server/client 双模式示例
├── go/                                 # Go 实现（1.21+，仅标准库）
│   ├── message_stream.go / rpc_protocol.go
│   ├── rpc_test.go                     # 回环测试（go test -race）
│   └── example/main.go                 # server/client 双模式示例
├── rust/                               # Rust 实现（零依赖，含内置 JSON）
│   ├── src/{json,message_stream,rpc_protocol,lib}.rs
│   ├── tests/loopback.rs               # 回环测试
│   └── src/bin/example.rs              # server/client 双模式示例
├── cpp/                                # C++17 实现（header-only，仅标准库）
│   ├── include/ipc/{json,message_stream,rpc_protocol,tcp}.hpp
│   ├── tests/loopback_test.cpp         # 回环测试
│   └── examples/example.cpp            # server/client 双模式示例
├── c/                                  # C11 实现（pthreads，Windows 下 Winsock）
│   ├── json.{h,c} / message_stream.{h,c} / rpc.{h,c} / tcp.{h,c}
│   ├── loopback_test.c                 # 回环测试
│   └── example.c                       # server/client 双模式示例
└── examples/                           # 各语言示例服务端/客户端（互操作用）
    ├── node_server.js / node_client.js
    ├── python_server.py
    └── csharp/
```

## 快速上手

### Node.js（原版）

```js
import net from 'net';
import { MessageStream } from './MessageStream.js';
import { RpcProtocol } from './RpcProtocol.js';

const socket = net.connect(9000, '127.0.0.1', async () => {
    const rpc = new RpcProtocol(new MessageStream({ socket }));
    rpc.on('error', (err) => console.error(err));   // 必须注册，见下方注意事项
    console.log(await rpc.sendRequest('add', [3, 4])); // 7
});
```

### Python

```python
from python.message_stream import connect_tcp, serve_tcp
from python.rpc_protocol import RpcProtocol

# 服务端
def on_conn(stream):
    rpc = RpcProtocol(stream)
    rpc.register_method('add', lambda a, b: a + b)

server = await serve_tcp(on_conn, port=9000)

# 客户端
stream = await connect_tcp('127.0.0.1', 9000)
rpc = RpcProtocol(stream)
result = await rpc.send_request('add', [3, 4])          # 7
data = await rpc.send_request_binary('upload', b'...', 'tag-1')
```

### C#

```csharp
using IpcProtocol;

// 服务端注册方法
var rpc = new RpcProtocol(new MessageStream(tcp.GetStream()));
rpc.RegisterMethod("add", a => RpcProtocol.Convert<int>(a[0]) + RpcProtocol.Convert<int>(a[1]));

// 客户端调用
var sum = await rpc.SendRequestAsync<int>("add", new object[] { 3, 4 });   // 7
var up  = await rpc.SendRequestBinaryAsync("upload", bytes, "tag-1");
rpc.Event += e => Console.WriteLine(e.Content);   // 接收事件
```

### Java

```java
import ipc.*;

// 服务端注册方法（RpcProtocol 构造时自动启动读循环）
RpcProtocol rpc = new RpcProtocol(new MessageStream(in, out));
rpc.registerMethod("add", a -> ((Number) a[0]).longValue() + ((Number) a[1]).longValue());

// 客户端调用
Object sum = rpc.sendRequest("add", Arrays.asList(3, 4)).get();            // 7
Object up  = rpc.sendRequestBinary("upload", bytes, "tag-1").get();
rpc.onEvent((contentType, content) -> System.out.println(content));
```

### Go

```go
// 服务端（NewRpcProtocol 内部先挂回调再启动读循环）
rpc := ipc.NewRpcProtocol(ipc.NewMessageStream(conn, conn))
rpc.RegisterMethod("add", func(args []any) (any, error) {
    return args[0].(float64) + args[1].(float64), nil
})

// 客户端（同步阻塞，可多 goroutine 并发调用）
sum, err := rpc.SendRequest("add", []any{3, 4})            // 7
up, err := rpc.SendRequestBinary("upload", data, "tag-1")
rpc.OnEvent = func(ct int, content any) { fmt.Println(content) }
```

### Rust

```rust
// 服务端（零依赖，内置 Json 枚举；Handler = Box<dyn Fn(Vec<Json>) -> Result<Json, String>>）
let rpc = RpcProtocol::new(MessageStream::new(reader, writer));
rpc.register_method("add", Box::new(|args: Vec<Json>| {
    Ok(Json::Num(args[0].as_f64().unwrap() + args[1].as_f64().unwrap()))
}));

// 客户端
let sum = rpc.send_request("add", Json::Arr(vec![Json::Num(3.0), Json::Num(4.0)]))?;
let up  = rpc.send_request_binary("upload", &data, vec![Json::Str("tag-1".into())])?;
rpc.on_event(Box::new(|ct, content| println!("{content:?}")));
```

### C++

```cpp
#include "ipc/rpc_protocol.hpp"

// 服务端（header-only，C++17）
auto rpc = std::make_shared<ipc::RpcProtocol>(ms);
rpc->registerMethod("add", [](const std::vector<ipc::Json>& a) {
    return ipc::Json::ofNum(a[0].asNum() + a[1].asNum());
});

// 客户端（同步阻塞，内部 condvar 等待）
auto sum = rpc->sendRequest("add", ipc::Json::ofArr({ipc::Json::ofNum(3), ipc::Json::ofNum(4)}));
auto up  = rpc->sendRequestBinary("upload", data, {ipc::Json::ofStr("tag-1")});
rpc->onEvent([](uint8_t ct, ipc::Json content) { /* ... */ });
```

### C

```c
#include "rpc.h"

/* 服务端：handler 返回接管所有权的结果，NULL 视为 JSON null */
static JsonValue* h_add(void* u, JsonValue** a, size_t n, char** err) {
    return json_num(a[0]->u.number + a[1]->u.number);
}
RpcProtocol* rpc = rpc_create(ms_create(tcp_io(conn), 1, 0));
rpc_register(rpc, "add", h_add, NULL);

/* 客户端：阻塞等待，结果用完须 rpc_result_free */
JsonValue* p = json_arr();
json_arr_push(p, json_num(3)); json_arr_push(p, json_num(4));
RpcResult r = rpc_send_request(rpc, "add", p, NULL);   /* r.value->u.number == 7 */
rpc_result_free(&r); json_free(p);
```

> 直接使用 `MessageStream`（不经 `RpcProtocol`）时，Java/C# 需**先挂监听器再调用
> `start()` / `Start()`**，避免早到消息在注册前被分发。详见 PROTOCOL.md §9.4。

## 运行示例与测试

环境要求：Node.js 18+、Python 3.10+、.NET SDK 8+、JDK 11+、Go 1.21+、Rust（cargo）、
C++17 编译器（如 MinGW g++）、C11 编译器 + pthreads（如 MinGW gcc）。

```powershell
npm install                                # 安装 Node 原版依赖（uuid）

# 回环测试（各语言自测，含 CRC 损坏注入与重同步恢复）
python python/test_loopback.py
python python/test_interop_node.py         # Python ↔ Node 原版互操作
dotnet run --project csharp/IpcProtocol.Tests
javac -encoding UTF-8 -d java/out java/src/ipc/*.java
java -cp java/out ipc.LoopbackTest
cd go; go test -count=1 -race ./...; cd ..
cd rust; cargo test --offline; cd ..
g++ -std=c++17 -O2 -Wall -Icpp/include cpp/tests/loopback_test.cpp -o cpp/out/loopback_test.exe -lws2_32
cpp/out/loopback_test.exe
gcc -std=c11 -O2 -Ic c/loopback_test.c c/json.c c/message_stream.c c/rpc.c c/tcp.c -o c/out/loopback_test.exe -lws2_32 -lpthread
c/out/loopback_test.exe

# 跨语言互操作：任选一端做服务端，另一端做客户端
node examples/node_server.js 9000                          # Node 服务端
python examples/python_server.py 9001                      # Python 服务端
dotnet run --project examples/csharp -- server 9002        # C# 服务端
java -cp java/out ipc.Example server 9003                  # Java 服务端
# Go/Rust 在各自目录下：go run ./example server 9004 / cargo run --offline --bin example -- server 9005
g++ -std=c++17 -O2 -Icpp/include cpp/examples/example.cpp -o cpp/out/example.exe -lws2_32
cpp/out/example.exe server 9006                            # C++ 服务端
gcc -std=c11 -O2 -Ic c/example.c c/json.c c/message_stream.c c/rpc.c c/tcp.c -o c/out/example.exe -lws2_32 -lpthread
c/out/example.exe server 9007                              # C 服务端

node examples/node_client.js <port>                        # Node 客户端
dotnet run --project examples/csharp -- client <port>      # C# 客户端
java -cp java/out ipc.Example client <port>                # Java 客户端
# Go/Rust 在各自目录下：go run ./example client <port> / cargo run --offline --bin example -- client <port>
cpp/out/example.exe client <port>                          # C++ 客户端
c/out/example.exe client <port>                            # C 客户端
```

已验证的互操作矩阵（客户端 × 服务端）：

| 客户端 \ 服务端 | Node | Python | C# | Java | Go | Rust | C++ | C |
|---|---|---|---|---|---|---|---|---|
| Node | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Python | ✅ | ✅ | — | — | — | — | — | — |
| C# | ✅ | ✅ | ✅ | ✅ | — | — | — | — |
| Java | ✅ | ✅ | ✅ | ✅ | — | — | — | — |
| Go | ✅ | — | — | — | ✅ | — | — | — |
| Rust | ✅ | — | — | — | — | ✅ | — | — |
| C++ | ✅ | — | — | — | — | — | ✅ | — |
| C | ✅ | — | — | — | — | — | — | ✅ |

✅ = 已实测通过（对角线为同语言 TCP 回环）；— = 未测（线格式一致，理论可互操作）。

覆盖场景：JSON/文本/二进制响应、Unicode、null、错误响应、方法不存在、75KB 二进制请求、
三类事件、50 并发请求、CRC 损坏注入后的自动重同步。

## 注意事项

1. **Node 侧必须注册 `error` 监听器**：`RpcProtocol` 会向上 emit `'error'`，
   Node EventEmitter 在无监听器时 emit error 会直接崩溃进程。
2. **CRC 开关与 maxBufferSize 需两端一致**（默认启用 CRC、256MiB 上限），协议内无协商机制。
3. **字节数组参与 JSON 序列化时是数字数组**（对齐 `JSON.stringify(Buffer)`），不是 Base64；
   C# 实现已内置转换器，自行扩展时请保持一致。
4. **C# 的 UUID 字节序**：勿用 `Guid.ToByteArray()` 默认混合小端，须按 RFC 4122 大端
   （PROTOCOL.md §5.1）。
5. **Windows/MinGW 编译 C/C++**：源码已定义 `WIN32_LEAN_AND_MEAN` 避免 Winsock 与
   Windows RPC 头冲突；链接需 `-lws2_32`，C 还需 `-lpthread`。
6. **C 的所有权约定**：handler 参数为借用、返回值由 rpc 层接管；`RpcResult` 用完须
   `rpc_result_free()`；事件内容需留存时按类型深拷贝（字节载荷不能经 JSON 往返）。

## 许可

内部项目，版权归 wgweb-group 所有。