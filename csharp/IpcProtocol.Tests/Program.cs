// C# 版回环测试：TCP 服务端 + 客户端在同一进程内验证全部协议路径。
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using IpcProtocol;

int passed = 0;
void Check(string name, bool cond)
{
    if (!cond) throw new Exception($"FAILED: {name}");
    passed++;
    Console.WriteLine($"  ok: {name}");
}

var listener = new TcpListener(IPAddress.Loopback, 0);
listener.Start();
int port = ((IPEndPoint)listener.LocalEndpoint).Port;

MessageStream? serverStream = null;
var serverReady = new TaskCompletionSource();
_ = Task.Run(async () =>
{
    var socket = await listener.AcceptTcpClientAsync();
    serverStream = new MessageStream(socket.GetStream());
    var rpc = new RpcProtocol(serverStream);
    rpc.RegisterMethod("add", args =>
        RpcProtocol.Convert<int>(args[0]) + RpcProtocol.Convert<int>(args[1]));
    rpc.RegisterMethod("echo", args => args[0]);
    rpc.RegisterMethod("greet", args => $"hello {RpcProtocol.Convert<string>(args[0])}");   // string -> TEXT
    rpc.RegisterMethod("blob", _ => new byte[] { 0x00, 0x01, 0x02, 0xFF });                // byte[] -> BINARY
    rpc.RegisterMethod("nothing", _ => (object?)null);                                     // null -> JSON null
    rpc.RegisterMethod("fail", _ => throw new Exception("boom"));
    rpc.RegisterMethod("upload", args =>
    {
        var data = (byte[])args[0]!;
        var tag = RpcProtocol.Convert<string>(args[1]);
        return new { size = data.Length, tag, head = data.Take(4).ToArray() };
    });
    serverReady.SetResult();
});

using var clientSocket = new TcpClient();
await clientSocket.ConnectAsync(IPAddress.Loopback, port);
var clientStream = new MessageStream(clientSocket.GetStream());
using var client = new RpcProtocol(clientStream);
await serverReady.Task;

var events = new List<RpcEvent>();
client.Event += e => { lock (events) events.Add(e); };

// 1. JSON 请求/响应
Check("json add", RpcProtocol.Convert<int>(await client.SendRequestAsync("add", new object[] { 3, 4 })) == 7);
var obj = new { name = "测试", list = new object[] { 1, 2.5, true, null! } };
var echo = await client.SendRequestAsync<JsonElement>("echo", new object[] { obj });
Check("json echo unicode", echo.GetProperty("name").GetString() == "测试" && echo.GetProperty("list")[1].GetDouble() == 2.5);

// 2. TEXT 响应
Check("text response", (string?)await client.SendRequestAsync("greet", new object[] { "世界" }) == "hello 世界");

// 3. BINARY 响应
Check("binary response", ((byte[]?)await client.SendRequestAsync("blob"))!.SequenceEqual(new byte[] { 0, 1, 2, 0xFF }));

// 4. null 响应
var nullResult = await client.SendRequestAsync("nothing");
Check("null response", nullResult is JsonElement { ValueKind: JsonValueKind.Null });

// 5. 错误响应
try { await client.SendRequestAsync("fail"); Check("error response", false); }
catch (RpcException e) { Check("error response", (string?)e.Error == "boom"); }

// 6. 方法不存在
try { await client.SendRequestAsync("nope"); Check("method not found", false); }
catch (RpcException e) { Check("method not found", e.Error!.ToString()!.Contains("Method not found: nope")); }

// 7. 二进制请求（~75KB，跨多个 TCP 分段）
var payload = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
payload = Enumerable.Repeat(payload, 300).SelectMany(x => x).ToArray();
var up = await client.SendRequestBinaryAsync("upload", payload, "tag-1") is JsonElement je ? je : default;
Check("binary request", up.GetProperty("size").GetInt32() == payload.Length
    && up.GetProperty("tag").GetString() == "tag-1"
    && up.GetProperty("head")[3].GetInt32() == 3);

// 8. 事件（TEXT/JSON/BINARY）：服务端 -> 客户端
await serverStream!.SendAsync(RpcProtocol.Serialize(RpcProtocol.Types.Event, RpcProtocol.ContentTypes.Json, Guid.NewGuid().ToString(), new { push = 1 }));
await serverStream.SendAsync(RpcProtocol.Serialize(RpcProtocol.Types.Event, RpcProtocol.ContentTypes.Text, Guid.NewGuid().ToString(), "srv-text"));
await serverStream.SendAsync(RpcProtocol.Serialize(RpcProtocol.Types.Event, RpcProtocol.ContentTypes.Binary, Guid.NewGuid().ToString(), new byte[] { 1, 2 }));
await Task.Delay(300);
lock (events)
{
    Check("events received", events.Count == 3
        && events[0].ContentType == ContentType.Json && ((JsonElement)events[0].Content!).GetProperty("push").GetInt32() == 1
        && events[1].ContentType == ContentType.Text && (string)events[1].Content! == "srv-text"
        && events[2].ContentType == ContentType.Binary && ((byte[])events[2].Content!).SequenceEqual(new byte[] { 1, 2 }));
}

// 9. 并发请求
var tasks = Enumerable.Range(0, 50).Select(i => client.SendRequestAsync<int>("add", new object[] { i, i })).ToArray();
var results = await Task.WhenAll(tasks);
Check("concurrent requests", results.SequenceEqual(Enumerable.Range(0, 50).Select(i => i * 2)));

// 10. CRC 错误恢复：客户端注入长度头 CRC 损坏的帧 -> 服务端检测错误并回错误包
//     -> 客户端回 RESET_CONFIRM -> 服务端 syncComplete，之后通信应正常
var srvErrors = new List<Exception>();
serverStream!.Error += e => srvErrors.Add(e);
var syncDone = new TaskCompletionSource();
serverStream.SyncComplete += () => syncDone.TrySetResult();

static byte[] Le32(uint v) => new[] { (byte)v, (byte)(v >> 8), (byte)(v >> 16), (byte)(v >> 24) };
var hello = "hello"u8.ToArray();
var bad = Le32(5).Concat(Le32(0xDEADBEEF)).Concat(hello).Concat(Le32(MessageStream.Crc32(hello))).ToArray();
await clientSocket.GetStream().WriteAsync(bad);
await syncDone.Task.WaitAsync(TimeSpan.FromSeconds(3));
Check("crc error detected", srvErrors.Count >= 1);
Check("resync then rpc works", RpcProtocol.Convert<int>(await client.SendRequestAsync("add", new object[] { 10, 20 })) == 30);

// 11. destroy 后资源清理
client.Dispose();
listener.Stop();
Console.WriteLine($"\nALL {passed} C# TESTS PASSED");