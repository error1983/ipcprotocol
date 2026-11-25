// RpcProtocol.cs — RpcProtocol.js 的 C# 移植版（线格式完全兼容）。
//
// 消息格式: [type u8][contentType u8][UUID 16B][payload...]
//   type:        0=REQUEST, 1=RESPONSE, 2=EVENT
//   contentType: 0=TEXT(utf8), 1=JSON(utf8), 2=BINARY(原始字节)
// JSON 请求体: {"method":...,"params":...,"to":...(可选)}；错误响应体: {"error":...}
// 二进制请求体: [methodLen u32BE][method utf8][paramsLen u32BE][params JSON utf8][binary...]
//   服务端 handler 收到的参数为 [binary, ...params]

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;

namespace IpcProtocol;

public enum MessageType : byte { Request = 0, Response = 1, Event = 2 }
public enum ContentType : byte { Text = 0, Json = 1, Binary = 2 }

/// <summary>对端返回的错误响应，Error 属性携带原始 error 值（string 或 JsonElement）。</summary>
public sealed class RpcException : Exception
{
    public object? Error { get; }
    public RpcException(object? error) : base(error?.ToString() ?? "null") => Error = error;
}

public sealed class RpcEvent
{
    public ContentType ContentType { get; init; }
    public object? Content { get; init; }
}

public sealed class RpcProtocol : IDisposable
{
    public static class Types
    {
        public const byte Request = 0, Response = 1, Event = 2;
    }
    public static class ContentTypes
    {
        public const byte Text = 0, Json = 1, Binary = 2;
    }

    /// <summary>收到事件消息（sendText/sendJson/sendBinary）。</summary>
    public event Action<RpcEvent>? Event;
    /// <summary>底层流错误。</summary>
    public event Action<Exception>? Error;

    private MessageStream? _messageStream;
    private readonly ConcurrentDictionary<string, Func<object?[], Task<object?>>> _handlers = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<object?>> _pendingRequests = new();
    private volatile bool _disposed;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        // 与 JSON.stringify 一致：不转义非 ASCII 之外的差异可接受，保持紧凑输出
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        // JS 中 Buffer/TypedArray 经 JSON.stringify 输出为数字数组，而非 Base64
        Converters = { new ByteArrayAsNumberArrayConverter() },
    };

    /// <summary>byte[] 与 JSON 数字数组互转，对齐 JS 的 JSON.stringify(Buffer) 行为。</summary>
    private sealed class ByteArrayAsNumberArrayConverter : System.Text.Json.Serialization.JsonConverter<byte[]>
    {
        public override byte[] Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartArray)
                throw new JsonException("Expected array of numbers for byte[]");
            var list = new List<byte>();
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                list.Add(reader.GetByte());
            return list.ToArray();
        }

        public override void Write(Utf8JsonWriter writer, byte[] value, JsonSerializerOptions options)
        {
            writer.WriteStartArray();
            foreach (var b in value) writer.WriteNumberValue(b);
            writer.WriteEndArray();
        }
    }

    public RpcProtocol(MessageStream messageStream)
    {
        _messageStream = messageStream ?? throw new ArgumentNullException(nameof(messageStream));
        _messageStream.Message += OnMessage;
        _messageStream.Error += OnStreamError;
        // 先挂好监听器再启动读循环，避免早到消息在注册前被分发的竞态
        _messageStream.Start();
    }

    // ========== 序列化 ==========

    public static byte[] Serialize(byte type, byte contentType, string id, object? data)
    {
        var header = new byte[18];
        header[0] = type;
        header[1] = contentType;
        System.Convert.FromHexString(id.Replace("-", "")).CopyTo(header, 2);

        byte[] payload = contentType switch
        {
            ContentTypes.Text => Encoding.UTF8.GetBytes((string)data!),
            ContentTypes.Json => JsonSerializer.SerializeToUtf8Bytes(data, JsonOptions),
            ContentTypes.Binary => (byte[])data!,
            _ => throw new NotSupportedException($"Unsupported content type: {contentType}"),
        };
        var result = new byte[18 + payload.Length];
        header.CopyTo(result, 0);
        payload.CopyTo(result, 18);
        return result;
    }

    public static (byte Type, byte ContentType, string Id, object? Content) Deserialize(byte[] buffer)
    {
        if (buffer.Length < 18)
            throw new InvalidDataException("Failed to deserialize message: buffer too short");
        byte type = buffer[0], contentType = buffer[1];
        string hex = System.Convert.ToHexString(buffer, 2, 16).ToLowerInvariant();
        string id = $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..]}";
        var payload = buffer.AsSpan(18);
        object? content = contentType switch
        {
            ContentTypes.Text => Encoding.UTF8.GetString(payload),
            ContentTypes.Json => JsonDocument.Parse(payload.ToArray()).RootElement.Clone(),
            ContentTypes.Binary => payload.ToArray(),
            _ => throw new InvalidDataException($"Failed to deserialize message: unknown content type {contentType}"),
        };
        return (type, contentType, id, content);
    }

    public static byte[] EncodeBinaryRequest(string method, object?[] parameters, byte[] binaryData)
    {
        var methodBuf = Encoding.UTF8.GetBytes(method);
        var paramsBuf = JsonSerializer.SerializeToUtf8Bytes(parameters, JsonOptions);
        var result = new byte[4 + methodBuf.Length + 4 + paramsBuf.Length + binaryData.Length];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(offset), (uint)methodBuf.Length); offset += 4;
        methodBuf.CopyTo(result, offset); offset += methodBuf.Length;
        BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(offset), (uint)paramsBuf.Length); offset += 4;
        paramsBuf.CopyTo(result, offset); offset += paramsBuf.Length;
        binaryData.CopyTo(result, offset);
        return result;
    }

    public static (string Method, JsonElement Params, byte[] BinaryData) DecodeBinaryRequest(byte[] buffer)
    {
        int offset = 0;
        if (buffer.Length < 4)
            throw new InvalidDataException("binary request too short");
        uint methodLength = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset)); offset += 4;
        if (buffer.Length < offset + (long)methodLength + 4)
            throw new InvalidDataException("binary request truncated (method)");
        string method = Encoding.UTF8.GetString(buffer, offset, (int)methodLength); offset += (int)methodLength;
        uint paramsLength = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset)); offset += 4;
        if (buffer.Length < offset + (long)paramsLength)
            throw new InvalidDataException("binary request truncated (params)");
        var parameters = JsonDocument.Parse(buffer.AsMemory(offset, (int)paramsLength)).RootElement.Clone(); offset += (int)paramsLength;
        return (method, parameters, buffer.AsSpan(offset).ToArray());
    }

    // ========== 接收处理 ==========

    private void OnMessage(byte[] buffer)
    {
        try
        {
            var (type, contentType, id, content) = Deserialize(buffer);
            if (type > 2) throw new InvalidDataException($"Invalid message type: {type}");
            if (contentType > 2) throw new InvalidDataException($"Invalid content type: {contentType}");

            switch (type)
            {
                case Types.Request:
                    _ = HandleRequestAsync(id, content);
                    break;
                case Types.Response:
                    HandleResponse(id, content);
                    break;
                case Types.Event:
                    Event?.Invoke(new RpcEvent { ContentType = (ContentType)contentType, Content = content });
                    break;
            }
        }
        catch (Exception err)
        {
            Console.Error.WriteLine($"Failed to handle message: {err.Message}");
            // 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
            if (buffer.Length >= 18 && buffer[0] == Types.Request)
            {
                try
                {
                    string hex = System.Convert.ToHexString(buffer, 2, 16).ToLowerInvariant();
                    string id = $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..]}";
                    _ = SendResponseAsync(id, new { error = "Invalid request payload" });
                }
                catch { /* ignore */ }
            }
        }
    }

    private async Task HandleRequestAsync(string id, object? request)
    {
        string? method;
        object?[] args;

        try
        {
            if (request is byte[] binary)
            {
                var (m, parameters, binaryData) = DecodeBinaryRequest(binary);
                method = m;
                var list = new List<object?> { binaryData };
                if (parameters.ValueKind == JsonValueKind.Array)
                    foreach (var item in parameters.EnumerateArray()) list.Add(item);
                args = list.ToArray();
            }
            else if (request is JsonElement json && json.ValueKind == JsonValueKind.Object)
            {
                method = json.TryGetProperty("method", out var m) && m.ValueKind == JsonValueKind.String ? m.GetString() : null;
                if (json.TryGetProperty("params", out var p) && p.ValueKind == JsonValueKind.Array)
                    args = p.EnumerateArray().Cast<object?>().ToArray();
                else if (json.TryGetProperty("params", out p) && p.ValueKind != JsonValueKind.Null && p.ValueKind != JsonValueKind.Undefined)
                    args = new object?[] { p };
                else
                    args = Array.Empty<object?>();
            }
            else
            {
                throw new InvalidDataException("request payload is not an object");
            }
            if (string.IsNullOrEmpty(method))
                throw new InvalidDataException("request method missing");
        }
        catch (Exception err)
        {
            await SendResponseAsync(id, new { error = $"Invalid request payload: {err.Message}" }).ConfigureAwait(false);
            return;
        }

        if (!_handlers.TryGetValue(method, out var handler))
        {
            await SendResponseAsync(id, new { error = $"Method not found: {method}" }).ConfigureAwait(false);
            return;
        }

        try
        {
            var result = await handler(args).ConfigureAwait(false);
            byte contentType = ContentTypes.Json;
            if (result is string) contentType = ContentTypes.Text;
            else if (result is byte[]) contentType = ContentTypes.Binary;
            await SendResponseAsync(id, result, contentType).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            await SendResponseAsync(id, new { error = err.Message }).ConfigureAwait(false);
        }
    }

    private void HandleResponse(string id, object? response)
    {
        if (!_pendingRequests.TryRemove(id, out var pending)) return;

        if (response is JsonElement json && json.ValueKind == JsonValueKind.Object
            && json.TryGetProperty("error", out var error) && IsTruthy(error))
        {
            object? errorValue = error.ValueKind == JsonValueKind.String ? error.GetString() : (object?)error;
            pending.TrySetException(new RpcException(errorValue));
        }
        else
        {
            pending.TrySetResult(response);
        }
    }

    // JS 真值语义：null/undefined/false/0/"" 为假
    private static bool IsTruthy(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined or JsonValueKind.False => false,
        JsonValueKind.String => !string.IsNullOrEmpty(e.GetString()),
        JsonValueKind.Number => e.GetDouble() != 0,
        _ => true,
    };

    private void OnStreamError(Exception error)
    {
        foreach (var key in _pendingRequests.Keys)
        {
            if (_pendingRequests.TryRemove(key, out var pending))
                pending.TrySetException(new RpcException($"Request failed: {error.Message}"));
        }
        Error?.Invoke(error);
    }

    // ========== 发送 ==========

    public Task SendRawAsync(byte type, byte contentType, string id, object? data)
    {
        var stream = _messageStream ?? throw new InvalidOperationException("messageStream is not initialized");
        return stream.SendAsync(Serialize(type, contentType, id, data));
    }

    /// <summary>发送 JSON 请求并等待响应。返回 string(TEXT)/byte[](BINARY)/JsonElement(JSON)。</summary>
    public async Task<object?> SendRequestAsync(string method, object? parameters = null, string? to = null)
    {
        var id = Guid.NewGuid().ToString();
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[id] = tcs;
        var body = to is null
            ? (object)new { method, @params = parameters }
            : new { method, @params = parameters, to };
        try
        {
            await SendRawAsync(Types.Request, ContentTypes.Json, id, body).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            _pendingRequests.TryRemove(id, out _);
            tcs.TrySetException(err);
        }
        return await tcs.Task.ConfigureAwait(false);
    }

    /// <summary>发送 JSON 请求并把 JSON 响应反序列化为 T。</summary>
    public async Task<T?> SendRequestAsync<T>(string method, object? parameters = null, string? to = null)
    {
        var result = await SendRequestAsync(method, parameters, to).ConfigureAwait(false);
        return Convert<T>(result);
    }

    /// <summary>发送二进制请求并等待响应。服务端 handler 收到 [data, ...args]。</summary>
    public async Task<object?> SendRequestBinaryAsync(string method, byte[] data, params object?[] args)
    {
        var id = Guid.NewGuid().ToString();
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[id] = tcs;
        var binaryRequest = EncodeBinaryRequest(method, args, data);
        try
        {
            await SendRawAsync(Types.Request, ContentTypes.Binary, id, binaryRequest).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            _pendingRequests.TryRemove(id, out _);
            tcs.TrySetException(err);
        }
        return await tcs.Task.ConfigureAwait(false);
    }

    public Task SendResponseAsync(string id, object? response, byte contentType = ContentTypes.Json)
        => SendRawAsync(Types.Response, contentType, id, response);

    // ========== 辅助方法 ==========

    public Task SendTextAsync(string text)
        => SendRawAsync(Types.Event, ContentTypes.Text, Guid.NewGuid().ToString(), text);

    public Task SendBinaryAsync(byte[] data)
        => SendRawAsync(Types.Event, ContentTypes.Binary, Guid.NewGuid().ToString(), data);

    public Task SendJsonAsync(object? data)
        => SendRawAsync(Types.Event, ContentTypes.Json, Guid.NewGuid().ToString(), data);

    /// <summary>注册异步方法处理器。JSON 参数以 JsonElement 传入，可用 Convert&lt;T&gt; 转换。</summary>
    public void RegisterMethod(string name, Func<object?[], Task<object?>> handler)
        => _handlers[name] = handler;

    /// <summary>注册同步方法处理器。</summary>
    public void RegisterMethod(string name, Func<object?[], object?> handler)
        => _handlers[name] = args => Task.FromResult(handler(args));

    /// <summary>把 JSON 参数/结果（JsonElement）转换为强类型对象。</summary>
    public static T? Convert<T>(object? value)
    {
        if (value is T typed) return typed;
        if (value is JsonElement element) return element.Deserialize<T>(JsonOptions);
        if (value is null) return default;
        return (T)System.Convert.ChangeType(value, typeof(T))!;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        foreach (var key in _pendingRequests.Keys)
        {
            if (_pendingRequests.TryRemove(key, out var pending))
                pending.TrySetException(new RpcException("RPC Protocol destroyed"));
        }
        if (_messageStream is not null)
        {
            _messageStream.Message -= OnMessage;
            _messageStream.Error -= OnStreamError;
            _messageStream.Dispose();
            _messageStream = null;
        }
        _handlers.Clear();
        Event = null;
        Error = null;
        Console.WriteLine("[RpcProtocol] Destroyed");
    }
}