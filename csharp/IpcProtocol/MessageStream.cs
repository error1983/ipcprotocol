// MessageStream.cs — MessageStream.js 的 C# 移植版（线格式完全兼容）。
//
// 帧格式（正常模式，enableCRC=true 时）:
//   [长度 u32LE][长度头CRC32 u32LE][数据...][数据CRC32 u32LE]
// 关闭 CRC 时: [长度 u32LE][数据...]
//
// 错误包（按普通帧封装，5 字节）: [0xEEEEEEEE][错误类型 1B]
//   1=LENGTH_CRC_FAILED, 2=MESSAGE_CRC_FAILED, 3=BUFFER_OVERFLOW
// 重同步协议消息（裸帧，无 CRC）: [0xFFFFFFFF][长度 u32LE][数据(如 'RESET_CONFIRM')]
//
// CRC32 与 JS 版一致: 反射多项式 0xEDB88320，初值/终值 0xFFFFFFFF。

using System.Buffers.Binary;

namespace IpcProtocol;

public sealed class MessageStream : IDisposable
{
    private static readonly byte[] ProtocolHeader = { 0xFF, 0xFF, 0xFF, 0xFF };
    private static readonly byte[] ErrorHeader = { 0xEE, 0xEE, 0xEE, 0xEE };

    private static readonly Dictionary<string, byte> ErrorTypes = new()
    {
        ["LENGTH_CRC_FAILED"] = 1,
        ["MESSAGE_CRC_FAILED"] = 2,
        ["BUFFER_OVERFLOW"] = 3,
    };
    private static readonly Dictionary<byte, string> ErrorTypeCodes = new()
    {
        [1] = "LENGTH_CRC_FAILED",
        [2] = "MESSAGE_CRC_FAILED",
        [3] = "BUFFER_OVERFLOW",
    };

    /// <summary>收到一条完整消息（不含帧头/CRC）。</summary>
    public event Action<byte[]>? Message;
    /// <summary>流错误（CRC 校验失败、连接断开等）。</summary>
    public event Action<Exception>? Error;
    /// <summary>重同步完成（收到 RESET_CONFIRM）。</summary>
    public event Action? SyncComplete;

    private readonly Stream _stream;
    private readonly bool _enableCRC;
    private readonly long _maxBufferSize;
    private readonly List<byte> _buffer = new();
    private int? _expectedLength;
    private bool _protocolMode;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly CancellationTokenSource _cts = new();
    private Task? _readTask;
    private int _started;
    private volatile bool _disposed;

    public MessageStream(Stream stream, bool enableCRC = true, long maxBufferSize = 256L * 1024 * 1024)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _enableCRC = enableCRC;
        _maxBufferSize = maxBufferSize;
    }

    /// <summary>启动读循环。幂等；应在挂好监听器之后调用（RpcProtocol 构造时会自动调用），
    /// 否则读循环可能在监听器注册前就分发早到的消息，造成竞态丢包。</summary>
    public void Start()
    {
        if (_disposed || Interlocked.Exchange(ref _started, 1) == 1) return;
        _readTask = Task.Run(() => ReadLoopAsync(_cts.Token));
    }

    // ========== 发送 ==========

    /// <summary>发送一条消息（带长度头与可选 CRC），并发调用按序串行化。</summary>
    public async Task SendAsync(byte[] data)
    {
        if (data.Length > _maxBufferSize)
            throw new InvalidOperationException("Data size exceeds maximum allowed limit");

        await _sendLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed) throw new IOException("Stream is not connected");
            var frame = BuildFrame(data);
            await _stream.WriteAsync(frame).ConfigureAwait(false);
            await _stream.FlushAsync().ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private byte[] BuildFrame(byte[] data)
    {
        var lengthHeader = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(lengthHeader, (uint)data.Length);
        if (!_enableCRC)
        {
            var frame = new byte[4 + data.Length];
            lengthHeader.CopyTo(frame, 0);
            data.CopyTo(frame, 4);
            return frame;
        }
        var result = new byte[4 + 4 + data.Length + 4];
        lengthHeader.CopyTo(result, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(4), Crc32(lengthHeader));
        data.CopyTo(result, 8);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(8 + data.Length), Crc32(data));
        return result;
    }

    /// <summary>发送错误包（按普通帧格式封装）。</summary>
    public async Task SendErrorAsync(string errorType)
    {
        var code = ErrorTypes.GetValueOrDefault(errorType, (byte)0);
        var errorData = new byte[] { 0xEE, 0xEE, 0xEE, 0xEE, code };
        try
        {
            await SendAsync(errorData).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            Console.Error.WriteLine($"Failed to send error packet: {err.Message}");
        }
    }

    /// <summary>发送协议消息: [FFFFFFFF][长度LE][数据]，不带 CRC。</summary>
    public async Task SendProtocolMessageAsync(string message)
    {
        var data = System.Text.Encoding.UTF8.GetBytes(message);
        var frame = new byte[8 + data.Length];
        ProtocolHeader.CopyTo(frame, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(4), (uint)data.Length);
        data.CopyTo(frame, 8);
        try
        {
            await _sendLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_disposed) throw new IOException("Stream is not connected");
                await _stream.WriteAsync(frame).ConfigureAwait(false);
                await _stream.FlushAsync().ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }
        catch (Exception err)
        {
            Console.Error.WriteLine($"Failed to send protocol message '{message}': {err.Message}");
        }
    }

    // ========== 接收 ==========

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        var chunk = new byte[65536];
        try
        {
            while (!ct.IsCancellationRequested)
            {
                int n = await _stream.ReadAsync(chunk, ct).ConfigureAwait(false);
                if (n == 0) break;
                OnData(chunk.AsSpan(0, n));
            }
        }
        catch (OperationCanceledException) { return; }
        catch (Exception err)
        {
            if (!_disposed) Error?.Invoke(err);
        }
        if (!_disposed) Error?.Invoke(new IOException("Connection closed"));
    }

    private void OnData(ReadOnlySpan<byte> data)
    {
        _buffer.AddRange(data.ToArray());

        if (_buffer.Count > _maxBufferSize)
        {
            Error?.Invoke(new InvalidOperationException($"Buffer size: {_buffer.Count} exceeds maximum allowed limit"));
            _ = SendErrorAsync("BUFFER_OVERFLOW");
            Reset();
            return;
        }

        while (_buffer.Count > 0)
        {
            var span = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_buffer);

            // 协议模式：只接受协议消息
            if (_protocolMode && _buffer.Count >= 8)
            {
                if (BinaryPrimitives.ReadUInt32LittleEndian(span) == 0xFFFFFFFF)
                {
                    uint length = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(4));
                    long total = 8 + length;
                    if (_buffer.Count >= total)
                    {
                        var messageData = span.Slice(8, (int)length).ToArray();
                        _buffer.RemoveRange(0, (int)total);
                        if (HandleProtocolMessage(messageData)) continue;
                    }
                    else break; // 等待更多数据
                }
                else
                {
                    // 协议模式下的残留数据，丢弃
                    Console.WriteLine("[MessageStream] Dropping non-protocol data in protocol mode");
                    _buffer.Clear();
                    break;
                }
            }

            if (_protocolMode) break; // 等待协议同步完成

            // 正常模式
            if (_expectedLength is null)
            {
                int headerSize = _enableCRC ? 8 : 4;
                if (_buffer.Count < headerSize) break;

                span = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_buffer);
                uint length = BinaryPrimitives.ReadUInt32LittleEndian(span);
                if (_enableCRC)
                {
                    uint expectedCRC = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(4));
                    uint calculated = Crc32(span.Slice(0, 4));
                    if (calculated != expectedCRC)
                    {
                        Console.Error.WriteLine($"Length header CRC check failed: expected {expectedCRC}, got {calculated}");
                        _ = SendErrorAsync("LENGTH_CRC_FAILED");
                        Error?.Invoke(new InvalidDataException("Length header CRC check failed"));
                        Reset();
                        return;
                    }
                }

                _expectedLength = (int)length;
                _buffer.RemoveRange(0, headerSize);
            }

            int expected = _expectedLength.Value;
            int messageTotalSize = _enableCRC ? expected + 4 : expected;
            if (_buffer.Count >= messageTotalSize)
            {
                span = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_buffer);
                var messageData = span.Slice(0, expected).ToArray();
                if (_enableCRC)
                {
                    uint messageCRC = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(expected));
                    uint calculated = Crc32(messageData);
                    if (calculated != messageCRC)
                    {
                        Console.Error.WriteLine($"Message CRC check failed: expected {messageCRC}, got {calculated}");
                        _ = SendErrorAsync("MESSAGE_CRC_FAILED");
                        Error?.Invoke(new InvalidDataException("Message CRC check failed"));
                        Reset();
                        return;
                    }
                }

                _buffer.RemoveRange(0, messageTotalSize);
                _expectedLength = null;

                // 错误包在交给上层之前拦截处理
                if (IsErrorPacket(messageData))
                {
                    var name = ErrorTypeCodes.GetValueOrDefault(messageData[4], "UNKNOWN_ERROR");
                    Console.WriteLine($"Received error packet: {name}");
                    Reset();
                    _ = SendProtocolMessageAsync("RESET_CONFIRM");
                    _protocolMode = false;
                    continue;
                }

                Message?.Invoke(messageData);
            }
            else break;
        }
    }

    private bool HandleProtocolMessage(byte[] messageData)
    {
        if (messageData.Length > 50) return false;
        var text = System.Text.Encoding.UTF8.GetString(messageData).Trim();
        if (text == "RESET_CONFIRM")
        {
            Console.WriteLine("Received reset confirmation");
            _protocolMode = false;
            _buffer.Clear();
            _expectedLength = null;
            SyncComplete?.Invoke();
            return true;
        }
        return false;
    }

    private static bool IsErrorPacket(byte[] data)
        => data.Length == 5 && data[0] == 0xEE && data[1] == 0xEE && data[2] == 0xEE && data[3] == 0xEE;

    /// <summary>清空接收状态并进入协议模式，等待对端 RESET_CONFIRM。</summary>
    public void Reset()
    {
        Console.WriteLine("[MessageStream] Resetting stream state");
        _buffer.Clear();
        _expectedLength = null;
        _protocolMode = true;
    }

    public object GetStatus() => new
    {
        bufferLength = _buffer.Count,
        expectedLength = _expectedLength,
        enableCRC = _enableCRC,
        protocolMode = _protocolMode,
    };

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cts.Cancel();
        try { _stream.Dispose(); } catch { /* ignore */ }
        _buffer.Clear();
        Message = null;
        Error = null;
        SyncComplete = null;
        Console.WriteLine("[MessageStream] Destroyed");
    }

    // ========== CRC32（与 JS 版一致） ==========

    private static readonly uint[] Crc32Table = BuildCrc32Table();

    private static uint[] BuildCrc32Table()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int j = 0; j < 8; j++)
                c = (c & 1) != 0 ? 0xEDB88320 ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }

    public static uint Crc32(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (byte b in data)
            crc = Crc32Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFF;
    }
}