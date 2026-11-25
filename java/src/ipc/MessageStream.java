package ipc;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * MessageStream.js 的 Java 移植版（线格式完全兼容，Java 11+，零依赖）。
 *
 * 帧格式（正常模式，enableCRC=true 时）:
 *   [长度 u32LE][长度头CRC32 u32LE][数据...][数据CRC32 u32LE]
 * 关闭 CRC 时: [长度 u32LE][数据...]
 *
 * 错误包（按普通帧封装，5 字节）: [0xEEEEEEEE][错误类型 1B]
 *   1=LENGTH_CRC_FAILED, 2=MESSAGE_CRC_FAILED, 3=BUFFER_OVERFLOW
 * 重同步协议消息（裸帧，无 CRC）: [0xFFFFFFFF][长度 u32LE][数据(如 'RESET_CONFIRM')]
 *
 * CRC32 使用 java.util.zip.CRC32，与 JS 版（多项式 0xEDB88320）一致。
 * 基于阻塞 IO：调用 start() 后启动守护读线程，send 同步且线程安全。
 * 注意：构造后需先挂好消息监听器再调用 start()（RpcProtocol 会自动处理），
 * 否则读线程可能在监听器注册前就分发早到的消息，造成竞态丢包。
 */
public class MessageStream implements Closeable {

    public interface MessageListener { void onMessage(byte[] data); }
    public interface ErrorListener { void onError(Exception err); }
    public interface SyncCompleteListener { void onSyncComplete(); }

    private static final long PROTOCOL_HEADER = 0xFFFFFFFFL;
    private static final long ERROR_HEADER = 0xEEEEEEEEL;

    private static final Map<String, Integer> ERROR_TYPES = new HashMap<>();
    private static final Map<Integer, String> ERROR_TYPE_CODES = new HashMap<>();
    static {
        ERROR_TYPES.put("LENGTH_CRC_FAILED", 1);
        ERROR_TYPES.put("MESSAGE_CRC_FAILED", 2);
        ERROR_TYPES.put("BUFFER_OVERFLOW", 3);
        ERROR_TYPE_CODES.put(1, "LENGTH_CRC_FAILED");
        ERROR_TYPE_CODES.put(2, "MESSAGE_CRC_FAILED");
        ERROR_TYPE_CODES.put(3, "BUFFER_OVERFLOW");
    }

    private final InputStream in;
    private final OutputStream out;
    private final boolean enableCRC;
    private final long maxBufferSize;

    private byte[] buffer = new byte[0];
    private Integer expectedLength = null;
    private boolean protocolMode = false;

    private final Object sendLock = new Object();
    private volatile boolean closed = false;
    private Thread readThread;
    private boolean started = false;

    private final List<MessageListener> messageListeners = new ArrayList<>();
    private final List<ErrorListener> errorListeners = new ArrayList<>();
    private final List<SyncCompleteListener> syncListeners = new ArrayList<>();

    public MessageStream(InputStream in, OutputStream out) {
        this(in, out, true, 256L * 1024 * 1024);
    }

    public MessageStream(InputStream in, OutputStream out, boolean enableCRC, long maxBufferSize) {
        if (in == null || out == null) throw new IllegalArgumentException("stream is undefined");
        this.in = in;
        this.out = out;
        this.enableCRC = enableCRC;
        this.maxBufferSize = maxBufferSize;
    }

    /** 启动读循环。幂等；应在挂好监听器之后调用（RpcProtocol 构造时会自动调用）。 */
    public synchronized void start() {
        if (started || closed) return;
        started = true;
        readThread = new Thread(this::readLoop, "MessageStream-reader");
        readThread.setDaemon(true);
        readThread.start();
    }

    // ========== 事件 ==========

    public void addMessageListener(MessageListener l) { synchronized (messageListeners) { messageListeners.add(l); } }
    public void addErrorListener(ErrorListener l) { synchronized (errorListeners) { errorListeners.add(l); } }
    public void addSyncCompleteListener(SyncCompleteListener l) { synchronized (syncListeners) { syncListeners.add(l); } }

    private void emitMessage(byte[] data) {
        List<MessageListener> copy;
        synchronized (messageListeners) { copy = new ArrayList<>(messageListeners); }
        for (MessageListener l : copy) {
            try { l.onMessage(data); } catch (Exception e) { System.err.println("[MessageStream] listener error: " + e); }
        }
    }

    private void emitError(Exception err) {
        List<ErrorListener> copy;
        synchronized (errorListeners) { copy = new ArrayList<>(errorListeners); }
        for (ErrorListener l : copy) {
            try { l.onError(err); } catch (Exception e) { System.err.println("[MessageStream] listener error: " + e); }
        }
    }

    private void emitSyncComplete() {
        List<SyncCompleteListener> copy;
        synchronized (syncListeners) { copy = new ArrayList<>(syncListeners); }
        for (SyncCompleteListener l : copy) {
            try { l.onSyncComplete(); } catch (Exception e) { System.err.println("[MessageStream] listener error: " + e); }
        }
    }

    // ========== 发送 ==========

    /** 发送一条消息（带长度头与可选 CRC），并发调用按序串行化。 */
    public void send(byte[] data) throws IOException {
        if (data.length > maxBufferSize)
            throw new IOException("Data size exceeds maximum allowed limit");
        synchronized (sendLock) {
            if (closed) throw new IOException("Stream is not connected");
            out.write(buildFrame(data));
            out.flush();
        }
    }

    private byte[] buildFrame(byte[] data) {
        byte[] lengthHeader = le32(data.length);
        if (!enableCRC) {
            byte[] frame = new byte[4 + data.length];
            System.arraycopy(lengthHeader, 0, frame, 0, 4);
            System.arraycopy(data, 0, frame, 4, data.length);
            return frame;
        }
        byte[] frame = new byte[8 + data.length + 4];
        System.arraycopy(lengthHeader, 0, frame, 0, 4);
        System.arraycopy(le32((int) crc32(lengthHeader)), 0, frame, 4, 4);
        System.arraycopy(data, 0, frame, 8, data.length);
        System.arraycopy(le32((int) crc32(data)), 0, frame, 8 + data.length, 4);
        return frame;
    }

    /** 发送错误包（按普通帧格式封装）。 */
    public void sendError(String errorType) {
        int code = ERROR_TYPES.getOrDefault(errorType, 0);
        byte[] errorData = new byte[] { (byte) 0xEE, (byte) 0xEE, (byte) 0xEE, (byte) 0xEE, (byte) code };
        try {
            send(errorData);
        } catch (IOException err) {
            System.err.println("Failed to send error packet: " + err.getMessage());
        }
    }

    /** 发送协议消息: [FFFFFFFF][长度LE][数据]，不带 CRC。 */
    public void sendProtocolMessage(String message) {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        byte[] frame = new byte[8 + data.length];
        System.arraycopy(le32(PROTOCOL_HEADER), 0, frame, 0, 4);
        System.arraycopy(le32(data.length), 0, frame, 4, 4);
        System.arraycopy(data, 0, frame, 8, data.length);
        try {
            synchronized (sendLock) {
                if (closed) throw new IOException("Stream is not connected");
                out.write(frame);
                out.flush();
            }
        } catch (IOException err) {
            System.err.println("Failed to send protocol message '" + message + "': " + err.getMessage());
        }
    }

    // ========== 接收 ==========

    private void readLoop() {
        byte[] chunk = new byte[65536];
        try {
            while (!closed) {
                int n = in.read(chunk);
                if (n < 0) break;
                synchronized (this) {
                    onData(Arrays.copyOf(chunk, n));
                }
            }
        } catch (IOException err) {
            if (!closed) emitError(err);
        }
        if (!closed) emitError(new IOException("Connection closed"));
    }

    private void onData(byte[] data) {
        buffer = concat(buffer, data);

        if (buffer.length > maxBufferSize) {
            emitError(new IOException("Buffer size: " + buffer.length + " exceeds maximum allowed limit"));
            sendError("BUFFER_OVERFLOW");
            reset();
            return;
        }

        while (buffer.length > 0) {
            // 协议模式：只接受协议消息
            if (protocolMode && buffer.length >= 8) {
                if (readLe32(buffer, 0) == PROTOCOL_HEADER) {
                    long length = readLe32(buffer, 4) & 0xFFFFFFFFL;
                    long total = 8 + length;
                    if (buffer.length >= total) {
                        byte[] messageData = Arrays.copyOfRange(buffer, 8, (int) total);
                        buffer = Arrays.copyOfRange(buffer, (int) total, buffer.length);
                        if (handleProtocolMessage(messageData)) continue;
                    } else {
                        break; // 等待更多数据
                    }
                } else {
                    // 协议模式下的残留数据，丢弃
                    System.out.println("[MessageStream] Dropping non-protocol data in protocol mode");
                    buffer = new byte[0];
                    break;
                }
            }

            if (protocolMode) break; // 等待协议同步完成

            // 正常模式
            if (expectedLength == null) {
                int headerSize = enableCRC ? 8 : 4;
                if (buffer.length < headerSize) break;

                int length = (int) readLe32(buffer, 0);
                if (enableCRC) {
                    long expectedCRC = readLe32(buffer, 4) & 0xFFFFFFFFL;
                    long calculated = crc32(Arrays.copyOf(buffer, 4));
                    if (calculated != expectedCRC) {
                        System.err.println("Length header CRC check failed: expected " + expectedCRC + ", got " + calculated);
                        sendError("LENGTH_CRC_FAILED");
                        emitError(new IOException("Length header CRC check failed"));
                        reset();
                        return;
                    }
                }

                expectedLength = length;
                buffer = Arrays.copyOfRange(buffer, headerSize, buffer.length);
            }

            int messageTotalSize = enableCRC ? expectedLength + 4 : expectedLength;
            if (buffer.length >= messageTotalSize) {
                byte[] messageData = Arrays.copyOf(buffer, expectedLength);
                if (enableCRC) {
                    long messageCRC = readLe32(buffer, expectedLength) & 0xFFFFFFFFL;
                    long calculated = crc32(messageData);
                    if (calculated != messageCRC) {
                        System.err.println("Message CRC check failed: expected " + messageCRC + ", got " + calculated);
                        sendError("MESSAGE_CRC_FAILED");
                        emitError(new IOException("Message CRC check failed"));
                        reset();
                        return;
                    }
                }

                buffer = Arrays.copyOfRange(buffer, messageTotalSize, buffer.length);
                expectedLength = null;

                // 错误包在交给上层之前拦截处理
                if (isErrorPacket(messageData)) {
                    String name = ERROR_TYPE_CODES.getOrDefault(messageData[4] & 0xFF, "UNKNOWN_ERROR");
                    System.out.println("Received error packet: " + name);
                    reset();
                    sendProtocolMessage("RESET_CONFIRM");
                    protocolMode = false;
                    continue;
                }

                emitMessage(messageData);
            } else {
                break;
            }
        }
    }

    private boolean handleProtocolMessage(byte[] messageData) {
        if (messageData.length > 50) return false;
        String text = new String(messageData, StandardCharsets.UTF_8).trim();
        if ("RESET_CONFIRM".equals(text)) {
            System.out.println("Received reset confirmation");
            protocolMode = false;
            buffer = new byte[0];
            expectedLength = null;
            emitSyncComplete();
            return true;
        }
        return false;
    }

    private static boolean isErrorPacket(byte[] data) {
        return data.length == 5 && readLe32(data, 0) == ERROR_HEADER;
    }

    /** 清空接收状态并进入协议模式，等待对端 RESET_CONFIRM。 */
    public synchronized void reset() {
        System.out.println("[MessageStream] Resetting stream state");
        buffer = new byte[0];
        expectedLength = null;
        protocolMode = true;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try { in.close(); } catch (IOException ignored) {}
        try { out.close(); } catch (IOException ignored) {}
        synchronized (this) {
            buffer = new byte[0];
            expectedLength = null;
        }
        synchronized (messageListeners) { messageListeners.clear(); }
        synchronized (errorListeners) { errorListeners.clear(); }
        synchronized (syncListeners) { syncListeners.clear(); }
        System.out.println("[MessageStream] Destroyed");
    }

    // ========== 工具 ==========

    public static long crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    static byte[] le32(long v) {
        return new byte[] { (byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24) };
    }

    static long readLe32(byte[] b, int off) {
        return (b[off] & 0xFFL) | ((b[off + 1] & 0xFFL) << 8)
                | ((b[off + 2] & 0xFFL) << 16) | ((b[off + 3] & 0xFFL) << 24);
    }

    static byte[] concat(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}