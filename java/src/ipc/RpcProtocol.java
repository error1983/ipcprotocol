package ipc;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * RpcProtocol.js 的 Java 移植版（线格式完全兼容，Java 11+，零依赖）。
 *
 * 消息格式: [type u8][contentType u8][UUID 16B][payload...]
 *   type:        0=REQUEST, 1=RESPONSE, 2=EVENT
 *   contentType: 0=TEXT(utf8), 1=JSON(utf8), 2=BINARY(原始字节)
 * JSON 请求体: {"method":...,"params":...,"to":...(可选)}；错误响应体: {"error":...}
 * 二进制请求体: [methodLen u32BE][method utf8][paramsLen u32BE][params JSON utf8][binary...]
 *   服务端 handler 收到的参数为 [binary, ...params]
 *
 * JSON 值映射: Map&lt;String,Object&gt; / List&lt;Object&gt; / String / Long / Double / Boolean / null。
 */
public class RpcProtocol implements Closeable {

    public static final int TYPE_REQUEST = 0, TYPE_RESPONSE = 1, TYPE_EVENT = 2;
    public static final int CONTENT_TEXT = 0, CONTENT_JSON = 1, CONTENT_BINARY = 2;

    /** 方法处理器，返回值映射: null->JSON null, String->TEXT, byte[]->BINARY, 其他->JSON。 */
    public interface Handler { Object handle(Object[] args) throws Exception; }

    public interface EventListener { void onEvent(int contentType, Object content); }
    public interface ErrorListener { void onError(Exception err); }

    /** 对端返回的错误响应，getError() 携带原始 error 值。 */
    public static class RpcException extends RuntimeException {
        private final Object error;
        public RpcException(Object error) {
            super(String.valueOf(error));
            this.error = error;
        }
        public Object getError() { return error; }
    }

    private MessageStream messageStream;
    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();
    private final List<EventListener> eventListeners = new ArrayList<>();
    private final List<ErrorListener> errorListeners = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "RpcProtocol-worker");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean closed = false;

    public RpcProtocol(MessageStream messageStream) {
        if (messageStream == null) throw new IllegalArgumentException("messageStream is undefined");
        this.messageStream = messageStream;
        messageStream.addMessageListener(this::handleMessage);
        messageStream.addErrorListener(err -> {
            handleStreamError(err);
            emitError(err);
        });
        // 先挂好监听器再启动读循环，避免早到消息在注册前被分发的竞态
        messageStream.start();
    }

    // ========== 事件 ==========

    public void onEvent(EventListener listener) { synchronized (eventListeners) { eventListeners.add(listener); } }
    public void addErrorListener(ErrorListener listener) { synchronized (errorListeners) { errorListeners.add(listener); } }

    private void emitEvent(int contentType, Object content) {
        List<EventListener> copy;
        synchronized (eventListeners) { copy = new ArrayList<>(eventListeners); }
        for (EventListener l : copy) {
            try { l.onEvent(contentType, content); } catch (Exception e) { System.err.println("[RpcProtocol] listener error: " + e); }
        }
    }

    private void emitError(Exception err) {
        List<ErrorListener> copy;
        synchronized (errorListeners) { copy = new ArrayList<>(errorListeners); }
        for (ErrorListener l : copy) {
            try { l.onError(err); } catch (Exception e) { System.err.println("[RpcProtocol] listener error: " + e); }
        }
    }

    private void handleStreamError(Exception error) {
        String message = error.getMessage() != null ? error.getMessage() : "Connection error";
        for (String id : new ArrayList<>(pendingRequests.keySet())) {
            CompletableFuture<Object> pending = pendingRequests.remove(id);
            if (pending != null) pending.completeExceptionally(new RpcException("Request failed: " + message));
        }
    }

    // ========== 序列化 ==========

    public static byte[] serialize(int type, int contentType, String id, Object data) throws IOException {
        byte[] header = new byte[18];
        header[0] = (byte) type;
        header[1] = (byte) contentType;
        UUID uuid = UUID.fromString(id);
        ByteBuffer.wrap(header, 2, 16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());

        byte[] payload;
        if (contentType == CONTENT_TEXT) {
            payload = ((String) data).getBytes(StandardCharsets.UTF_8);
        } else if (contentType == CONTENT_JSON) {
            payload = Json.stringify(data).getBytes(StandardCharsets.UTF_8);
        } else if (contentType == CONTENT_BINARY) {
            payload = (byte[]) data;
        } else {
            throw new IOException("Unsupported content type: " + contentType);
        }
        return MessageStream.concat(header, payload);
    }

    public static class Message {
        public final int type, contentType;
        public final String id;
        public final Object content;
        Message(int type, int contentType, String id, Object content) {
            this.type = type; this.contentType = contentType; this.id = id; this.content = content;
        }
    }

    public static Message deserialize(byte[] buffer) throws IOException {
        if (buffer.length < 18) throw new IOException("Failed to deserialize message: buffer too short");
        int type = buffer[0] & 0xFF;
        int contentType = buffer[1] & 0xFF;
        ByteBuffer bb = ByteBuffer.wrap(buffer, 2, 16);
        String id = new UUID(bb.getLong(), bb.getLong()).toString();
        byte[] payload = Arrays.copyOfRange(buffer, 18, buffer.length);
        Object content;
        if (contentType == CONTENT_TEXT) {
            content = new String(payload, StandardCharsets.UTF_8);
        } else if (contentType == CONTENT_JSON) {
            content = Json.parse(new String(payload, StandardCharsets.UTF_8));
        } else if (contentType == CONTENT_BINARY) {
            content = payload;
        } else {
            throw new IOException("Failed to deserialize message: unknown content type " + contentType);
        }
        return new Message(type, contentType, id, content);
    }

    public static byte[] encodeBinaryRequest(String method, List<Object> params, byte[] binaryData) {
        byte[] methodBuf = method.getBytes(StandardCharsets.UTF_8);
        byte[] paramsBuf = Json.stringify(params).getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[4 + methodBuf.length + 4 + paramsBuf.length + binaryData.length];
        int offset = 0;
        System.arraycopy(be32(methodBuf.length), 0, result, offset, 4); offset += 4;
        System.arraycopy(methodBuf, 0, result, offset, methodBuf.length); offset += methodBuf.length;
        System.arraycopy(be32(paramsBuf.length), 0, result, offset, 4); offset += 4;
        System.arraycopy(paramsBuf, 0, result, offset, paramsBuf.length); offset += paramsBuf.length;
        System.arraycopy(binaryData, 0, result, offset, binaryData.length);
        return result;
    }

    public static class BinaryRequest {
        public final String method;
        public final List<Object> params;
        public final byte[] binaryData;
        BinaryRequest(String method, List<Object> params, byte[] binaryData) {
            this.method = method; this.params = params; this.binaryData = binaryData;
        }
    }

    @SuppressWarnings("unchecked")
    public static BinaryRequest decodeBinaryRequest(byte[] buffer) {
        int offset = 0;
        if (buffer.length < 4) throw new IllegalArgumentException("binary request too short");
        long methodLength = readBe32(buffer, offset); offset += 4;
        if (buffer.length < offset + methodLength + 4) throw new IllegalArgumentException("binary request truncated (method)");
        String method = new String(buffer, offset, (int) methodLength, StandardCharsets.UTF_8); offset += (int) methodLength;
        long paramsLength = readBe32(buffer, offset); offset += 4;
        if (buffer.length < offset + paramsLength) throw new IllegalArgumentException("binary request truncated (params)");
        Object parsed = Json.parse(new String(buffer, offset, (int) paramsLength, StandardCharsets.UTF_8)); offset += (int) paramsLength;
        List<Object> params = parsed instanceof List ? (List<Object>) parsed : new ArrayList<>(Arrays.asList(parsed));
        byte[] binaryData = Arrays.copyOfRange(buffer, offset, buffer.length);
        return new BinaryRequest(method, params, binaryData);
    }

    // ========== 接收处理 ==========

    private void handleMessage(byte[] buffer) {
        try {
            Message msg = deserialize(buffer);
            if (msg.type < 0 || msg.type > 2) throw new IOException("Invalid message type: " + msg.type);
            if (msg.contentType < 0 || msg.contentType > 2) throw new IOException("Invalid content type: " + msg.contentType);

            if (msg.type == TYPE_REQUEST) {
                executor.submit(() -> handleRequest(msg.id, msg.content));
            } else if (msg.type == TYPE_RESPONSE) {
                handleResponse(msg.id, msg.content);
            } else {
                emitEvent(msg.contentType, msg.content);
            }
        } catch (Exception err) {
            System.err.println("Failed to handle message: " + err.getMessage());
            // 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
            if (buffer.length >= 18 && (buffer[0] & 0xFF) == TYPE_REQUEST) {
                try {
                    ByteBuffer bb = ByteBuffer.wrap(buffer, 2, 16);
                    String id = new UUID(bb.getLong(), bb.getLong()).toString();
                    Map<String, Object> error = new LinkedHashMap<>();
                    error.put("error", "Invalid request payload");
                    sendResponse(id, error, CONTENT_JSON);
                } catch (Exception ignored) { }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleRequest(String id, Object request) {
        String method;
        Object[] args;

        try {
            if (request instanceof byte[]) {
                BinaryRequest decoded = decodeBinaryRequest((byte[]) request);
                method = decoded.method;
                List<Object> list = new ArrayList<>();
                list.add(decoded.binaryData);
                list.addAll(decoded.params);
                args = list.toArray();
            } else if (request instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) request;
                Object m = map.get("method");
                method = m instanceof String ? (String) m : null;
                Object params = map.get("params");
                if (params instanceof List) {
                    args = ((List<Object>) params).toArray();
                } else if (params == null) {
                    args = new Object[0];
                } else {
                    args = new Object[] { params };
                }
            } else {
                throw new IllegalArgumentException("request payload is not an object");
            }
            if (method == null || method.isEmpty()) {
                throw new IllegalArgumentException("request method missing");
            }
        } catch (Exception err) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", "Invalid request payload: " + err.getMessage());
            sendResponse(id, error, CONTENT_JSON);
            return;
        }

        Handler handler = handlers.get(method);
        if (handler == null) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", "Method not found: " + method);
            sendResponse(id, error, CONTENT_JSON);
            return;
        }

        try {
            Object result = handler.handle(args);
            int contentType = CONTENT_JSON;
            if (result instanceof String) contentType = CONTENT_TEXT;
            else if (result instanceof byte[]) contentType = CONTENT_BINARY;
            sendResponse(id, result, contentType);
        } catch (Exception err) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", err.getMessage() != null ? err.getMessage() : err.toString());
            sendResponse(id, error, CONTENT_JSON);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleResponse(String id, Object response) {
        CompletableFuture<Object> pending = pendingRequests.remove(id);
        if (pending == null) return;
        if (response instanceof Map && isTruthy(((Map<String, Object>) response).get("error"))) {
            pending.completeExceptionally(new RpcException(((Map<String, Object>) response).get("error")));
        } else {
            pending.complete(response);
        }
    }

    // JS 真值语义：null/false/0/"" 为假
    private static boolean isTruthy(Object v) {
        if (v == null || Boolean.FALSE.equals(v)) return false;
        if (v instanceof String) return !((String) v).isEmpty();
        if (v instanceof Number) return ((Number) v).doubleValue() != 0;
        return true;
    }

    // ========== 发送 ==========

    public void sendRaw(int type, int contentType, String id, Object data) throws IOException {
        if (messageStream == null) throw new IOException("messageStream is not initialized");
        messageStream.send(serialize(type, contentType, id, data));
    }

    /** 发送 JSON 请求并等待响应。params 为 List 时展开为多参数。 */
    public CompletableFuture<Object> sendRequest(String method, Object params) {
        return sendRequest(method, params, null);
    }

    public CompletableFuture<Object> sendRequest(String method, Object params, String to) {
        String id = UUID.randomUUID().toString();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put(id, future);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("method", method);
        body.put("params", params);
        if (to != null) body.put("to", to);
        try {
            sendRaw(TYPE_REQUEST, CONTENT_JSON, id, body);
        } catch (IOException err) {
            pendingRequests.remove(id);
            future.completeExceptionally(err);
        }
        return future;
    }

    /** 发送二进制请求并等待响应。服务端 handler 收到 [data, ...args]。 */
    public CompletableFuture<Object> sendRequestBinary(String method, byte[] data, Object... args) {
        String id = UUID.randomUUID().toString();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put(id, future);
        byte[] binaryRequest = encodeBinaryRequest(method, Arrays.asList(args), data);
        try {
            sendRaw(TYPE_REQUEST, CONTENT_BINARY, id, binaryRequest);
        } catch (IOException err) {
            pendingRequests.remove(id);
            future.completeExceptionally(err);
        }
        return future;
    }

    public void sendResponse(String id, Object response, int contentType) {
        try {
            sendRaw(TYPE_RESPONSE, contentType, id, response);
        } catch (IOException err) {
            System.err.println("Failed to send response: " + err.getMessage());
        }
    }

    // ========== 辅助方法 ==========

    public void sendText(String text) {
        try { sendRaw(TYPE_EVENT, CONTENT_TEXT, UUID.randomUUID().toString(), text); }
        catch (IOException err) { System.err.println("Failed to send text event: " + err.getMessage()); }
    }

    public void sendBinary(byte[] data) {
        try { sendRaw(TYPE_EVENT, CONTENT_BINARY, UUID.randomUUID().toString(), data); }
        catch (IOException err) { System.err.println("Failed to send binary event: " + err.getMessage()); }
    }

    public void sendJson(Object data) {
        try { sendRaw(TYPE_EVENT, CONTENT_JSON, UUID.randomUUID().toString(), data); }
        catch (IOException err) { System.err.println("Failed to send json event: " + err.getMessage()); }
    }

    public void registerMethod(String name, Handler handler) {
        handlers.put(name, handler);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        for (String id : new ArrayList<>(pendingRequests.keySet())) {
            CompletableFuture<Object> pending = pendingRequests.remove(id);
            if (pending != null) pending.completeExceptionally(new RpcException("RPC Protocol destroyed"));
        }
        if (messageStream != null) {
            messageStream.close();
            messageStream = null;
        }
        handlers.clear();
        synchronized (eventListeners) { eventListeners.clear(); }
        synchronized (errorListeners) { errorListeners.clear(); }
        executor.shutdownNow();
        System.out.println("[RpcProtocol] Destroyed");
    }

    // ========== 工具 ==========

    static byte[] be32(long v) {
        return new byte[] { (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v };
    }

    static long readBe32(byte[] b, int off) {
        return ((b[off] & 0xFFL) << 24) | ((b[off + 1] & 0xFFL) << 16)
                | ((b[off + 2] & 0xFFL) << 8) | (b[off + 3] & 0xFFL);
    }
}