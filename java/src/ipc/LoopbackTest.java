package ipc;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Java 版回环测试：TCP 服务端 + 客户端在同一进程内验证全部协议路径。 */
public class LoopbackTest {

    static int passed = 0;

    static void check(String name, boolean cond) {
        if (!cond) throw new RuntimeException("FAILED: " + name);
        passed++;
        System.out.println("  ok: " + name);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        int port = serverSocket.getLocalPort();

        final MessageStream[] serverStreamHolder = new MessageStream[1];
        CountDownLatch serverReady = new CountDownLatch(1);
        Thread serverThread = new Thread(() -> {
            try {
                Socket socket = serverSocket.accept();
                MessageStream stream = new MessageStream(socket.getInputStream(), socket.getOutputStream());
                serverStreamHolder[0] = stream;
                RpcProtocol rpc = new RpcProtocol(stream);
                rpc.registerMethod("add", a -> ((Number) a[0]).longValue() + ((Number) a[1]).longValue());
                rpc.registerMethod("echo", a -> a[0]);
                rpc.registerMethod("greet", a -> "hello " + a[0]);                       // String -> TEXT
                rpc.registerMethod("blob", a -> new byte[] { 0x00, 0x01, 0x02, (byte) 0xFF }); // byte[] -> BINARY
                rpc.registerMethod("nothing", a -> null);                                // null -> JSON null
                rpc.registerMethod("fail", a -> { throw new Exception("boom"); });
                rpc.registerMethod("upload", a -> {
                    byte[] data = (byte[]) a[0];
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("size", (long) data.length);
                    result.put("tag", a[1]);
                    List<Object> head = new ArrayList<>();
                    for (int i = 0; i < 4; i++) head.add((long) (data[i] & 0xFF));
                    result.put("head", head);
                    return result;
                });
                serverReady.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        Socket clientSocket = new Socket(InetAddress.getLoopbackAddress(), port);
        InputStream cin = clientSocket.getInputStream();
        OutputStream cout = clientSocket.getOutputStream();
        MessageStream clientStream = new MessageStream(cin, cout);
        RpcProtocol client = new RpcProtocol(clientStream);
        serverReady.await();

        List<Object[]> events = new ArrayList<>();
        client.onEvent((contentType, content) -> { synchronized (events) { events.add(new Object[] { contentType, content }); } });

        // 1. JSON 请求/响应
        check("json add", ((Number) client.sendRequest("add", Arrays.asList(3, 4)).get(3, TimeUnit.SECONDS)).longValue() == 7);
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("name", "测试");
        obj.put("list", Arrays.asList(1L, 2.5, true, null));
        Map<String, Object> echo = (Map<String, Object>) client.sendRequest("echo", Arrays.asList(obj)).get(3, TimeUnit.SECONDS);
        check("json echo unicode", "测试".equals(echo.get("name")) && ((Number) ((List<Object>) echo.get("list")).get(1)).doubleValue() == 2.5);

        // 2. TEXT 响应
        check("text response", "hello 世界".equals(client.sendRequest("greet", Arrays.asList("世界")).get(3, TimeUnit.SECONDS)));

        // 3. BINARY 响应
        check("binary response", Arrays.equals(new byte[] { 0, 1, 2, (byte) 0xFF },
                (byte[]) client.sendRequest("blob", null).get(3, TimeUnit.SECONDS)));

        // 4. null 响应
        check("null response", client.sendRequest("nothing", null).get(3, TimeUnit.SECONDS) == null);

        // 5. 错误响应
        try {
            client.sendRequest("fail", null).get(3, TimeUnit.SECONDS);
            check("error response", false);
        } catch (java.util.concurrent.ExecutionException e) {
            check("error response", e.getCause() instanceof RpcProtocol.RpcException
                    && "boom".equals(((RpcProtocol.RpcException) e.getCause()).getError()));
        }

        // 6. 方法不存在
        try {
            client.sendRequest("nope", null).get(3, TimeUnit.SECONDS);
            check("method not found", false);
        } catch (java.util.concurrent.ExecutionException e) {
            check("method not found", String.valueOf(((RpcProtocol.RpcException) e.getCause()).getError()).contains("Method not found: nope"));
        }

        // 7. 二进制请求（~75KB，跨多个 TCP 分段）
        byte[] payload = new byte[256 * 300];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
        Map<String, Object> up = (Map<String, Object>) client.sendRequestBinary("upload", payload, "tag-1").get(5, TimeUnit.SECONDS);
        check("binary request", ((Number) up.get("size")).longValue() == payload.length
                && "tag-1".equals(up.get("tag"))
                && ((Number) ((List<Object>) up.get("head")).get(3)).longValue() == 3);

        // 8. 事件（TEXT/JSON/BINARY）：服务端 -> 客户端
        MessageStream srv = serverStreamHolder[0];
        Map<String, Object> push = new LinkedHashMap<>();
        push.put("push", 1L);
        srv.send(RpcProtocol.serialize(RpcProtocol.TYPE_EVENT, RpcProtocol.CONTENT_JSON, UUID.randomUUID().toString(), push));
        srv.send(RpcProtocol.serialize(RpcProtocol.TYPE_EVENT, RpcProtocol.CONTENT_TEXT, UUID.randomUUID().toString(), "srv-text"));
        srv.send(RpcProtocol.serialize(RpcProtocol.TYPE_EVENT, RpcProtocol.CONTENT_BINARY, UUID.randomUUID().toString(), new byte[] { 1, 2 }));
        Thread.sleep(300);
        synchronized (events) {
            check("events received", events.size() == 3
                    && (int) events.get(0)[0] == RpcProtocol.CONTENT_JSON && ((Map<String, Object>) events.get(0)[1]).get("push").equals(1L)
                    && (int) events.get(1)[0] == RpcProtocol.CONTENT_TEXT && "srv-text".equals(events.get(1)[1])
                    && (int) events.get(2)[0] == RpcProtocol.CONTENT_BINARY && Arrays.equals(new byte[] { 1, 2 }, (byte[]) events.get(2)[1]));
        }

        // 9. 并发请求
        List<CompletableFuture<Object>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) futures.add(client.sendRequest("add", Arrays.asList(i, i)));
        boolean allOk = true;
        for (int i = 0; i < 50; i++)
            allOk &= ((Number) futures.get(i).get(5, TimeUnit.SECONDS)).longValue() == (long) i * 2;
        check("concurrent requests", allOk);

        // 10. CRC 错误恢复：客户端注入长度头 CRC 损坏的帧 -> 服务端检测错误并回错误包
        //     -> 客户端回 RESET_CONFIRM -> 服务端 syncComplete，之后通信应正常
        List<Exception> srvErrors = new ArrayList<>();
        srv.addErrorListener(srvErrors::add);
        CountDownLatch syncDone = new CountDownLatch(1);
        srv.addSyncCompleteListener(syncDone::countDown);

        byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] bad = MessageStream.concat(
                MessageStream.concat(MessageStream.le32(5), MessageStream.le32(0xDEADBEEFL)),
                MessageStream.concat(hello, MessageStream.le32(MessageStream.crc32(hello))));
        cout.write(bad);
        cout.flush();
        check("resync completed", syncDone.await(3, TimeUnit.SECONDS));
        check("crc error detected", !srvErrors.isEmpty());
        check("resync then rpc works", ((Number) client.sendRequest("add", Arrays.asList(10, 20)).get(3, TimeUnit.SECONDS)).longValue() == 30);

        // 11. destroy 后资源清理
        client.close();
        serverSocket.close();
        System.out.println("\nALL " + passed + " JAVA TESTS PASSED");
    }
}