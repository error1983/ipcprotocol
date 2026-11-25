/* C 回环测试：
 * gcc -std=c11 -O2 -I. loopback_test.c json.c message_stream.c rpc.c tcp.c -o out/loopback_test.exe -lws2_32 -lpthread
 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <windows.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "json.h"
#include "message_stream.h"
#include "rpc.h"
#include "tcp.h"

static int passed = 0, failed = 0;

static void check(const char* name, int ok) {
    if (ok) { passed++; printf("PASS %s\n", name); }
    else { failed++; printf("FAIL %s\n", name); }
}

/* ---- 测试服务端 ---- */

typedef struct {
    TcpListener* ln;
    RpcProtocol* rpc;
    TcpConn* conn;
    volatile int ready;
    volatile int synced;
} TestServer;

static JsonValue* h_add(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n; (void)err;
    return json_num(a[0]->u.number + a[1]->u.number);
}
static JsonValue* h_echo(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n; (void)err;
    /* 深拷贝：dump+parse */
    char* s = json_dump(a[0]);
    JsonValue* v = json_parse(s);
    free(s);
    return v;
}
static JsonValue* h_greet(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n; (void)err;
    char buf[256];
    snprintf(buf, sizeof(buf), "你好, %s!", a[0]->u.str);
    return json_str(buf);
}
static JsonValue* h_blob(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)a; (void)n; (void)err;
    unsigned char b[] = {0, 1, 2, 250, 255};
    return json_bytes(b, sizeof(b));
}
static JsonValue* h_nothing(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)a; (void)n; (void)err;
    return NULL;
}
static JsonValue* h_fail(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n;
    char buf[256];
    snprintf(buf, sizeof(buf), "boom: %s", a[0]->u.str);
    *err = _strdup(buf);
    return NULL;
}
static JsonValue* h_upload(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n; (void)err;
    JsonValue* head = json_arr();
    for (size_t i = 0; i < 4 && i < a[0]->u.bytes.len; i++)
        json_arr_push(head, json_num(a[0]->u.bytes.data[i]));
    JsonValue* r = json_obj();
    json_obj_set(r, "size", json_num((double)a[0]->u.bytes.len));
    json_obj_set(r, "tag", h_echo(NULL, a + 1, 1, NULL));
    json_obj_set(r, "head", head);
    return r;
}
static JsonValue* h_push(void* u, JsonValue** a, size_t n, char** err) {
    (void)a; (void)n; (void)err;
    RpcProtocol* rpc = (RpcProtocol*)u;
    rpc_send_text(rpc, "text-event");
    JsonValue* ev = json_obj();
    json_obj_set(ev, "n", json_num(42));
    rpc_send_json(rpc, ev);
    json_free(ev);
    unsigned char b[] = {9, 8, 7};
    rpc_send_binary(rpc, b, sizeof(b));
    return json_str("sent");
}
static JsonValue* h_slow(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)a; (void)n; (void)err;
    Sleep(500);
    return json_str("late");
}

static void silent_error(void* u, const char* msg) { (void)u; (void)msg; }

static void on_sync(void* u) {
    TestServer* s = (TestServer*)u;
    s->synced = 1;
}

static void* server_thread(void* arg) {
    TestServer* s = (TestServer*)arg;
    s->conn = tcp_accept(s->ln);
    MessageStream* ms = ms_create(tcp_io(s->conn), 1, 0);
    ms_on_sync_complete(ms, on_sync, s);
    s->rpc = rpc_create(ms);
    rpc_on_error(s->rpc, silent_error, NULL);
    rpc_register(s->rpc, "add", h_add, NULL);
    rpc_register(s->rpc, "echo", h_echo, NULL);
    rpc_register(s->rpc, "greet", h_greet, NULL);
    rpc_register(s->rpc, "blob", h_blob, NULL);
    rpc_register(s->rpc, "nothing", h_nothing, NULL);
    rpc_register(s->rpc, "fail", h_fail, NULL);
    rpc_register(s->rpc, "upload", h_upload, NULL);
    rpc_register(s->rpc, "pushEvents", h_push, s->rpc);
    rpc_register(s->rpc, "slow", h_slow, NULL);
    s->ready = 1;
    return NULL;
}

static TestServer* start_server(void) {
    TestServer* s = (TestServer*)calloc(1, sizeof(TestServer));
    s->ln = tcp_listen("127.0.0.1", 0);
    pthread_t t;
    pthread_create(&t, NULL, server_thread, s);
    pthread_detach(t);
    return s;
}

static void wait_flag(volatile int* flag) {
    for (int i = 0; i < 300 && !*flag; i++) Sleep(10);
}

static RpcProtocol* dial(TestServer* s, TcpConn** connOut) {
    TcpConn* conn = tcp_connect("127.0.0.1", tcp_listener_port(s->ln));
    wait_flag(&s->ready);
    *connOut = conn;
    RpcProtocol* rpc = rpc_create(ms_create(tcp_io(conn), 1, 0));
    rpc_on_error(rpc, silent_error, NULL);
    return rpc;
}

static void stop_server(TestServer* s) {
    if (s->rpc) rpc_destroy(s->rpc);
    tcp_conn_free(s->conn);
    tcp_listener_free(s->ln);
    free(s);
}

/* ---- 事件收集 ---- */

typedef struct {
    JsonValue* items[8];
    int count;
} EventSink;

static void collect_event(void* u, int ct, JsonValue* content) {
    (void)ct;
    EventSink* sink = (EventSink*)u;
    if (sink->count < 8) {
        if (content->type == JSON_BYTES) {
            /* 字节载荷无法经 JSON 往返，直接拷贝 */
            sink->items[sink->count++] = json_bytes(content->u.bytes.data, content->u.bytes.len);
        } else {
            char* s = json_dump(content);
            sink->items[sink->count++] = json_parse(s);
            free(s);
        }
    }
}

typedef struct { RpcProtocol* rpc; int i; int ok; } ConcArg;

static void* conc_fn(void* a) {
    ConcArg* ca = (ConcArg*)a;
    JsonValue* p = json_arr();
    json_arr_push(p, json_num(ca->i));
    json_arr_push(p, json_num(1));
    RpcResult r = rpc_send_request(ca->rpc, "add", p, NULL);
    ca->ok = r.value && r.value->u.number == ca->i + 1;
    rpc_result_free(&r);
    json_free(p);
    return NULL;
}

int main(void) {
    /* 1. JSON 请求 + Unicode */
    {
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);
        JsonValue* params = json_arr();
        json_arr_push(params, json_num(3));
        json_arr_push(params, json_num(4));
        RpcResult r = rpc_send_request(rpc, "add", params, NULL);
        check("add", r.value && r.value->u.number == 7);
        rpc_result_free(&r);
        json_free(params);

        JsonValue* x = json_arr();
        json_arr_push(x, json_num(1));
        json_arr_push(x, json_str("二"));
        json_arr_push(x, json_bool(1));
        json_arr_push(x, json_null());
        JsonValue* obj = json_obj();
        json_obj_set(obj, "x", x);
        r = rpc_send_request(rpc, "echo", obj, NULL);
        JsonValue* rx = json_get(r.value, "x");
        check("echo unicode", rx && rx->u.arr.items[1]->type == JSON_STR &&
                              strcmp(rx->u.arr.items[1]->u.str, "二") == 0);
        rpc_result_free(&r);
        json_free(obj);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }
    /* 2. TEXT / BINARY / null */
    {
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);
        JsonValue* name = json_str("世界");
        RpcResult r = rpc_send_request(rpc, "greet", name, NULL);
        check("greet", r.value && strcmp(r.value->u.str, "你好, 世界!") == 0);
        rpc_result_free(&r);
        json_free(name);

        r = rpc_send_request(rpc, "blob", NULL, NULL);
        unsigned char expect[] = {0, 1, 2, 250, 255};
        check("blob", r.value && r.value->type == JSON_BYTES && r.value->u.bytes.len == 5 &&
                      memcmp(r.value->u.bytes.data, expect, 5) == 0);
        rpc_result_free(&r);

        r = rpc_send_request(rpc, "nothing", NULL, NULL);
        check("nothing", r.value && r.value->type == JSON_NULL);
        rpc_result_free(&r);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }
    /* 3. 错误响应 / 方法不存在 */
    {
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);
        JsonValue* x = json_str("x");
        RpcResult r = rpc_send_request(rpc, "fail", x, NULL);
        check("error response", r.remote && r.error && strcmp(r.error->u.str, "boom: x") == 0);
        rpc_result_free(&r);
        json_free(x);

        r = rpc_send_request(rpc, "nope", NULL, NULL);
        check("method not found", r.remote && r.error &&
                                  strcmp(r.error->u.str, "Method not found: nope") == 0);
        rpc_result_free(&r);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }
    /* 4. 75KB 二进制请求 */
    {
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);
        size_t n = 75 * 1024;
        unsigned char* data = (unsigned char*)malloc(n);
        for (size_t i = 0; i < n; i++) data[i] = (unsigned char)(i % 251);
        JsonValue* args = json_arr();
        json_arr_push(args, json_str("tag-1"));
        RpcResult r = rpc_send_request_binary(rpc, "upload", data, n, args);
        JsonValue* size = json_get(r.value, "size");
        JsonValue* tag = json_get(r.value, "tag");
        JsonValue* head = json_get(r.value, "head");
        check("upload", size && size->u.number == (double)n && tag && strcmp(tag->u.str, "tag-1") == 0 &&
                      head && head->u.arr.len == 4 && head->u.arr.items[3]->u.number == 3);
        rpc_result_free(&r);
        json_free(args);
        free(data);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }
    /* 5. 事件 */
    {
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);
        EventSink sink = {0};
        rpc_on_event(rpc, collect_event, &sink);
        RpcResult r = rpc_send_request(rpc, "pushEvents", NULL, NULL);
        rpc_result_free(&r);
        for (int i = 0; i < 300 && sink.count < 3; i++) Sleep(10);
        unsigned char expect[] = {9, 8, 7};
        check("events", sink.count == 3 &&
                      strcmp(sink.items[0]->u.str, "text-event") == 0 &&
                      json_get(sink.items[1], "n")->u.number == 42 &&
                      sink.items[2]->type == JSON_BYTES &&
                      memcmp(sink.items[2]->u.bytes.data, expect, 3) == 0);
        for (int i = 0; i < sink.count; i++) json_free(sink.items[i]);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }
    /* 6. 50 并发 */
    {
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);
        ConcArg cargs[50];
        pthread_t threads[50];
        for (int i = 0; i < 50; i++) {
            cargs[i] = (ConcArg){rpc, i, 0};
            pthread_create(&threads[i], NULL, conc_fn, &cargs[i]);
        }
        int allOk = 1;
        for (int i = 0; i < 50; i++) {
            pthread_join(threads[i], NULL);
            if (!cargs[i].ok) allOk = 0;
        }
        check("concurrent 50", allOk);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }
    /* 7. CRC 损坏注入 → 重同步 → 恢复 */
    {
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);

        unsigned char frame[17];
        const char* payload = "hello";
        frame[0] = 5; frame[1] = 0; frame[2] = 0; frame[3] = 0;
        frame[4] = 0xEF; frame[5] = 0xBE; frame[6] = 0xAD; frame[7] = 0xDE; /* 0xDEADBEEF LE */
        memcpy(frame + 8, payload, 5);
        unsigned crc = ipc_crc32((const unsigned char*)payload, 5);
        frame[13] = (unsigned char)crc;
        frame[14] = (unsigned char)(crc >> 8);
        frame[15] = (unsigned char)(crc >> 16);
        frame[16] = (unsigned char)(crc >> 24);
        IpcIo io = tcp_io(conn);
        io.write(io.ctx, frame, sizeof(frame));

        wait_flag(&s->synced);
        check("syncComplete", s->synced);
        JsonValue* p = json_arr();
        json_arr_push(p, json_num(10));
        json_arr_push(p, json_num(20));
        RpcResult r = rpc_send_request(rpc, "add", p, NULL);
        check("after resync", r.value && r.value->u.number == 30);
        rpc_result_free(&r);
        json_free(p);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }
    /* 8. RPC 超时：pthread_cond_timedwait 触发 ETIMEDOUT，pending 清理后迟到响应不影响后续调用 */
    {
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);
        RpcResult r = rpc_send_request_timeout(rpc, "slow", NULL, NULL, 100);
        check("rpc timeout ETIMEDOUT", !r.value && r.error && r.code &&
                                      strcmp(r.code, "ETIMEDOUT") == 0 && strstr(r.error->u.str, "slow"));
        rpc_result_free(&r);
        JsonValue* p = json_arr();
        json_arr_push(p, json_num(1));
        json_arr_push(p, json_num(2));
        r = rpc_send_request(rpc, "add", p, NULL);
        check("after timeout", r.value && r.value->u.number == 3);
        rpc_result_free(&r);
        json_free(p);
        Sleep(600); /* 等 slow 的迟到响应到达（应被静默丢弃） */
        p = json_arr();
        json_arr_push(p, json_num(5));
        json_arr_push(p, json_num(6));
        r = rpc_send_request(rpc, "add", p, NULL);
        check("late response ignored", r.value && r.value->u.number == 11);
        rpc_result_free(&r);
        json_free(p);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }
    /* 9. 深嵌套 JSON：解析器 64 层上限，超限走 Invalid request payload 路径不崩溃 */
    {
        /* 单元：200 层嵌套 parse 应返回 NULL；60 层正常 */
        char deep[401];
        memset(deep, '[', 200);
        memset(deep + 200, ']', 200);
        deep[400] = '\0';
        check("parse depth limit", json_parse(deep) == NULL);
        char ok60[121];
        memset(ok60, '[', 60);
        memset(ok60 + 60, ']', 60);
        ok60[120] = '\0';
        JsonValue* v60 = json_parse(ok60);
        check("parse within limit", v60 != NULL);
        json_free(v60);

        /* 端到端：客户端 dump 200 层参数发出，服务端解析失败回错误响应，后续通信正常 */
        TestServer* s = start_server();
        TcpConn* conn;
        RpcProtocol* rpc = dial(s, &conn);
        JsonValue* nested = json_num(1);
        for (int i = 0; i < 200; i++) {
            JsonValue* w = json_arr();
            json_arr_push(w, nested);
            nested = w;
        }
        RpcResult r = rpc_send_request(rpc, "echo", nested, NULL);
        check("deep frame error response", r.remote && r.error &&
                                           strstr(r.error->u.str, "Invalid request payload"));
        rpc_result_free(&r);
        json_free(nested);
        JsonValue* p = json_arr();
        json_arr_push(p, json_num(7));
        json_arr_push(p, json_num(8));
        r = rpc_send_request(rpc, "add", p, NULL);
        check("after deep frame", r.value && r.value->u.number == 15);
        rpc_result_free(&r);
        json_free(p);
        rpc_destroy(rpc);
        tcp_conn_free(conn);
        stop_server(s);
    }

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed == 0 ? 0 : 1;
}