#include "rpc.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static char* dup_cstr(const char* s) {
    size_t n = strlen(s) + 1;
    char* p = (char*)malloc(n);
    memcpy(p, s, n);
    return p;
}

#define TYPE_REQUEST  0
#define TYPE_RESPONSE 1
#define TYPE_EVENT    2

#define CONTENT_TEXT   0
#define CONTENT_JSON   1
#define CONTENT_BINARY 2

typedef struct Pending {
    char id[37];
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int done;
    RpcResult result;
    struct Pending* next;
} Pending;

typedef struct HandlerEntry {
    char* name;
    IpcHandler fn;
    void* user;
    struct HandlerEntry* next;
} HandlerEntry;

struct RpcProtocol {
    MessageStream* ms;
    pthread_mutex_t mu; /* 保护 handlers/pending/回调/active */
    HandlerEntry* handlers;
    Pending* pending;
    void (*event_cb)(void*, int, JsonValue*);
    void* event_user;
    void (*error_cb)(void*, const char*);
    void* error_user;
    int active; /* 进行中的请求处理线程数 */
    pthread_cond_t active_cv;
    int closed;
};

typedef struct {
    RpcProtocol* rpc;
    char id[37];
    int contentType;
    unsigned char* raw;
    size_t rawLen;
    JsonValue* content;
} RequestJob;

/* ---- UUID（RFC 4122 大端字节序；xorshift PRNG 保证唯一性）---- */

static pthread_mutex_t uuid_mu = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long uuid_state = 0;

static unsigned long long uuid_rand(void) {
    if (uuid_state == 0) {
        uuid_state = (unsigned long long)time(NULL) * 2654435761ull ^
                     (unsigned long long)(uintptr_t)&uuid_state;
        if (uuid_state == 0) uuid_state = 1;
    }
    uuid_state ^= uuid_state << 13;
    uuid_state ^= uuid_state >> 7;
    uuid_state ^= uuid_state << 17;
    return uuid_state;
}

static void new_uuid(char out[37]) {
    pthread_mutex_lock(&uuid_mu);
    unsigned char b[16];
    unsigned long long a = uuid_rand(), c = uuid_rand();
    memcpy(b, &a, 8);
    memcpy(b + 8, &c, 8);
    pthread_mutex_unlock(&uuid_mu);
    b[6] = (unsigned char)((b[6] & 0x0f) | 0x40);
    b[8] = (unsigned char)((b[8] & 0x3f) | 0x80);
    snprintf(out, 37, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}

static void uuid_to_bytes(const char* id, unsigned char out[16]) {
    char hex[33];
    int j = 0;
    for (int i = 0; id[i] && j < 32; i++) if (id[i] != '-') hex[j++] = id[i];
    hex[j] = '\0';
    for (int i = 0; i < 16; i++) {
        char pair[3] = {hex[i * 2], hex[i * 2 + 1], '\0'};
        out[i] = (unsigned char)strtoul(pair, NULL, 16);
    }
}

static void bytes_to_uuid(const unsigned char* b, char out[37]) {
    snprintf(out, 37, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}

/* ---- 消息构建/解析辅助 ---- */

static unsigned char* build_message(int msgType, int contentType, const char* id,
                                    const unsigned char* payload, size_t len, size_t* outLen) {
    unsigned char* msg = (unsigned char*)malloc(18 + len);
    msg[0] = (unsigned char)msgType;
    msg[1] = (unsigned char)contentType;
    uuid_to_bytes(id, msg + 2);
    memcpy(msg + 18, payload, len);
    *outLen = 18 + len;
    return msg;
}

static JsonValue* deserialize(int contentType, const unsigned char* payload, size_t len) {
    char* text = (char*)malloc(len + 1);
    memcpy(text, payload, len);
    text[len] = '\0';
    JsonValue* v = NULL;
    switch (contentType) {
        case CONTENT_TEXT: v = json_str(text); break;
        case CONTENT_JSON: v = json_parse(text); break;
        case CONTENT_BINARY: v = json_bytes(payload, len); break;
        default: break;
    }
    free(text);
    return v;
}

static void write_be32(unsigned char* p, unsigned v) {
    p[0] = (unsigned char)(v >> 24);
    p[1] = (unsigned char)(v >> 16);
    p[2] = (unsigned char)(v >> 8);
    p[3] = (unsigned char)v;
}

static unsigned read_be32(const unsigned char* p) {
    return ((unsigned)p[0] << 24) | ((unsigned)p[1] << 16) | ((unsigned)p[2] << 8) | (unsigned)p[3];
}

/* [methodLen u32BE][method][paramsLen u32BE][paramsJSON][binary] */
static unsigned char* encode_binary_request(const char* method, const unsigned char* data, size_t dataLen,
                                            JsonValue* argsArr, size_t* outLen) {
    char* paramsJson = argsArr ? json_dump(argsArr) : NULL;
    if (!paramsJson) { paramsJson = (char*)malloc(3); strcpy(paramsJson, "[]"); }
    size_t methodLen = strlen(method), paramsLen = strlen(paramsJson);
    size_t total = 4 + methodLen + 4 + paramsLen + dataLen;
    unsigned char* out = (unsigned char*)malloc(total);
    write_be32(out, (unsigned)methodLen);
    memcpy(out + 4, method, methodLen);
    write_be32(out + 4 + methodLen, (unsigned)paramsLen);
    memcpy(out + 8 + methodLen, paramsJson, paramsLen);
    memcpy(out + 8 + methodLen + paramsLen, data, dataLen);
    free(paramsJson);
    *outLen = total;
    return out;
}

/* ---- pending 管理 ---- */

static Pending* pending_take(RpcProtocol* rpc, const char* id) {
    pthread_mutex_lock(&rpc->mu);
    Pending** pp = &rpc->pending;
    while (*pp) {
        if (strcmp((*pp)->id, id) == 0) {
            Pending* p = *pp;
            *pp = p->next;
            pthread_mutex_unlock(&rpc->mu);
            return p;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&rpc->mu);
    return NULL;
}

static void pending_remove(RpcProtocol* rpc, const char* id) {
    Pending* p = pending_take(rpc, id);
    if (p) {
        pthread_mutex_destroy(&p->mu);
        pthread_cond_destroy(&p->cv);
        free(p);
    }
}

static void deliver(Pending* p, JsonValue* value, JsonValue* error, int remote) {
    pthread_mutex_lock(&p->mu);
    p->result.value = value;
    p->result.error = error;
    p->result.remote = remote;
    p->done = 1;
    pthread_cond_signal(&p->cv);
    pthread_mutex_unlock(&p->mu);
}

static void reject_all(RpcProtocol* rpc, const char* msg) {
    pthread_mutex_lock(&rpc->mu);
    Pending* p = rpc->pending;
    rpc->pending = NULL;
    pthread_mutex_unlock(&rpc->mu);
    while (p) {
        Pending* next = p->next;
        deliver(p, NULL, json_str(msg), 0);
        p = next;
    }
}

/* ---- 请求处理 ---- */

static void send_response(RpcProtocol* rpc, const char* id, JsonValue* result, char* errMsg) {
    int respType;
    unsigned char* body = NULL;
    size_t bodyLen = 0;
    if (errMsg) {
        JsonValue* e = json_obj();
        json_obj_set(e, "error", json_str(errMsg));
        char* s = json_dump(e);
        json_free(e);
        respType = CONTENT_JSON;
        bodyLen = strlen(s);
        body = (unsigned char*)s; /* 直接转移所有权 */
    } else if (!result || result->type == JSON_NULL) {
        respType = CONTENT_JSON;
        body = (unsigned char*)malloc(4);
        memcpy(body, "null", 4);
        bodyLen = 4;
    } else if (result->type == JSON_STR) {
        respType = CONTENT_TEXT;
        bodyLen = strlen(result->u.str);
        body = (unsigned char*)malloc(bodyLen ? bodyLen : 1);
        memcpy(body, result->u.str, bodyLen);
    } else if (result->type == JSON_BYTES) {
        respType = CONTENT_BINARY;
        bodyLen = result->u.bytes.len;
        body = (unsigned char*)malloc(bodyLen ? bodyLen : 1);
        memcpy(body, result->u.bytes.data, bodyLen);
    } else {
        char* s = json_dump(result);
        respType = CONTENT_JSON;
        bodyLen = strlen(s);
        body = (unsigned char*)s;
    }
    size_t msgLen;
    unsigned char* msg = build_message(TYPE_RESPONSE, respType, id, body, bodyLen, &msgLen);
    ms_send(rpc->ms, msg, msgLen);
    free(msg);
    free(body);
}

static void* request_thread(void* arg) {
    RequestJob* job = (RequestJob*)arg;
    RpcProtocol* rpc = job->rpc;

    char* method = NULL;
    JsonValue** args = NULL;
    size_t nargs = 0;
    JsonValue* ownedBytes = NULL;

    if (job->contentType == CONTENT_BINARY) {
        const unsigned char* raw = job->raw;
        size_t n = job->rawLen;
        if (n >= 4) {
            unsigned methodLen = read_be32(raw);
            if (n >= 4 + methodLen + 4) {
                method = (char*)malloc(methodLen + 1);
                memcpy(method, raw + 4, methodLen);
                method[methodLen] = '\0';
                size_t off = 4 + methodLen;
                unsigned paramsLen = read_be32(raw + off);
                if (n >= off + 4 + paramsLen) {
                    char* paramsText = (char*)malloc(paramsLen + 1);
                    memcpy(paramsText, raw + off + 4, paramsLen);
                    paramsText[paramsLen] = '\0';
                    JsonValue* params = json_parse(paramsText);
                    free(paramsText);
                    size_t extra = (params && params->type == JSON_ARR) ? params->u.arr.len : 0;
                    args = (JsonValue**)malloc((extra + 1) * sizeof(JsonValue*));
                    ownedBytes = json_bytes(raw + off + 4 + paramsLen, n - off - 4 - paramsLen);
                    args[0] = ownedBytes;
                    nargs = 1;
                    for (size_t i = 0; i < extra; i++) args[nargs++] = params->u.arr.items[i];
                    /* 元素所有权转移给 args，仅释放容器 */
                    params->u.arr.len = 0;
                    json_free(params);
                }
            }
        }
    } else if (job->content && job->content->type == JSON_OBJ) {
        JsonValue* m = json_get(job->content, "method");
        if (m && m->type == JSON_STR) {
            method = dup_cstr(m->u.str);
            JsonValue* params = json_get(job->content, "params");
            if (params && params->type == JSON_ARR) {
                nargs = params->u.arr.len;
                args = (JsonValue**)malloc((nargs ? nargs : 1) * sizeof(JsonValue*));
                for (size_t i = 0; i < nargs; i++) args[i] = params->u.arr.items[i];
            } else if (params && params->type != JSON_NULL) {
                args = (JsonValue**)malloc(sizeof(JsonValue*));
                args[0] = params;
                nargs = 1;
            }
        }
    }

    if (!method || !method[0]) {
        /* 畸形请求（非对象/无 method/二进制解码失败）：回错误响应而非静默丢弃 */
        send_response(rpc, job->id, NULL, "Invalid request payload");
    } else {
        IpcHandler fn = NULL;
        void* user = NULL;
        pthread_mutex_lock(&rpc->mu);
        for (HandlerEntry* e = rpc->handlers; e; e = e->next) {
            if (strcmp(e->name, method) == 0) { fn = e->fn; user = e->user; break; }
        }
        pthread_mutex_unlock(&rpc->mu);

        if (fn) {
            char* errMsg = NULL;
            JsonValue* result = fn(user, args, nargs, &errMsg);
            send_response(rpc, job->id, result, errMsg);
            json_free(result);
            free(errMsg);
        } else {
            char* msg = (char*)malloc(strlen(method) + 32);
            sprintf(msg, "Method not found: %s", method);
            send_response(rpc, job->id, NULL, msg);
            free(msg);
        }
    }

    free(method);
    /* 二进制路径的附加参数已从 params 容器接管所有权，需逐个释放 */
    if (job->contentType == CONTENT_BINARY) {
        for (size_t i = 1; i < nargs; i++) json_free(args[i]);
    }
    free(args);
    json_free(ownedBytes);
    json_free(job->content);
    free(job->raw);

    pthread_mutex_lock(&rpc->mu);
    rpc->active--;
    pthread_cond_broadcast(&rpc->active_cv);
    pthread_mutex_unlock(&rpc->mu);
    free(job);
    return NULL;
}

/* ---- 流回调 ---- */

static void on_stream_message(void* user, const unsigned char* data, size_t len) {
    RpcProtocol* rpc = (RpcProtocol*)user;
    if (len < 18) return;
    int msgType = data[0], contentType = data[1];
    char id[37];
    bytes_to_uuid(data + 2, id);
    JsonValue* content = deserialize(contentType, data + 18, len - 18);
    if (!content) {
        /* 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版） */
        if (msgType == TYPE_REQUEST) {
            send_response(rpc, id, NULL, "Invalid request payload");
        }
        return;
    }

    if (msgType == TYPE_REQUEST) {
        RequestJob* job = (RequestJob*)calloc(1, sizeof(RequestJob));
        job->rpc = rpc;
        strcpy(job->id, id);
        job->contentType = contentType;
        job->rawLen = len - 18;
        job->raw = (unsigned char*)malloc(job->rawLen ? job->rawLen : 1);
        memcpy(job->raw, data + 18, job->rawLen);
        job->content = content;
        pthread_mutex_lock(&rpc->mu);
        rpc->active++;
        pthread_mutex_unlock(&rpc->mu);
        pthread_t t;
        if (pthread_create(&t, NULL, request_thread, job) != 0) {
            pthread_mutex_lock(&rpc->mu);
            rpc->active--;
            pthread_mutex_unlock(&rpc->mu);
            json_free(content);
            free(job->raw);
            free(job);
            return;
        }
        pthread_detach(t);
    } else if (msgType == TYPE_RESPONSE) {
        Pending* p = pending_take(rpc, id);
        if (p) {
            JsonValue* ev = content->type == JSON_OBJ ? json_get(content, "error") : NULL;
            if (ev && json_truthy(ev)) {
                /* 复制原始 error 值交付（content 随后释放） */
                char* s = json_dump(ev);
                JsonValue* copy = json_parse(s);
                free(s);
                deliver(p, NULL, copy ? copy : json_null(), 1);
            } else {
                deliver(p, content, NULL, 0);
                content = NULL; /* 所有权转移 */
            }
            /* p 由等待方 wait_pending 释放，避免交付后立即释放造成竞态 */
        }
        json_free(content);
    } else if (msgType == TYPE_EVENT) {
        pthread_mutex_lock(&rpc->mu);
        void (*cb)(void*, int, JsonValue*) = rpc->event_cb;
        void* cbUser = rpc->event_user;
        pthread_mutex_unlock(&rpc->mu);
        if (cb) cb(cbUser, contentType, content);
        json_free(content);
    } else {
        json_free(content);
    }
}

static void on_stream_error(void* user, const char* msg) {
    RpcProtocol* rpc = (RpcProtocol*)user;
    char buf[512];
    snprintf(buf, sizeof(buf), "Request failed: %s", msg);
    reject_all(rpc, buf);
    pthread_mutex_lock(&rpc->mu);
    void (*cb)(void*, const char*) = rpc->error_cb;
    void* cbUser = rpc->error_user;
    pthread_mutex_unlock(&rpc->mu);
    if (cb) cb(cbUser, msg);
}

/* ---- 公共 API ---- */

RpcProtocol* rpc_create(MessageStream* ms) {
    RpcProtocol* rpc = (RpcProtocol*)calloc(1, sizeof(RpcProtocol));
    rpc->ms = ms;
    pthread_mutex_init(&rpc->mu, NULL);
    pthread_cond_init(&rpc->active_cv, NULL);
    ms_on_message(ms, on_stream_message, rpc);
    ms_on_error(ms, on_stream_error, rpc);
    ms_start(ms);
    return rpc;
}

void rpc_register(RpcProtocol* rpc, const char* name, IpcHandler h, void* user) {
    HandlerEntry* e = (HandlerEntry*)calloc(1, sizeof(HandlerEntry));
    e->name = dup_cstr(name);
    e->fn = h;
    e->user = user;
    pthread_mutex_lock(&rpc->mu);
    e->next = rpc->handlers;
    rpc->handlers = e;
    pthread_mutex_unlock(&rpc->mu);
}

void rpc_on_event(RpcProtocol* rpc, void (*cb)(void*, int, JsonValue*), void* user) {
    pthread_mutex_lock(&rpc->mu);
    rpc->event_cb = cb;
    rpc->event_user = user;
    pthread_mutex_unlock(&rpc->mu);
}

void rpc_on_error(RpcProtocol* rpc, void (*cb)(void*, const char*), void* user) {
    pthread_mutex_lock(&rpc->mu);
    rpc->error_cb = cb;
    rpc->error_user = user;
    pthread_mutex_unlock(&rpc->mu);
}

static RpcResult wait_pending(RpcProtocol* rpc, Pending* p) {
    (void)rpc;
    pthread_mutex_lock(&p->mu);
    while (!p->done) pthread_cond_wait(&p->cv, &p->mu);
    RpcResult r = p->result;
    pthread_mutex_unlock(&p->mu);
    /* deliver 方已不再触碰 p，此处安全释放 */
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->cv);
    free(p);
    return r;
}

static RpcResult send_and_wait(RpcProtocol* rpc, Pending* p, int msgType, int contentType,
                               const unsigned char* body, size_t bodyLen) {
    size_t msgLen;
    unsigned char* msg = build_message(msgType, contentType, p->id, body, bodyLen, &msgLen);
    int ok = ms_send(rpc->ms, msg, msgLen);
    free(msg);
    if (!ok) {
        pending_remove(rpc, p->id);
        RpcResult r = {NULL, json_str("send failed"), 0};
        return r;
    }
    return wait_pending(rpc, p);
}

static Pending* pending_new(RpcProtocol* rpc) {
    Pending* p = (Pending*)calloc(1, sizeof(Pending));
    new_uuid(p->id);
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cv, NULL);
    pthread_mutex_lock(&rpc->mu);
    p->next = rpc->pending;
    rpc->pending = p;
    pthread_mutex_unlock(&rpc->mu);
    return p;
}

RpcResult rpc_send_request(RpcProtocol* rpc, const char* method, JsonValue* params, const char* to) {
    Pending* p = pending_new(rpc);
    /* 组装 {"method":...,"params":...[,"to":...]}（params 借用，单独 dump） */
    JsonValue* mv = json_str(method);
    char* methodJson = json_dump(mv);
    json_free(mv);
    char* paramsJson = params ? json_dump(params) : NULL;
    if (!paramsJson) { paramsJson = (char*)malloc(5); strcpy(paramsJson, "null"); }
    size_t cap = strlen(methodJson) + strlen(paramsJson) + (to ? strlen(to) + 16 : 0) + 48;
    char* body = (char*)malloc(cap);
    if (to) {
        JsonValue* tv = json_str(to);
        char* toJson = json_dump(tv);
        json_free(tv);
        snprintf(body, cap, "{\"method\":%s,\"params\":%s,\"to\":%s}", methodJson, paramsJson, toJson);
        free(toJson);
    } else {
        snprintf(body, cap, "{\"method\":%s,\"params\":%s}", methodJson, paramsJson);
    }
    free(methodJson);
    free(paramsJson);
    RpcResult r = send_and_wait(rpc, p, TYPE_REQUEST, CONTENT_JSON, (unsigned char*)body, strlen(body));
    free(body);
    return r;
}

RpcResult rpc_send_request_binary(RpcProtocol* rpc, const char* method,
                                  const unsigned char* data, size_t len, JsonValue* argsArr) {
    Pending* p = pending_new(rpc);
    size_t bodyLen;
    unsigned char* body = encode_binary_request(method, data, len, argsArr, &bodyLen);
    RpcResult r = send_and_wait(rpc, p, TYPE_REQUEST, CONTENT_BINARY, body, bodyLen);
    free(body);
    return r;
}

void rpc_result_free(RpcResult* r) {
    json_free(r->value);
    json_free(r->error);
    r->value = NULL;
    r->error = NULL;
}

int rpc_send_text(RpcProtocol* rpc, const char* text) {
    char id[37];
    new_uuid(id);
    size_t msgLen;
    unsigned char* msg = build_message(TYPE_EVENT, CONTENT_TEXT, id, (const unsigned char*)text, strlen(text), &msgLen);
    int ok = ms_send(rpc->ms, msg, msgLen);
    free(msg);
    return ok;
}

int rpc_send_json(RpcProtocol* rpc, JsonValue* value) {
    char id[37];
    new_uuid(id);
    char* body = json_dump(value);
    size_t msgLen;
    unsigned char* msg = build_message(TYPE_EVENT, CONTENT_JSON, id, (unsigned char*)body, strlen(body), &msgLen);
    int ok = ms_send(rpc->ms, msg, msgLen);
    free(msg);
    free(body);
    return ok;
}

int rpc_send_binary(RpcProtocol* rpc, const unsigned char* data, size_t len) {
    char id[37];
    new_uuid(id);
    size_t msgLen;
    unsigned char* msg = build_message(TYPE_EVENT, CONTENT_BINARY, id, data, len, &msgLen);
    int ok = ms_send(rpc->ms, msg, msgLen);
    free(msg);
    return ok;
}

void rpc_close(RpcProtocol* rpc) {
    if (rpc->closed) return;
    rpc->closed = 1;
    reject_all(rpc, "RPC Protocol destroyed");
    ms_close(rpc->ms);
}

void rpc_destroy(RpcProtocol* rpc) {
    rpc_close(rpc);
    /* 等待进行中的请求处理线程结束 */
    pthread_mutex_lock(&rpc->mu);
    while (rpc->active > 0) pthread_cond_wait(&rpc->active_cv, &rpc->mu);
    pthread_mutex_unlock(&rpc->mu);
    ms_destroy(rpc->ms);
    HandlerEntry* e = rpc->handlers;
    while (e) {
        HandlerEntry* next = e->next;
        free(e->name);
        free(e);
        e = next;
    }
    pthread_mutex_destroy(&rpc->mu);
    pthread_cond_destroy(&rpc->active_cv);
    free(rpc);
}