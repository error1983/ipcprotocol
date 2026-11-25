/* RpcProtocol 消息层（C11 + pthreads）：请求/响应/事件，与 RpcProtocol.js 线格式兼容。 */
#ifndef IPC_RPC_H
#define IPC_RPC_H

#include "json.h"
#include "message_stream.h"

typedef struct RpcProtocol RpcProtocol;

/* 服务端方法处理器。
 * args 为借用指针数组；返回值为接管所有权的结果（NULL 视为 JSON null）。
 * 出错时返回 NULL 并设置 *errMsg（malloc 字符串，rpc 层负责 free）。 */
typedef JsonValue* (*IpcHandler)(void* user, JsonValue** args, size_t nargs, char** errMsg);

typedef struct {
    JsonValue* value; /* 成功结果（所有权归调用方） */
    JsonValue* error; /* 失败时的错误值：remote=1 为对端原始 error 值，否则为 JSON_STR 消息 */
    int remote;
} RpcResult;

/* 接管 ms 所有权；先注册回调再启动读循环 */
RpcProtocol* rpc_create(MessageStream* ms);
void rpc_register(RpcProtocol* rpc, const char* name, IpcHandler h, void* user);
/* 事件回调中 content 为借用，回调返回后释放 */
void rpc_on_event(RpcProtocol* rpc, void (*cb)(void* user, int contentType, JsonValue* content), void* user);
void rpc_on_error(RpcProtocol* rpc, void (*cb)(void* user, const char* msg), void* user);

/* params 为借用（NULL 视为 null）；to 可为 NULL。阻塞等待响应。 */
RpcResult rpc_send_request(RpcProtocol* rpc, const char* method, JsonValue* params, const char* to);
/* argsArr 为借用的 JSON_ARR（可为 NULL）；handler 实参为 [Bytes(data), ...args] */
RpcResult rpc_send_request_binary(RpcProtocol* rpc, const char* method,
                                  const unsigned char* data, size_t len, JsonValue* argsArr);
void rpc_result_free(RpcResult* r);

int rpc_send_text(RpcProtocol* rpc, const char* text);
int rpc_send_json(RpcProtocol* rpc, JsonValue* value); /* 借用 */
int rpc_send_binary(RpcProtocol* rpc, const unsigned char* data, size_t len);

void rpc_close(RpcProtocol* rpc);   /* 拒绝所有 pending 并关闭流 */
void rpc_destroy(RpcProtocol* rpc); /* close + 等请求处理完 + 释放（含 ms） */

#endif