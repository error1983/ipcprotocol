// RpcProtocol 消息层：请求/响应/事件，与 RpcProtocol.js 线格式兼容。
#pragma once

#include <condition_variable>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "json.hpp"
#include "message_stream.hpp"

namespace ipc {

constexpr uint8_t TYPE_REQUEST = 0;
constexpr uint8_t TYPE_RESPONSE = 1;
constexpr uint8_t TYPE_EVENT = 2;

constexpr uint8_t CONTENT_TEXT = 0;
constexpr uint8_t CONTENT_JSON = 1;
constexpr uint8_t CONTENT_BINARY = 2;

// RPC 错误：remote=true 时 error 携带对端原始 error 值（对齐 JS reject(response.error)）。
class RpcException : public std::runtime_error {
public:
    Json error;
    bool remote;

    explicit RpcException(const std::string& msg)
        : std::runtime_error(msg), remote(false) {}
    explicit RpcException(Json remoteValue)
        : std::runtime_error("rpc error: " + remoteValue.dump()), error(std::move(remoteValue)), remote(true) {}
};

// 服务端方法处理器：抛 std::runtime_error 转为 {"error": ...} 响应。
using Handler = std::function<Json(const std::vector<Json>&)>;

class RpcProtocol {
public:
    // 包装消息流：先注册回调再启动读循环，避免早到消息丢失。
    explicit RpcProtocol(std::shared_ptr<MessageStream> ms) : state_(std::make_shared<State>()), ms_(std::move(ms)) {
        auto state = state_;
        auto msCopy = ms_;
        ms_->onMessage = [state, msCopy](const std::vector<uint8_t>& data) { handleMessage(data, state, msCopy); };
        ms_->onError = [state](const std::string& msg) {
            rejectAll(state, "Request failed: " + msg);
            std::lock_guard<std::mutex> lock(state->mu);
            if (state->onError) state->onError(msg);
        };
        ms_->start();
    }

    ~RpcProtocol() { close(); }

    void registerMethod(const std::string& name, Handler h) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->handlers[name] = std::move(h);
    }

    void onEvent(std::function<void(uint8_t, Json)> cb) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->onEvent = std::move(cb);
    }

    void onError(std::function<void(const std::string&)> cb) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->onError = std::move(cb);
    }

    // 发送 JSON 请求并阻塞等待响应；对端错误响应抛 RpcException(remote=true)。
    Json sendRequest(const std::string& method, Json params = Json::null(), const std::string& to = "") {
        std::string id = newUUID();
        auto pending = std::make_shared<Pending>();
        {
            std::lock_guard<std::mutex> lock(state_->mu);
            state_->pending[id] = pending;
        }
        Json payload = Json::object();
        payload.set("method", Json::ofStr(method));
        payload.set("params", std::move(params));
        if (!to.empty()) payload.set("to", Json::ofStr(to));
        std::string body = payload.dump();
        if (!ms_->send(buildMessage(TYPE_REQUEST, CONTENT_JSON, id,
                                    reinterpret_cast<const uint8_t*>(body.data()), body.size()))) {
            removePending(id);
            throw RpcException("send failed");
        }
        return waitResult(id, pending);
    }

    // 二进制请求：handler 实参为 [Bytes(data), ...args]。
    Json sendRequestBinary(const std::string& method, const std::vector<uint8_t>& data,
                           const std::vector<Json>& args = {}) {
        std::string id = newUUID();
        auto pending = std::make_shared<Pending>();
        {
            std::lock_guard<std::mutex> lock(state_->mu);
            state_->pending[id] = pending;
        }
        std::vector<uint8_t> body = encodeBinaryRequest(method, data, args);
        if (!ms_->send(buildMessage(TYPE_REQUEST, CONTENT_BINARY, id, body.data(), body.size()))) {
            removePending(id);
            throw RpcException("send failed");
        }
        return waitResult(id, pending);
    }

    // 单向事件
    bool sendText(const std::string& text) {
        return ms_->send(buildMessage(TYPE_EVENT, CONTENT_TEXT, newUUID(),
                                      reinterpret_cast<const uint8_t*>(text.data()), text.size()));
    }

    bool sendJson(const Json& value) {
        std::string body = value.dump();
        return ms_->send(buildMessage(TYPE_EVENT, CONTENT_JSON, newUUID(),
                                      reinterpret_cast<const uint8_t*>(body.data()), body.size()));
    }

    bool sendBinary(const std::vector<uint8_t>& data) {
        return ms_->send(buildMessage(TYPE_EVENT, CONTENT_BINARY, newUUID(), data.data(), data.size()));
    }

    // 销毁：拒绝所有 pending 并关闭流。
    void close() {
        bool expected = false;
        if (!closed_.compare_exchange_strong(expected, true)) return;
        rejectAll(state_, "RPC Protocol destroyed");
        ms_->close();
    }

private:
    struct Pending {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;
        Json value;
        std::unique_ptr<RpcException> err;
    };

    struct State {
        std::mutex mu;
        std::map<std::string, Handler> handlers;
        std::map<std::string, std::shared_ptr<Pending>> pending;
        std::function<void(uint8_t, Json)> onEvent;
        std::function<void(const std::string&)> onError;
    };

    std::shared_ptr<State> state_;
    std::atomic<bool> closed_{false};
    std::shared_ptr<MessageStream> ms_; // 最后声明：析构时先停读线程

    void removePending(const std::string& id) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->pending.erase(id);
    }

    Json waitResult(const std::string& id, std::shared_ptr<Pending> pending) {
        Json value;
        std::unique_ptr<RpcException> err;
        {
            std::unique_lock<std::mutex> lock(pending->mu);
            pending->cv.wait(lock, [&] { return pending->done; });
            value = std::move(pending->value);
            err = std::move(pending->err);
        } // 先释放 pending 锁再取 state 锁，避免与 rejectAll 的锁序形成环
        removePending(id);
        if (err) throw std::move(*err);
        return value;
    }

    static void rejectAll(const std::shared_ptr<State>& state, const std::string& msg) {
        std::lock_guard<std::mutex> lock(state->mu);
        for (auto& [id, p] : state->pending) {
            std::lock_guard<std::mutex> pl(p->mu);
            p->err = std::make_unique<RpcException>(msg);
            p->done = true;
            p->cv.notify_all();
        }
        state->pending.clear();
    }

    static void deliver(const std::shared_ptr<Pending>& p, Json value, std::unique_ptr<RpcException> err) {
        std::lock_guard<std::mutex> lock(p->mu);
        p->value = std::move(value);
        p->err = std::move(err);
        p->done = true;
        p->cv.notify_all();
    }

    static void handleMessage(const std::vector<uint8_t>& data, const std::shared_ptr<State>& state,
                              const std::shared_ptr<MessageStream>& ms) {
        if (data.size() < 18) return;
        uint8_t msgType = data[0];
        uint8_t contentType = data[1];
        std::string id = bytesToUUID(data.data() + 2);
        const uint8_t* payload = data.data() + 18;
        size_t payloadLen = data.size() - 18;

        Json content;
        try {
            content = deserialize(contentType, payload, payloadLen);
        } catch (...) {
            // 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
            if (msgType == TYPE_REQUEST) {
                sendErrorResponse(ms, id, "Invalid request payload");
            }
            return; // 解析失败：丢弃，不影响后续消息
        }

        if (msgType == TYPE_REQUEST) {
            std::vector<uint8_t> raw(payload, payload + payloadLen);
            std::thread([state, ms, id, contentType, content, raw]() mutable {
                handleRequest(id, contentType, content, raw, state, ms);
            }).detach();
        } else if (msgType == TYPE_RESPONSE) {
            std::shared_ptr<Pending> p;
            {
                std::lock_guard<std::mutex> lock(state->mu);
                auto it = state->pending.find(id);
                if (it != state->pending.end()) {
                    p = it->second;
                    state->pending.erase(it);
                }
            }
            if (p) {
                const Json* ev = content.find("error");
                if (content.type == Json::Type::Obj && ev && ev->isTruthy()) {
                    deliver(p, Json::null(), std::make_unique<RpcException>(*ev));
                } else {
                    deliver(p, std::move(content), nullptr);
                }
            }
        } else if (msgType == TYPE_EVENT) {
            std::lock_guard<std::mutex> lock(state->mu);
            if (state->onEvent) state->onEvent(contentType, std::move(content));
        }
    }

    static void sendErrorResponse(const std::shared_ptr<MessageStream>& ms, const std::string& id,
                                  const std::string& msg) {
        std::string body = Json::object().set("error", Json::ofStr(msg)).dump();
        ms->send(buildMessage(TYPE_RESPONSE, CONTENT_JSON, id,
                              reinterpret_cast<const uint8_t*>(body.data()), body.size()));
    }

    static void handleRequest(const std::string& id, uint8_t contentType, const Json& content,
                              const std::vector<uint8_t>& raw, const std::shared_ptr<State>& state,
                              const std::shared_ptr<MessageStream>& ms) {
        std::string method;
        std::vector<Json> args;
        if (contentType == CONTENT_BINARY) {
            if (!decodeBinaryRequest(raw, method, args)) {
                sendErrorResponse(ms, id, "Invalid request payload: bad binary request");
                return;
            }
        } else {
            if (content.type != Json::Type::Obj) {
                sendErrorResponse(ms, id, "Invalid request payload: not an object");
                return;
            }
            const Json* m = content.find("method");
            if (!m || m->type != Json::Type::Str) {
                sendErrorResponse(ms, id, "Invalid request payload: method missing");
                return;
            }
            method = m->str;
            const Json* params = content.find("params");
            args = expandParams(params ? *params : Json::null());
        }
        if (method.empty()) {
            sendErrorResponse(ms, id, "Invalid request payload: method missing");
            return;
        }

        Handler handler;
        {
            std::lock_guard<std::mutex> lock(state->mu);
            auto it = state->handlers.find(method);
            if (it != state->handlers.end()) handler = it->second;
        }

        uint8_t respType;
        std::vector<uint8_t> respBody;
        if (!handler) {
            respType = CONTENT_JSON;
            std::string body = Json::object().set("error", Json::ofStr("Method not found: " + method)).dump();
            respBody.assign(body.begin(), body.end());
        } else {
            try {
                Json result = handler(args);
                serializeResponse(result, respType, respBody);
            } catch (const std::exception& e) {
                respType = CONTENT_JSON;
                std::string body = Json::object().set("error", Json::ofStr(e.what())).dump();
                respBody.assign(body.begin(), body.end());
            }
        }
        ms->send(buildMessage(TYPE_RESPONSE, respType, id, respBody.data(), respBody.size()));
    }

    // ---- 序列化辅助 ----

    static Json deserialize(uint8_t contentType, const uint8_t* payload, size_t len) {
        std::string text(reinterpret_cast<const char*>(payload), len);
        switch (contentType) {
            case CONTENT_TEXT: return Json::ofStr(std::move(text));
            case CONTENT_JSON: return Json::parse(text);
            case CONTENT_BINARY: return Json::ofBytes(std::vector<uint8_t>(payload, payload + len));
            default: throw std::runtime_error("unknown content type");
        }
    }

    static void serializeResponse(const Json& v, uint8_t& type, std::vector<uint8_t>& body) {
        switch (v.type) {
            case Json::Type::Null:
                type = CONTENT_JSON;
                body = {'n', 'u', 'l', 'l'};
                break;
            case Json::Type::Str:
                type = CONTENT_TEXT;
                body.assign(v.str.begin(), v.str.end());
                break;
            case Json::Type::Bytes:
                type = CONTENT_BINARY;
                body = v.bytes;
                break;
            default: {
                type = CONTENT_JSON;
                std::string s = v.dump();
                body.assign(s.begin(), s.end());
                break;
            }
        }
    }

    static std::vector<Json> expandParams(const Json& params) {
        if (params.type == Json::Type::Null) return {};
        if (params.type == Json::Type::Arr) return params.arr;
        return {params};
    }

    static std::vector<uint8_t> buildMessage(uint8_t msgType, uint8_t contentType, const std::string& id,
                                             const uint8_t* payload, size_t len) {
        std::vector<uint8_t> msg(18 + len);
        msg[0] = msgType;
        msg[1] = contentType;
        auto idBytes = uuidToBytes(id);
        std::memcpy(msg.data() + 2, idBytes.data(), 16);
        std::memcpy(msg.data() + 18, payload, len);
        return msg;
    }

    static void writeBe32(std::vector<uint8_t>& out, uint32_t v) {
        out.push_back(static_cast<uint8_t>(v >> 24));
        out.push_back(static_cast<uint8_t>(v >> 16));
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    }

    static uint32_t readBe32(const uint8_t* p) {
        return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
               (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
    }

    // [methodLen u32BE][method][paramsLen u32BE][paramsJSON][binary]
    static std::vector<uint8_t> encodeBinaryRequest(const std::string& method, const std::vector<uint8_t>& data,
                                                    const std::vector<Json>& args) {
        std::string paramsJson = Json::ofArr(args).dump();
        std::vector<uint8_t> out;
        writeBe32(out, static_cast<uint32_t>(method.size()));
        out.insert(out.end(), method.begin(), method.end());
        writeBe32(out, static_cast<uint32_t>(paramsJson.size()));
        out.insert(out.end(), paramsJson.begin(), paramsJson.end());
        out.insert(out.end(), data.begin(), data.end());
        return out;
    }

    static bool decodeBinaryRequest(const std::vector<uint8_t>& raw, std::string& method, std::vector<Json>& args) {
        if (raw.size() < 4) return false;
        uint32_t methodLen = readBe32(raw.data());
        if (raw.size() < 4 + methodLen + 4) return false;
        method.assign(reinterpret_cast<const char*>(raw.data() + 4), methodLen);
        size_t off = 4 + methodLen;
        uint32_t paramsLen = readBe32(raw.data() + off);
        if (raw.size() < off + 4 + paramsLen) return false;
        try {
            Json params = Json::parse(std::string(reinterpret_cast<const char*>(raw.data() + off + 4), paramsLen));
            args.clear();
            args.push_back(Json::ofBytes(std::vector<uint8_t>(raw.begin() + off + 4 + paramsLen, raw.end())));
            if (params.type == Json::Type::Arr) {
                for (auto& p : params.arr) args.push_back(std::move(p));
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    // ---- UUID（RFC 4122 大端字节序）----

    static std::string newUUID() {
        static std::mutex mu;
        static std::mt19937_64 rng(std::random_device{}());
        std::lock_guard<std::mutex> lock(mu);
        uint64_t a = rng(), b = rng();
        uint8_t bytes[16];
        std::memcpy(bytes, &a, 8);
        std::memcpy(bytes + 8, &b, 8);
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        return bytesToUUID(bytes);
    }

    static std::vector<uint8_t> uuidToBytes(const std::string& id) {
        std::string hex;
        for (char c : id) if (c != '-') hex += c;
        std::vector<uint8_t> out(16);
        for (int i = 0; i < 16; i++) {
            out[i] = static_cast<uint8_t>(std::stoul(hex.substr(i * 2, 2), nullptr, 16));
        }
        return out;
    }

    static std::string bytesToUUID(const uint8_t* b) {
        char buf[37];
        snprintf(buf, sizeof(buf),
                 "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                 b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                 b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
        return std::string(buf);
    }
};

} // namespace ipc