// MessageStream 帧层：长度头分帧 + 可选 CRC32 + 错误包与重同步。
// 回调必须在 start() 之前设置；RpcProtocol 会自动处理。
#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace ipc {

constexpr uint8_t ERR_LENGTH_CRC_FAILED = 1;
constexpr uint8_t ERR_MESSAGE_CRC_FAILED = 2;
constexpr uint8_t ERR_BUFFER_OVERFLOW = 3;

// 传输抽象：任意双向字节流（TCP/管道等）。read 返回字节数，0=EOF，-1=错误。
class ByteStream {
public:
    virtual ~ByteStream() = default;
    virtual int read(uint8_t* buf, int max) = 0;
    virtual bool write(const uint8_t* data, size_t len) = 0;
    virtual void close() = 0;
};

// CRC32（反射多项式 0xEDB88320，与 zlib 一致）
inline uint32_t crc32(const uint8_t* data, size_t len) {
    // C++11 魔法静态：线程安全的一次性初始化
    static const std::vector<uint32_t> table = [] {
        std::vector<uint32_t> t(256);
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return t;
    }();
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

inline uint32_t readLe32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

inline void writeLe32(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v);
    p[1] = static_cast<uint8_t>(v >> 8);
    p[2] = static_cast<uint8_t>(v >> 16);
    p[3] = static_cast<uint8_t>(v >> 24);
}

class MessageStream {
public:
    std::function<void(const std::vector<uint8_t>&)> onMessage;
    std::function<void(const std::string&)> onError;
    std::function<void()> onSyncComplete;

    MessageStream(std::shared_ptr<ByteStream> io, bool enableCRC = true,
                  size_t maxBufferSize = 256 * 1024 * 1024)
        : io_(std::move(io)), enableCRC_(enableCRC), maxBufferSize_(maxBufferSize) {}

    ~MessageStream() {
        close();
        if (readThread_.joinable()) readThread_.join();
    }

    // 启动读循环（幂等）。必须先设置回调。
    void start() {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true)) return;
        readThread_ = std::thread([this] { readLoop(); });
    }

    void close() {
        closed_.store(true);
        io_->close();
    }

    // 发送一个完整帧（并发安全）。
    bool send(const uint8_t* data, size_t len) {
        if (closed_.load()) return false;
        if (len > maxBufferSize_) return false;
        size_t headerSize = enableCRC_ ? 8 : 4;
        std::vector<uint8_t> frame(headerSize + len + (enableCRC_ ? 4 : 0));
        writeLe32(frame.data(), static_cast<uint32_t>(len));
        if (enableCRC_) writeLe32(frame.data() + 4, crc32(frame.data(), 4));
        std::memcpy(frame.data() + headerSize, data, len);
        if (enableCRC_) writeLe32(frame.data() + headerSize + len, crc32(data, len));
        std::lock_guard<std::mutex> lock(sendMu_);
        return io_->write(frame.data(), frame.size());
    }

    bool send(const std::vector<uint8_t>& data) { return send(data.data(), data.size()); }

    // 错误包（按正常帧封装的 EEEEEEEE+code）
    bool sendError(uint8_t code) {
        uint8_t pkt[5] = {0xEE, 0xEE, 0xEE, 0xEE, code};
        return send(pkt, 5);
    }

    // 裸协议帧（FFFFFFFF+长度+数据，无 CRC）
    bool sendProtocolMessage(const std::string& data) {
        std::vector<uint8_t> frame(8 + data.size());
        writeLe32(frame.data(), 0xFFFFFFFFu);
        writeLe32(frame.data() + 4, static_cast<uint32_t>(data.size()));
        std::memcpy(frame.data() + 8, data.data(), data.size());
        std::lock_guard<std::mutex> lock(sendMu_);
        return io_->write(frame.data(), frame.size());
    }

private:
    std::shared_ptr<ByteStream> io_;
    bool enableCRC_;
    size_t maxBufferSize_;
    std::mutex sendMu_;
    std::atomic<bool> started_{false};
    std::atomic<bool> closed_{false};
    std::thread readThread_;

    // 以下状态仅读线程访问
    std::vector<uint8_t> buf_;
    bool protocolMode_ = false;
    long expectedLen_ = -1;

    void emitError(const std::string& msg) { if (onError) onError(msg); }

    void reset() {
        protocolMode_ = true;
        buf_.clear();
        expectedLen_ = -1;
    }

    void readLoop() {
        std::vector<uint8_t> tmp(65536);
        for (;;) {
            int n = io_->read(tmp.data(), static_cast<int>(tmp.size()));
            if (n > 0) {
                buf_.insert(buf_.end(), tmp.begin(), tmp.begin() + n);
                processBuffer();
            }
            if (n <= 0) {
                if (!closed_.load()) emitError(n == 0 ? "stream ended" : "stream read error");
                return;
            }
        }
    }

    void processBuffer() {
        for (;;) {
            if (buf_.size() > maxBufferSize_) {
                emitError("buffer overflow");
                sendError(ERR_BUFFER_OVERFLOW);
                reset();
                return;
            }
            if (protocolMode_) {
                if (buf_.size() >= 8) {
                    if (readLe32(buf_.data()) == 0xFFFFFFFFu) {
                        uint32_t length = readLe32(buf_.data() + 4);
                        if (length <= 50 && buf_.size() >= 8 + length) {
                            std::string text(reinterpret_cast<const char*>(buf_.data() + 8), length);
                            buf_.erase(buf_.begin(), buf_.begin() + 8 + length);
                            // trim
                            size_t b = text.find_first_not_of(" \t\n\r");
                            size_t e = text.find_last_not_of(" \t\n\r");
                            std::string trimmed = (b == std::string::npos) ? "" : text.substr(b, e - b + 1);
                            if (trimmed == "RESET_CONFIRM") {
                                protocolMode_ = false;
                                buf_.clear();
                                if (onSyncComplete) onSyncComplete();
                            }
                            continue;
                        }
                        return; // 协议消息未收全
                    }
                    buf_.clear(); // 无协议头：丢弃脏数据
                }
                return;
            }

            size_t headerSize = enableCRC_ ? 8 : 4;
            if (expectedLen_ < 0) {
                if (buf_.size() < headerSize) return;
                uint32_t length = readLe32(buf_.data());
                if (enableCRC_) {
                    uint32_t expected = readLe32(buf_.data() + 4);
                    if (crc32(buf_.data(), 4) != expected) {
                        sendError(ERR_LENGTH_CRC_FAILED);
                        emitError("length header CRC check failed");
                        reset();
                        return;
                    }
                }
                expectedLen_ = static_cast<long>(length);
                buf_.erase(buf_.begin(), buf_.begin() + headerSize);
            }

            size_t totalSize = static_cast<size_t>(expectedLen_) + (enableCRC_ ? 4 : 0);
            if (buf_.size() < totalSize) return;
            std::vector<uint8_t> data(buf_.begin(), buf_.begin() + expectedLen_);
            if (enableCRC_) {
                uint32_t expected = readLe32(buf_.data() + expectedLen_);
                if (crc32(data.data(), data.size()) != expected) {
                    sendError(ERR_MESSAGE_CRC_FAILED);
                    emitError("message CRC check failed");
                    reset();
                    return;
                }
            }
            buf_.erase(buf_.begin(), buf_.begin() + totalSize);
            expectedLen_ = -1;
            // 错误包可能 reset 清空缓冲区，故先消费再分发
            if (data.size() == 5 && data[0] == 0xEE && data[1] == 0xEE && data[2] == 0xEE && data[3] == 0xEE) {
                reset();
                sendProtocolMessage("RESET_CONFIRM");
                protocolMode_ = false;
            } else if (onMessage) {
                onMessage(data);
            }
            if (protocolMode_) return;
        }
    }
};

} // namespace ipc