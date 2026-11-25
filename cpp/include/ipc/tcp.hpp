// TCP 传输封装（Windows/Winsock；其他平台可替换为自行实现的 ByteStream）。
#pragma once

#ifdef _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>

#include <memory>
#include <stdexcept>
#include <string>

#include "message_stream.hpp"

namespace ipc {

namespace detail {
inline void wsaInit() {
    static bool done = [] {
        WSADATA data;
        WSAStartup(MAKEWORD(2, 2), &data);
        return true;
    }();
    (void)done;
}
} // namespace detail

class TcpStream : public ByteStream {
public:
    explicit TcpStream(SOCKET s) : s_(s) { detail::wsaInit(); }
    ~TcpStream() override { close(); }

    int read(uint8_t* buf, int max) override {
        int n = ::recv(s_, reinterpret_cast<char*>(buf), max, 0);
        if (n == SOCKET_ERROR) return -1;
        return n;
    }

    bool write(const uint8_t* data, size_t len) override {
        size_t off = 0;
        while (off < len) {
            int n = ::send(s_, reinterpret_cast<const char*>(data + off), static_cast<int>(len - off), 0);
            if (n == SOCKET_ERROR || n == 0) return false;
            off += static_cast<size_t>(n);
        }
        return true;
    }

    void close() override {
        SOCKET s = s_;
        if (s != INVALID_SOCKET) {
            s_ = INVALID_SOCKET;
            ::shutdown(s, SD_BOTH);
            ::closesocket(s);
        }
    }

    SOCKET native() const { return s_; }

private:
    SOCKET s_;
};

class TcpListener {
public:
    TcpListener(const std::string& host, uint16_t port) {
        detail::wsaInit();
        s_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (s_ == INVALID_SOCKET) throw std::runtime_error("socket failed");
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
        if (::bind(s_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
            ::closesocket(s_);
            throw std::runtime_error("bind failed");
        }
        if (::listen(s_, 16) == SOCKET_ERROR) {
            ::closesocket(s_);
            throw std::runtime_error("listen failed");
        }
        socklen_t len = sizeof(addr);
        ::getsockname(s_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
    }

    ~TcpListener() {
        if (s_ != INVALID_SOCKET) ::closesocket(s_);
    }

    uint16_t port() const { return port_; }

    std::shared_ptr<TcpStream> accept() {
        SOCKET c = ::accept(s_, nullptr, nullptr);
        if (c == INVALID_SOCKET) throw std::runtime_error("accept failed");
        return std::make_shared<TcpStream>(c);
    }

private:
    SOCKET s_ = INVALID_SOCKET;
    uint16_t port_ = 0;
};

inline std::shared_ptr<TcpStream> tcpConnect(const std::string& host, uint16_t port) {
    detail::wsaInit();
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) throw std::runtime_error("socket failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    if (::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
        ::closesocket(s);
        throw std::runtime_error("connect failed");
    }
    return std::make_shared<TcpStream>(s);
}

} // namespace ipc

#endif // _WIN32