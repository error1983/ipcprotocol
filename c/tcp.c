#include "tcp.h"

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>

#include <stdlib.h>
#include <string.h>

struct TcpConn { SOCKET s; };
struct TcpListener { SOCKET s; unsigned short port; };

static void wsa_init(void) {
    static int done = 0;
    if (!done) {
        WSADATA data;
        WSAStartup(MAKEWORD(2, 2), &data);
        done = 1;
    }
}

static int io_read(void* ctx, unsigned char* buf, int max) {
    TcpConn* c = (TcpConn*)ctx;
    int n = recv(c->s, (char*)buf, max, 0);
    return n == SOCKET_ERROR ? -1 : n;
}

static int io_write(void* ctx, const unsigned char* data, size_t len) {
    TcpConn* c = (TcpConn*)ctx;
    size_t off = 0;
    while (off < len) {
        int n = send(c->s, (const char*)(data + off), (int)(len - off), 0);
        if (n == SOCKET_ERROR || n == 0) return 0;
        off += (size_t)n;
    }
    return 1;
}

static void io_close(void* ctx) {
    TcpConn* c = (TcpConn*)ctx;
    if (c->s != INVALID_SOCKET) {
        shutdown(c->s, SD_BOTH);
        closesocket(c->s);
        c->s = INVALID_SOCKET;
    }
}

IpcIo tcp_io(TcpConn* c) {
    IpcIo io;
    io.read = io_read;
    io.write = io_write;
    io.close = io_close;
    io.ctx = c;
    return io;
}

TcpConn* tcp_connect(const char* host, unsigned short port) {
    wsa_init();
    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) return NULL;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);
    if (connect(s, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        closesocket(s);
        return NULL;
    }
    TcpConn* c = (TcpConn*)malloc(sizeof(TcpConn));
    c->s = s;
    return c;
}

void tcp_conn_free(TcpConn* c) {
    if (!c) return;
    io_close(c);
    free(c);
}

TcpListener* tcp_listen(const char* host, unsigned short port) {
    wsa_init();
    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) return NULL;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);
    if (bind(s, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR || listen(s, 16) == SOCKET_ERROR) {
        closesocket(s);
        return NULL;
    }
    int len = sizeof(addr);
    getsockname(s, (struct sockaddr*)&addr, &len);
    TcpListener* l = (TcpListener*)malloc(sizeof(TcpListener));
    l->s = s;
    l->port = ntohs(addr.sin_port);
    return l;
}

unsigned short tcp_listener_port(TcpListener* l) { return l->port; }

TcpConn* tcp_accept(TcpListener* l) {
    SOCKET s = accept(l->s, NULL, NULL);
    if (s == INVALID_SOCKET) return NULL;
    TcpConn* c = (TcpConn*)malloc(sizeof(TcpConn));
    c->s = s;
    return c;
}

void tcp_listener_free(TcpListener* l) {
    if (!l) return;
    if (l->s != INVALID_SOCKET) closesocket(l->s);
    free(l);
}