#include "message_stream.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

typedef struct { unsigned char* data; size_t len, cap; } Buf;

static void buf_append(Buf* b, const unsigned char* data, size_t n) {
    if (b->len + n > b->cap) {
        while (b->len + n > b->cap) b->cap = b->cap ? b->cap * 2 : 4096;
        b->data = (unsigned char*)realloc(b->data, b->cap);
    }
    memcpy(b->data + b->len, data, n);
    b->len += n;
}

static void buf_consume(Buf* b, size_t n) {
    memmove(b->data, b->data + n, b->len - n);
    b->len -= n;
}

static unsigned read_le32(const unsigned char* p) {
    return (unsigned)p[0] | ((unsigned)p[1] << 8) | ((unsigned)p[2] << 16) | ((unsigned)p[3] << 24);
}

static void write_le32(unsigned char* p, unsigned v) {
    p[0] = (unsigned char)v;
    p[1] = (unsigned char)(v >> 8);
    p[2] = (unsigned char)(v >> 16);
    p[3] = (unsigned char)(v >> 24);
}

static unsigned crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

static void crc_init(void) {
    for (unsigned i = 0; i < 256; i++) {
        unsigned c = i;
        for (int k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc_table[i] = c;
    }
}

unsigned ipc_crc32(const unsigned char* data, size_t len) {
    pthread_once(&crc_once, crc_init);
    unsigned crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) crc = crc_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

struct MessageStream {
    IpcIo io;
    int enableCRC;
    size_t maxBufferSize;

    void (*on_message)(void*, const unsigned char*, size_t);
    void* on_message_user;
    void (*on_error)(void*, const char*);
    void* on_error_user;
    void (*on_sync)(void*);
    void* on_sync_user;

    pthread_mutex_t send_mu;
    int started;
    int closed;
    pthread_t thread;

    /* 以下仅读线程访问 */
    Buf buf;
    int protocolMode;
    long expectedLen;
};

MessageStream* ms_create(IpcIo io, int enableCRC, size_t maxBufferSize) {
    MessageStream* ms = (MessageStream*)calloc(1, sizeof(MessageStream));
    ms->io = io;
    ms->enableCRC = enableCRC;
    ms->maxBufferSize = maxBufferSize ? maxBufferSize : 256 * 1024 * 1024;
    ms->expectedLen = -1;
    pthread_mutex_init(&ms->send_mu, NULL);
    return ms;
}

void ms_on_message(MessageStream* ms, void (*cb)(void*, const unsigned char*, size_t), void* user) {
    ms->on_message = cb;
    ms->on_message_user = user;
}

void ms_on_error(MessageStream* ms, void (*cb)(void*, const char*), void* user) {
    ms->on_error = cb;
    ms->on_error_user = user;
}

void ms_on_sync_complete(MessageStream* ms, void (*cb)(void*), void* user) {
    ms->on_sync = cb;
    ms->on_sync_user = user;
}

static void emit_error(MessageStream* ms, const char* msg) {
    if (ms->on_error) ms->on_error(ms->on_error_user, msg);
}

static void ms_reset(MessageStream* ms) {
    ms->protocolMode = 1;
    ms->buf.len = 0;
    ms->expectedLen = -1;
}

int ms_send(MessageStream* ms, const unsigned char* data, size_t len) {
    if (ms->closed) return 0;
    if (len > ms->maxBufferSize) return 0;
    size_t headerSize = ms->enableCRC ? 8 : 4;
    size_t total = headerSize + len + (ms->enableCRC ? 4 : 0);
    unsigned char* frame = (unsigned char*)malloc(total);
    write_le32(frame, (unsigned)len);
    if (ms->enableCRC) write_le32(frame + 4, ipc_crc32(frame, 4));
    memcpy(frame + headerSize, data, len);
    if (ms->enableCRC) write_le32(frame + headerSize + len, ipc_crc32(data, len));
    pthread_mutex_lock(&ms->send_mu);
    int ok = ms->io.write(ms->io.ctx, frame, total);
    pthread_mutex_unlock(&ms->send_mu);
    free(frame);
    return ok;
}

int ms_send_error(MessageStream* ms, unsigned char code) {
    unsigned char pkt[5] = {0xEE, 0xEE, 0xEE, 0xEE, code};
    return ms_send(ms, pkt, 5);
}

int ms_send_protocol(MessageStream* ms, const char* data, size_t len) {
    size_t total = 8 + len;
    unsigned char* frame = (unsigned char*)malloc(total);
    write_le32(frame, 0xFFFFFFFFu);
    write_le32(frame + 4, (unsigned)len);
    memcpy(frame + 8, data, len);
    pthread_mutex_lock(&ms->send_mu);
    int ok = ms->io.write(ms->io.ctx, frame, total);
    pthread_mutex_unlock(&ms->send_mu);
    free(frame);
    return ok;
}

static void process_buffer(MessageStream* ms) {
    for (;;) {
        if (ms->buf.len > ms->maxBufferSize) {
            emit_error(ms, "buffer overflow");
            ms_send_error(ms, IPC_ERR_BUFFER_OVERFLOW);
            ms_reset(ms);
            return;
        }
        if (ms->protocolMode) {
            if (ms->buf.len >= 8) {
                if (read_le32(ms->buf.data) == 0xFFFFFFFFu) {
                    unsigned length = read_le32(ms->buf.data + 4);
                    if (length <= 50 && ms->buf.len >= 8 + length) {
                        char text[64];
                        memcpy(text, ms->buf.data + 8, length);
                        text[length] = '\0';
                        buf_consume(&ms->buf, 8 + length);
                        /* trim */
                        char* t = text;
                        while (*t == ' ' || *t == '\t' || *t == '\n' || *t == '\r') t++;
                        size_t n = strlen(t);
                        while (n > 0 && (t[n - 1] == ' ' || t[n - 1] == '\t' || t[n - 1] == '\n' || t[n - 1] == '\r')) n--;
                        t[n] = '\0';
                        if (strcmp(t, "RESET_CONFIRM") == 0) {
                            ms->protocolMode = 0;
                            ms->buf.len = 0;
                            if (ms->on_sync) ms->on_sync(ms->on_sync_user);
                        }
                        continue;
                    }
                    return; /* 协议消息未收全 */
                }
                ms->buf.len = 0; /* 无协议头：丢弃脏数据 */
            }
            return;
        }

        size_t headerSize = ms->enableCRC ? 8 : 4;
        if (ms->expectedLen < 0) {
            if (ms->buf.len < headerSize) return;
            unsigned length = read_le32(ms->buf.data);
            if (ms->enableCRC) {
                unsigned expected = read_le32(ms->buf.data + 4);
                if (ipc_crc32(ms->buf.data, 4) != expected) {
                    ms_send_error(ms, IPC_ERR_LENGTH_CRC_FAILED);
                    emit_error(ms, "length header CRC check failed");
                    ms_reset(ms);
                    return;
                }
            }
            ms->expectedLen = (long)length;
            buf_consume(&ms->buf, headerSize);
        }

        size_t totalSize = (size_t)ms->expectedLen + (ms->enableCRC ? 4 : 0);
        if (ms->buf.len < totalSize) return;
        unsigned char* data = (unsigned char*)malloc((size_t)ms->expectedLen);
        memcpy(data, ms->buf.data, (size_t)ms->expectedLen);
        if (ms->enableCRC) {
            unsigned expected = read_le32(ms->buf.data + ms->expectedLen);
            if (ipc_crc32(data, (size_t)ms->expectedLen) != expected) {
                free(data);
                ms_send_error(ms, IPC_ERR_MESSAGE_CRC_FAILED);
                emit_error(ms, "message CRC check failed");
                ms_reset(ms);
                return;
            }
        }
        size_t dataLen = (size_t)ms->expectedLen;
        buf_consume(&ms->buf, totalSize);
        ms->expectedLen = -1;
        /* 错误包可能 reset 清空缓冲区，故先消费再分发 */
        if (dataLen == 5 && data[0] == 0xEE && data[1] == 0xEE && data[2] == 0xEE && data[3] == 0xEE) {
            free(data);
            ms_reset(ms);
            ms_send_protocol(ms, "RESET_CONFIRM", 13);
            ms->protocolMode = 0;
        } else {
            if (ms->on_message) ms->on_message(ms->on_message_user, data, dataLen);
            free(data);
        }
        if (ms->protocolMode) return;
    }
}

static void* read_loop(void* arg) {
    MessageStream* ms = (MessageStream*)arg;
    unsigned char tmp[65536];
    for (;;) {
        int n = ms->io.read(ms->io.ctx, tmp, (int)sizeof(tmp));
        if (n > 0) {
            buf_append(&ms->buf, tmp, (size_t)n);
            process_buffer(ms);
        }
        if (n <= 0) {
            if (!ms->closed) emit_error(ms, n == 0 ? "stream ended" : "stream read error");
            return NULL;
        }
    }
}

void ms_start(MessageStream* ms) {
    if (ms->started) return;
    ms->started = 1;
    pthread_create(&ms->thread, NULL, read_loop, ms);
}

void ms_close(MessageStream* ms) {
    ms->closed = 1;
    if (ms->io.close) ms->io.close(ms->io.ctx);
}

void ms_destroy(MessageStream* ms) {
    ms_close(ms);
    if (ms->started) pthread_join(ms->thread, NULL);
    free(ms->buf.data);
    pthread_mutex_destroy(&ms->send_mu);
    free(ms);
}