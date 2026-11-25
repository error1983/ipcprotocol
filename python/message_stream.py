"""
MessageStream 的 Python 移植版（与 MessageStream.js 线格式完全兼容）。

帧格式（正常模式，enable_crc=True 时）:
    [长度 u32LE][长度头CRC32 u32LE][数据...][数据CRC32 u32LE]
关闭 CRC 时:
    [长度 u32LE][数据...]

错误包（作为普通帧发送，5 字节）:
    [0xEE 0xEE 0xEE 0xEE][错误类型 1B]
    错误类型: 1=LENGTH_CRC_FAILED, 2=MESSAGE_CRC_FAILED, 3=BUFFER_OVERFLOW

重同步协议消息（不带 CRC 的裸帧）:
    [0xFF 0xFF 0xFF 0xFF][长度 u32LE][数据(如 'RESET_CONFIRM')]

CRC32 与 JS 版一致: 多项式 0xEDB88320（反射），初值 0xFFFFFFFF，结果异或 0xFFFFFFFF，
等价于 zlib.crc32。
"""

import asyncio
import struct
import zlib

PROTOCOL_HEADER = b'\xFF\xFF\xFF\xFF'
ERROR_HEADER = b'\xEE\xEE\xEE\xEE'

ERROR_TYPES = {
    'LENGTH_CRC_FAILED': 1,
    'MESSAGE_CRC_FAILED': 2,
    'BUFFER_OVERFLOW': 3,
}
ERROR_TYPE_CODES = {v: k for k, v in ERROR_TYPES.items()}


class MessageStream:
    """基于 asyncio StreamReader/StreamWriter 的消息流，语义对齐 MessageStream.js。

    事件（通过 on() 注册回调）:
        'message'      -> callback(data: bytes)
        'error'        -> callback(err: Exception | str)
        'syncComplete' -> callback()
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 enable_crc: bool = True, max_buffer_size: int = 256 * 1024 * 1024):
        if reader is None or writer is None:
            raise ValueError('reader/writer is undefined')
        self._reader = reader
        self._writer = writer
        self.enable_crc = enable_crc
        self.max_buffer_size = max_buffer_size

        self._buffer = bytearray()
        self._expected_length = None
        self._protocol_mode = False
        self._send_lock = asyncio.Lock()
        self._listeners = {}
        self._closed = False
        self._read_task = asyncio.get_event_loop().create_task(self._read_loop())

    # ========== 事件 ==========
    def on(self, event, callback):
        self._listeners.setdefault(event, []).append(callback)

    def _emit(self, event, *args):
        for cb in list(self._listeners.get(event, [])):
            try:
                cb(*args)
            except Exception as err:  # 与 JS EventEmitter 不同，回调异常不应中断读循环
                print(f'[MessageStream] listener error: {err}')

    # ========== 发送 ==========
    async def send(self, data: bytes):
        """发送一条消息（带长度头与可选 CRC），按调用顺序串行化。"""
        if len(data) > self.max_buffer_size:
            raise ValueError('Data size exceeds maximum allowed limit')
        async with self._send_lock:
            if self._closed:
                raise ConnectionError('Stream is not connected')
            frame = self._build_frame(data)
            self._writer.write(frame)
            await self._writer.drain()

    def _build_frame(self, data: bytes) -> bytes:
        length_header = struct.pack('<I', len(data))
        if self.enable_crc:
            return (length_header
                    + struct.pack('<I', zlib.crc32(length_header))
                    + data
                    + struct.pack('<I', zlib.crc32(data)))
        return length_header + data

    async def send_error(self, error_type: str):
        """发送错误包（按普通帧格式封装）。"""
        code = ERROR_TYPES.get(error_type, 0)
        try:
            await self.send(ERROR_HEADER + bytes([code]))
        except Exception as err:
            print(f'Failed to send error packet: {err}')

    async def send_protocol_message(self, message: str):
        """发送协议消息: [FFFFFFFF][长度LE][数据]，不带 CRC。"""
        data = message.encode('utf8')
        frame = PROTOCOL_HEADER + struct.pack('<I', len(data)) + data
        try:
            async with self._send_lock:
                if self._closed:
                    raise ConnectionError('Stream is not connected')
                self._writer.write(frame)
                await self._writer.drain()
        except Exception as err:
            print(f"Failed to send protocol message '{message}': {err}")

    # ========== 接收 ==========
    async def _read_loop(self):
        try:
            while not self._closed:
                data = await self._reader.read(65536)
                if not data:
                    break
                self._on_data(data)
        except (ConnectionError, OSError) as err:
            if not self._closed:
                self._emit('error', err)
        finally:
            if not self._closed:
                self._emit('error', ConnectionError('Connection closed'))

    def _on_data(self, data: bytes):
        self._buffer.extend(data)

        if len(self._buffer) > self.max_buffer_size:
            self._emit('error', f'Buffer size: {len(self._buffer)} exceeds maximum allowed limit')
            asyncio.get_event_loop().create_task(self.send_error('BUFFER_OVERFLOW'))
            self.reset()
            return

        while len(self._buffer) > 0:
            # 协议模式：只接受协议消息
            if self._protocol_mode and len(self._buffer) >= 8:
                if bytes(self._buffer[:4]) == PROTOCOL_HEADER:
                    length = struct.unpack_from('<I', self._buffer, 4)[0]
                    total = 8 + length
                    if len(self._buffer) >= total:
                        message_data = bytes(self._buffer[8:total])
                        del self._buffer[:total]
                        if self._handle_protocol_message(message_data):
                            continue
                    else:
                        break  # 等待更多数据
                else:
                    # 协议模式下的残留数据，丢弃
                    print('[MessageStream] Dropping non-protocol data in protocol mode')
                    self._buffer.clear()
                    break

            if self._protocol_mode:
                break  # 等待协议同步完成

            # 正常模式
            if self._expected_length is None:
                header_size = 8 if self.enable_crc else 4
                if len(self._buffer) < header_size:
                    break

                length = struct.unpack_from('<I', self._buffer, 0)[0]
                if self.enable_crc:
                    expected_crc = struct.unpack_from('<I', self._buffer, 4)[0]
                    calculated = zlib.crc32(bytes(self._buffer[:4]))
                    if calculated != expected_crc:
                        print(f'Length header CRC check failed: expected {expected_crc}, got {calculated}')
                        asyncio.get_event_loop().create_task(self.send_error('LENGTH_CRC_FAILED'))
                        self._emit('error', ValueError('Length header CRC check failed'))
                        self.reset()
                        return

                self._expected_length = length
                del self._buffer[:header_size]

            total = self._expected_length + (4 if self.enable_crc else 0)
            if len(self._buffer) >= total:
                message_data = bytes(self._buffer[:self._expected_length])
                if self.enable_crc:
                    message_crc = struct.unpack_from('<I', self._buffer, self._expected_length)[0]
                    calculated = zlib.crc32(message_data)
                    if calculated != message_crc:
                        print(f'Message CRC check failed: expected {message_crc}, got {calculated}')
                        asyncio.get_event_loop().create_task(self.send_error('MESSAGE_CRC_FAILED'))
                        self._emit('error', ValueError('Message CRC check failed'))
                        self.reset()
                        return

                del self._buffer[:total]
                self._expected_length = None

                # 错误包在交给上层之前拦截处理
                if self._is_error_packet(message_data):
                    code = message_data[4]
                    print('Received error packet:', ERROR_TYPE_CODES.get(code, 'UNKNOWN_ERROR'))
                    self.reset()
                    asyncio.get_event_loop().create_task(self.send_protocol_message('RESET_CONFIRM'))
                    self._protocol_mode = False
                    continue

                self._emit('message', message_data)
            else:
                break

    def _handle_protocol_message(self, message_data: bytes) -> bool:
        if len(message_data) > 50:
            return False
        text = message_data.decode('utf8', errors='replace').strip()
        if text == 'RESET_CONFIRM':
            print('Received reset confirmation')
            self._protocol_mode = False
            self._buffer.clear()
            self._expected_length = None
            self._emit('syncComplete')
            return True
        return False

    @staticmethod
    def _is_error_packet(data: bytes) -> bool:
        return len(data) == 5 and data[:4] == ERROR_HEADER

    def reset(self):
        """清空接收状态并进入协议模式，等待对端 RESET_CONFIRM。"""
        print('[MessageStream] Resetting stream state')
        self._buffer.clear()
        self._expected_length = None
        self._protocol_mode = True

    def get_status(self):
        return {
            'bufferLength': len(self._buffer),
            'expectedLength': self._expected_length,
            'enableCRC': self.enable_crc,
            'protocolMode': self._protocol_mode,
        }

    async def destroy(self):
        if self._closed:
            return
        self._closed = True
        self._read_task.cancel()
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass
        self._buffer.clear()
        self._listeners.clear()
        print('[MessageStream] Destroyed')


async def connect_tcp(host: str, port: int, **kwargs) -> MessageStream:
    """便捷方法：建立 TCP 连接并包装为 MessageStream。"""
    reader, writer = await asyncio.open_connection(host, port)
    return MessageStream(reader, writer, **kwargs)


async def serve_tcp(handler, host: str = '127.0.0.1', port: int = 0, **kwargs):
    """便捷方法：启动 TCP 服务器，每个连接回调 handler(MessageStream)。

    返回 asyncio.Server，可通过 server.sockets[0].getsockname() 获取实际端口。
    """
    async def on_conn(reader, writer):
        stream = MessageStream(reader, writer, **kwargs)
        result = handler(stream)
        if asyncio.iscoroutine(result):
            await result

    return await asyncio.start_server(on_conn, host, port)