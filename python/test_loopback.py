"""Python 版回环测试：TCP 服务端 + 客户端在同一进程内验证全部协议路径。"""
import asyncio
import struct
import zlib

from message_stream import MessageStream, serve_tcp
from rpc_protocol import RpcProtocol, RpcError

PASS = []


def check(name, cond):
    assert cond, f'FAILED: {name}'
    PASS.append(name)
    print(f'  ok: {name}')


async def main():
    server_streams = []

    def on_server(stream):
        server_streams.append(stream)
        rpc = RpcProtocol(stream)
        rpc.register_method('add', lambda a, b: a + b)
        rpc.register_method('echo', lambda obj: obj)
        rpc.register_method('greet', lambda name: f'hello {name}')          # str -> TEXT
        rpc.register_method('blob', lambda: b'\x00\x01\x02\xff')            # bytes -> BINARY
        rpc.register_method('nothing', lambda: None)                        # None -> JSON null
        rpc.register_method('fail', lambda: (_ for _ in ()).throw(RuntimeError('boom')))
        rpc.register_method('upload', lambda data, tag: {'size': len(data), 'tag': tag,
                                                         'head': list(data[:4])})  # 二进制请求
        rpc.register_method('push', lambda: asyncio.ensure_future(rpc.send_json({'push': 1})) or None)

        async def slow():
            await asyncio.sleep(0.5)
            return 'late'
        rpc.register_method('slow', slow)

    server = await serve_tcp(on_server, port=0)
    port = server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    client_stream = MessageStream(reader, writer)
    client = RpcProtocol(client_stream)

    events = []
    client.on_event(lambda e: events.append(e))

    # 1. JSON 请求/响应
    check('json add', await client.send_request('add', [3, 4]) == 7)
    obj = {'name': '测试', 'list': [1, 2.5, True, None]}
    check('json echo unicode', await client.send_request('echo', [obj]) == obj)

    # 2. TEXT 响应
    check('text response', await client.send_request('greet', ['世界']) == 'hello 世界')

    # 3. BINARY 响应
    check('binary response', await client.send_request('blob') == b'\x00\x01\x02\xff')

    # 4. null 响应
    check('null response', await client.send_request('nothing') is None)

    # 5. 错误响应
    try:
        await client.send_request('fail')
        check('error response', False)
    except RpcError as e:
        check('error response', e.error == 'boom')

    # 6. 方法不存在
    try:
        await client.send_request('nope')
        check('method not found', False)
    except RpcError as e:
        check('method not found', 'Method not found: nope' in str(e.error))

    # 7. 二进制请求
    payload = bytes(range(256)) * 300  # ~75KB，超过默认 64KB 分片
    res = await client.send_request_binary('upload', payload, 'tag-1')
    check('binary request', res == {'size': len(payload), 'tag': 'tag-1', 'head': [0, 1, 2, 3]})

    # 8. 事件（TEXT/JSON/BINARY）：服务端 -> 客户端
    srv = server_streams[0]
    from rpc_protocol import serialize, TYPES, CONTENT_TYPES
    import uuid as _uuid
    await srv.send(serialize(TYPES['EVENT'], CONTENT_TYPES['JSON'], str(_uuid.uuid4()), {'push': 1}))
    await srv.send(serialize(TYPES['EVENT'], CONTENT_TYPES['TEXT'], str(_uuid.uuid4()), 'srv-text'))
    await srv.send(serialize(TYPES['EVENT'], CONTENT_TYPES['BINARY'], str(_uuid.uuid4()), b'\x01\x02'))
    await asyncio.sleep(0.2)
    check('events received', len(events) == 3
          and events[0] == {'contentType': 1, 'content': {'push': 1}}
          and events[1] == {'contentType': 0, 'content': 'srv-text'}
          and events[2] == {'contentType': 2, 'content': b'\x01\x02'})

    # 9. 并发请求
    results = await asyncio.gather(*[client.send_request('add', [i, i]) for i in range(50)])
    check('concurrent requests', results == [i * 2 for i in range(50)])

    # 10. CRC 错误恢复：客户端注入长度头 CRC 损坏的帧 -> 服务端检测到错误并回错误包
    #     -> 客户端回 RESET_CONFIRM -> 服务端 syncComplete，之后通信应正常
    srv_errors = []
    srv.on('error', lambda e: srv_errors.append(e))
    sync_done = asyncio.Event()
    srv.on('syncComplete', lambda: sync_done.set())

    bad = struct.pack('<I', 5) + struct.pack('<I', 0xDEADBEEF) + b'hello' + struct.pack('<I', zlib.crc32(b'hello'))
    writer.write(bad)
    await writer.drain()
    await asyncio.wait_for(sync_done.wait(), timeout=3)
    check('crc error detected', len(srv_errors) >= 1)
    check('resync then rpc works', await client.send_request('add', [10, 20]) == 30)

    # 11. RPC 超时：显式短超时触发 ETIMEDOUT，pending 清理后迟到响应不影响后续调用
    try:
        await client.send_request('slow', timeout=0.1)
        check('rpc timeout ETIMEDOUT', False)
    except RpcError as e:
        check('rpc timeout ETIMEDOUT', e.code == 'ETIMEDOUT' and 'slow' in str(e.error))
    check('after timeout rpc works', await client.send_request('add', [1, 2]) == 3)
    await asyncio.sleep(0.6)  # 等 slow 的迟到响应到达（应被静默丢弃）
    check('late response ignored', await client.send_request('add', [5, 6]) == 11)
    check('default timeout is 15s', client.default_timeout == 15.0)

    # 12. destroy 后资源清理
    await client.destroy()
    for s in server_streams:
        await s.destroy()
    server.close()
    print(f'\nALL {len(PASS)} PYTHON TESTS PASSED')


asyncio.run(main())