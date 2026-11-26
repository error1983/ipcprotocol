"""常驻子进程客户端示例：自动握手 + 断线重连 + 优雅停机。

用途：作为 Node 主进程拉起的长期子进程，连上主进程后常驻。
自带能力：
  1. 每次（重）连都自动 `await rpc.hello()` —— 完成 token 鉴权与能力协商；
  2. 连接断开（对端退出、管道/套接字断开、心跳超时）自动指数退避重连，
     退避带随机抖动，避免主进程重启时的重连风暴；
  3. 注册 `system.shutdown`，主进程在退出前通过 RPC 下发即可让本进程优雅退出
     （Windows 上这是唯一可靠方式：Windows 没有真正的 SIGTERM）；
  4. POSIX 上同时支持 SIGTERM / SIGINT；收到信号后不再重连，关闭连接后退出。

用法:
    python examples/reconnect_client_grace.py 127.0.0.1:9000
    python examples/reconnect_client_grace.py --host 127.0.0.1 --port 9000
    IPC_TOKEN=xxx python examples/reconnect_client_grace.py 127.0.0.1:9000

主进程侧（Node）优雅关停：
    await rpc.call('system.shutdown', ['主进程退出']);   // 等待子进程自己关连接退出
    // 兜底：3s 内没退出再 SIGKILL

说明：协议层心跳（PING/PONG）由 RpcProtocol 内置，`hello()` 后随能力协商自动开启，
本示例不再自行发心跳，避免与内置心跳重复。
"""
import argparse
import asyncio
import os
import random
import signal
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'python'))

from message_stream import connect_tcp          # noqa: E402
from rpc_protocol import RpcProtocol            # noqa: E402


def log(text: str) -> None:
    print(f'[{time.strftime("%H:%M:%S")}] {text}', flush=True)


class GracefulReconnectClient:
    """常驻客户端：连上 -> 等断开/停机信号 -> 退避 -> 重连。"""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.host = args.host
        self.port = args.port
        self.stop = asyncio.Event()          # 停机标记：置位后不再重连
        self.closed = asyncio.Event()        # 当前连接的断开标记
        self.loop = None
        self.backoff = args.backoff_min
        self.attempts = 0                    # 连续失败次数（连上即清零）
        self.connects = 0                    # 累计成功连接次数
        self.connected_at = None
        self.last_error = None

    # ---------------- 信号与父进程监视 ----------------
    def _request_stop(self, reason: str) -> None:
        if self.stop.is_set():
            return
        log(f'收到 {reason}，准备优雅退出')
        self.stop.set()

    def _install_signals(self) -> None:
        """POSIX 用 add_signal_handler；Windows 只对 Ctrl-C 有效，其余靠 system.shutdown。"""
        self.loop = asyncio.get_running_loop()
        installed = []
        for name in ('SIGINT', 'SIGTERM'):
            sig = getattr(signal, name, None)
            if sig is None:
                continue
            try:
                self.loop.add_signal_handler(sig, self._request_stop, name)
                installed.append(name)
            except (NotImplementedError, RuntimeError):
                pass
        if not installed:
            # Windows（ProactorEventLoop）只能同步注册 Ctrl-C
            try:
                signal.signal(signal.SIGINT, lambda *_: self._request_stop('SIGINT'))
                installed.append('SIGINT')
            except (ValueError, OSError):
                pass
        if installed:
            log(f'已注册信号: {", ".join(installed)}')
            if os.name == 'nt':
                log('Windows 下 SIGTERM 不可达，请让主进程改用 RPC 方法 system.shutdown')

    async def _watch_parent(self) -> None:
        """可选：主进程消失后自行退出，避免变成孤儿常驻进程。"""
        parent = os.getppid()
        while not self.stop.is_set():
            await asyncio.sleep(2.0)
            if os.getppid() != parent:
                log(f'父进程已退出（{parent} -> {os.getppid()}），自行停机')
                self.stop.set()
                return

    # ---------------- 注册给对方调用的方法 ----------------
    def _on_shutdown(self, reason=None):
        """主进程通过 RPC 下发的停机指令；返回慢速回执后本进程才关闭连接。"""
        self._request_stop(f'system.shutdown(reason={reason!r})')
        return {'ok': True, 'pid': os.getpid(), 'reason': reason}

    def _on_status(self):
        uptime = time.monotonic() - self.connected_at if self.connected_at else 0.0
        return {
            'pid': os.getpid(),
            'uptimeSec': round(uptime, 3),
            'connects': self.connects,
            'attempts': self.attempts,
            'lastError': self.last_error,
        }

    # ---------------- 单次连接 ----------------
    async def _connect_once(self) -> RpcProtocol:
        stream = await asyncio.wait_for(
            connect_tcp(self.host, self.port), timeout=self.args.connect_timeout)
        # token 不显式传时自动继承环境变量 IPC_TOKEN（与 Node 主进程注入方式一致）
        rpc = RpcProtocol(
            stream,
            default_timeout=self.args.rpc_timeout,
            auth_token=self.args.token or None,
        )
        rpc.register_method('system.shutdown', self._on_shutdown)
        rpc.register_method('system.status', self._on_status)
        await asyncio.wait_for(rpc.hello(), timeout=self.args.handshake_timeout)
        return rpc

    async def _serve(self, rpc: RpcProtocol) -> None:
        """阻塞到「连接断开」或「收到停机指令」，二者先到先返回。"""
        self.closed = asyncio.Event()

        def on_error(err):
            self.last_error = f'{type(err).__name__}: {err}'
            log(f'链路异常，准备重连：{self.last_error}')
            self.closed.set()

        def on_heartbeat_disabled(reason):
            log(f'心跳已关闭：{reason}')

        def on_event(evt):
            log(f'收到事件 type={evt.get("contentType")} content={evt.get("content")!r}')

        rpc.on('error', on_error)
        rpc.on('heartbeatDisabled', on_heartbeat_disabled)
        rpc.on('event', on_event)

        wait_tasks = [asyncio.create_task(self.closed.wait()),
                      asyncio.create_task(self.stop.wait())]
        try:
            await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in wait_tasks:
                if not task.done():
                    task.cancel()

    # ---------------- 主循环 ----------------
    def _next_delay(self) -> float:
        self.attempts += 1
        base = min(self.backoff, self.args.backoff_max)
        # 抖动：0 ~ 50% 的 base，既避免同时重连，也不至于让退避失效
        delay = min(base + random.uniform(0, base * 0.5), self.args.backoff_max)
        self.backoff = min(self.backoff * 2, self.args.backoff_max)
        return delay

    async def run(self) -> int:
        self._install_signals()
        if self.args.exit_with_parent:
            asyncio.create_task(self._watch_parent())

        while not self.stop.is_set():
            rpc = None
            try:
                rpc = await self._connect_once()
                self.connects += 1
                self.attempts = 0
                self.backoff = self.args.backoff_min
                self.connected_at = time.monotonic()
                self.last_error = None
                log(f'已连接 {self.host}:{self.port}，握手完成（peer caps={rpc.server_capabilities}）')
                await self._serve(rpc)
                if not self.stop.is_set():
                    log('连接正常结束')
            except asyncio.CancelledError:
                raise
            except Exception as err:  # 连接失败 / 握手失败 / 鉴权失败
                self.last_error = f'{type(err).__name__}: {err}'
                log(f'连接未建立：{self.last_error}')
            finally:
                if rpc is not None:
                    try:
                        await asyncio.wait_for(rpc.destroy(), timeout=self.args.close_timeout)
                    except Exception:
                        pass
                self.connected_at = None

            if self.stop.is_set():
                break
            delay = self._next_delay()
            log(f'{delay:.2f}s 后重连（连续失败 {self.attempts} 次）')
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

        log('已停止，子进程退出')
        return 0


def parse_target(value: str):
    if ':' in value:
        host, _, port = value.rpartition(':')
        return host or '127.0.0.1', int(port)
    return '127.0.0.1', int(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='常驻子进程客户端示例：自动握手、断线重连、system.shutdown 优雅停机')
    parser.add_argument('target', nargs='?', default='127.0.0.1:9000',
                        help='host:port 或仅端口，默认 127.0.0.1:9000')
    parser.add_argument('--host', default=None)
    parser.add_argument('--port', type=int, default=None)
    parser.add_argument('--token', default=None, help='鉴权 token，缺省读环境变量 IPC_TOKEN')
    parser.add_argument('--connect-timeout', type=float, default=3.0)
    parser.add_argument('--handshake-timeout', type=float, default=5.0)
    parser.add_argument('--rpc-timeout', type=float, default=30.0, help='单次 RPC 默认超时（秒）')
    parser.add_argument('--close-timeout', type=float, default=2.0)
    parser.add_argument('--backoff-min', type=float, default=0.5)
    parser.add_argument('--backoff-max', type=float, default=30.0)
    parser.add_argument('--exit-with-parent', action='store_true',
                        help='父进程退出后自行停机（推荐给被拉起的子进程）')
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    host, port = parse_target(args.target)
    args.host = args.host or host
    args.port = args.port if args.port is not None else port
    args.backoff_max = max(args.backoff_max, args.backoff_min)

    client = GracefulReconnectClient(args)
    try:
        return asyncio.run(client.run())
    except KeyboardInterrupt:
        return 0


if __name__ == '__main__':
    sys.exit(main())
