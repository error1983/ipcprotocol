// modules/nativeService.js — 修复版
import { spawn, exec } from 'child_process';
import { EventEmitter } from 'events';
import { MessageStream } from './MessageStream.js';
import { RpcProtocol } from './RpcProtocol.js';
import net from 'net';
import fs from 'fs';

export class NativeService extends EventEmitter {
    constructor(name, options) {
        super();
        const { cmd, args = [], env = {}, windowsHide = true, stdio = ['pipe', 'pipe', 'pipe'] } = options;
        this.name = name;
        this.cmd = cmd;
        this.args = args;
        this.env = env;
        this.windowsHide = windowsHide;
        this.stdio = stdio;

        this.process = null;
        this.pipeServer = null;
        this.pipeName = options.pipeName || this.generatePipeName(this.name);
        this.rpcProtocol = null;
        this.handlers = {};

        this.isConnected = false;
        this._stopping = false;
        this._connectWaiters = [];

        // 兜底：避免上层没挂 error 监听时把进程打崩
        this.on('error', (err) => console.error(`[${this.name}] error:`, err?.code || '', err?.message || err));

        this.call = new Proxy({}, {
            get: (_target, method) => {
                return async (...args) => {
                    await this.waitForConnection();
                    if (!this.rpcProtocol) throw new RpcErrorLocal(`[${this.name}] RPC session lost`);
                    if (args.length > 0 && (Buffer.isBuffer(args[0]) || args[0] instanceof Uint8Array)) {
                        const buffer = Buffer.isBuffer(args[0]) ? args[0] : Buffer.from(args[0]);
                        return this.sendRequestBinary(method, buffer, ...args.slice(1));
                    }
                    return this.sendRequest(method, args);
                };
            }
        });

        this.startServer();
    }

    generatePipeName(baseName) {
        const pid = process.pid;
        const timestamp = Date.now();
        const randomId = Math.floor(Math.random() * 10000);
        if (process.platform === 'win32') {
            const sessionId = process.env.SESSIONNAME ? `_${process.env.SESSIONNAME}` : '';
            return `\\\\.\\pipe\\native_service_${baseName}${sessionId}_${pid}_${timestamp}_${randomId}`;
        }
        return `/tmp/native_service_${baseName}_${pid}_${timestamp}_${randomId}.sock`;
    }

    // ===== 连接等待 =====
    waitForConnection(timeout = 30000) {
        if (this.isConnected) return Promise.resolve();
        return new Promise((resolve, reject) => {
            const waiter = { resolve, reject, timer: null, onConnect: null, onError: null };
            const cleanup = () => {
                clearTimeout(waiter.timer);
                this.off('connect', waiter.onConnect);
                this.off('error', waiter.onError);
                this._connectWaiters = this._connectWaiters.filter(w => w !== waiter);
            };
            waiter.onConnect = () => { cleanup(); resolve(); };
            waiter.onError = (err) => { cleanup(); reject(err); };
            waiter.timer = setTimeout(() => {
                cleanup();
                reject(new RpcErrorLocal(`[${this.name}] connection timeout after ${timeout}ms`, 'ETIMEDOUT'));
            }, timeout);
            waiter.timer.unref?.();
            this.once('connect', waiter.onConnect);
            this.once('error', waiter.onError);
            this._connectWaiters.push(waiter);
        });
    }

    async waitForReady(timeout = 30000) {
        if (!this.process) this.start();
        await this.waitForConnection(timeout);
        return true;
    }

    registerMethod(name, handler) {
        if (!name || typeof name !== 'string') {
            console.error(`[${this.name}] Invalid method name: ${name}`);
            return;
        }
        if (typeof handler !== 'function') {
            console.error(`[${this.name}] Invalid handler for: ${name}`);
            return;
        }
        this.handlers[name] = handler;
        if (this.rpcProtocol) this.rpcProtocol.registerMethod(name, handler);
    }

    // ===== 发送（失败时给 Promise，而不是 undefined） =====
    sendRequest(method, params, to) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        if (!method || typeof method !== 'string') return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid method`, 'EINVAL'));
        return this.rpcProtocol.sendRequest(method, params, to);
    }

    sendText(text) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        if (typeof text !== 'string') return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid text`, 'EINVAL'));
        return this.rpcProtocol.sendText(text);
    }

    sendJson(data) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        if (typeof data !== 'object' || data === null) return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid json`, 'EINVAL'));
        return this.rpcProtocol.sendJson(data);
    }

    sendBinary(data) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        const buf = Buffer.isBuffer(data) ? data : (data instanceof Uint8Array ? Buffer.from(data) : null);
        if (!buf) return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid binary data`, 'EINVAL'));
        return this.rpcProtocol.sendBinary(buf);
    }

    sendRequestBinary(method, buffer, ...args) {
        if (!this.rpcProtocol) return Promise.reject(new RpcErrorLocal(`[${this.name}] RPC not initialized`, 'ENOTCONN'));
        if (typeof method !== 'string') return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid method`, 'EINVAL'));
        const buf = Buffer.isBuffer(buffer) ? buffer : (buffer instanceof Uint8Array ? Buffer.from(buffer) : null);
        if (!buf) return Promise.reject(new RpcErrorLocal(`[${this.name}] invalid buffer`, 'EINVAL'));
        return this.rpcProtocol.sendRequestBinary(method, buf, ...args);
    }

    // ===== 服务端 =====
    startServer() {
        if (this.pipeServer) return;

        if (process.platform !== 'win32' && fs.existsSync(this.pipeName)) {
            try { fs.unlinkSync(this.pipeName); } catch { }
        }

        const server = net.createServer((socket) => {
            // ★ 单客户端门禁：拒绝第二个连接，避免覆盖已有链路
            if (this.isConnected && this.rpcProtocol) {
                console.warn(`[${this.name}] extra client rejected`);
                socket.destroy();
                return;
            }

            socket.setNoDelay?.(true);
            const messageStream = new MessageStream({ socket });
            const rpc = new RpcProtocol(messageStream);
            this.rpcProtocol = rpc;

            for (const [name, handler] of Object.entries(this.handlers)) rpc.registerMethod(name, handler);

            rpc.on('event', (message) => this.emit('event', message));
            rpc.on('error', (err) => this.emit('error', err));

            socket.on('close', () => {
                const wasActive = this.rpcProtocol === rpc;
                this.isConnected = false;
                if (wasActive) {
                    this.rpcProtocol = null;
                    try { rpc.destroy(); } catch { }
                }
                this.emit('disconnect');
            });

            socket.on('error', (err) => {
                this.isConnected = false;
                this.emit('error', err);
            });

            this.isConnected = true;
            this.emit('connect', socket);

            this.sendText('hello from native service').catch(() => { });
        });

        server.listen(this.pipeName, () => {
            console.log(`[${this.name}] server listening on ${this.pipeName}`);
        });

        server.on('error', (err) => {
            console.error(`[${this.name}] pipe server error:`, err.message);
            this.emit('error', err);
        });

        this.pipeServer = server;
    }

    cleanupRpcResources() {
        if (this.rpcProtocol) {
            try { this.rpcProtocol.destroy(); } catch { }
            this.rpcProtocol = null;
        }
    }

    stopServer() {
        if (!this.pipeServer) return;
        this.cleanupRpcResources();
        this.isConnected = false;
        const server = this.pipeServer;
        this.pipeServer = null;
        try {
            server.close(() => {
                if (process.platform !== 'win32' && fs.existsSync(this.pipeName)) {
                    try { fs.unlinkSync(this.pipeName); } catch { }
                }
            });
        } catch (err) {
            console.error(`[${this.name}] close server error:`, err.message);
        }
    }

    // ===== 子进程 =====
    start() {
        if (this.process) {
            console.warn(`[${this.name}] already running`);
            return false;
        }
        if (!this.pipeServer) this.startServer();

        this.process = spawn(this.cmd, this.args, {
            env: { ...process.env, ...this.env, PIPE_NAME: this.pipeName },
            windowsHide: this.windowsHide,
            stdio: this.stdio,
        });

        if (this.process.stdout) this.process.stdout.on('data', d => console.log(`[${this.name} stdout]:`, d.toString().trimEnd()));
        if (this.process.stderr) this.process.stderr.on('data', d => console.error(`[${this.name} stderr]:`, d.toString().trimEnd()));

        this.process.on('error', (err) => this.emit('error', err));
        this.process.on('close', (code) => {
            console.log(`[${this.name}] exited code=${code}`);
            this.process = null;
            this.isConnected = false;
        });
        return true;
    }

    stop(timeout = 2000) {
        if (this._stopping) return false;
        if (!this.process) return false;
        this._stopping = true;

        const proc = this.process;
        const pid = proc.pid;
        this.cleanupRpcResources();
        this.isConnected = false;

        let finished = false;
        const finish = () => {
            if (finished) return;
            finished = true;
            this._stopping = false;
            this.process = null;
            this.stopServer();
            this.emit('stopped');
        };

        try { proc.kill('SIGTERM'); } catch { }

        const killTimer = setTimeout(() => {
            if (finished) return;
            try { proc.kill('SIGKILL'); } catch {
                if (process.platform === 'win32') exec(`taskkill /PID "${pid}" /T /F`, () => finish());
                else finish();
                return;
            }
            setTimeout(finish, 800);
        }, timeout);
        killTimer.unref?.();

        proc.once('exit', () => { clearTimeout(killTimer); finish(); });
        return true;
    }

    async restart(stopTimeout = 2000) {
        if (this.process) {
            const stopped = new Promise(r => this.once('stopped', r));
            this.stop(stopTimeout);
            await Promise.race([stopped, new Promise(r => setTimeout(r, stopTimeout + 1500))]);
        }
        if (!this.pipeServer) this.startServer();
        if (!this.start()) return false;
        try {
            await this.waitForConnection();
            return true;
        } catch (e) {
            console.error(`[${this.name}] restart connect failed:`, e.message);
            return false;
        }
    }

    getRpc() { return this.rpcProtocol; }

    getProcessStatus() {
        const status = {
            running: !!this.process,
            pid: this.process?.pid ?? null,
            isConnected: this.isConnected,
        };
        if (this.process) {
            status.killed = this.process.killed ?? false;
            if (this.process.exitCode !== null) status.exitCode = this.process.exitCode;
            if (this.process.signalCode !== null) status.signalCode = this.process.signalCode;
        }
        return status;
    }
}

class RpcErrorLocal extends Error {
    constructor(message, code = 'EIPC') {
        super(message);
        this.name = 'RpcError';
        this.code = code;
    }
}
