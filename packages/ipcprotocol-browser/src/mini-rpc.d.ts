// 类型声明：前端 RPC 层（低层 API，与 Node 版同形请看 ./compat）
import type { MiniMessageStream } from './mini-stream.js';

export interface MiniRpcOptions {
    /** 客户端凭据；服务端开了鉴权时必须提供 */
    authToken?: string | null;
    /** 服务端要求客户端出示的 token */
    requiredToken?: string | null;
    /** 服务端接受的 token 集合（优先于 requiredToken） */
    requiredTokens?: string[];
    /** HELLO 里声明的能力，默认 ['ping','binary','event'] */
    capabilities?: string[];
    /** 实现名，出现在 HELLO 里 */
    implName?: string;
    /** 空闲多久发 PING；0 关闭心跳 */
    heartbeatIntervalMs?: number;
    /** 多久收不到任何入站消息判定断链；0 关闭 */
    heartbeatTimeoutMs?: number;
    /** 仅当对端声明 ping 能力才发心跳 */
    heartbeatRequireCapability?: boolean;
    /** sendRequest 默认超时 */
    defaultTimeoutMs?: number;
}

export interface RpcEventMessage {
    /** 0=TEXT 1=JSON 2=BINARY */
    contentType: number;
    content: string | unknown | Uint8Array;
}

export type RpcListener = (...args: any[]) => void;

export declare class MiniRpcError extends Error {
    constructor(message: string, options?: { code?: string; remote?: boolean });
    code: string;
    remote: boolean;
    name: string;
}

export declare class MiniRpc {
    constructor(stream: MiniMessageStream, options?: MiniRpcOptions);

    readonly stream: MiniMessageStream;
    authToken: string | null;
    requiredTokens: Set<string>;
    capabilities: string[];
    implName: string;
    peerCapabilities: string[] | null;
    serverCapabilities: string[] | null;
    heartbeatIntervalMs: number;
    heartbeatTimeoutMs: number;
    heartbeatRequireCapability: boolean;
    heartbeatDisabledReason: string | null;
    defaultTimeoutMs: number;

    hello(timeoutMs?: number): Promise<{ version: number; ok: true; caps: string[]; impl: string }>;
    sendRequest<T = unknown>(method: string, params?: unknown[] | unknown, timeoutMs?: number): Promise<T>;
    sendRequestBinary<T = unknown>(method: string, buffer: Uint8Array | ArrayBuffer, ...args: any[]): Promise<T>;
    registerMethod(name: string, handler: (...args: any[]) => any): void;
    sendText(text: string): Promise<void>;
    sendJson(data: unknown): Promise<void>;
    sendBinary(bytes: Uint8Array | ArrayBuffer): Promise<void>;
    destroy(): void;

    on(event: 'event', listener: (msg: RpcEventMessage) => void): this;
    on(event: 'error', listener: (err: MiniRpcError) => void): this;
    on(event: 'close' | 'heartbeatDisabled' | string, listener: RpcListener): this;
    once(event: string, listener: RpcListener): this;
    off(event: string, listener?: RpcListener): this;
    listenerCount(event: string): number;
}

export default MiniRpc;
