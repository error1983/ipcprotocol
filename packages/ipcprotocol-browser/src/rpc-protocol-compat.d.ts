// 类型声明：与 Node 版 RpcProtocol 同形的浏览器实现
import type { MiniMessageStream } from './mini-stream.js';
import type { MiniRpcError } from './mini-rpc.js';

/** 与 Node 版 RpcError 同形；实际就是 MiniRpcError，instanceof 判定成立 */
export declare const RpcError: typeof MiniRpcError;

export interface RpcProtocolOptions {
    heartbeatIntervalMs?: number;
    heartbeatTimeoutMs?: number;
    /** 浏览器里恒为 true，设 false 会被忽略并打印一次警告 */
    heartbeatSkipWhilePending?: boolean;
    authToken?: string | null;
    requiredToken?: string | null;
    requiredTokens?: string[];
    capabilities?: string[];
    implName?: string;
    heartbeatRequireCapability?: boolean;
    defaultTimeoutMs?: number;
}

export declare const RpcProtocolTypes: Readonly<{
    REQUEST: 0; RESPONSE: 1; EVENT: 2; PING: 3; PONG: 4; HELLO: 5; HELLO_ACK: 6;
}>;

export declare const RpcContentTypes: Readonly<{ TEXT: 0; JSON: 1; BINARY: 2 }>;

export interface DecodedMessage {
    type: number;
    contentType: number;
    id: string;
    content: string | unknown | Uint8Array;
}

export interface DecodedBinaryRequest {
    method: string;
    params: unknown[];
    binaryData: Uint8Array;
}

export interface HelloAck {
    version: number;
    ok: true;
    caps: string[];
    impl: string;
}

export interface RpcEventMessage {
    contentType: number;
    content: string | unknown | Uint8Array;
}

export type RpcListener = (...args: any[]) => void;

export declare class RpcProtocol {
    constructor(messageStream: MiniMessageStream, options?: RpcProtocolOptions);

    static TYPES: typeof RpcProtocolTypes;
    static CONTENT_TYPES: typeof RpcContentTypes;
    static serialize(type: number, contentType: number, id: string, data: unknown): Uint8Array;
    static deserialize(bytes: Uint8Array): DecodedMessage;
    static encodeBinaryRequest(method: string, params: unknown, binaryData: Uint8Array): Uint8Array;
    static decodeBinaryRequest(bytes: Uint8Array): DecodedBinaryRequest;

    readonly messageStream: MiniMessageStream;
    /** 与 Node 版同形：普通对象；直接赋值也会同步到内部 */
    handlers: Record<string, (...args: any[]) => any>;
    readonly pendingRequests: Map<string, unknown>;

    readonly capabilities: string[];
    readonly implName: string;
    readonly peerCapabilities: string[] | null;
    readonly serverCapabilities: string[] | null;
    readonly heartbeatDisabledReason: string | null;
    readonly heartbeatIntervalMs: number;
    readonly heartbeatTimeoutMs: number;
    readonly heartbeatRequireCapability: boolean;
    readonly heartbeatSkipWhilePending: boolean;
    readonly authToken: string | null;
    readonly requiredToken: string | null;
    readonly requiredTokens: Set<string>;
    readonly defaultTimeoutMs: number;

    hello(timeoutMs?: number): Promise<HelloAck>;
    sendRequest<T = unknown>(method: string, params?: unknown[] | unknown, to?: string | number, timeoutMs?: number): Promise<T>;
    sendRequestBinary<T = unknown>(method: string, buffer: Uint8Array | ArrayBuffer, ...args: any[]): Promise<T>;
    sendResponse(id: string, response: unknown, contentType?: number): Promise<void>;
    sendRaw(type: number, contentType: number, id: string, data: unknown): Promise<void>;
    registerMethod(name: string, handler: (...args: any[]) => any): void;
    onEvent(listener: (msg: RpcEventMessage) => void): void;
    sendText(text: string): Promise<void>;
    sendJson(data: unknown): Promise<void>;
    sendBinary(bytes: Uint8Array | ArrayBuffer): Promise<void>;
    setRequiredTokens(tokens: string[]): void;
    handleMessage(bytes: Uint8Array): void;
    destroy(): void;

    on(event: 'event', listener: (msg: RpcEventMessage) => void): this;
    on(event: 'error', listener: (err: MiniRpcError) => void): this;
    on(event: 'heartbeatDisabled' | 'close' | 'syncComplete' | string, listener: RpcListener): this;
    once(event: string, listener: RpcListener): this;
    off(event: string, listener?: RpcListener): this;
    removeAllListeners(event?: string): this;
    listenerCount(event: string): number;
}

export default RpcProtocol;
