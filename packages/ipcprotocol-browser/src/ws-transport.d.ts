// 类型声明：WebSocket 传输适配器
export interface WsTransportOptions {
    /** 本端 peerId，网关用它寻址；同一 id 重复连接会被拒（HTTP 409） */
    peer: string;
    /** 对端 peerId；空串表示广播 */
    to?: string;
    /** 断线自动重连。⚠️ 重连后旧的流/协议对象全部失效，必须整条重建 */
    autoReconnect?: boolean;
    reconnectMinDelayMs?: number;
    reconnectMaxDelayMs?: number;
    /** 未连接时允许缓存的写入字节上限 */
    maxPendingBytes?: number;
    /** WS 发送缓冲超过它 write() 返回 false，让上层等 drain */
    highWaterMark?: number;
}

export type WsTransportEventName =
    | 'open'
    | 'connect'
    | 'data'
    | 'drain'
    | 'close'
    | 'end'
    | 'offline'
    | 'reconnecting'
    | 'error';

export type Listener = (...args: any[]) => void;

export declare class WsTransport {
    constructor(url: string, options: WsTransportOptions);

    readonly url: string;
    peer: string;
    to: string;
    destroyed: boolean;
    readonly readyState: number;

    /** 运行中切换目标 peer（复用同一条连接） */
    setTarget(peerId: string): void;
    write(chunk: Uint8Array | ArrayBuffer | ArrayBufferView): boolean;
    end(): void;
    destroy(): void;
    setNoDelay(noDelay?: boolean): void;

    on(event: WsTransportEventName | string, listener: Listener): this;
    once(event: string, listener: Listener): this;
    off(event: string, listener?: Listener): this;
    listenerCount(event: string): number;
}

export default WsTransport;
