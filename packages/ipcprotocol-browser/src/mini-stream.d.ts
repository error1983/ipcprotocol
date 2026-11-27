// 类型声明：前端帧层
import type { WsTransport } from './ws-transport.js';

export interface MiniMessageStreamOptions {
    /** 与对端必须一致；默认 true，关闭后线格式只有 [len][payload] */
    enableCrc?: boolean;
    /** 单帧载荷上限，超出直接 ETOOBIG */
    maxBufferSize?: number;
    /** 重同步等待对端 RESET_CONFIRM 的超时 */
    syncTimeoutMs?: number;
}

export type Listener = (...args: any[]) => void;

export declare class MiniMessageStream {
    constructor(link: WsTransport | any, options?: MiniMessageStreamOptions);

    readonly link: any;
    enableCrc: boolean;
    maxBufferSize: number;
    syncTimeoutMs: number;

    /** 发送一整帧（自动加长度头与 CRC） */
    send(payload: Uint8Array | ArrayBuffer): Promise<void>;
    destroy(): void;

    /** 'message' 回调拿到的是完整报文（一帧的载荷） */
    on(event: 'message' | 'error' | 'close' | 'syncComplete' | string, listener: Listener): this;
    once(event: string, listener: Listener): this;
    off(event: string, listener?: Listener): this;
    listenerCount(event: string): number;
}

export default MiniMessageStream;
