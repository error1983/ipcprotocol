// 类型声明：子包统一出口
export { WsTransport } from './ws-transport.js';
export type { WsTransportOptions, WsTransportEventName } from './ws-transport.js';
export { MiniMessageStream } from './mini-stream.js';
export type { MiniMessageStreamOptions } from './mini-stream.js';
export { MiniRpc, MiniRpcError } from './mini-rpc.js';
export type { MiniRpcOptions, RpcEventMessage } from './mini-rpc.js';
export { RpcProtocol, RpcError, RpcProtocolTypes, RpcContentTypes } from './rpc-protocol-compat.js';
export type {
    RpcProtocolOptions,
    HelloAck,
    DecodedMessage,
    DecodedBinaryRequest,
} from './rpc-protocol-compat.js';

export declare const VERSION: string;
