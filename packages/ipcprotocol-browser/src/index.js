// packages/ipcprotocol-browser/src/index.js — 子包统一出口
//
// 浏览器里只需要这一个 import：
//   import { WsTransport, RpcProtocol } from 'ipcprotocol-browser';
//
// 三层结构（各层也可以单独 import 子路径）：
//   WsTransport —— 传输层：一条 WebSocket 伪装成 net.Socket，自动加路由信封
//   MiniMessageStream —— 帧层：`[len][lenCrc][payload][payloadCrc]`，与 Node 版逐字节一致
//   MiniRpc —— 报文层：REQUEST/RESPONSE/EVENT/PING/PONG/HELLO/HELLO_ACK
//   RpcProtocol —— 把上面两层包成 Node 版 RpcProtocol 的接口形状（推荐业务代码用）

export { WsTransport } from './ws-transport.js';
export { MiniMessageStream } from './mini-stream.js';
export { MiniRpc, MiniRpcError } from './mini-rpc.js';
export {
    RpcProtocol,
    RpcError,
    RpcProtocolTypes,
    RpcContentTypes,
} from './rpc-protocol-compat.js';

/** 与主仓库版本对齐 */
export const VERSION = '1.2.0';
