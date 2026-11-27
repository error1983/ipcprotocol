// 兼容旧路径的转发文件：实现已移到 packages/ipcprotocol-browser/src/mini-stream.js
// 新代码建议直接 `import { MiniMessageStream } from 'ipcprotocol-browser'`
export * from '../../packages/ipcprotocol-browser/src/mini-stream.js';
export { default } from '../../packages/ipcprotocol-browser/src/mini-stream.js';
