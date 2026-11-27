// examples/browser/serve.js — 零依赖静态服务器（只为在浏览器里打开 demo）
// 用法: node examples/browser/serve.js [port]
// 然后访问 http://127.0.0.1:8080/examples/browser/rpc-demo.html?peer=alice&to=bob
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const PORT = Number(process.argv[2] || 8080);
const MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.mjs': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.md': 'text/markdown; charset=utf-8',
    '.png': 'image/png',
    '.svg': 'image/svg+xml',
};

const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    let pathname = decodeURIComponent(url.pathname);
    if (pathname === '/') {
        res.writeHead(302, { location: '/examples/browser/rpc-demo.html?peer=alice&to=bob' });
        res.end();
        return;
    }

    const target = path.resolve(ROOT, '.' + pathname);
    if (!target.startsWith(ROOT + path.sep)) {   // 防目录穿越
        res.writeHead(403).end('forbidden');
        return;
    }

    try {
        const stat = await fs.stat(target);
        if (stat.isDirectory()) {
            res.writeHead(404).end('not found');
            return;
        }
        const body = await fs.readFile(target);
        res.writeHead(200, {
            'content-type': MIME[path.extname(target).toLowerCase()] || 'application/octet-stream',
            'cache-control': 'no-store',
        });
        res.end(body);
    } catch {
        res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }).end(`not found: ${pathname}`);
    }
});

server.listen(PORT, '127.0.0.1', () => {
    console.log(`静态服务: http://127.0.0.1:${PORT}/examples/browser/rpc-demo.html?peer=alice&to=bob`);
    console.log(`双标签页演示: 另一个标签用 ?peer=bob&to=alice`);
});
