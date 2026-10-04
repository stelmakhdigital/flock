// In-core OpenAI-responses shim for the codex runtime.
//
// codex sends its system prompt as `developer`-role messages inside the
// responses `input`; vLLM's /v1/responses rejects that role (400 "Unexpected
// message role.") but accepts the same text in `instructions`. The shim sits
// at 127.0.0.1:<FLOCK_PORT+11>, rewrites /v1/responses request bodies
// (developer -> instructions, codex-protocol.rewriteResponsesRequest) and
// proxies everything else verbatim — streaming included (raw chunk pipe).
// Local-only: no auth, no TLS (the pod's codex is the only client).

import http from 'node:http';
import { rewriteResponsesRequest } from './codex-protocol.js';

export interface CodexShimHandle {
  port: number;
  stop: () => Promise<void>;
}

export function startCodexShim(port: number, upstream: string): CodexShimHandle {
  const upstreamUrl = new URL(upstream.replace(/\/$/, ''));
  const server = http.createServer((req, res) => {
    let body = '';
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => {
      chunks.push(c);
      body += c.toString('utf8');
      // a sane cap: a poisoned/huge body is an error, not a buffer
      if (body.length > 64 * 1024 * 1024) {
        res.writeHead(413);
        res.end();
        req.destroy();
      }
    });
    req.on('end', () => {
      let forward = Buffer.concat(chunks);
      if (req.method === 'POST' && req.url?.startsWith('/v1/responses') && forward.length) {
        const rw = rewriteResponsesRequest(body);
        if (rw.changed) forward = Buffer.from(rw.body, 'utf8');
      }
      const up = http.request(
        {
          hostname: upstreamUrl.hostname,
          port: upstreamUrl.port || 80,
          path: req.url ?? '/',
          method: req.method ?? 'GET',
          headers: { ...req.headers, host: upstreamUrl.host, 'content-length': String(forward.length) },
        },
        (upRes) => {
          res.writeHead(upRes.statusCode ?? 502, upRes.headers);
          upRes.pipe(res);
        },
      );
      up.on('error', (e) => {
        res.writeHead(502, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: { message: `shim upstream error: ${e.message}` } }));
      });
      up.end(forward);
    });
  });
  server.listen(port, '127.0.0.1');
  return {
    port,
    stop: () =>
      new Promise((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections?.();
      }),
  };
}
