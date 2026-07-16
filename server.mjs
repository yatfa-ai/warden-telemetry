// Minimal HTTP server wiring for the ingest keystone (WARDEN-547). Plain
// `node:http` — no framework (roadmap invariant: minimal & self-hostable).
//
// This file is the ONLY place that (a) loads the vendored `./schema.ts` via
// Node's native type-stripping (requires Node ≥ 22.6; target 24) and (b) opens
// the real persistence file. Both are INJECTED into `ingest()`, so the pure
// pipeline — and every test — depends on neither.
//
// Run:  node server.mjs
// Env:  PORT   (default 7421)   — the port a warden client's endpointUrl points at
//       STORE  (default ./telemetry.ndjson) — the durable NDJSON store path
//
// The receiver owns its routes: POST /ingest (write) and GET /summary (read —
// the maintainer aggregate surface, WARDEN-567). The client POSTs the batch
// verbatim to its configured endpointUrl (e.g. http://host:7421/ingest) and
// never rewrites the host, so these route paths are the receiver's to define.

import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { SCHEMA_VERSION, validateEvent } from './schema.ts';
import { createNdjsonStore, fileSink, fileSource } from './store.mjs';
import { ingest } from './ingest.mjs';
import { summarize } from './summary.mjs';

export const DEFAULT_PORT = 7421;
export const DEFAULT_STORE_PATH = new URL('./telemetry.ndjson', import.meta.url).pathname;
export const INGEST_PATH = '/ingest';
export const SUMMARY_PATH = '/summary';

// The default shared-schema deps — the vendored schema.ts, loaded once at module
// init via Node's native type-stripping. Overridable per-handler for tests.
export const DEFAULT_SCHEMA = { SCHEMA_VERSION, validateEvent };

/**
 * Read a request body fully into a string. Telemetry batches are small, bounded,
 * and fire-and-forget, so buffering the whole body is fine (no streaming parse).
 * Exported so the handler is unit-testable without binding a socket.
 *
 * @param {import('node:http').IncomingMessage} req
 * @returns {Promise<string>}
 */
export function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify(body));
}

/**
 * Build the request handler. `store` and `schema` are injected so the handler is
 * testable with a capturing store and WITHOUT a live port (tests call the handler
 * directly with a fake req/res). The defaults wire the real file-backed store and
 * the vendored schema for production.
 *
 * @param {{ store: object, schema?: { SCHEMA_VERSION: number, validateEvent: (e: unknown) => boolean } }} deps
 * @returns {(req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => Promise<void>}
 */
export function createRequestHandler({ store, schema = DEFAULT_SCHEMA } = {}) {
  if (!store) throw new TypeError('createRequestHandler: `store` is required');
  return async (req, res) => {
    const { pathname } = new URL(req.url, 'http://localhost');

    // GET /summary — the maintainer read surface (WARDEN-567). Returns AGGREGATES
    // of the already-validated, already-redacted events persisted by POST /ingest
    // (counts / per-type / top error names / schema-version histogram only —
    // never raw events, never extended-tier names). No request body is read.
    //
    // AUTH IS DELIBERATELY DEFERRED to its own later slice, per the README's own
    // ordering (query/view API before auth). This local read endpoint exposes no
    // secrets — it returns aggregates of anonymous, pre-redacted events and
    // routes to no third party. Do NOT forget to gate it when the auth slice lands.
    if (req.method === 'GET' && pathname === SUMMARY_PATH) {
      try {
        const events = await store.readEvents();
        return sendJson(res, 200, summarize(events));
      } catch (e) {
        return sendJson(res, 500, { error: `could not read summary: ${e?.message ?? e}` });
      }
    }

    // Route: only POST /ingest is ingest. Anything else is a 404 (so a maintainer
    // scanning logs can tell probe noise from a receiver bug).
    if (req.method !== 'POST' || pathname !== INGEST_PATH) {
      return sendJson(res, 404, { error: 'not found' });
    }

    let body;
    try {
      body = await readBody(req);
    } catch (e) {
      return sendJson(res, 400, { error: `could not read request body: ${e?.message ?? e}` });
    }

    // ingest's own rejection discipline guarantees a non-retryable 4xx (or 202);
    // map the result straight onto the response.
    const result = await ingest({ headers: req.headers, body }, { ...schema, store });
    return sendJson(res, result.status, result.body);
  };
}

/**
 * Create (and by default start) a receiver. Every dependency is injectable; the
 * defaults wire the real file-backed store + vendored schema.
 *
 * @param {{ port?: number, storePath?: string, store?: object, schema?: object }} [opts]
 * @returns {import('node:http').Server}
 */
export function createReceiver({
  port = process.env.PORT ? Number(process.env.PORT) : DEFAULT_PORT,
  storePath = process.env.STORE ?? DEFAULT_STORE_PATH,
  store = createNdjsonStore({ sink: fileSink(storePath), source: fileSource(storePath) }),
  schema = DEFAULT_SCHEMA,
} = {}) {
  const handler = createRequestHandler({ store, schema });
  const server = createServer(handler);
  if (port != null) server.listen(port);
  return server;
}

// Direct entrypoint: `node server.mjs`.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const server = createReceiver();
  const storePath = process.env.STORE ?? DEFAULT_STORE_PATH;
  server.on('listening', () => {
    const addr = server.address();
    const port = typeof addr === 'object' && addr ? addr.port : '(unknown)';
    console.log(
      `warden-telemetry receiver listening on :${port} (POST ${INGEST_PATH}, GET ${SUMMARY_PATH}; store: ${storePath})`
    );
  });
}
