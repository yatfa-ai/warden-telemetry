// Server wiring tests (WARDEN-547). Drives createRequestHandler directly with
// fake req/res objects — NO socket is bound, NO real network is opened (the
// roadmap discipline: zero real network in tests, mirroring the client's
// fetchImpl-injected suite). This proves the http wiring (routing, body read,
// status/body mapping) actually delivers the ingest result to the wire shape.
//
// createReceiver's .listen() binding is intentionally NOT exercised here — that
// would open a real socket. The handler is the testable seam.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createRequestHandler } from '../server.mjs';
import { SCHEMA_VERSION, validateEvent } from '../schema.ts';
import { createNdjsonStore } from '../store.mjs';

// A fake IncomingMessage: an EventEmitter whose body chunks emit on nextTick
// (mimicking a real readable stream). readBody consumes via .on('data'|'end').
function fakeReq({ method = 'POST', url = '/ingest', headers = {}, body = '' } = {}) {
  const req = new EventEmitter();
  req.method = method;
  req.url = url;
  req.headers = headers;
  process.nextTick(() => {
    if (body) req.emit('data', body);
    req.emit('end');
  });
  return req;
}

// A fake ServerResponse: captures status, headers, and the JSON payload passed
// to .end(), instead of writing to a real socket.
function fakeRes() {
  const res = { statusCode: null, headers: {}, body: null, ended: false };
  res.setHeader = (k, v) => {
    res.headers[k] = v;
  };
  res.end = (payload) => {
    res.body = payload;
    res.ended = true;
  };
  return res;
}

const validError = {
  schemaVersion: 1,
  type: 'error',
  runtime: 'main',
  timestamp: 1,
  name: 'E',
  message: 'm',
  frames: [],
};
const validBody = JSON.stringify({ schemaVersion: 1, events: [validError] });
const headersV1 = { 'x-telemetry-schema': '1' };

// Build a handler wired to a capturing store; returns { handler, captured }.
function wiring() {
  const captured = [];
  const store = createNdjsonStore({ sink: async (l) => void captured.push(l) });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  return { handler, captured };
}

test('POST /ingest with a valid batch → 202, JSON body, events reach the store', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), res);
  assert.equal(res.statusCode, 202);
  assert.equal(res.headers['content-type'], 'application/json');
  assert.deepEqual(JSON.parse(res.body), { accepted: 1 });
  assert.equal(captured.length, 1);
  assert.deepEqual(JSON.parse(captured[0]), validError);
});

test('POST /ingest with unknown schema version → 4xx, body never reaches the store', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { 'x-telemetry-schema': '2' }, body: 'GARBAGE NOT JSON' }),
    res
  );
  assert.ok(res.statusCode >= 400 && res.statusCode <= 499);
  assert.equal(captured.length, 0);
});

test('POST /ingest with an out-of-schema event → 422, nothing persisted', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  const badBody = JSON.stringify({
    schemaVersion: 1,
    events: [{ ...validError, runtime: 'worker' }],
  });
  await handler(fakeReq({ headers: headersV1, body: badBody }), res);
  assert.equal(res.statusCode, 422);
  assert.equal(captured.length, 0);
});

test('POST /ingest with malformed JSON → 400', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: 'not json' }), res);
  assert.equal(res.statusCode, 400);
  assert.equal(captured.length, 0);
});

test('a non-POST method → 404 (not ingest)', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET' }), res);
  assert.equal(res.statusCode, 404);
  assert.equal(captured.length, 0);
});

test('POST to a path other than /ingest → 404', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ url: '/somewhere-else', body: validBody }), res);
  assert.equal(res.statusCode, 404);
  assert.equal(captured.length, 0);
});

test('POST /ingest?foo=bar still routes to ingest (query string ignored)', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ url: '/ingest?foo=bar', headers: headersV1, body: validBody }), res);
  assert.equal(res.statusCode, 202);
  assert.equal(captured.length, 1);
});

test('createRequestHandler requires a store (fails loud, not silent)', () => {
  assert.throws(() => createRequestHandler(), /store/);
});

// ── GET /summary — the maintainer read surface (WARDEN-567) ───────────────────
// A store pre-seeded with a known event mix via an INJECTED source (reads return
// the seeded events; writes go nowhere). Still ZERO real fs, ZERO real network —
// the handler is driven directly with fake req/res like the ingest tests above.
function readableStore(events) {
  return createNdjsonStore({
    sink: async () => {},
    source: () => events.map((e) => structuredClone(e)),
  });
}

const errorEvent = {
  schemaVersion: 1,
  type: 'error',
  runtime: 'main',
  timestamp: 5,
  name: 'TypeError',
  message: 'm',
  frames: [],
};
const crashEvent = {
  schemaVersion: 1,
  type: 'crash',
  runtime: 'renderer',
  timestamp: 9,
  reason: 'oom',
};

test('GET /summary returns the aggregate over a pre-populated store → 200 + JSON', async () => {
  const store = readableStore([errorEvent, crashEvent]);
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  assert.equal(res.headers['content-type'], 'application/json');
  const body = JSON.parse(res.body);
  assert.equal(body.total, 2);
  assert.deepEqual(body.byType, { error: 1, crash: 1, 'performance-stall': 0 });
  assert.deepEqual(body.topErrorNames, [{ name: 'TypeError', count: 1 }]);
  assert.deepEqual(body.schemaVersions, { '1': 2 });
  assert.equal(body.firstSeen, 5);
  assert.equal(body.lastSeen, 9);
});

test('GET /summary on an empty store → 200, total: 0, zeroed counters (not an error)', async () => {
  const store = readableStore([]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 0);
  assert.deepEqual(body.byType, { error: 0, crash: 0, 'performance-stall': 0 });
  assert.deepEqual(body.topErrorNames, []);
  assert.deepEqual(body.schemaVersions, {});
  assert.equal(body.firstSeen, null);
  assert.equal(body.lastSeen, null);
});

test('GET /summary never echoes raw events or extended-tier names (aggregates only)', async () => {
  const store = readableStore([
    { ...errorEvent, chatName: 'Refactor auth', sessionName: 'claude-7b3a2f1', message: 'secret' },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  const json = res.body;
  assert.equal(json.includes('Refactor auth'), false, 'no chatName');
  assert.equal(json.includes('claude-7b3a2f1'), false, 'no sessionName');
  assert.equal(json.includes('secret'), false, 'no message');
});

test('GET /summary?foo=bar still routes (query string ignored, like POST /ingest)', async () => {
  const store = readableStore([]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?foo=bar' }), res);
  assert.equal(res.statusCode, 200);
});

test('GET /summary reflects events appended since the handler was built (live read)', async () => {
  // sink + source share one array: appends become visible to subsequent reads.
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });

  // empty before any ingest
  let res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(JSON.parse(res.body).total, 0);

  // ingest one event, then summary sees it
  await handler(fakeReq({ headers: headersV1, body: validBody }), fakeRes());
  res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(JSON.parse(res.body).total, 1);
});

test('both POST /ingest and GET /summary are routed by the SAME createRequestHandler (no route drift)', async () => {
  // The drift-style assertion: the read route is wired through createRequestHandler
  // ALONGSIDE POST /ingest — one handler serves both, proving the read surface did
  // not displace or shadow the ingest route.
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });

  const ingestRes = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), ingestRes);
  assert.equal(ingestRes.statusCode, 202, 'POST /ingest still routes through the handler');

  const summaryRes = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), summaryRes);
  assert.equal(summaryRes.statusCode, 200, 'GET /summary is wired through the same handler');
});

test('GET /ingest still 404s (GET is not ingest; only GET /summary is a read route)', async () => {
  const { handler } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/ingest' }), res);
  assert.equal(res.statusCode, 404);
});

test('POST /summary → 404 (only GET /summary is wired; POST /summary is not ingest)', async () => {
  const { handler } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ method: 'POST', url: '/summary', body: validBody }), res);
  assert.equal(res.statusCode, 404);
});

test('GET /summary on a write-only (source-less) store → 500, not a crash', async () => {
  // A store wired without a source has a readEvents() that throws loud. The handler
  // must surface that as a clean 500, never let it kill the server process.
  const store = createNdjsonStore({ sink: async () => {} }); // no source
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 500);
  assert.match(JSON.parse(res.body).error, /source|summary/);
});
