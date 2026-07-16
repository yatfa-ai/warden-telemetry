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
