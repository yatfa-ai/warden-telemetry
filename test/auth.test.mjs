// Shared-secret bearer auth tests (WARDEN-569). Drives createRequestHandler
// directly with fake req/res — NO socket is bound, NO real network opened (the
// same zero-real-socket discipline as server.test.mjs / ingest.test.mjs). The
// handler is the testable seam; auth is an injected `authToken` dep.
//
// The auth gate is OPTIONAL but, when set, enforced BEFORE routing — so it gates
// EVERY route uniformly. A request missing a valid `Authorization: Bearer <token>`
// is rejected with 401 (a non-retryable 4xx — the client drops the batch, never
// loops) before ingest() runs, so nothing is persisted on a reject. Unset
// authToken = today's open behavior (the keystone stays runnable bare for dev).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { createRequestHandler } from '../server.mjs';
import { SCHEMA_VERSION, validateEvent } from '../schema.ts';
import { createNdjsonStore } from '../store.mjs';

// A fake IncomingMessage: an EventEmitter whose body chunks emit on nextTick.
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

// A fake ServerResponse: captures status, headers, and the JSON payload.
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

const SECRET = 's3cret-shared-token';

// Build a handler wired to a capturing store + the given authToken; returns
// { handler, captured }.
function wiring(authToken) {
  const captured = [];
  const store = createNdjsonStore({ sink: async (l) => void captured.push(l) });
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    ...(authToken !== undefined ? { authToken } : {}),
  });
  return { handler, captured };
}

// ── SUCCESS CRITERION 1: AUTH_TOKEN set + unauthenticated → 401, nothing persisted ─

test('AUTH_TOKEN set: POST /ingest with no Authorization header → 401, nothing persisted', async () => {
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), res);
  assert.equal(res.statusCode, 401);
  assert.equal(captured.length, 0, 'ingest() never ran → store untouched');
});

test('AUTH_TOKEN set: same request WITH a valid Authorization: Bearer → 202 + persisted', async () => {
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...headersV1, authorization: `Bearer ${SECRET}` }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 202);
  assert.deepEqual(JSON.parse(res.body), { accepted: 1 });
  assert.equal(captured.length, 1, 'authenticated request persisted normally');
  assert.deepEqual(JSON.parse(captured[0]), validError);
});

// ── SUCCESS CRITERION 1b: wrong / malformed token → 401 ──────────────────────────

test('AUTH_TOKEN set: wrong token → 401, nothing persisted', async () => {
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...headersV1, authorization: 'Bearer totally-wrong-token' }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 401);
  assert.equal(captured.length, 0);
});

test('AUTH_TOKEN set: header without the "Bearer " scheme → 401', async () => {
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...headersV1, authorization: SECRET }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 401);
  assert.equal(captured.length, 0);
});

test('AUTH_TOKEN set: "Bearer " with an empty token → 401', async () => {
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...headersV1, authorization: 'Bearer ' }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 401);
  assert.equal(captured.length, 0);
});

test('AUTH_TOKEN set: valid token but wrong-length variant → 401 (no length leak / no throw)', async () => {
  // A provided token of a different length exercises the unequal-length branch of
  // the constant-time compare (timingSafeEqual would throw on raw unequal lengths
  // — the helper must not). Still rejected, still nothing persisted.
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...headersV1, authorization: 'Bearer xx' }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 401);
  assert.equal(captured.length, 0);
});

test('AUTH_TOKEN set: the scheme match is case-insensitive (bearer TOKEN → 202)', async () => {
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...headersV1, authorization: `bearer ${SECRET}` }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 202);
  assert.equal(captured.length, 1);
});

test('AUTH_TOKEN set: the Authorization header lookup is case-insensitive', async () => {
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...headersV1, Authorization: `Bearer ${SECRET}` }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 202);
  assert.equal(captured.length, 1);
});

// ── The 401 is a NON-RETRYABLE 4xx (client drops, never loops) ───────────────────

test('the auth-reject status is 401 — never 429, never 5xx (client drops, never retries)', async () => {
  const { handler } = wiring(SECRET);
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), res);
  assert.ok(res.statusCode >= 400 && res.statusCode <= 499, 'is a 4xx');
  assert.notEqual(res.statusCode, 429, 'never 429 (the client would retry that)');
  assert.ok(res.statusCode < 500, 'never 5xx (the client would retry that)');
});

// ── SUCCESS CRITERION 2: AUTH_TOKEN UNSET → today's open behavior ────────────────

test('AUTH_TOKEN unset: no Authorization header required → 202 + persisted (open behavior)', async () => {
  const { handler, captured } = wiring(undefined); // no authToken → gate disabled
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), res);
  assert.equal(res.statusCode, 202);
  assert.equal(captured.length, 1);
});

test('AUTH_TOKEN unset: a stray Authorization header is ignored (still 202)', async () => {
  const { handler, captured } = wiring(undefined);
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...headersV1, authorization: 'Bearer whatever' }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 202);
  assert.equal(captured.length, 1);
});

test('backward-compat: createRequestHandler({ store }) with no authToken stays open (existing suite shape)', async () => {
  // The handler signature/defaults must stay backward-compatible: a caller that
  // does not pass authToken gets today's open behavior (the existing server.test
  // suite calls createRequestHandler exactly this way).
  const captured = [];
  const store = createNdjsonStore({ sink: async (l) => void captured.push(l) });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), res);
  assert.equal(res.statusCode, 202);
  assert.equal(captured.length, 1);
});

// ── The gate is uniform over ALL routes: checked BEFORE routing ──────────────────

test('AUTH_TOKEN set: an unauthenticated request to a NON-EXISTENT path → 401 (not 404)', async () => {
  // Proves the gate fires before the route/404 dispatch: even an unknown path
  // can't be probed without the secret. This is what makes the gate uniform over
  // every route — including a future read surface — without rework.
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/no-such-route' }), res);
  assert.equal(res.statusCode, 401, 'auth rejected before the 404 route check');
  assert.equal(captured.length, 0);
});

test('AUTH_TOKEN set: an AUTHENTICATED request to a non-ingest path → 404 (routing proceeds past the gate)', async () => {
  // Positive control: a valid token lets routing proceed normally, so a real but
  // non-ingest path still 404s (the gate does not swallow valid routes).
  const { handler, captured } = wiring(SECRET);
  const res = fakeRes();
  await handler(
    fakeReq({ method: 'GET', url: '/no-such-route', headers: { authorization: `Bearer ${SECRET}` } }),
    res
  );
  assert.equal(res.statusCode, 404);
  assert.equal(captured.length, 0);
});

test('AUTH_TOKEN set: unauthenticated GET /ingest → 401 (GET is gated too, not just POST)', async () => {
  const { handler } = wiring(SECRET);
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/ingest' }), res);
  assert.equal(res.statusCode, 401);
});

// ── The read surface GET /summary (WARDEN-567) is gated too ──────────────────────
// /summary is now a REAL route (it was only "upcoming" when this ticket opened). The
// auth gate runs BEFORE routing, so an unauthenticated reader can't reach the
// aggregates either — extended-tier-derived names never broadcast to anyone on the
// LAN. These pin that property so a future handler reorder (route before gate) can't
// silently re-expose the read surface. Uses a bare fake store: the /summary path only
// needs readEvents(), and the reject path must not touch even that.

test('AUTH_TOKEN set: unauthenticated GET /summary → 401, readEvents never runs (the read surface is gated)', async () => {
  let reads = 0;
  const store = { readEvents: () => { reads += 1; return []; } };
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent }, authToken: SECRET });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 401, 'the read surface is rejected before routing');
  assert.equal(reads, 0, 'readEvents never ran → no aggregate path exercised on a reject');
});

test('AUTH_TOKEN set: GET /summary WITH a valid token → 200 (authenticated read proceeds past the gate)', async () => {
  const store = { readEvents: () => [validError] };
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent }, authToken: SECRET });
  const res = fakeRes();
  await handler(
    fakeReq({ method: 'GET', url: '/summary', headers: { authorization: `Bearer ${SECRET}` } }),
    res
  );
  assert.equal(res.statusCode, 200, 'a valid token lets the read route proceed — the gate does not swallow it');
  assert.equal(JSON.parse(res.body).total, 1);
});

// ── Constant-time compare is rejection-safe (never throws on edge inputs) ────────

test('tokensMatch path: an empty provided token against a set secret is rejected without throwing', async () => {
  // readBearerToken returns null for a missing/empty token, so the gate rejects
  // before any compare — but assert the handler never throws on auth edge cases.
  const { handler } = wiring(SECRET);
  const res = fakeRes();
  await assert.doesNotReject(async () => handler(fakeReq({ headers: { ...headersV1, authorization: '' } }), res));
  assert.equal(res.statusCode, 401);
});
