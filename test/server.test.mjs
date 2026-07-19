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
import { createRequestHandler, createRetentionTrigger, createRejectionTally, createPersistErrorTally, createSeenKeys, DEFAULT_MAX_EVENTS, DEFAULT_MAX_BODY_BYTES, readBody } from '../server.mjs';
import { SCHEMA_VERSION, validateEvent } from '../schema.ts';
import { createNdjsonStore, parseNdjson } from '../store.mjs';

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
  schemaVersion: SCHEMA_VERSION,
  type: 'error',
  runtime: 'main',
  timestamp: 1,
  name: 'E',
  message: 'm',
  frames: [],
};
const validBody = JSON.stringify({ schemaVersion: SCHEMA_VERSION, events: [validError] });
const schemaHeaders = { 'x-telemetry-schema': String(SCHEMA_VERSION) };

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
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
  assert.equal(res.statusCode, 202);
  assert.equal(res.headers['content-type'], 'application/json');
  assert.deepEqual(JSON.parse(res.body), { accepted: 1 });
  assert.equal(captured.length, 1);
  // The persisted record is the verbatim client payload PLUS the receiver's
  // receivedAt stamp (WARDEN-692). Assert the stamp is finite and the client
  // payload round-trips verbatim alongside it.
  const persisted = JSON.parse(captured[0]);
  assert.equal(Number.isFinite(persisted.receivedAt), true, 'receiver stamps a finite receivedAt');
  const { receivedAt, ...clientPayload } = persisted;
  assert.deepEqual(clientPayload, validError);
});

test('POST /ingest with unknown schema version → 4xx, body never reaches the store', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { 'x-telemetry-schema': String(SCHEMA_VERSION + 1) }, body: 'GARBAGE NOT JSON' }),
    res
  );
  assert.ok(res.statusCode >= 400 && res.statusCode <= 499);
  assert.equal(captured.length, 0);
});

test('POST /ingest with an out-of-schema event → 422, nothing persisted', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  const badBody = JSON.stringify({
    schemaVersion: SCHEMA_VERSION,
    events: [{ ...validError, runtime: 'worker' }],
  });
  await handler(fakeReq({ headers: schemaHeaders, body: badBody }), res);
  assert.equal(res.statusCode, 422);
  assert.equal(captured.length, 0);
});

test('POST /ingest with malformed JSON → 400', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: 'not json' }), res);
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

// ── Malformed request-target → 400 (WARDEN-673) ───────────────────────────────
// llhttp accepts the origin-form targets `///` and `//` (common scanner probes:
// GET //etc/passwd) and delivers them as req.url, but `new URL('///', ...)` throws
// ERR_INVALID_URL. Pre-fix, that throw escaped the async handler as an unhandled
// rejection and terminated the receiver process. Now it is a clean 400.

test('a malformed request-target (///) → 400 invalid request-target; no rejection, no socket hang', async () => {
  const { handler } = wiring();
  const res = fakeRes();
  // `await` proves no rejection escapes: a thrown handler would reject here and
  // fail the test (the pre-fix crash). The response must be ended (res.end called)
  // so the socket does not hang.
  await handler(fakeReq({ method: 'GET', url: '///' }), res);
  assert.equal(res.statusCode, 400);
  assert.equal(res.ended, true, 'response is ended — the socket does not hang');
  assert.deepEqual(JSON.parse(res.body), { error: 'invalid request-target' });
});

test('a malformed request-target (//) → 400 too (both scanner forms reproduce)', async () => {
  const { handler } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '//' }), res);
  assert.equal(res.statusCode, 400);
  assert.equal(res.ended, true);
  assert.deepEqual(JSON.parse(res.body), { error: 'invalid request-target' });
});

test('a malformed request-target is recorded in the rejections tally (scanner noise is visible, fixed-string reason)', async () => {
  // The 400 feeds the WARDEN-591 tally so a maintainer sees scanner noise in
  // GET /summary.rejections.byStatus. The recorded reason is a FIXED string
  // (never the raw client request-target) — the rejection-seam trust model.
  const rejections = createRejectionTally();
  const handler = createRequestHandler({
    store: createNdjsonStore({ sink: async () => {} }),
    rejections,
  });
  await handler(fakeReq({ method: 'GET', url: '///' }), fakeRes());
  const snap = rejections.snapshot();
  assert.ok(snap.byStatus['400'] >= 1, 'the 400 was recorded');
  assert.equal(snap.lastStatus, 400);
  assert.equal(snap.lastReason, 'invalid request-target');
});

test('a malformed request-target still 400s when auth is set (the throw is after the gate, reachable by a token-holder)', async () => {
  // The parse runs AFTER the auth gate, so a valid token reaches it; auth does
  // not accidentally mask the 400 (nor the pre-fix crash).
  const handler = createRequestHandler({
    store: createNdjsonStore({ sink: async () => {} }),
    authToken: 'sekret',
  });
  const res = fakeRes();
  await handler(
    fakeReq({ method: 'GET', url: '///', headers: { authorization: 'Bearer sekret' } }),
    res
  );
  assert.equal(res.statusCode, 400);
  assert.deepEqual(JSON.parse(res.body), { error: 'invalid request-target' });
});

test('POST /ingest?foo=bar still routes to ingest (query string ignored)', async () => {
  const { handler, captured } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ url: '/ingest?foo=bar', headers: schemaHeaders, body: validBody }), res);
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
  // The new failure-signature aggregate (WARDEN-707) flows through the
  // `...summarize(events)` spread at the /summary handler. errorEvent has empty
  // frames → name-only signature; crashEvent has no exitCode → `crash:oom`.
  // Both count 1 → tie-broken by signature asc ('T' < 'c').
  assert.deepEqual(body.topSignatures, [
    { signature: 'TypeError', type: 'error', count: 1 },
    { signature: 'crash:oom', type: 'crash', count: 1 },
  ]);
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
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), fakeRes());
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
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), ingestRes);
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

// ── GET /capabilities — the config-time verification surface (WARDEN-595) ───────
// A client's Settings "Test connection" probe reads this to confirm the receiver
// is reachable + schema-matched before relying on it. It returns the receiver's
// SCHEMA_VERSION (sourced from the vendored schema — never a parallel literal) and
// whether auth is required. Same zero-real-socket discipline: fake req/res, a bare
// store (the path touches neither readEvents nor appendEvents).

test('GET /capabilities → 200 + JSON with the receiver schemaVersion and authRequired:false (open receiver)', async () => {
  const store = createNdjsonStore({ sink: async () => {} });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/capabilities' }), res);
  assert.equal(res.statusCode, 200);
  assert.equal(res.headers['content-type'], 'application/json');
  // schemaVersion is sourced from the vendored schema, NOT a parallel literal —
  // assert it equals the canonical SCHEMA_VERSION so a future bump can't drift here.
  assert.equal(JSON.parse(res.body).schemaVersion, SCHEMA_VERSION);
  assert.equal(JSON.parse(res.body).authRequired, false, 'an open receiver reports authRequired:false');
});

test('GET /capabilities schemaVersion tracks the injected schema (drift would surface here, not via a parallel literal)', async () => {
  // If a future receiver schema bump forgets to update the vendored SCHEMA_VERSION,
  // the capabilities body must still reflect whatever the injected schema declares —
  // there is no second literal to fall out of sync with.
  const store = createNdjsonStore({ sink: async () => {} });
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION: 999, validateEvent },
  });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/capabilities' }), res);
  assert.equal(JSON.parse(res.body).schemaVersion, 999);
});

test('GET /capabilities does NOT read the store (a probe touches no persisted data)', async () => {
  let reads = 0;
  const store = { readEvents: () => { reads += 1; return []; } };
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/capabilities' }), res);
  assert.equal(res.statusCode, 200);
  assert.equal(reads, 0, 'capabilities never calls readEvents — it is a pure self-description');
});

test('GET /capabilities?foo=bar still routes (query string ignored, like /ingest and /summary)', async () => {
  const store = createNdjsonStore({ sink: async () => {} });
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/capabilities?foo=bar' }), res);
  assert.equal(res.statusCode, 200);
  assert.equal(JSON.parse(res.body).schemaVersion, SCHEMA_VERSION);
});

test('POST /capabilities → 404 (only GET /capabilities is wired; POST is not ingest)', async () => {
  const { handler } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ method: 'POST', url: '/capabilities', body: validBody }), res);
  assert.equal(res.statusCode, 404);
});

test('PUT /capabilities → 404 (only GET is wired)', async () => {
  const { handler } = wiring();
  const res = fakeRes();
  await handler(fakeReq({ method: 'PUT', url: '/capabilities', body: validBody }), res);
  assert.equal(res.statusCode, 404);
});

// ── GET /events — routing integration (WARDEN-599) ────────────────────────────
// The full-fidelity drill-down surface is wired through createRequestHandler
// ALONGSIDE /ingest, /summary, and /capabilities (no route drift), inherits the
// auth gate, and does not swallow the 404 fall-through. The selectEvents matrix +
// the full GET /events handler behavior (filters, bound, DONE criterion,
// readEvents-never-runs on reject) lives in test/events.test.mjs; THIS block pins
// the ROUTING properties that are this file's concern: cross-route wiring through
// one handler + the inherited gate + the preserved 404.

test('every route is served by the SAME createRequestHandler (no route drift across /ingest, /summary, /capabilities, /events)', async () => {
  // Drift-style: every route is wired THROUGH createRequestHandler — one handler
  // serves them all, proving none displaced or shadowed another. WARDEN-595 added
  // /capabilities and WARDEN-599 added /events; both coexist with the originals.
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });

  const ingestRes = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), ingestRes);
  assert.equal(ingestRes.statusCode, 202, 'POST /ingest still routes through the handler');

  const summaryRes = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), summaryRes);
  assert.equal(summaryRes.statusCode, 200, 'GET /summary is still wired (not displaced)');

  // WARDEN-595: /capabilities coexists with the originals.
  const capsRes = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/capabilities' }), capsRes);
  assert.equal(capsRes.statusCode, 200, 'GET /capabilities is wired through the handler');
  assert.equal(JSON.parse(capsRes.body).schemaVersion, SCHEMA_VERSION);

  // WARDEN-599: /events coexists too — and returns the ingested event at full fidelity.
  const eventsRes = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events' }), eventsRes);
  assert.equal(eventsRes.statusCode, 200, 'GET /events is wired through the same handler');
  const body = JSON.parse(eventsRes.body);
  assert.equal(body.total, 1, 'the ingested event is readable at full fidelity via /events');
  assert.equal(body.events[0].name, 'E', 'the diagnostic name /summary would also surface');
});

test('GET /events inherits the auth gate — no token with AUTH_TOKEN set → 401', async () => {
  // The gate runs BEFORE routing, so /events inherits it with zero new auth code.
  const store = readableStore([errorEvent]);
  const handler = createRequestHandler({ store, authToken: 'server-test-secret' });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events' }), res);
  assert.equal(res.statusCode, 401);
});

test('GET /events does NOT swallow the 404 fall-through — an unknown path still 404s', async () => {
  const store = readableStore([errorEvent]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events-typo' }), res);
  assert.equal(res.statusCode, 404);
});

// ── TIMELINE DISTRIBUTION (WARDEN-603) ────────────────────────────────────────
// The bounded temporal-distribution field on GET /summary — event counts per time
// bucket over a rolling recent window, so a maintainer can distinguish a recent
// volume spike (a regression / deploy) from a long-running baseline. Mirrors the
// rejections tally's injected-seam discipline: the handler takes an OPTIONAL `now`
// (default Date.now) so the read-the-shape-with-a-fake-clock tests below stay
// deterministic. Still ZERO real fs, ZERO real network — driven with fake req/res.
//
// The handler composes summarizeTimeline(events, { now }) with its DEFAULT window
// (24h) and maxBuckets (48) → bucketMs = 1_800_000 (30 min). Every test below seeds
// timestamps against a fake now = 86_400_000 so the window is exactly [0, 86_400_000]
// and a timestamp maps to bucket floor(ts / 1_800_000) — the arithmetic is exact and
// legible, never dependent on the real wall clock.

test('GET /summary on an empty store carries a ZEROED timeline (no false alarm — additive field always present)', async () => {
  const store = readableStore([]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.ok(body.timeline, 'timeline field is present (additive — carried on every response)');
  assert.deepEqual(body.timeline.buckets, [], 'no buckets on an empty store');
  assert.equal(body.timeline.bucketMs, 1_800_000, 'bucketMs conveys the granularity (24h window / 48 buckets)');
});

test('GET /summary timeline reflects a known seeded time spread — a recent spike vs an earlier baseline', async () => {
  // Seed a SPIKE (3 events in the newest bucket, one of them exactly == now so the
  // top-boundary fold is exercised) plus a single earlier baseline event, then read
  // the distribution back. bucket 1 = [1.8M, 3.6M); bucket 47 = [84.6M, 86.4M).
  const newestStart = 84_600_000; // bucket 47
  const store = readableStore([
    { ...errorEvent, timestamp: 1_800_000 },      // bucket 1 (baseline)
    { ...errorEvent, timestamp: newestStart },     // bucket 47 (spike)
    { ...errorEvent, timestamp: newestStart + 1 }, // bucket 47 (spike)
    { ...crashEvent, timestamp: 86_400_000 },      // bucket 47 (=== now → newest via boundary fold)
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const { timeline } = JSON.parse(res.body);
  assert.equal(timeline.buckets.length, 2, 'two distinct buckets: baseline + spike');
  assert.deepEqual(timeline.buckets, [
    { bucketStart: 1_800_000, bucketEnd: 3_600_000, count: 1 },   // baseline
    { bucketStart: 84_600_000, bucketEnd: 86_400_000, count: 3 }, // recent spike (newest)
  ]);
  assert.equal(timeline.bucketMs, 1_800_000);
});

test('GET /summary timeline never echoes raw events or extended-tier names (counts only — in-window event)', async () => {
  // The seeded event is IN the rolling window (so the timeline actually fires a
  // bucket) yet carries extended-tier identifiers + a raw message. The timeline
  // reads ONLY event.timestamp and emits a COUNT — there is no path by which an
  // identifier could reach the distribution. (Parity with the body-level trust-model
  // test above, but exercised against a bucket that actually fired.)
  const store = readableStore([
    {
      ...errorEvent,
      timestamp: 1_800_000,
      message: 'super secret stack detail',
      chatName: 'Refactor auth',
      sessionName: 'claude-7b3a2f1',
    },
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  const { timeline } = JSON.parse(res.body);
  assert.equal(timeline.buckets.length, 1, 'the in-window event fired a bucket');
  const json = JSON.stringify(timeline);
  assert.equal(json.includes('Refactor auth'), false, 'no chatName in timeline');
  assert.equal(json.includes('claude-7b3a2f1'), false, 'no sessionName in timeline');
  assert.equal(json.includes('super secret stack detail'), false, 'no message in timeline');
  assert.equal(json.includes('TypeError'), false, 'no error name either — timestamp is the only field read');
});

test('GET /summary carries the timeline field for a handler wired WITHOUT an explicit now (backward-compatible additive shape)', async () => {
  // A handler wired the pre-WARDEN-603 way — { store }, no `now` — still carries the
  // additive `timeline` field (defaulting to the real clock). The seeded events use
  // epoch-near-zero timestamps (5 and 9, far outside any real 24h window), so the
  // timeline is zeroed — proving the field is ALWAYS present with a STABLE shape,
  // never an absent key that would break a caller composed the old way. (Deterministic:
  // bucketMs is constant and 5/9 are always pre-window regardless of the real clock.)
  const store = readableStore([errorEvent, crashEvent]); // timestamps 5 and 9
  const handler = createRequestHandler({ store }); // no `now` → real clock (default)
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.ok(body.timeline, 'timeline field present even without an explicit now');
  assert.ok(Array.isArray(body.timeline.buckets), 'buckets is always an array');
  assert.deepEqual(body.timeline.buckets, [], 'timestamps 5/9 are far pre-window → zeroed distribution');
  assert.equal(body.timeline.bucketMs, 1_800_000, 'bucketMs always conveyed');
});

// ── SCOPED /summary — ?type= / ?platform= / ?appVersion= / ?since= (WARDEN-727) ─
// The scoped-OVERVIEW complement to /events' scoped drill-down: the SAME conjunctive
// filters select which ALREADY-redacted, ALREADY-validated events get aggregated, so
// a maintainer who spots a win32 spike or a v0.1.18 volume bubble on
// /summary.platforms / appVersions can scope /summary to read that slice's
// topErrorNames / topSignatures / timeline without hand-parsing /events. Driven with
// fake req/res + an INJECTED readable store; ZERO real fs, ZERO real network.
//
// `total` stays the FULL persisted count (mirrors /events' `total: events.length`);
// `matched` is the scoped subset size (≤ total). `rejections` / `persistErrors` are
// UNSCOPED (receiver health, not the event subset). The timestamps below are seeded
// in-window against a fake now = 86_400_000 (window [0, 86_400_000], bucketMs
// 1_800_000) so the timeline assertions are exact, never wall-clock-dependent.

test('GET /summary?platform=win32 scopes byType / topErrorNames / topSignatures / platforms to win32 only', async () => {
  // Three events across two platforms + two distinct error names. Scoping to win32
  // must keep the win32 error + win32 crash and drop the darwin error entirely.
  const store = readableStore([
    { ...errorEvent, platform: 'win32', name: 'TypeError', timestamp: 1_800_000 },
    { ...errorEvent, platform: 'darwin', name: 'RangeError', timestamp: 1_800_000 },
    { ...crashEvent, platform: 'win32', reason: 'oom', timestamp: 1_800_000 },
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  // total is the FULL set across platforms; matched is the win32 subset.
  assert.equal(body.total, 3, 'total is the full persisted count, unfiltered');
  assert.equal(body.matched, 2, 'matched is the win32 subset');
  assert.ok(body.matched <= body.total, 'matched never exceeds total');
  // byType reflects ONLY the win32 events: one error + one crash.
  assert.deepEqual(body.byType, { error: 1, crash: 1, 'performance-stall': 0 });
  // topErrorNames is win32-only: RangeError (darwin) is gone.
  assert.deepEqual(body.topErrorNames, [{ name: 'TypeError', count: 1 }]);
  // topSignatures is win32-only: the darwin RangeError signature is gone.
  assert.deepEqual(body.topSignatures, [
    { signature: 'TypeError', type: 'error', count: 1 },
    { signature: 'crash:oom', type: 'crash', count: 1 },
  ]);
  // platforms collapses to the scoped platform only.
  assert.deepEqual(body.platforms, { win32: 2 }, 'platforms collapses to {win32: matched}');
});

test('GET /summary?platform=win32 scopes the TIMELINE to win32 arrivals only', async () => {
  // Seed a win32 spike (3 in the newest bucket) + a darwin baseline (1 in an earlier
  // bucket). Scoping to win32 must drop the darwin baseline from the distribution.
  const newestStart = 84_600_000; // bucket 47
  const store = readableStore([
    { ...errorEvent, platform: 'darwin', timestamp: 1_800_000 },     // bucket 1 (darwin baseline)
    { ...errorEvent, platform: 'win32', timestamp: newestStart },    // bucket 47 (win32 spike)
    { ...errorEvent, platform: 'win32', timestamp: newestStart + 1 },
    { ...crashEvent, platform: 'win32', timestamp: 86_400_000 },
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const { timeline, matched, total } = JSON.parse(res.body);
  assert.equal(matched, 3, '3 win32 events matched');
  assert.equal(total, 4, 'total is the full set');
  // The darwin baseline bucket is GONE — only the win32 spike bucket remains.
  assert.deepEqual(timeline.buckets, [
    { bucketStart: 84_600_000, bucketEnd: 86_400_000, count: 3 },
  ]);
  assert.equal(timeline.bucketMs, 1_800_000);
});

test('GET /summary?platform=win32 scopes firstSeen / lastSeen to the win32 subset', async () => {
  // firstSeen/lastSeen are part of summarize()'s spread, so they too must reflect
  // only the scoped subset (the win32 window), not the full retained set.
  const store = readableStore([
    { ...errorEvent, platform: 'darwin', timestamp: 5 },    // outside the win32 window
    { ...errorEvent, platform: 'win32', timestamp: 100 },
    { ...errorEvent, platform: 'win32', timestamp: 300 },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.matched, 2);
  assert.equal(body.firstSeen, 100, 'firstSeen is the win32 subset min, not the darwin 5');
  assert.equal(body.lastSeen, 300, 'lastSeen is the win32 subset max');
});

test('GET /summary?type=crash scopes the aggregates to a single base type', async () => {
  const store = readableStore([
    { ...errorEvent, name: 'TypeError', timestamp: 1_800_000 },
    { ...crashEvent, reason: 'oom', timestamp: 1_800_000 },
    { ...crashEvent, reason: 'killed', timestamp: 1_800_000 },
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?type=crash' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 3);
  assert.equal(body.matched, 2);
  assert.deepEqual(body.byType, { error: 0, crash: 2, 'performance-stall': 0 });
  assert.deepEqual(body.topErrorNames, [], 'no error names — errors were filtered out');
});

test('GET /summary?since= scopes the aggregates to events at/after the cutoff (receivedAt ?? timestamp)', async () => {
  const store = readableStore([
    { ...errorEvent, timestamp: 100, receivedAt: 100 },
    { ...errorEvent, timestamp: 200, receivedAt: 200 },
    { ...errorEvent, timestamp: 300, receivedAt: 300 },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?since=200' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 3);
  assert.equal(body.matched, 2, '200 (>=) + 300 survive; 100 dropped');
  assert.equal(body.firstSeen, 200);
  assert.equal(body.lastSeen, 300);
});

test('GET /summary?appVersion=0.1.18&platform=darwin&type=crash intersects all filters (regression attribution)', async () => {
  // The end-to-end attribution question: "is the v0.1.18 darwin crash real?" Only
  // one event survives the conjunctive intersection; the rest are excluded for a
  // distinct reason each.
  const store = readableStore([
    { ...crashEvent, platform: 'darwin', appVersion: '0.1.18', reason: 'mac-oom', timestamp: 1_800_000 }, // MATCH
    { ...crashEvent, platform: 'win32', appVersion: '0.1.18', reason: 'win-oom', timestamp: 1_800_000 },  // wrong platform
    { ...crashEvent, platform: 'darwin', appVersion: '0.1.19', reason: 'mac-new', timestamp: 1_800_000 }, // wrong version
    { ...errorEvent, platform: 'darwin', appVersion: '0.1.18', name: 'mac-err', timestamp: 1_800_000 },   // wrong type
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?appVersion=0.1.18&platform=darwin&type=crash' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 4, 'total is the full set');
  assert.equal(body.matched, 1, 'only the one darwin/0.1.18/crash event survives');
  assert.deepEqual(body.byType, { error: 0, crash: 1, 'performance-stall': 0 });
  assert.deepEqual(body.topSignatures, [{ signature: 'crash:mac-oom', type: 'crash', count: 1 }]);
  assert.deepEqual(body.platforms, { darwin: 1 });
  assert.deepEqual(body.appVersions, { '0.1.18': 1 });
});

test('GET /summary with NO filters is backward compatible — matched === total, aggregates over the whole set', async () => {
  // The legacy unscoped path must be unchanged: matched equals total, and the
  // aggregates match the pre-WARDEN-727 expectations for this exact fixture.
  const store = readableStore([errorEvent, crashEvent]);
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 2);
  assert.equal(body.matched, 2, 'unfiltered → matched === total');
  assert.deepEqual(body.byType, { error: 1, crash: 1, 'performance-stall': 0 });
  assert.deepEqual(body.topErrorNames, [{ name: 'TypeError', count: 1 }]);
  assert.deepEqual(body.topSignatures, [
    { signature: 'TypeError', type: 'error', count: 1 },
    { signature: 'crash:oom', type: 'crash', count: 1 },
  ]);
  assert.deepEqual(body.schemaVersions, { '1': 2 });
  assert.equal(body.firstSeen, 5);
  assert.equal(body.lastSeen, 9);
});

test('GET /summary?platform=win32 on a store with NO win32 events → matched 0, total full, zeroed aggregates', async () => {
  const store = readableStore([
    { ...errorEvent, platform: 'darwin', timestamp: 1_800_000 },
    { ...crashEvent, platform: 'darwin', timestamp: 1_800_000 },
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 2, 'total still the full persisted count');
  assert.equal(body.matched, 0, 'nothing matched the win32 filter');
  assert.deepEqual(body.byType, { error: 0, crash: 0, 'performance-stall': 0 });
  assert.deepEqual(body.topErrorNames, []);
  assert.deepEqual(body.platforms, {});
  assert.deepEqual(body.timeline.buckets, []);
  assert.equal(body.firstSeen, null);
  assert.equal(body.lastSeen, null);
});

test('GET /summary?platform=win32 leaves rejections / persistErrors UNSCOPED (receiver health, not the event subset)', async () => {
  // rejections/persistErrors tally the REQUEST seam on THIS receiver — a platform
  // filter must NOT hide them. Wire a tally + seeded mixed-platform store, trigger a
  // rejection, then scope to a platform that matches NOTHING and assert the rejection
  // still surfaces (the filter scopes events, not receiver health).
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const rejections = createRejectionTally();
  const persistErrors = createPersistErrorTally();
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    rejections,
    persistErrors,
  });

  // Seed a darwin-only store + trigger one 404 rejection (a routing miss).
  await handler(fakeReq({ headers: schemaHeaders, body: JSON.stringify({ schemaVersion: SCHEMA_VERSION, events: [{ ...validError, platform: 'darwin' }] }) }), fakeRes());
  await handler(fakeReq({ method: 'GET', url: '/no-such-route' }), fakeRes());

  // Scope to win32 (matches nothing) — rejections/persistErrors must still read.
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.matched, 0, 'no win32 events');
  assert.equal(body.total, 1, 'the one darwin event is the full set');
  // The 404 rejection survives the win32 filter (receiver health is unscoped).
  assert.ok(body.rejections.total >= 1, 'rejections survive the platform filter');
  assert.ok(body.rejections.byStatus['404'] >= 1, 'the 404 is still surfaced');
  // persistErrors is present with its stable shape (zeroed here — no persist failure).
  assert.equal(body.persistErrors.total, 0);
});

test('GET /summary ignores an unrecognized query param (no spurious scoping — backward compatible)', async () => {
  // A param the filter does not consume (?foo=bar) applies NO filter: matched === total.
  const store = readableStore([errorEvent, crashEvent]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?foo=bar' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 2);
  assert.equal(body.matched, 2, 'an unknown param does not scope the aggregates');
});

// ── RETENTION (WARDEN-579) ────────────────────────────────────────────────────
// The maintenance trigger keeps the persisted store bounded. It runs prune OFF
// the request path on a debounced (>=1 min), re-entrancy-guarded cadence — never
// a synchronous rewrite per event (WARDEN-88). Below: the trigger is driven with
// an INJECTED fake clock + scheduler (no real timer), and the handler is proven
// to record appends fire-and-forget. Still ZERO real fs, ZERO real network.

// A fake scheduler + clock: setTimer/clearTimer manage an in-memory queue; now()
// is a fixed, controllable value. Lets the trigger's debounce/re-entrancy logic be
// exercised deterministically without a single real setTimeout.
function fakeClock() {
  const timers = [];
  let nowVal = 0;
  return {
    setTimer: (fn) => {
      const id = { fn };
      timers.push(id);
      return id;
    },
    clearTimer: (id) => {
      const i = timers.indexOf(id);
      if (i >= 0) timers.splice(i, 1);
    },
    now: () => nowVal,
    setNow: (v) => {
      nowVal = v;
    },
    pending: () => timers.length,
    flushAll: () => {
      while (timers.length) timers.shift().fn();
    },
    flushNext: () => {
      if (timers.length) timers.shift().fn();
    },
  };
}

// An in-memory FILE MIRROR: sink appends, source parses, rewrite replaces —
// exactly the production fileSink/fileSource/fileRewrite contract, no disk.
function inMemoryFile() {
  let text = '';
  return {
    sink: async (line) => {
      text += `${line}\n`;
    },
    source: () => parseNdjson(text),
    rewrite: async (t) => {
      text = t;
    },
    read: () => parseNdjson(text),
  };
}

function storeWithRetentionSpy({ pruneImpl } = {}) {
  const calls = [];
  return {
    calls,
    prune: async (opts) => {
      calls.push(opts);
      if (pruneImpl) await pruneImpl(opts);
      return { before: 0, after: 0, pruned: 0, rewrote: false };
    },
  };
}

test('retention DEFAULT is bounded — DEFAULT_MAX_EVENTS is a finite positive count cap (the unbounded-growth bug is fixed by default)', () => {
  assert.ok(
    typeof DEFAULT_MAX_EVENTS === 'number' && DEFAULT_MAX_EVENTS > 0 && Number.isFinite(DEFAULT_MAX_EVENTS),
    'the default count cap must be a finite positive bound, never unbounded'
  );
});

test('retention: afterAppend BELOW the count bound arms NO prune (no per-event rewrite on the request path)', () => {
  const store = storeWithRetentionSpy();
  const clock = fakeClock();
  const trigger = createRetentionTrigger(store, {
    maxEvents: 100,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  trigger.afterAppend(5);
  trigger.afterAppend(10);
  assert.equal(store.calls.length, 0, 'no prune ran');
  assert.equal(clock.pending(), 0, 'no debounce timer was armed');
});

test('retention: crossing the count bound arms ONE debounced prune — a burst coalesces into a single prune', () => {
  const store = storeWithRetentionSpy();
  const clock = fakeClock();
  clock.setNow(12345);
  const trigger = createRetentionTrigger(store, {
    maxEvents: 10,
    maxAgeMs: 3600_000,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  trigger.afterAppend(5);
  assert.equal(clock.pending(), 0, 'below the bound: not armed');
  trigger.afterAppend(6); // 11 total → crosses 10
  assert.equal(clock.pending(), 1, 'crossed the bound: one debounce timer armed');
  trigger.afterAppend(50); // more appends while armed
  assert.equal(clock.pending(), 1, 'a burst coalesces into the SAME single timer');
  clock.flushAll();
  assert.equal(store.calls.length, 1, 'exactly one prune ran');
  assert.deepEqual(store.calls[0], { maxEvents: 10, maxAgeMs: 3600_000, now: 12345 });
});

test('retention: a prune mid-flight blocks a SECOND concurrent prune (re-entrancy guard)', async () => {
  let resolvePrune;
  const inFlight = new Promise((r) => {
    resolvePrune = r;
  });
  const calls = [];
  const store = {
    prune: async (opts) => {
      calls.push(opts);
      await inFlight;
    },
  };
  const clock = fakeClock();
  const trigger = createRetentionTrigger(store, {
    maxEvents: 1,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  trigger.afterAppend(1); // arm
  clock.flushNext(); // fires flush → starts a prune (stalls on `inFlight`)
  assert.equal(calls.length, 1, 'first prune started');

  // While that prune is mid-flight, arm + flush again — must NOT start a 2nd prune.
  trigger.afterAppend(1);
  clock.flushAll();
  assert.equal(calls.length, 1, 'no concurrent prune started while one is in flight');

  resolvePrune();
  await inFlight;
  await new Promise((r) => setTimeout(r, 0)); // let the .finally re-arm settle
  assert.equal(calls.length, 1, 'still one — the re-armed timer was not flushed');
});

test('retention: sweep() arms a debounced prune unconditionally (the age-expiry path on a quiet store)', () => {
  const store = storeWithRetentionSpy();
  const clock = fakeClock();
  const trigger = createRetentionTrigger(store, {
    maxEvents: 0, // count disabled → afterAppend never arms
    maxAgeMs: 1000,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  trigger.afterAppend(999);
  assert.equal(clock.pending(), 0, 'with count disabled, appends do not arm');
  trigger.sweep();
  assert.equal(clock.pending(), 1, 'sweep arms a debounced prune');
  clock.flushAll();
  assert.equal(store.calls.length, 1);
});

test('retention: cancel() clears a pending debounced prune before it fires', () => {
  const store = storeWithRetentionSpy();
  const clock = fakeClock();
  const trigger = createRetentionTrigger(store, {
    maxEvents: 1,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  trigger.afterAppend(1);
  assert.equal(clock.pending(), 1);
  trigger.cancel();
  assert.equal(clock.pending(), 0, 'pending timer cleared');
  clock.flushAll(); // nothing to flush
  assert.equal(store.calls.length, 0, 'no prune ran');
});

test('retention: a prune FAILURE is swallowed — it never crashes the receiver', async () => {
  const store = { prune: async () => { throw new Error('disk full'); } };
  const clock = fakeClock();
  const trigger = createRetentionTrigger(store, {
    maxEvents: 1,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  trigger.afterAppend(1);
  assert.doesNotThrow(() => clock.flushAll(), 'flushing a failed prune must not throw');
  await new Promise((r) => setTimeout(r, 0)); // let the rejected prune's .catch settle
});

test('retention: ingest below the count bound triggers NO rewrite (the request path performs only the fast append)', async () => {
  const f = inMemoryFile();
  let rewriteCalls = 0;
  const store = createNdjsonStore({
    sink: f.sink,
    source: f.source,
    rewrite: async (t) => {
      rewriteCalls += 1;
      await f.rewrite(t);
    },
  });
  const clock = fakeClock();
  const retention = createRetentionTrigger(store, {
    maxEvents: 1000,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent }, retention });

  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
  assert.equal(res.statusCode, 202);
  assert.equal(rewriteCalls, 0, 'no rewrite seam call on a sub-bound ingest');
  assert.equal(clock.pending(), 0, 'no prune armed on a sub-bound ingest');
});

test('retention: sustained ingest past the count bound arms a debounced prune that compacts the store to the cap', async () => {
  const f = inMemoryFile();
  const store = createNdjsonStore(f);
  const clock = fakeClock();
  const retention = createRetentionTrigger(store, {
    maxEvents: 3,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent }, retention });

  // Ingest 4 one-event batches; maxEvents=3 → a debounced prune is armed once the
  // bound is crossed, but it has NOT fired yet (the response path does not flush it).
  for (let i = 0; i < 4; i++) {
    const res = fakeRes();
    await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
    assert.equal(res.statusCode, 202);
  }
  assert.equal(clock.pending(), 1, 'a debounced prune is armed once the bound is crossed');
  assert.equal(f.read().length, 4, 'pre-prune: all 4 events still persisted');

  clock.flushAll(); // fire the off-path, debounced prune
  await new Promise((r) => setTimeout(r, 0)); // let the async compaction settle

  assert.equal(f.read().length, 3, 'post-prune: store bounded to the count cap');
});

test('retention: /summary stays self-consistent over a pruned store — aggregates reflect the retained set only', async () => {
  const f = inMemoryFile();
  const store = createNdjsonStore(f);
  const handler = createRequestHandler({ store });

  // Seed a known mix: 3 errors (2 distinct names) + 1 crash across a time window.
  await store.appendEvents([
    { schemaVersion: 1, type: 'error', runtime: 'main', timestamp: 100, name: 'TypeError', message: 'm', frames: [] },
    { schemaVersion: 1, type: 'error', runtime: 'main', timestamp: 200, name: 'RangeError', message: 'm', frames: [] },
    { schemaVersion: 1, type: 'error', runtime: 'main', timestamp: 300, name: 'TypeError', message: 'm', frames: [] },
    { schemaVersion: 1, type: 'crash', runtime: 'renderer', timestamp: 400, reason: 'oom' },
  ]);

  // Prune to the newest 2 by count → drops the two oldest errors.
  await store.prune({ maxEvents: 2 });

  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);

  // Retained = the last 2 appended: error@300 (TypeError) + crash@400.
  assert.equal(body.total, 2);
  assert.deepEqual(body.byType, { error: 1, crash: 1, 'performance-stall': 0 });
  assert.deepEqual(body.topErrorNames, [{ name: 'TypeError', count: 1 }], 'RangeError was pruned');
  // topSignatures reflects ONLY the retained set: RangeError was pruned, so the
  // sole error signature is the name-only `TypeError`; crash@400 → `crash:oom`.
  assert.deepEqual(body.topSignatures, [
    { signature: 'TypeError', type: 'error', count: 1 },
    { signature: 'crash:oom', type: 'crash', count: 1 },
  ]);
  assert.deepEqual(body.schemaVersions, { '1': 2 });
  assert.equal(body.firstSeen, 300, 'firstSeen bounds the RETAINED window (100 was pruned)');
  assert.equal(body.lastSeen, 400);
});

test('retention: an absent retention dep leaves the handler unchanged (today behavior — no afterAppend call)', async () => {
  const captured = [];
  const store = createNdjsonStore({ sink: async (l) => void captured.push(l) });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } }); // no retention
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
  assert.equal(res.statusCode, 202);
  assert.equal(captured.length, 1);
});

// ── REJECTIONS TALLY (WARDEN-591) ─────────────────────────────────────────────
// The in-memory, receiver-local tally surfaces "traffic arriving and being
// hard-rejected" vs "no traffic at all" in GET /summary. It mirrors the retention
// trigger's injected-seam discipline (an optional dep + an injected `now` for a
// fake clock). Bounded: counts by status + a single most-recent sample, never one
// record per rejection. Below: the tally is driven directly with an INJECTED fake
// clock, then the handler is proven to record at EVERY rejection site (the
// auth-gate 401, the 404, the body-read 400, AND the 400/415/422 from ingest).

test('rejections tally: a fresh tally snapshots to the zeroed shape (parity with an idle receiver)', () => {
  const tally = createRejectionTally({ now: () => 0 });
  assert.deepEqual(tally.snapshot(), {
    total: 0,
    byStatus: {},
    lastStatus: null,
    lastReason: null,
    lastSeen: null,
  });
});

test('rejections tally: record() accumulates per-status counts + tracks the most-recent occurrence', () => {
  let clock = 1000;
  const tally = createRejectionTally({ now: () => clock });
  tally.record({ status: 415, reason: 'unsupported telemetry schema version: expected "1", got "2"' });
  clock = 2000;
  tally.record({ status: 415, reason: 'unsupported telemetry schema version: expected "1", got "3"' });
  clock = 3000;
  tally.record({ status: 422, reason: 'one or more events failed schema validation; batch rejected' });

  const snap = tally.snapshot();
  assert.equal(snap.total, 3);
  assert.deepEqual(snap.byStatus, { '415': 2, '422': 1 });
  assert.equal(snap.lastStatus, 422, 'lastStatus is the most-recently-recorded status');
  assert.equal(
    snap.lastReason,
    'one or more events failed schema validation; batch rejected',
    'lastReason is the single most-recent sample'
  );
  assert.equal(snap.lastSeen, 3000, 'lastSeen is the injected now() of the most-recent record');
});

test('rejections tally: snapshot() is a stable point-in-time copy — a later record does not mutate it', () => {
  let clock = 5000;
  const tally = createRejectionTally({ now: () => clock });
  tally.record({ status: 401, reason: 'unauthorized' });
  const snap = tally.snapshot();
  clock = 6000;
  tally.record({ status: 404, reason: 'not found' });
  // the earlier snapshot is unchanged by the later record
  assert.deepEqual(snap, {
    total: 1,
    byStatus: { '401': 1 },
    lastStatus: 401,
    lastReason: 'unauthorized',
    lastSeen: 5000,
  });
  // a fresh snapshot reflects the new state
  assert.equal(tally.snapshot().total, 2);
  assert.equal(tally.snapshot().lastStatus, 404);
});

test('rejections tally: BOUNDED — many records with varied reasons never grow unbounded (counts by status, one sample)', () => {
  const tally = createRejectionTally({ now: () => 0 });
  for (let i = 0; i < 1000; i++) {
    tally.record({ status: 415, reason: `drift reason #${i}` }); // 1000 distinct reasons
  }
  const snap = tally.snapshot();
  assert.equal(snap.total, 1000);
  assert.deepEqual(snap.byStatus, { '415': 1000 }, 'one count key — not 1000 entries');
  assert.equal(snap.lastReason, 'drift reason #999', 'only the single most-recent sample reason is retained');
});

test('rejections tally: record() with no status is a defensive no-op (never throws)', () => {
  const tally = createRejectionTally({ now: () => 0 });
  tally.record({ reason: 'no status' });
  tally.record({});
  assert.deepEqual(tally.snapshot(), {
    total: 0,
    byStatus: {},
    lastStatus: null,
    lastReason: null,
    lastSeen: null,
  });
});

test('rejections tally: record() with a non-string reason stores null (no unbounded/garbage reason)', () => {
  const tally = createRejectionTally({ now: () => 0 });
  tally.record({ status: 400, reason: 12345 }); // wrong type — not a payload string
  const snap = tally.snapshot();
  assert.equal(snap.total, 1);
  assert.equal(snap.lastStatus, 400);
  assert.equal(snap.lastReason, null, 'a non-string reason is not retained');
});

// ── REJECTIONS AGGREGATE surfaced in GET /summary (WARDEN-591) ─────────────────
// A tally wired into the handler records at EVERY rejection site; GET /summary
// reads its snapshot. A handler + tally wired to a LIVE (sink+source) store: a
// rejection recorded on one request is visible to a follow-up GET /summary on the
// SAME handler (the tally is a handler closure, so it persists across requests).
const TALLY_SECRET = 'tally-shared-token';

function wiringWithTally(authToken) {
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const rejections = createRejectionTally();
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    rejections,
    ...(authToken ? { authToken } : {}),
  });
  return { handler, rejections };
}

// Drive GET /summary on the handler and return just the `rejections` aggregate.
async function summaryRejections(handler, headers) {
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary', headers }), res);
  assert.equal(res.statusCode, 200, 'summary read must succeed to inspect the tally');
  return JSON.parse(res.body).rejections;
}

test('a 415 rejection (unknown schema) is surfaced in GET /summary — schema drift made visible', async () => {
  const { handler } = wiringWithTally();
  const res = fakeRes();
  await handler(fakeReq({ headers: { 'x-telemetry-schema': String(SCHEMA_VERSION + 1) }, body: 'GARBAGE NOT JSON' }), res);
  assert.equal(res.statusCode, 415);

  const rej = await summaryRejections(handler);
  assert.ok(rej.byStatus['415'] >= 1, 'the 415 was recorded');
  assert.ok(rej.total >= 1);
  assert.equal(rej.lastStatus, 415);
  assert.notEqual(rej.lastSeen, null);
  assert.match(rej.lastReason, /unsupported telemetry schema version/, 'sample reason is the receiver diagnostic, not a payload');
});

test('a 422 rejection (out-of-schema event) is surfaced in GET /summary', async () => {
  const { handler } = wiringWithTally();
  const badBody = JSON.stringify({ schemaVersion: SCHEMA_VERSION, events: [{ ...validError, runtime: 'worker' }] });
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: badBody }), res);
  assert.equal(res.statusCode, 422);

  const rej = await summaryRejections(handler);
  assert.ok(rej.byStatus['422'] >= 1);
  assert.equal(rej.lastStatus, 422);
});

test('a 400 rejection (malformed JSON body) is surfaced in GET /summary', async () => {
  const { handler } = wiringWithTally();
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: 'not json' }), res);
  assert.equal(res.statusCode, 400);

  const rej = await summaryRejections(handler);
  assert.ok(rej.byStatus['400'] >= 1);
  assert.equal(rej.lastStatus, 400);
});

test('a 404 rejection (routing miss) is surfaced in GET /summary', async () => {
  const { handler } = wiringWithTally();
  const res = fakeRes();
  await handler(fakeReq({ url: '/somewhere-else', body: validBody }), res);
  assert.equal(res.statusCode, 404);

  const rej = await summaryRejections(handler);
  assert.ok(rej.byStatus['404'] >= 1);
  assert.equal(rej.lastStatus, 404);
});

test('a 401 rejection (auth gate) is surfaced in GET /summary — the tally covers the PRE-ingest path', async () => {
  // The 401 is the trickiest path: the auth gate returns BEFORE ingest() runs, so a
  // naive "record on !result.ok" tally would silently MISS it. This proves the
  // auth-gate site records. The tally is read via an AUTHENTICATED GET /summary so
  // the read itself does not add a 401 to the count.
  const { handler } = wiringWithTally(TALLY_SECRET);
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res); // no bearer → 401 at the gate
  assert.equal(res.statusCode, 401);

  const rej = await summaryRejections(handler, { authorization: `Bearer ${TALLY_SECRET}` });
  assert.ok(rej.byStatus['401'] >= 1, 'the auth-gate 401 was recorded (not just ingest-result rejections)');
  assert.equal(rej.lastStatus, 401);
  assert.equal(rej.lastReason, 'unauthorized');
});

test('a successful ingest (202) does NOT increment rejections (accepted traffic is never counted as rejected)', async () => {
  const { handler } = wiringWithTally();
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
  assert.equal(res.statusCode, 202);

  const rej = await summaryRejections(handler);
  assert.deepEqual(rej, { total: 0, byStatus: {}, lastStatus: null, lastReason: null, lastSeen: null });
});

test('an idle receiver (no traffic) returns zeroed rejections in GET /summary (parity with today — no false alarm)', async () => {
  const { handler } = wiringWithTally();
  const rej = await summaryRejections(handler);
  assert.deepEqual(rej, { total: 0, byStatus: {}, lastStatus: null, lastReason: null, lastSeen: null });
});

test('rejections accumulate across requests and stay bounded — mixed statuses surface a byStatus histogram', async () => {
  const { handler } = wiringWithTally();
  await handler(fakeReq({ headers: { 'x-telemetry-schema': String(SCHEMA_VERSION + 1) }, body: 'x' }), fakeRes()); // 415
  await handler(fakeReq({ headers: schemaHeaders, body: 'not json' }), fakeRes()); // 400
  await handler(fakeReq({ url: '/no-such-route' }), fakeRes()); // 404

  const rej = await summaryRejections(handler);
  assert.equal(rej.total, 3);
  assert.deepEqual(rej.byStatus, { '415': 1, '400': 1, '404': 1 });
  assert.equal(rej.lastStatus, 404, 'lastStatus reflects the most-recent rejection');
});

test('GET /summary WITHOUT a wired tally still returns a zeroed rejections field (backward-compatible additive shape)', async () => {
  // A caller that does not pass a tally (e.g. the existing test wirings) still gets
  // the rejections field, zeroed — the handler is unchanged for callers that don't
  // wire the tally, exactly like an absent retention dep.
  const store = readableStore([]);
  const handler = createRequestHandler({ store }); // no tally, no schema override
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(JSON.parse(res.body).rejections, {
    total: 0,
    byStatus: {},
    lastStatus: null,
    lastReason: null,
    lastSeen: null,
  });
});

// ── PERSIST-ERROR TALLY (WARDEN-607) ─────────────────────────────────────────
// The WRITE-path twin of the rejections tally. Before WARDEN-607, a persist
// failure (store.appendEvents() throwing — disk full / EACCES / EISDIR / a missing
// store file / a sink rejection) was invisible twice over: the ingest handler
// awaited ingest() with NO try/catch, so a persist throw rejected the handler
// promise and res.end() was NEVER called (the client's fetch hung until socket
// timeout), AND no tally covered the persist path (the rejections tally is
// HTTP-rejection-sites-only), so an all-un-storable receiver returned the SAME
// empty /summary as an idle one. Below: the tally is driven directly with an
// INJECTED fake clock (mirroring the rejections tally unit tests), then the
// handler is proven to (1) catch a persist throw → a clean retryable 503 with no
// hung socket, (2) surface a BOUNDED persistErrors aggregate on GET /summary, and
// (3) keep it a SEPARATE signal from rejections. Still ZERO real fs, ZERO real
// network — driven with fake req/res and a capturing-but-throwing sink.

// A store whose appendEvents() THROWS on every write (a persist failure),
// mirroring the production case (disk full / EACCES / a sink rejection). A source
// is wired too so a follow-up GET /summary can read the persistErrors tally on the
// SAME handler. `makeError` controls the throw (varied messages for the bounded
// test); it defaults to a single 'disk full'.
function failingPersistStore(makeError = () => new Error('disk full')) {
  const lines = [];
  return createNdjsonStore({
    sink: async () => {
      throw makeError();
    },
    source: () => lines.map((l) => JSON.parse(l)),
  });
}

// Wire a handler to a store whose appendEvents() throws + a persistErrors tally
// with an INJECTED clock (so lastSeen is deterministic). Mirrors wiringWithTally.
function wiringWithPersistErrors({ now, makeError } = {}) {
  const store = failingPersistStore(makeError);
  const persistErrors = createPersistErrorTally(now != null ? { now } : {});
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    persistErrors,
  });
  return { handler, persistErrors, store };
}

// Drive GET /summary on the handler and return just the `persistErrors` aggregate.
async function summaryPersistErrors(handler) {
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200, 'summary read must succeed to inspect the tally');
  return JSON.parse(res.body).persistErrors;
}

test('persistErrors tally: a fresh tally snapshots to the zeroed shape (parity with a healthy receiver)', () => {
  const tally = createPersistErrorTally({ now: () => 0 });
  assert.deepEqual(tally.snapshot(), {
    total: 0,
    lastReason: null,
    lastSeen: null,
  });
});

test('persistErrors tally: record() accumulates the total + tracks the most-recent occurrence', () => {
  let clock = 1000;
  const tally = createPersistErrorTally({ now: () => clock });
  tally.record({ reason: 'ENOSPC: no space left on device, write' });
  clock = 2000;
  tally.record({ reason: "EACCES: permission denied, open '/var/lib/warden/events.ndjson'" });

  const snap = tally.snapshot();
  assert.equal(snap.total, 2);
  assert.equal(
    snap.lastReason,
    "EACCES: permission denied, open '/var/lib/warden/events.ndjson'",
    'lastReason is the single most-recent sample (the receiver/store diagnostic)'
  );
  assert.equal(snap.lastSeen, 2000, 'lastSeen is the injected now() of the most-recent record');
});

test('persistErrors tally: snapshot() is a stable point-in-time copy — a later record does not mutate it', () => {
  let clock = 5000;
  const tally = createPersistErrorTally({ now: () => clock });
  tally.record({ reason: 'disk full' });
  const snap = tally.snapshot();
  clock = 6000;
  tally.record({ reason: 'EISDIR: illegal operation on a directory, write' });
  // the earlier snapshot is unchanged by the later record
  assert.deepEqual(snap, { total: 1, lastReason: 'disk full', lastSeen: 5000 });
  // a fresh snapshot reflects the new state
  assert.equal(tally.snapshot().total, 2);
  assert.equal(tally.snapshot().lastReason, 'EISDIR: illegal operation on a directory, write');
});

test('persistErrors tally: BOUNDED — many records with varied reasons never grow unbounded (one count, one sample)', () => {
  // Mirrors "rejections tally: BOUNDED". 1000 distinct reasons must accumulate to
  // a single COUNT + the single most-recent sample, never one entry per failure.
  const tally = createPersistErrorTally({ now: () => 0 });
  for (let i = 0; i < 1000; i++) {
    tally.record({ reason: `outage reason #${i}` }); // 1000 distinct reasons
  }
  const snap = tally.snapshot();
  assert.equal(snap.total, 1000);
  assert.equal(snap.lastReason, 'outage reason #999', 'only the single most-recent sample reason is retained');
  assert.deepEqual(Object.keys(snap).sort(), ['lastReason', 'lastSeen', 'total'], 'shape stays bounded — no per-failure growth');
});

test('persistErrors tally: record() with a non-string/missing reason stores null (no unbounded/garbage reason)', () => {
  const tally = createPersistErrorTally({ now: () => 0 });
  tally.record({ reason: 12345 }); // wrong type — not a diagnostic string
  tally.record({}); // missing reason
  const snap = tally.snapshot();
  assert.equal(snap.total, 2);
  assert.equal(snap.lastReason, null, 'a non-string/missing reason is not retained');
});

// ── PERSIST-ERROR AGGREGATE surfaced in GET /summary + clean 503 (WARDEN-607) ──
// A persist failure is caught → clean retryable 503 (no hung socket) AND recorded
// into a tally whose snapshot GET /summary reads. Driven through the handler with
// a throwing sink; the tally persists across requests on the SAME handler closure.

test('a persist failure (store.appendEvents throws) → clean retryable 503, no hung/rejected response', async () => {
  // The WARDEN-607 bug: before the fix, a persist throw rejected the handler
  // promise and res.end() was never called — the client's fetch hung until socket
  // timeout. The handler must now RESOLVE with a clean 503 (res.ended === true).
  const { handler } = wiringWithPersistErrors();
  const res = fakeRes();
  // This await RESOLVES — today's bug made it reject. (If it rejected, node:test
  // surfaces the rejection as a failure before the assertions below run.)
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
  assert.equal(res.ended, true, 'the response is ended — no hung socket');
  assert.equal(res.statusCode, 503, 'a persist failure is a clean retryable 503 (5xx, not a 4xx drop)');
  assert.equal(res.headers['content-type'], 'application/json');
  assert.deepEqual(JSON.parse(res.body), { error: 'could not persist telemetry batch' });
});

test('a persist failure 503 never echoes the raw event payload (trust model preserved)', async () => {
  // The 503 body is the receiver's own FIXED diagnostic — never the event bytes
  // that failed to persist. The seeded event is schema-VALID (so ingest reaches the
  // sink and the throw fires) yet carries a raw message + an extended-tier name;
  // neither must reach the response (parity with the rejection-tally trust tests).
  const secretBody = JSON.stringify({
    schemaVersion: SCHEMA_VERSION,
    events: [{ ...validError, name: 'SecretErrorName', message: 'super secret stack detail', chatName: 'Refactor auth' }],
  });
  const { handler } = wiringWithPersistErrors();
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: secretBody }), res);
  assert.equal(res.statusCode, 503, 'the valid event reached the sink and tripped the persist failure');
  const json = res.body;
  assert.equal(json.includes('super secret stack detail'), false, 'no raw message in the 503');
  assert.equal(json.includes('Refactor auth'), false, 'no extended-tier identifier in the 503');
  assert.equal(json.includes('SecretErrorName'), false, 'no error name in the 503');
});

test('a persist failure is surfaced in GET /summary — "validated but un-storable" made visible', async () => {
  const { handler } = wiringWithPersistErrors({ now: () => 12345 });
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
  assert.equal(res.statusCode, 503);

  const pe = await summaryPersistErrors(handler);
  assert.ok(pe.total >= 1, 'the persist failure was recorded');
  assert.equal(pe.lastReason, 'disk full', 'sample reason is the store diagnostic, not a payload');
  assert.equal(pe.lastSeen, 12345, 'lastSeen is the injected now() of the failure');
});

test('persistErrors stay BOUNDED across many failures with varied reasons — one count + single most-recent sample', async () => {
  // Drive many persist failures with varied sink-throw messages; the tally must
  // accumulate a COUNT and retain only the single most-recent sample (mirroring the
  // "rejections tally: BOUNDED" test). lastSeen advances via the injected clock.
  let clock = 0;
  let i = 0;
  const { handler } = wiringWithPersistErrors({
    now: () => (clock += 1000),
    makeError: () => new Error(`outage #${i++}`),
  });
  for (let n = 0; n < 50; n++) {
    await handler(fakeReq({ headers: schemaHeaders, body: validBody }), fakeRes());
  }
  const pe = await summaryPersistErrors(handler);
  assert.equal(pe.total, 50);
  assert.equal(pe.lastReason, 'outage #49', 'only the single most-recent sample reason is retained');
  assert.equal(pe.lastSeen, 50_000, 'lastSeen is the injected now() of the most-recent failure');
  assert.deepEqual(Object.keys(pe).sort(), ['lastReason', 'lastSeen', 'total'], 'shape stays bounded — no per-failure growth');
});

test('a persist failure records into persistErrors, NOT into the rejections tally (separate signals)', async () => {
  // A persist failure is a distinct "validated but un-storable" class — it must not
  // overload the rejection tally's HTTP-rejection-sites-only contract. Wire BOTH
  // tallies; a persist throw bumps persistErrors and leaves rejections at zero.
  let clock = 7000;
  const store = failingPersistStore();
  const rejections = createRejectionTally({ now: () => clock });
  const persistErrors = createPersistErrorTally({ now: () => clock });
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    rejections,
    persistErrors,
  });
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
  assert.equal(res.statusCode, 503);

  const summaryRes = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), summaryRes);
  const body = JSON.parse(summaryRes.body);
  assert.equal(body.persistErrors.total, 1, 'the persist failure was recorded in its own tally');
  assert.deepEqual(
    body.rejections,
    { total: 0, byStatus: {}, lastStatus: null, lastReason: null, lastSeen: null },
    'rejections untouched — a persist failure is not an HTTP rejection'
  );
});

test('a successful ingest (202) does NOT increment persistErrors (a healthy write is never a failure)', async () => {
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const persistErrors = createPersistErrorTally({ now: () => 0 });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent }, persistErrors });
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
  assert.equal(res.statusCode, 202);

  const pe = await summaryPersistErrors(handler);
  assert.deepEqual(pe, { total: 0, lastReason: null, lastSeen: null });
});

test('an idle receiver (no traffic) returns zeroed persistErrors in GET /summary (parity with today — no false alarm)', async () => {
  const { handler } = wiringWithPersistErrors();
  const pe = await summaryPersistErrors(handler);
  assert.deepEqual(pe, { total: 0, lastReason: null, lastSeen: null });
});

test('GET /summary WITHOUT a wired persistErrors tally still returns a zeroed persistErrors field (backward-compatible additive shape)', async () => {
  // A caller that does not pass a persistErrors tally (e.g. the existing test
  // wirings) still gets the field, zeroed — the handler is unchanged for callers
  // that don't wire the tally, exactly like an absent rejections dep.
  const store = readableStore([]);
  const handler = createRequestHandler({ store }); // no persistErrors, no schema override
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(JSON.parse(res.body).persistErrors, {
    total: 0,
    lastReason: null,
    lastSeen: null,
  });
});

// ── INGEST BODY CAP (WARDEN-627) ─────────────────────────────────────────────
// Retention (WARDEN-579) bounded the unbounded STORE; the body cap bounds the one
// remaining unbounded INPUT — the POST /ingest request body, which readBody once
// buffered fully into memory with no limit. A single oversized POST could exhaust
// receiver RSS and take down a persistent service the self-hosting maintainer runs
// (and the receiver is open by default in dev, so an unauthenticated attacker OR a
// buggy client can trigger it). An oversized body is rejected with a non-retryable
// 413 BEFORE it can be fully buffered, recorded at the existing rejection seam so it
// surfaces on GET /summary.rejections.byStatus['413'] the same way 415s already do.
//
// Two pre-/mid-buffer checks: a Content-Length PRE-CHECK that 413's without reading a
// byte, and a cap-aware readBody that 413's mid-read when an unknown-length (chunked)
// body crosses the cap. A schema-handshake pre-check also runs before the body is
// buffered (defense-in-depth alongside ingest()'s own check) so a wrong-version
// request is 415'd without paying its body's memory cost. Still ZERO real fs, ZERO
// real network — readBody is driven directly (it is exported for exactly this) and the
// handler is driven with fake req/res like every block above.

// ── readBody seam: the mid-read bound ─────────────────────────────────────────
// A minimal readable-stream double: emits the given chunks then `end` on nextTick
// (mimicking a real readable stream). readBody consumes via .on('data'|'end'|'error').
function streamReq(chunks = []) {
  const req = new EventEmitter();
  process.nextTick(() => {
    for (const c of chunks) req.emit('data', c);
    req.emit('end');
  });
  return req;
}

test('readBody: default (maxBytes=0) is UNBOUNDED — reads a large body fully (today behavior preserved for any caller)', async () => {
  // `0` is the unbounded escape hatch (INGEST_MAX_BODY_BYTES=0). A caller that does
  // not wire a cap — including any future readBody caller outside /ingest — gets the
  // original accumulate-the-whole-body behavior, unchanged.
  const big = 'x'.repeat(100_000);
  const body = await readBody(streamReq([big]));
  assert.equal(body.length, 100_000);
});

test('readBody: maxBytes rejects with a TAGGED PAYLOAD_TOO_LARGE error when the body exceeds the cap', async () => {
  // The tag is LOAD-BEARING: the handler maps PAYLOAD_TOO_LARGE → 413, whereas a plain
  // read error stays the existing 400. Without the tag the cap case would silently
  // fall into the 400 path. So the rejection MUST carry a distinguishable code.
  await assert.rejects(
    readBody(streamReq(['x'.repeat(100)]), { maxBytes: 10 }),
    (err) => {
      assert.ok(err instanceof Error, 'still an Error (interoperable with the existing read-error catch)');
      assert.equal(err.code, 'PAYLOAD_TOO_LARGE', 'the tag the handler keys on to map → 413');
      assert.equal(err.limit, 10, 'the limit is surfaced for diagnostics');
      assert.match(err.message, /exceeds size limit/);
      return true;
    }
  );
});

test('readBody: maxBytes resolves the full body when it is AT the cap (the bound is exclusive — size > maxBytes aborts)', async () => {
  // The check is `size > maxBytes`, so a body of EXACTLY maxBytes bytes is accepted —
  // an off-by-one that rejected an at-cap body would drop a legit batch sized to the cap.
  const exact = 'x'.repeat(10);
  const body = await readBody(streamReq([exact]), { maxBytes: 10 });
  assert.equal(body, exact);
});

test('readBody: the cap counts BYTES not string chars — a multibyte payload over the byte cap still rejects (no under-count sneak-past)', async () => {
  // Content-Length is in bytes, so the cap must measure bytes too. 'é' is 2 UTF-8
  // bytes but 1 char: 2 chars = 4 bytes > a 3-byte cap → MUST reject. A char-counting
  // cap would see "2 chars < 3" and wrongly resolve, letting an attacker sneak a
  // multibyte payload past the byte limit.
  await assert.rejects(
    readBody(streamReq(['éé']), { maxBytes: 3 }),
    (err) => { assert.equal(err.code, 'PAYLOAD_TOO_LARGE'); return true; }
  );
});

test('readBody: a body split across CHUNKS accumulates — crossing the cap on a LATER chunk still aborts', async () => {
  // Each chunk is independently under the cap, but the running total crosses it on the
  // second chunk. readBody checks the accumulated size on every chunk, not just the first.
  await assert.rejects(
    readBody(streamReq(['x'.repeat(8), 'x'.repeat(8)]), { maxBytes: 10 }),
    (err) => { assert.equal(err.code, 'PAYLOAD_TOO_LARGE'); return true; }
  );
});

test('readBody: on cap exceed, the stream is RESUMED (drained) — NOT destroyed — so the shared socket stays alive for the 413 response', async () => {
  // req and res share a socket: req.destroy() would abort the connection and the
  // handler's 413 would NEVER reach the client (the client would see a connection
  // reset, not the clean non-retryable 4xx it already drops on). resume() drains-and-
  // discards the remainder (no buffering) while keeping the connection alive. When BOTH
  // are present, resume is preferred and destroy is NOT called. (A live oversized
  // chunked POST confirmed this: destroy lost the 413, resume delivers it.)
  const req = new EventEmitter();
  let resumed = 0;
  let destroyed = false;
  req.resume = () => { resumed += 1; };
  req.destroy = () => { destroyed = true; };
  process.nextTick(() => {
    req.emit('data', 'x'.repeat(100)); // exceeds cap → abort + drain mid-read
    req.emit('end');
  });
  await assert.rejects(readBody(req, { maxBytes: 10 }), /exceeds size limit/);
  assert.equal(resumed, 1, 'resume() drained the remainder');
  assert.equal(destroyed, false, 'destroy() NOT called — it would tear down the socket and lose the 413');
});

test('readBody: on cap exceed with a resume-less stream, falls back to destroy() (last-resort stop for a non-stream test double)', async () => {
  // A stream with no resume (a minimal double) still must be stopped; destroy is the
  // fallback. A real IncomingMessage always has resume, so this path is for unusual
  // callers only. Either way the abort never throws.
  const req = new EventEmitter();
  let destroyed = false;
  req.destroy = () => { destroyed = true; };
  process.nextTick(() => {
    req.emit('data', 'x'.repeat(100));
  });
  await assert.rejects(readBody(req, { maxBytes: 10 }), /exceeds size limit/);
  assert.equal(destroyed, true, 'destroy() fallback fired (resume absent)');
});

test('readBody: a PLAIN read error (no PAYLOAD_TOO_LARGE tag) rejects with the raw error — stays distinguishable from the cap case', async () => {
  // The handler maps PAYLOAD_TOO_LARGE → 413 and everything else → 400. So a plain
  // transport error MUST reject WITHOUT the tag, or it would be mis-mapped to a 413.
  const req = new EventEmitter();
  process.nextTick(() => req.emit('error', new Error('ECONNRESET: socket hang up')));
  await assert.rejects(readBody(req, { maxBytes: 10 }), (err) => {
    assert.equal(err.code, undefined, 'no PAYLOAD_TOO_LARGE tag — this is a plain read error, not the cap');
    assert.match(err.message, /ECONNRESET/);
    return true;
  });
});

// ── handler: the Content-Length pre-check + cap-aware readBody → clean 413 ─────
// A fakeReq that records whether the handler began reading the body: it counts
// `data` listener registrations. The pre-read 413 (Content-Length) and pre-read 415
// (schema handshake) paths return WITHOUT attaching a `data` listener (the body is
// never buffered — "don't buffer what you'll reject"). readBody is the ONLY code that
// attaches a `data` listener, so a count of 0 means the body was never read.
function bodySpyReq(opts) {
  const req = fakeReq(opts);
  let dataListeners = 0;
  const origOn = req.on.bind(req);
  req.on = (event, ...rest) => {
    if (event === 'data') dataListeners += 1;
    return origOn(event, ...rest);
  };
  req.bodyListenerCount = () => dataListeners;
  return req;
}

// Wire a handler to a capturing store + rejection tally + a body cap. Mirrors the
// existing wiring helpers; the cap is the WARDEN-627 addition.
function wiringWithCap({ maxBodyBytes, rejections = createRejectionTally() } = {}) {
  const captured = [];
  const store = createNdjsonStore({ sink: async (l) => void captured.push(l) });
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    rejections,
    maxBodyBytes,
  });
  return { handler, captured, rejections };
}

test('the body-cap DEFAULT is bounded — DEFAULT_MAX_BODY_BYTES is a finite positive byte cap (the oversized-POST OOM bug is fixed by default)', () => {
  // Mirrors the retention-default test: the default IS bounded (unbounded input was
  // the bug), never unbounded. A maintainer who runs the receiver bare gets the cap
  // without opting in — they must explicitly set INGEST_MAX_BODY_BYTES=0 to unbind it.
  assert.ok(
    typeof DEFAULT_MAX_BODY_BYTES === 'number' && DEFAULT_MAX_BODY_BYTES > 0 && Number.isFinite(DEFAULT_MAX_BODY_BYTES),
    'the default body cap must be a finite positive bound, never unbounded'
  );
});

test('oversized body (no Content-Length) over the cap → 413 via the cap-aware readBody path + recorded in the tally', async () => {
  // The chunked / unknown-length case: no Content-Length header to pre-check, so the
  // bound fires MID-READ inside readBody (the tagged PAYLOAD_TOO_LARGE rejection → 413,
  // NOT the 400 a plain read error records). The 413 lands in the rejection tally the
  // same way a 415 does — so oversized traffic is distinguishable from no traffic.
  const { handler, rejections } = wiringWithCap({ maxBodyBytes: 10 });
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: 'x'.repeat(100) }), res);
  assert.equal(res.statusCode, 413);
  assert.deepEqual(JSON.parse(res.body), { error: 'request body too large' });
  const snap = rejections.snapshot();
  assert.equal(snap.byStatus['413'], 1, 'the 413 was recorded at the rejection seam');
  assert.equal(snap.lastStatus, 413);
  assert.equal(snap.lastReason, 'request body too large', 'the sample reason is the receiver diagnostic, not the payload');
});

test('Content-Length over the cap → 413 PRE-READ, the body is NEVER buffered (no data listener attached)', async () => {
  // The cheapest bound: the header DECLARES a length over the cap, so 413 WITHOUT
  // reading a byte. The declared length is the attack vector (a client can claim a
  // huge body); the actual body content is irrelevant because it is never read.
  const { handler } = wiringWithCap({ maxBodyBytes: 1024 });
  const res = fakeRes();
  const req = bodySpyReq({
    headers: { ...schemaHeaders, 'content-length': String(10 * 1024 * 1024) }, // 10 MiB declared
    body: 'x'.repeat(100), // small actual body — never read anyway
  });
  await handler(req, res);
  assert.equal(res.statusCode, 413);
  assert.deepEqual(JSON.parse(res.body), { error: 'request body too large' });
  assert.equal(req.bodyListenerCount(), 0, 'readBody was never called — no data listener attached, the body was not buffered');
});

test('Content-Length AT/UNDER the cap → 202 unchanged (a normal batch still ingests); no 413 recorded', async () => {
  // A legit batch — tiny relative to the 1 MiB default — is unaffected: it ingests 202
  // and the rejection tally records no 413. Guards against an over-eager cap dropping
  // real traffic. (validBody is ASCII, so its char length == its byte length.)
  const { handler, captured, rejections } = wiringWithCap({ maxBodyBytes: DEFAULT_MAX_BODY_BYTES });
  const res = fakeRes();
  await handler(
    fakeReq({ headers: { ...schemaHeaders, 'content-length': String(validBody.length) }, body: validBody }),
    res
  );
  assert.equal(res.statusCode, 202);
  assert.equal(captured.length, 1, 'the event was persisted — a normal batch is unaffected');
  assert.equal(rejections.snapshot().byStatus['413'], undefined, 'no 413 recorded for an under-cap body');
});

test('maxBodyBytes=0 (the escape hatch) → an oversized body does NOT 413 (today behavior preserved)', async () => {
  // INGEST_MAX_BODY_BYTES=0 disables the cap entirely. An oversized body is then read
  // fully and reaches ingest — here it 400's as garbage JSON (proving the FULL body was
  // buffered and reached the normal path), NOT 413'd. Guards the opt-out: a maintainer
  // who explicitly unbounds the cap gets the original behavior back.
  const { handler, rejections } = wiringWithCap({ maxBodyBytes: 0 });
  const res = fakeRes();
  await handler(fakeReq({ headers: schemaHeaders, body: 'x'.repeat(100) }), res);
  assert.equal(res.statusCode, 400, 'cap=0 → the body was fully read then 400’d by ingest (garbage JSON), NOT 413');
  assert.equal(rejections.snapshot().byStatus['413'], undefined, 'the cap is off — no 413 is ever recorded');
});

test('wrong x-telemetry-schema → 415 WITHOUT buffering the body (pre-read handshake; data listener never attached)', async () => {
  // Slice 5 (defense-in-depth, option b): the schema handshake is hoisted BEFORE
  // readBody, so a wrong-version request — the WARDEN-591 chief-risk drift symptom (a
  // flood of 415s under a schema mismatch) — is 415'd at ZERO body memory cost. The
  // canonical check still lives in ingest() (its pure-function contract + suite assert
  // there); this is an EARLY copy that collapses the drift case's memory cost to zero.
  const { handler, rejections } = wiringWithCap({ maxBodyBytes: 1024 });
  const res = fakeRes();
  const req = bodySpyReq({ headers: { 'x-telemetry-schema': String(SCHEMA_VERSION + 1) }, body: 'GARBAGE NOT JSON' });
  await handler(req, res);
  assert.equal(res.statusCode, 415);
  assert.equal(req.bodyListenerCount(), 0, 'the body was never buffered — the 415 fired pre-read');
  const snap = rejections.snapshot();
  assert.equal(snap.byStatus['415'], 1);
  assert.match(snap.lastReason, /unsupported telemetry schema version/);
});

test('the 413 body is the receiver FIXED diagnostic, never the oversized payload (trust model preserved)', async () => {
  // Parity with every other rejection site: the recorded reason + response body are the
  // receiver's own short string, never the raw bytes that were rejected. readBody also
  // discards the accumulated buffer on cap-exceed (it returns before `data += chunk`),
  // so the oversized payload never reaches the handler at all.
  const { handler } = wiringWithCap({ maxBodyBytes: 10 });
  const res = fakeRes();
  const payload = 'SECRET-' + 'x'.repeat(100); // oversized and carries a marker
  await handler(fakeReq({ headers: schemaHeaders, body: payload }), res);
  assert.equal(res.statusCode, 413);
  assert.equal(res.body.includes('SECRET'), false, 'the oversized payload is not echoed');
  assert.deepEqual(JSON.parse(res.body), { error: 'request body too large' });
});

// ── IDEMPOTENT INGEST / SEEN-KEY DEDUP (WARDEN-666) ───────────────────────────
// A retried batch whose 2xx was lost would otherwise be appended AGAIN (the client
// reuses the identical bytes). createSeenKeys remembers an accepted batch's key;
// the handler threads it into ingest(), which answers a retry 202 {accepted:0,
// deduped:true} WITHOUT re-persisting. These prove the wiring end-to-end AND that
// the maintainer read surfaces (/summary, /events) reflect the deduplicated count.

test('(WARDEN-666) a retried batch (same idempotency-key) is deduped: ONE store copy; /summary + /events reflect ONE event, not N', async () => {
  // sink + source share one array: appends become visible to subsequent reads, so
  // the read surfaces reflect exactly what was persisted.
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const seenKeys = createSeenKeys();
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent }, seenKeys });
  const headers = { ...schemaHeaders, 'idempotency-key': 'batch-xyz' };

  // First POST: accepted + persisted normally.
  const r1 = fakeRes();
  await handler(fakeReq({ headers, body: validBody }), r1);
  assert.equal(r1.statusCode, 202);
  assert.deepEqual(JSON.parse(r1.body), { accepted: 1 });

  // Second POST — the retry (same key, identical bytes): a 202 SUCCESS so the client
  // stops retrying, but DEDUPED — the store is NOT appended again.
  const r2 = fakeRes();
  await handler(fakeReq({ headers, body: validBody }), r2);
  assert.equal(r2.statusCode, 202, 'a dedup is still a 2xx so the client stops retrying');
  assert.deepEqual(JSON.parse(r2.body), { accepted: 0, deduped: true });
  assert.equal(lines.length, 1, 'the store holds ONE copy — the retry did not double-count');

  // /summary reflects the DEDUPLICATED count (1 crash, not 2–4).
  const summary = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), summary);
  assert.equal(JSON.parse(summary.body).total, 1, '/summary total is the real count, not inflated by the lost-2xx retry');

  // /events lists the payload ONCE.
  const events = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events' }), events);
  const evBody = JSON.parse(events.body);
  assert.equal(evBody.events.length, 1, '/events lists one event, not the retried duplicate');
  assert.equal(evBody.total, 1);
});

test('(WARDEN-666) a handler with NO seenKeys dep does NOT dedup (backward-compatible — today behavior)', async () => {
  // wiring() wires NO seenKeys, so the key header is ignored — both posts persist.
  // An unwired receiver (or an old client that sends no key header) is unchanged.
  const { handler, captured } = wiring();
  const headers = { ...schemaHeaders, 'idempotency-key': 'dup' };
  await handler(fakeReq({ headers, body: validBody }), fakeRes());
  await handler(fakeReq({ headers, body: validBody }), fakeRes());
  assert.equal(captured.length, 2, 'no seenKeys wired → no dedup → both stored (today behavior)');
});

test('(WARDEN-666) distinct idempotency-keys are distinct batches (both persist through the handler)', async () => {
  const seenKeys = createSeenKeys();
  const handler = createRequestHandler({ store: createNdjsonStore({ sink: async () => {} }), schema: { SCHEMA_VERSION, validateEvent }, seenKeys });
  const r1 = fakeRes();
  await handler(fakeReq({ headers: { ...schemaHeaders, 'idempotency-key': 'A' }, body: validBody }), r1);
  const r2 = fakeRes();
  await handler(fakeReq({ headers: { ...schemaHeaders, 'idempotency-key': 'B' }, body: validBody }), r2);
  assert.deepEqual(JSON.parse(r1.body), { accepted: 1 });
  assert.deepEqual(JSON.parse(r2.body), { accepted: 1 }, 'a distinct key is a distinct batch — never deduped');
});

// ── createSeenKeys unit contract (the dedup factory) ──────────────────────────

test('createSeenKeys: has() is false before record and true after', () => {
  const keys = createSeenKeys({ now: () => 0 });
  assert.equal(keys.has('k'), false);
  keys.record('k');
  assert.equal(keys.has('k'), true);
  assert.equal(keys.snapshot().size, 1);
});

test('createSeenKeys: a key expires after ttlMs (fake clock) and can be re-recorded fresh', () => {
  // The injected `now` makes the TTL unit-testable with no real timer (mirrors the
  // tally factories' fake-clock discipline).
  let t = 1000;
  const keys = createSeenKeys({ ttlMs: 5000, maxKeys: 100, now: () => t });
  keys.record('k'); // expires at 6000
  t = 5000;
  assert.equal(keys.has('k'), true, 'within the TTL window the key is still seen');
  t = 6000;
  assert.equal(keys.has('k'), false, 'at/after ttlMs the key has expired (dedup window closed)');
  // Lazy purge lets an expired key be recorded fresh again.
  keys.record('k'); // now expires at 11000
  t = 7000;
  assert.equal(keys.has('k'), true, 'a re-recorded key is seen again');
});

test('createSeenKeys: a FIFO cap evicts the oldest distinct key (bounded — a flood of distinct keys cannot grow it)', () => {
  const keys = createSeenKeys({ ttlMs: 1e9, maxKeys: 2, now: () => 0 });
  keys.record('a');
  keys.record('b');
  keys.record('c'); // exceeds maxKeys=2 → evicts the OLDEST ('a')
  assert.equal(keys.snapshot().size, 2, 'the set stays at the cap, never unbounded');
  assert.equal(keys.has('a'), false, 'the oldest key was evicted');
  assert.equal(keys.has('b'), true);
  assert.equal(keys.has('c'), true);
});

test('createSeenKeys: a non-string/empty key is a no-op (an absent header never dedups)', () => {
  const keys = createSeenKeys({ now: () => 0 });
  keys.record('');
  keys.record(undefined);
  assert.equal(keys.has(''), false);
  assert.equal(keys.has(undefined), false);
  assert.equal(keys.snapshot().size, 0);
});
