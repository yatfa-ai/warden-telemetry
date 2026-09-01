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
import { createRequestHandler, createRetentionTrigger, createRejectionTally, createPersistErrorTally, createRetentionTally, createDedupTally, createSeenKeys, DEFAULT_MAX_EVENTS, DEFAULT_MAX_BODY_BYTES, DEFAULT_DEDUP_MAX_KEYS, DEFAULT_DEDUP_TTL_MS, readBody } from '../server.mjs';
import { SCHEMA_VERSION, validateEvent } from '../schema.ts';
import { createNdjsonStore, parseNdjson } from '../store.mjs';
import { EVENTS_LIMIT_DEFAULT, EVENTS_LIMIT_MAX } from '../events.mjs';
import { signatureOf } from '../summary.mjs';

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
const stallEvent = {
  schemaVersion: 1,
  type: 'performance-stall',
  runtime: 'main',
  timestamp: 7,
  lagMs: 750,
  source: 'event-loop',
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
  assert.deepEqual(body.byType, { error: 1, crash: 1, 'performance-stall': 0, 'operational-metrics': 0 });
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
  assert.deepEqual(body.byType, { error: 0, crash: 0, 'performance-stall': 0, 'operational-metrics': 0 });
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

// ── GET /summary.startedAt — the receiver boot timestamp (WARDEN-768) ─────────
// A top-level epoch-ms recording when THIS receiver (re)booted, captured ONCE at
// createRequestHandler construction (the handler owns the receiver's clock via its
// `now` dep — the boot timestamp IS that clock at construction). It exists so a
// maintainer reading the restart-wiped tallies (rejections / persistErrors /
// retention / deduped — all in-memory and restart-wiped BY DESIGN per README; NOTE
// seenKeys is NOT among them: it is persisted beside telemetry.ndjson by WARDEN-803
// so idempotent-ingest dedup survives a restart) can
// tell a healthy quiet receiver (startedAt = hours ago) from a crash-looping one
// that zeroed every tally seconds ago (startedAt = seconds ago). Those two states
// read byte-identical on /summary without this field. The frozen-at-boot contract
// (now() called once at construction, never per-request) is the load-bearing
// assertion below: a MUTATING fake clock is used so a per-request regression would
// actually change startedAt between reads and trip the test. Driven with fake req/
// res + an INJECTED readable store; ZERO real fs, ZERO real network — same seam as
// the timeline tests above.

test('GET /summary carries a top-level startedAt epoch-ms equal to the handler injected boot clock', async () => {
  // Construct with a fixed fake clock = 86_400_000. startedAt is captured once at
  // construction, so it must equal that boot instant exactly.
  const store = readableStore([]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(typeof body.startedAt, 'number', 'startedAt is a number (flat epoch-ms, like firstSeen/lastSeen)');
  assert.equal(body.startedAt, 86_400_000, 'startedAt equals the boot instant (the injected clock at construction)');
});

test('GET /summary startedAt is frozen at boot — unchanged across a second read of the SAME handler', async () => {
  // A MUTATING clock: each now() call advances by 1000. The handler calls now()
  // ONCE at construction (startedAt = first value), then summarizeTimeline calls
  // now() again per request. If startedAt were (wrongly) recomputed per request,
  // the two reads would diverge — this assertion catches that regression.
  let tick = 0;
  const now = () => 86_400_000 + 1000 * tick++;
  const store = readableStore([]);
  const handler = createRequestHandler({ store, now });
  // first read (a "later" maintainer read of the same process)
  const res1 = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res1);
  const startedAt1 = JSON.parse(res1.body).startedAt;
  // second read of the SAME handler instance
  const res2 = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res2);
  const startedAt2 = JSON.parse(res2.body).startedAt;
  assert.equal(startedAt1, 86_400_000, 'first read reflects the boot instant (construction-time now())');
  assert.equal(startedAt2, startedAt1, 'second read is IDENTICAL — now() was called once at boot, not per request');
});

test('GET /summary startedAt is a plausible epoch-ms under the DEFAULT real clock (backward-compatible shape)', async () => {
  // A handler wired the production way — { store }, no `now` — captures the real
  // boot instant. Bracket construction with Date.now() so the assertion is
  // deterministic: startedAt must fall within [before, after] and be a finite
  // positive epoch-ms (proving the field is ALWAYS present with a real value,
  // never an absent key or null that would break a caller).
  const store = readableStore([]);
  const before = Date.now();
  const handler = createRequestHandler({ store }); // no `now` → real clock (default)
  const after = Date.now();
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const { startedAt } = JSON.parse(res.body);
  assert.equal(typeof startedAt, 'number', 'startedAt is a number even under the default clock');
  assert.ok(Number.isFinite(startedAt) && startedAt > 0, 'startedAt is a finite positive epoch-ms');
  assert.ok(startedAt >= before && startedAt <= after, 'startedAt is the real boot instant (bracketed by handler construction)');
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
  assert.deepEqual(body.byType, { error: 1, crash: 1, 'performance-stall': 0, 'operational-metrics': 0 });
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

test('GET /summary?platform=win32 scopes the stalls magnitude aggregate to win32 stalls only', async () => {
  // The new `stalls` field flows through the `...summarize(filtered)` spread, so
  // the SAME pre-summarize filter (WARDEN-727) scopes it for free: a maintainer who
  // spots a win32 freeze spike can scope /summary?platform=win32 and read that
  // platform's stall SEVERITY with no new filter. Seed a darwin 50ms micro-hitch +
  // a win32 5s hard freeze; scoping to win32 must drop the darwin stall entirely
  // (count === matched, max reflects ONLY the win32 freeze).
  const store = readableStore([
    { ...stallEvent, platform: 'darwin', lagMs: 50, source: 'event-loop', timestamp: 1_800_000 },
    { ...stallEvent, platform: 'win32', lagMs: 5000, source: 'unresponsive', timestamp: 1_800_000 },
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.matched, 1, 'only the win32 stall survives the filter');
  assert.equal(body.stalls.count, 1, 'count === the scoped subset');
  assert.equal(body.stalls.count, body.byType['performance-stall'], 'count-match invariant holds when scoped');
  assert.equal(body.stalls.max, 5000, 'max reflects ONLY the win32 freeze, not the dropped darwin hitch');
  assert.deepEqual(body.stalls.bySource, {
    unresponsive: { count: 1, min: 5000, avg: 5000, max: 5000 },
  });
});

test('GET /summary?platform=win32 scopes the crashReasons histogram to win32 crashes only', async () => {
  // The new `crashReasons` field (WARDEN-872) flows through the `...summarize
  // (filtered)` spread, so the SAME pre-summarize filter (WARDEN-727) scopes it for
  // free: a maintainer who spots a win32 crash spike can scope
  // /summary?platform=win32 and read that platform's crash CAUSE (oom vs killed)
  // with no new filter. Seed a darwin oom + a win32 killed + a win32 oom; scoping
  // to win32 must drop the darwin oom entirely.
  const store = readableStore([
    { ...crashEvent, platform: 'darwin', reason: 'oom', timestamp: 1_800_000 },
    { ...crashEvent, platform: 'win32', reason: 'killed', timestamp: 1_800_000 },
    { ...crashEvent, platform: 'win32', reason: 'oom', timestamp: 1_800_000 },
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.matched, 2, 'only the two win32 crashes survive the filter');
  // crashReasons reflects ONLY the win32 crashes: the darwin oom is gone.
  assert.deepEqual(body.crashReasons, { killed: 1, oom: 1 });
  // the histogram sum never exceeds the scoped crash count (here equality — every
  // surviving crash carries a reason).
  const sum = Object.values(body.crashReasons).reduce((a, b) => a + b, 0);
  assert.ok(sum <= body.byType.crash, 'histogram sum never exceeds the crash count');
});

test('GET /summary on an empty store carries a ZEROED stallsTimeline (additive — always present, no false alarm)', async () => {
  // stallsTimeline is a top-level sibling of `timeline`, handler-composed (NOT inside
  // summarize()) because it needs the injected `now` to anchor the rolling window.
  // It is ALWAYS present (additive, backward-compatible) and zeroed on a quiet store.
  const store = readableStore([]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.ok(body.stallsTimeline, 'stallsTimeline field is present (additive — on every response)');
  assert.deepEqual(body.stallsTimeline.buckets, [], 'no buckets on an empty store');
  assert.equal(
    body.stallsTimeline.bucketMs, 1_800_000,
    'same granularity as the top-level timeline (24h window / 48 buckets)'
  );
});

test('GET /summary stallsTimeline places the worst freeze in time — an ACTIVE regression (newest bucket) apart from a RESOLVED blip (older bucket)', async () => {
  // THE HEADLINE through the handler: stalls.max collapses both 5s freezes into one
  // number (the snapshot cannot answer "is this recent?"); stallsTimeline shows the
  // NEWEST-bucket one is happening now vs the older one that has passed. now =
  // 86_400_000 → bucket 47 = [84_600_000, 86_400_000]; bucket 0 = [0, 1_800_000].
  const store = readableStore([
    { ...stallEvent, timestamp: 0, lagMs: 5000, source: 'unresponsive' },          // bucket 0 (RESOLVED blip)
    { ...stallEvent, timestamp: 84_600_000, lagMs: 5000, source: 'unresponsive' },  // bucket 47 (ACTIVE regression)
    { ...stallEvent, timestamp: 84_600_000, lagMs: 50, source: 'event-loop' },      // bucket 47 (a micro-hitch in the same bucket)
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  // the snapshot collapses both 5s freezes into the same max — it cannot place them
  assert.equal(body.stalls.max, 5000);
  // the timeline places them in TIME: two buckets, the newest one max 5000 (ACTIVE),
  // the older one max 5000 (RESOLVED); the 50ms hitch rides in the newest bucket's
  // count + bySource but never buries the 5s freeze in the max.
  assert.deepEqual(body.stallsTimeline.buckets, [
    {
      bucketStart: 0, bucketEnd: 1_800_000, count: 1, max: 5000,
      bySource: { unresponsive: { count: 1, max: 5000 } },
    },
    {
      bucketStart: 84_600_000, bucketEnd: 86_400_000, count: 2, max: 5000,
      bySource: {
        unresponsive: { count: 1, max: 5000 },
        'event-loop': { count: 1, max: 50 },
      },
    },
  ]);
});

test('GET /summary?platform=win32 scopes the stallsTimeline to win32 freezes only', async () => {
  // stallsTimeline is handler-composed off `filtered` (just like `timeline`), so the
  // SAME pre-summarize filter scopes it for free: a maintainer who spots a win32
  // freeze spike can scope /summary?platform=win32 and read that platform's freeze
  // TIMELINE. Seed a darwin 9000ms freeze in an older bucket (the WORST overall) +
  // a win32 5000ms freeze in the newest; scoping to win32 must drop the darwin freeze
  // entirely (the 9000ms worst is GONE from the scoped timeline + snapshot).
  const newestStart = 84_600_000; // bucket 47
  const store = readableStore([
    { ...stallEvent, platform: 'darwin', timestamp: 0, lagMs: 9000, source: 'unresponsive' },          // bucket 0 (darwin — dropped)
    { ...stallEvent, platform: 'win32', timestamp: newestStart, lagMs: 5000, source: 'unresponsive' }, // bucket 47 (win32 — ACTIVE)
  ]);
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.matched, 1, 'only the win32 stall survives the filter');
  // the darwin 9000ms freeze (the worst overall) is GONE from the scoped timeline
  assert.deepEqual(body.stallsTimeline.buckets, [
    {
      bucketStart: 84_600_000, bucketEnd: 86_400_000, count: 1, max: 5000,
      bySource: { unresponsive: { count: 1, max: 5000 } },
    },
  ]);
  // and the scoped stalls snapshot agrees (the 9000ms darwin freeze is gone there too)
  assert.equal(body.stalls.max, 5000);
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
  assert.deepEqual(body.byType, { error: 0, crash: 2, 'performance-stall': 0, 'operational-metrics': 0 });
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
  assert.deepEqual(body.byType, { error: 0, crash: 1, 'performance-stall': 0, 'operational-metrics': 0 });
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
  assert.deepEqual(body.byType, { error: 1, crash: 1, 'performance-stall': 0, 'operational-metrics': 0 });
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
  assert.deepEqual(body.byType, { error: 0, crash: 0, 'performance-stall': 0, 'operational-metrics': 0 });
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
  // The additive rejections.timeline (WARDEN-798) carries the SAME shape and
  // granularity as persistErrors.timeline — byte-for-byte parity between the two
  // tallies' temporal axes (both mirror the read-path timeline's window/bucketMs).
  assert.ok(body.rejections.timeline, 'rejections.timeline present (additive — carried on every response)');
  assert.deepEqual(
    Object.keys(body.rejections.timeline).sort(),
    ['bucketMs', 'buckets'],
    'rejections.timeline shape: buckets + bucketMs only'
  );
  assert.equal(
    body.rejections.timeline.bucketMs,
    body.persistErrors.timeline.bucketMs,
    'same granularity as persistErrors.timeline (both default to the read-path DEFAULT_TIMELINE_* constants)'
  );
  assert.ok(body.rejections.timeline.buckets.length >= 1, 'the 404 rejection fired a timeline bucket');
  // persistErrors is present with its stable shape (zeroed here — no persist failure).
  assert.equal(body.persistErrors.total, 0);
  assert.deepEqual(
    body.persistErrors.timeline,
    { buckets: [], bucketMs: 1_800_000 },
    'zeroed timeline on a quiet receiver — shape-stable, no false alarm (parity with the read-path timeline)'
  );
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

// ── MATCHED + PAGING on GET /events — ?offset= (WARDEN-755) ──────────────────
// The /events-side twin of the truncation WARDEN-727 closed on /summary: the
// drill-down surface silently capped at the newest-200 with no way to tell a
// complete window from a truncation, nor to page the older matches. /events now
// carries `matched` (how many events match the filters, pre-bound — via the SAME
// shared filterEvents core /summary uses, never a third path) + `limit` / `offset`
// (the RESOLVED bound + page offset actually applied) and accepts `?offset=` to
// page older matches. Driven with fake req/res + an INJECTED readable store; ZERO
// real fs, ZERO real network. Mirrors the scoped-/summary block above field-for-
// field: `total` stays the FULL persisted count, `matched` is the scoped subset
// (≤ total), and with NO filters matched === total (strictly additive).

test('GET /events with NO filters is backward compatible — additive matched/limit/offset, matched === total', async () => {
  // The pre-existing { events, total } response gains matched/limit/offset and
  // NOTHING else changes. Un-filtered: matched === total (every event matches),
  // limit echoes the resolved default (no ?limit= given), offset echoes 0.
  const store = readableStore([errorEvent, crashEvent]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(Array.isArray(body.events), true);
  assert.equal(body.events.length, 2);
  assert.equal(body.total, 2, 'total is the full persisted count');
  assert.equal(body.matched, 2, 'matched === total when no filter is applied');
  assert.equal(body.limit, EVENTS_LIMIT_DEFAULT, 'limit echoes the resolved default');
  assert.equal(body.offset, 0, 'offset echoes 0 when no ?offset= is given');
  assert.ok(body.matched <= body.total, 'matched never exceeds total');
});

test('GET /events?platform=win32 scopes matched + the events window to win32 only', async () => {
  // Mirrors GET /summary?platform=win32: total stays the full set across
  // platforms; matched is the win32 subset; the bounded events array holds only
  // win32 payloads. limit/offset echo the (default) resolved bound + 0 offset.
  const store = readableStore([
    { ...errorEvent, platform: 'win32', name: 'win-err' },
    { ...errorEvent, platform: 'darwin', name: 'mac-err' },
    { ...crashEvent, platform: 'win32', reason: 'win-oom' },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 3, 'total is the full persisted count, unfiltered');
  assert.equal(body.matched, 2, 'matched is the win32 subset');
  assert.ok(body.matched < body.total, 'a scoped filter makes matched < total');
  assert.ok(body.matched <= body.total, 'matched never exceeds total');
  assert.equal(body.events.length, 2, 'the window holds only win32 payloads');
  assert.ok(body.events.every((e) => e.platform === 'win32'), 'every returned event is win32');
  assert.equal(body.limit, EVENTS_LIMIT_DEFAULT);
  assert.equal(body.offset, 0);
});

test('GET /events?type=crash scopes matched to a single base type', async () => {
  const store = readableStore([
    { ...errorEvent, name: 'TypeError' },
    { ...crashEvent, reason: 'oom' },
    { ...crashEvent, reason: 'killed' },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?type=crash' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 3, 'total is the full set across types');
  assert.equal(body.matched, 2, 'matched is the crash subset');
  assert.ok(body.events.every((e) => e.type === 'crash'));
});

test('GET /events?appVersion=0.1.19 scopes matched to a single release label', async () => {
  const store = readableStore([
    { ...errorEvent, appVersion: '0.1.19' },
    { ...crashEvent, appVersion: '0.1.20' },
    { ...errorEvent, appVersion: '0.1.19', name: 'second' },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?appVersion=0.1.19' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 3);
  assert.equal(body.matched, 2);
  assert.ok(body.events.every((e) => e.appVersion === '0.1.19'));
});

test('GET /events?since= scopes matched to events at/after the cutoff (receivedAt ?? timestamp)', async () => {
  const store = readableStore([
    { ...errorEvent, timestamp: 100, receivedAt: 100 },
    { ...errorEvent, timestamp: 200, receivedAt: 200 },
    { ...errorEvent, timestamp: 300, receivedAt: 300 },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?since=200' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 3);
  assert.equal(body.matched, 2, '200 (>=) + 300 survive; 100 dropped');
  assert.deepEqual(body.events.map((e) => e.timestamp), [200, 300]);
});

test('GET /events?signature= scopes matched to that distinct failure (signature × matched compose — WARDEN-746 × WARDEN-755 merge seam)', async () => {
  // THE MERGE SEAM: matched is `filterEvents(events, filterOpts).length` and
  // filterOpts carries `signature` (WARDEN-746 threaded it; WARDEN-755 added the
  // matched field + the filterOpts restructure). So a ?signature= drill-down MUST
  // scope matched to that distinct failure exactly — the two features compose on
  // the shared filterEvents core, never a third path. total stays the full
  // persisted count (mirrors /summary). (Signature became a filter axis on main
  // AFTER this PR was proposed, so the original per-axis matched matrix listed
  // only type/platform/appVersion/since — this closes the gap the merge opened.)
  const sigErr = signatureOf(errorEvent); // 'TypeError' (name-only, empty frames)
  const store = readableStore([
    { ...errorEvent, name: 'TypeError' },                       // MATCH (sigErr)
    { ...errorEvent, name: 'RangeError', message: 'r' },        // different name → different sig
    crashEvent,                                                  // crash:oom → different sig
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: `/events?signature=${encodeURIComponent(sigErr)}` }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 3, 'total is the full persisted count, unfiltered');
  assert.equal(body.matched, 1, 'matched is the single TypeError-distinct failure');
  assert.ok(body.matched < body.total, 'a signature drill-down makes matched < total');
  assert.ok(body.matched <= body.total, 'matched never exceeds total');
  assert.equal(body.events.length, 1, 'the window holds only that distinct failure');
  assert.equal(signatureOf(body.events[0]), sigErr, 'the returned payload is that distinct failure');
  assert.equal(body.offset, 0, 'offset echoes 0 (no ?offset= given)');
});

test('GET /events?type=crash&platform=win32 intersects all filters in matched (regression attribution)', async () => {
  // The same conjunctive intersection /summary tests: only the events surviving
  // EVERY filter are counted in matched and returned in the window.
  const store = readableStore([
    { ...crashEvent, platform: 'darwin', reason: 'mac-oom' },  // wrong platform
    { ...crashEvent, platform: 'win32', reason: 'win-oom' },   // MATCH
    { ...errorEvent, platform: 'win32', name: 'win-err' },     // wrong type
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?type=crash&platform=win32' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 3);
  assert.equal(body.matched, 1, 'only the win32 crash survives the intersection');
  assert.equal(body.events.length, 1);
  assert.equal(body.events[0].reason, 'win-oom');
});

test('GET /events echoes the RESOLVED limit actually applied, not the raw query (the bound that shaped the page)', async () => {
  // The echo contract: ?limit=50000 surfaces as limit: 200 (the cap that bound
  // the page), never the raw 50000; ?limit=3 → 3; no ?limit= → the default. The
  // echoed value is provably the bound selectEvents applied (resolveLimit is the
  // shared helper both call), so a maintainer reading `limit` knows the page size.
  const events = Array.from({ length: 500 }, (_, i) => ({ type: 'error', timestamp: i }));
  const store = readableStore(events);
  const handler = createRequestHandler({ store });

  const over = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?limit=50000' }), over);
  const overBody = JSON.parse(over.body);
  assert.equal(overBody.limit, EVENTS_LIMIT_MAX, '?limit=50000 echoes the cap (200), not 50000');
  assert.equal(overBody.events.length, EVENTS_LIMIT_MAX, 'the page is the cap');

  const exact = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?limit=3' }), exact);
  assert.equal(JSON.parse(exact.body).limit, 3, '?limit=3 echoes 3');

  const none = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events' }), none);
  assert.equal(JSON.parse(none.body).limit, EVENTS_LIMIT_DEFAULT, 'no ?limit= echoes the default');
});

test('GET /events echoes the RESOLVED offset actually applied (0 by default, the skip when ?offset= is set)', async () => {
  const events = Array.from({ length: 500 }, (_, i) => ({ type: 'error', timestamp: i }));
  const store = readableStore(events);
  const handler = createRequestHandler({ store });

  const none = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events' }), none);
  assert.equal(JSON.parse(none.body).offset, 0, 'no ?offset= echoes 0');

  const paged = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?offset=200' }), paged);
  assert.equal(JSON.parse(paged.body).offset, 200, '?offset=200 echoes 200');

  const explicit0 = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?offset=0' }), explicit0);
  assert.equal(JSON.parse(explicit0.body).offset, 0, '?offset=0 echoes 0');
});

test('GET /events?offset= pages OLDER matches — against a >200 scoped subset, offset=200 returns the next page', async () => {
  // The ticket's success criterion: a maintainer reading /events?type=error
  // against a large scoped subset sees matched alongside the newest page and
  // pages by offset until offset + events.length >= matched — reaching every
  // matching payload without the response ever exceeding the 200 cap.
  // 500 type=error events (+ 5 crashes so total != matched). ?type=error&limit=200.
  const events = [
    ...Array.from({ length: 500 }, (_, i) => ({ type: 'error', timestamp: i })),
    ...Array.from({ length: 5 }, (_, i) => ({ type: 'crash', timestamp: 1000 + i })),
  ];
  const store = readableStore(events);
  const handler = createRequestHandler({ store });

  const page0 = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?type=error&limit=200' }), page0);
  const b0 = JSON.parse(page0.body);
  assert.equal(b0.matched, 500, 'matched is the full error subset');
  assert.equal(b0.total, 505, 'total is the full persisted set (errors + crashes)');
  assert.equal(b0.events.length, 200, 'page 0 is the newest 200 errors');
  assert.ok(b0.events.length <= EVENTS_LIMIT_MAX, 'never exceeds the 200 cap');
  // page 0 = the NEWEST 200 errors → timestamps 300..499 (newest last).
  assert.deepEqual(
    b0.events.map((e) => e.timestamp),
    Array.from({ length: 200 }, (_, i) => 300 + i)
  );

  const page1 = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?type=error&limit=200&offset=200' }), page1);
  const b1 = JSON.parse(page1.body);
  assert.equal(b1.matched, 500, 'matched is unchanged across pages');
  assert.equal(b1.offset, 200, 'the echoed offset is the page requested');
  assert.equal(b1.events.length, 200, 'page 1 is the next 200 errors');
  assert.ok(b1.events.length <= EVENTS_LIMIT_MAX);
  assert.deepEqual(
    b1.events.map((e) => e.timestamp),
    Array.from({ length: 200 }, (_, i) => 100 + i),
    'page 1 = timestamps 100..299 (the 200 before the newest 200)'
  );

  const page2 = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?type=error&limit=200&offset=400' }), page2);
  const b2 = JSON.parse(page2.body);
  assert.equal(b2.events.length, 100, 'the final page is the oldest 100 errors (a partial page)');
  assert.ok(b2.events.length <= EVENTS_LIMIT_MAX);
  // offset + events.length has now reached matched — every error has been paged.
  assert.equal(400 + b2.events.length, 500, 'offset + events.length === matched → traversal complete');
  assert.deepEqual(
    b2.events.map((e) => e.timestamp),
    Array.from({ length: 100 }, (_, i) => 0 + i),
    'page 2 = timestamps 0..99 (the oldest errors)'
  );

  // Paging past the end → [] (clean stop; the maintainer knows they're done).
  const past = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?type=error&limit=200&offset=500' }), past);
  const bp = JSON.parse(past.body);
  assert.equal(bp.matched, 500);
  assert.equal(bp.events.length, 0, 'offset === matched → empty page (no older matches)');
});

test('GET /events?offset= never exceeds the 200-event cap on ANY page and pages only MATCHED events', async () => {
  // 847 errors (the ticket's scenario size) + a few crashes. Page the error
  // subset at the default limit; every page is bounded, and offset never
  // surfaces a crash — it pages the MATCHED set, not the unfiltered set.
  const events = [
    ...Array.from({ length: 847 }, (_, i) => ({ type: 'error', timestamp: i })),
    ...Array.from({ length: 3 }, (_, i) => ({ type: 'crash', timestamp: 9000 + i })),
  ];
  const store = readableStore(events);
  const handler = createRequestHandler({ store });
  const seen = [];
  for (let offset = 0; offset < 847; offset += EVENTS_LIMIT_DEFAULT) {
    const res = fakeRes();
    await handler(fakeReq({ method: 'GET', url: `/events?type=error&offset=${offset}` }), res);
    const body = JSON.parse(res.body);
    assert.equal(body.matched, 847, 'matched is the full error subset on every page');
    assert.ok(body.events.length <= EVENTS_LIMIT_MAX, `offset=${offset} page bounded by the cap`);
    assert.ok(body.events.every((e) => e.type === 'error'), 'no crash ever leaks into an offset page');
    seen.push(...body.events.map((e) => e.timestamp));
    if (offset + body.events.length >= body.matched) break;
  }
  // Every error reached exactly once — union is the full matched set, no dups/gaps.
  assert.equal(seen.length, 847, 'every matched error paged exactly once');
  assert.deepEqual(
    [...seen].sort((a, b) => a - b),
    Array.from({ length: 847 }, (_, i) => i),
    'no gaps, no duplicates across pages'
  );
});

test('GET /events?platform=win32 on a store with NO win32 events → matched 0, events [], total full', async () => {
  const store = readableStore([
    { ...errorEvent, platform: 'darwin' },
    { ...crashEvent, platform: 'linux' },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?platform=win32' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 2, 'total is still the full persisted count');
  assert.equal(body.matched, 0, 'nothing matched the win32 filter');
  assert.deepEqual(body.events, [], 'an empty window for an empty matched set');
  assert.equal(body.offset, 0);
});

test('GET /events ignores an unrecognized query param (no spurious matched — backward compatible)', async () => {
  // A param the filter does not consume (?foo=bar) applies NO filter: matched === total.
  const store = readableStore([errorEvent, crashEvent]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/events?foo=bar' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 2);
  assert.equal(body.matched, 2, 'an unknown param does not scope the window');
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
      // Honor a supplied pruneImpl's RETURN so a test can simulate a prune that
      // DROPPED events (e.g. {before:5, after:3, pruned:2, rewrote:true}) for the
      // retention-health tally assertions (WARDEN-743). Defaults to the no-op
      // shape used by the arming/debounce suite.
      if (pruneImpl) {
        const result = await pruneImpl(opts);
        return result ?? { before: 0, after: 0, pruned: 0, rewrote: false };
      }
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
  assert.deepEqual(body.byType, { error: 1, crash: 1, 'performance-stall': 0, 'operational-metrics': 0 });
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
    byDeclaredVersion: {},
    lastStatus: null,
    lastReason: null,
    lastSeen: null,
    timeline: { buckets: [], bucketMs: 1_800_000 },
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
  // the earlier snapshot is unchanged by the later record — its scalar aggregate
  // AND its timeline are both frozen. (The one record at clock=5000 is the newest
  // event in a 24h window ending at 5000, so it lands in the newest bucket, whose
  // right edge is exactly 5000 — bucketEnd === lastSeen.)
  assert.deepEqual(snap, {
    total: 1,
    byStatus: { '401': 1 },
    byDeclaredVersion: {},
    lastStatus: 401,
    lastReason: 'unauthorized',
    lastSeen: 5000,
    timeline: { buckets: [{ bucketStart: -1_795_000, bucketEnd: 5_000, count: 1 }], bucketMs: 1_800_000 },
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
  assert.deepEqual(Object.keys(snap).sort(), ['byDeclaredVersion', 'byStatus', 'lastReason', 'lastSeen', 'lastStatus', 'timeline', 'total'], 'shape stays bounded — no per-rejection growth');
});

test('rejections tally: record() with no status is a defensive no-op (never throws)', () => {
  const tally = createRejectionTally({ now: () => 0 });
  tally.record({ reason: 'no status' });
  tally.record({});
  assert.deepEqual(tally.snapshot(), {
    total: 0,
    byStatus: {},
    byDeclaredVersion: {},
    lastStatus: null,
    lastReason: null,
    lastSeen: null,
    timeline: { buckets: [], bucketMs: 1_800_000 },
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

// ── byDeclaredVersion: the 415 drift breakdown (WARDEN-761) ───────────────────
// A histogram of the DECLARED schema version on 415-rejected batches — the drift
// population (which client versions are still sending during a bump). Bounded by a
// top-N distinct-key cap + ONE overflow bucket (WARDEN-829), NOT enum-bounded: a
// client-declared version is the raw, attacker-controlled `x-telemetry-schema`
// header, NOT a fixed enum like `byStatus`. Populated ONLY by 415s (only they pass
// a declaredVersion); the cap is exercised in the WARDEN-829 adversarial test below.

test('byDeclaredVersion: 415s with DIFFERENT declared versions bucket distinctly (one drifting client vs many)', () => {
  const tally = createRejectionTally({ now: () => 0 });
  tally.record({ status: 415, reason: '...got "3"', declaredVersion: '3' });
  tally.record({ status: 415, reason: '...got "3"', declaredVersion: '3' });
  tally.record({ status: 415, reason: '...got "5"', declaredVersion: '5' });
  const snap = tally.snapshot();
  assert.deepEqual(snap.byDeclaredVersion, { '3': 2, '5': 1 }, 'each declared version buckets distinctly');
  assert.equal(snap.byStatus['415'], 3, 'byStatus still accumulates in parallel');
});

test('byDeclaredVersion: NON-415 rejections do NOT populate it (401/404/400/422 pass no declaredVersion)', () => {
  const tally = createRejectionTally({ now: () => 0 });
  tally.record({ status: 401, reason: 'unauthorized' });
  tally.record({ status: 404, reason: 'not found' });
  tally.record({ status: 400, reason: 'malformed body' });
  tally.record({ status: 422, reason: 'out-of-schema event' });
  const snap = tally.snapshot();
  assert.deepEqual(snap.byDeclaredVersion, {}, 'no declared version is bucketed for non-415 rejections');
  assert.equal(snap.total, 4);
});

test('byDeclaredVersion: a missing-header 415 (declaredVersion absent) records the 415 but no bucket', () => {
  // A missing x-telemetry-schema header still 415's (declared !== SCHEMA_VERSION),
  // but declaredVersion is undefined → no bucket (mirror summary.mjs: absent → skip).
  const tally = createRejectionTally({ now: () => 0 });
  tally.record({ status: 415, reason: '...got null', declaredVersion: undefined });
  const snap = tally.snapshot();
  assert.equal(snap.byStatus['415'], 1, 'the 415 is still counted in byStatus');
  assert.deepEqual(snap.byDeclaredVersion, {}, 'an absent declared version yields no bucket');
});

test('byDeclaredVersion: a non-numeric/scanner declaredVersion is bucketed verbatim (real signal never dropped)', () => {
  const tally = createRejectionTally({ now: () => 0 });
  tally.record({ status: 415, reason: '...got "abc"', declaredVersion: 'abc' });
  tally.record({ status: 415, reason: '...got ""', declaredVersion: '' });
  const snap = tally.snapshot();
  assert.deepEqual(snap.byDeclaredVersion, { abc: 1, '': 1 }, 'scanner values bucket verbatim, not validated away');
});

test('byDeclaredVersion: BOUNDED — a sustained drift storm collapses to enum keys, never one entry per rejection', () => {
  const tally = createRejectionTally({ now: () => 0 });
  for (let i = 0; i < 1000; i++) {
    // Only two declared versions in the wild — a thousand 415s collapse to two keys.
    tally.record({ status: 415, reason: `drift #${i}`, declaredVersion: i % 2 === 0 ? '3' : '5' });
  }
  const snap = tally.snapshot();
  assert.equal(snap.total, 1000);
  assert.deepEqual(snap.byDeclaredVersion, { '3': 500, '5': 500 }, 'two enum keys — not 1000 entries');
});

test('byDeclaredVersion: WARDEN-829 adversarial bound — ≥10k UNIQUE declared versions collapse to N+1 keys (no memory/response growth)', () => {
  // The receiver is OPEN by default; `x-telemetry-schema` is raw attacker-controlled
  // header text. Pre-WARDEN-829 every distinct value grew `byDeclaredVersion` by one
  // key FOREVER — unbounded live memory + an ever-growing GET /summary payload (one-
  // sided amplification: a cheap probe per distinct header value). The top-N +
  // overflow cap must bound the distinct-key cardinality regardless of input size.
  // N is set tiny (4) so the cap is exercised deterministically, independent of the
  // 32 default, and the first-N insertion-order tracking is observable exactly.
  const N = 4;
  const tally = createRejectionTally({ now: () => 0, maxDeclaredVersions: N });
  const UNIQUE = 10_000;
  for (let i = 0; i < UNIQUE; i++) {
    tally.record({ status: 415, reason: 'drift', declaredVersion: `v${i}` }); // 10k DISTINCT values
  }
  const snap = tally.snapshot();

  // (a) Cardinality bound: exactly N tracked buckets + ONE overflow bucket — NOT 10000 keys.
  //     This is the /summary-payload-does-not-grow-linearly-with-request-count check.
  assert.equal(Object.keys(snap.byDeclaredVersion).length, N + 1, 'collapses to N+1 keys regardless of input cardinality — /summary stays bounded');

  // (b) The first N distinct values are tracked as their own buckets (insertion order);
  //     every further distinct value folds into the single counted `__overflow__` bucket.
  assert.deepEqual(
    snap.byDeclaredVersion,
    { v0: 1, v1: 1, v2: 1, v3: 1, __overflow__: UNIQUE - N },
    'first N distinct values tracked; the remaining UNIQUE-N fold into ONE overflow bucket',
  );

  // (c) No count loss: the overflow COUNTS (not drops), so the sum of byDeclaredVersion
  //     values still reflects every recorded 415...
  const sum = Object.values(snap.byDeclaredVersion).reduce((a, b) => a + b, 0);
  assert.equal(sum, UNIQUE, 'overflow counts — every 415 is still tallied, none silently dropped');
  // ...and `total` / byStatus carry the true count (not a capped approximation).
  assert.equal(snap.total, UNIQUE, 'total is the true count, not a capped approximation');
  assert.equal(snap.byStatus['415'], UNIQUE, 'byStatus is the true 415 count (the bound is on inner cardinality, not total)');
});

test('byDeclaredVersion: WARDEN-829 — repeats of an already-tracked version bump their OWN bucket, never overflow', () => {
  // Once a distinct version occupies one of the N tracked slots, further 415s from
  // that SAME version must increment its own bucket — NOT the overflow bucket (only
  // a NEW distinct value over the cap overflows). Otherwise legit drift signal (a
  // single stubborn version hammered repeatedly) would be mis-counted as overflow.
  const tally = createRejectionTally({ now: () => 0, maxDeclaredVersions: 2 });
  tally.record({ status: 415, declaredVersion: '3' }); // tracked slot 1
  tally.record({ status: 415, declaredVersion: '5' }); // tracked slot 2 → cap reached
  tally.record({ status: 415, declaredVersion: '3' }); // REPEAT of a tracked version → own bucket
  tally.record({ status: 415, declaredVersion: '9' }); // NEW distinct value over cap → overflow
  const { byDeclaredVersion: dv } = tally.snapshot();
  assert.deepEqual(dv, { '3': 2, '5': 1, __overflow__: 1 }, 'repeats bump the tracked bucket; only a NEW distinct value over the cap overflows');
});

test('byDeclaredVersion: WARDEN-829 — the default cap (32) preserves a realistic distinct-version set with NO overflow', () => {
  // The cap sits comfortably above the realistic distinct-version count (a handful
  // during a coordinated bump), so legit drift signal is fully enumerated — only a
  // broad storm (>32 distinct values) reaches the overflow bucket. Confirms the bound
  // does not blind the maintainer to ordinary drift.
  const tally = createRejectionTally({ now: () => 0 }); // default cap = DEFAULT_REJECTION_MAX_DECLARED_VERSIONS (32)
  for (let i = 1; i <= 10; i++) tally.record({ status: 415, declaredVersion: String(i) }); // 10 realistic versions
  const { byDeclaredVersion: dv } = tally.snapshot();
  assert.equal(Object.keys(dv).length, 10, 'all 10 realistic versions tracked distinctly');
  assert.equal(dv.__overflow__, undefined, 'no overflow bucket — realistic distinct counts stay under the cap');
});

test('byDeclaredVersion: snapshot() copies the histogram — a later record does not mutate a prior snapshot', () => {
  const tally = createRejectionTally({ now: () => 0 });
  tally.record({ status: 415, reason: '...got "3"', declaredVersion: '3' });
  const snap = tally.snapshot();
  tally.record({ status: 415, reason: '...got "5"', declaredVersion: '5' });
  assert.deepEqual(snap.byDeclaredVersion, { '3': 1 }, 'the earlier snapshot is unchanged by the later record');
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
  assert.equal(rej.byDeclaredVersion[String(SCHEMA_VERSION + 1)], 1, 'the declared version buckets on the drift axis');
  // The additive timeline (WARDEN-798) surfaces the drift's TIMING: the 415 just
  // fired, so it lands in the newest bucket — the ONGOING-drift signal that
  // total + lastSeen alone cannot convey (a spike that recovered would leave the
  // newest bucket empty). The two Date.now() reads (record + snapshot) are
  // microseconds apart, so they cannot straddle a 30-min bucket boundary.
  assert.ok(rej.timeline, 'timeline field present (additive — carried on every response)');
  assert.ok(rej.timeline.buckets.length >= 1, 'the recent 415 fired a timeline bucket');
  const newest = rej.timeline.buckets[rej.timeline.buckets.length - 1];
  assert.ok(newest.count >= 1, 'the recent 415 lands in the newest bucket — ONGOING drift (newest bucket non-empty)');
  assert.equal(rej.timeline.bucketMs, 1_800_000, 'same granularity as the read-path timeline');
});

test('415s from MIXED declared versions surface a bounded byDeclaredVersion histogram (one drifting client vs many)', async () => {
  // The flagship signal: during a coordinated bump, two client versions still
  // drift. GET /summary must tell them apart so the maintainer knows whether the
  // bump is safe to complete.
  const { handler } = wiringWithTally();
  // v3 (below current) and SCHEMA_VERSION+1 (a client AHEAD of the receiver) both
  // miss the strict handshake → 415. Never use the CURRENT version as the drifting
  // literal: it passes the handshake and lands as a 400 (malformed body) instead,
  // silently losing the byDeclaredVersion bucket.
  const futureVersion = String(SCHEMA_VERSION + 1);
  await handler(fakeReq({ headers: { 'x-telemetry-schema': '3' }, body: 'x' }), fakeRes()); // 415, declares v3
  await handler(fakeReq({ headers: { 'x-telemetry-schema': '3' }, body: 'x' }), fakeRes()); // 415, declares v3
  await handler(fakeReq({ headers: { 'x-telemetry-schema': futureVersion }, body: 'x' }), fakeRes()); // 415, declares the future version

  const rej = await summaryRejections(handler);
  assert.deepEqual(rej.byDeclaredVersion, { '3': 2, [futureVersion]: 1 }, 'each declared version is bucketed distinctly');
  assert.equal(rej.byStatus['415'], 3, 'the 415s are also counted in byStatus');
});

test('a 415 with a MISSING header surfaces byStatus but NOT byDeclaredVersion (absent → no bucket, end-to-end)', async () => {
  const { handler } = wiringWithTally();
  await handler(fakeReq({ headers: {}, body: 'x' }), fakeRes()); // no header → 415, declaredVersion undefined
  const rej = await summaryRejections(handler);
  assert.ok(rej.byStatus['415'] >= 1, 'the 415 is still recorded in byStatus');
  assert.deepEqual(rej.byDeclaredVersion, {}, 'a missing header buckets no declared version');
});

test('a NON-415 rejection does NOT populate byDeclaredVersion on the handler path', async () => {
  // A 422 passes the handshake (header matches) and fails event validation — it
  // carries no declaredVersion, so byDeclaredVersion stays empty even though the
  // 422 lands in byStatus.
  const { handler } = wiringWithTally();
  const badBody = JSON.stringify({ schemaVersion: SCHEMA_VERSION, events: [{ ...validError, runtime: 'worker' }] });
  await handler(fakeReq({ headers: schemaHeaders, body: badBody }), fakeRes());
  const rej = await summaryRejections(handler);
  assert.ok(rej.byStatus['422'] >= 1);
  assert.deepEqual(rej.byDeclaredVersion, {}, 'only 415s populate byDeclaredVersion');
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
  assert.deepEqual(rej, { total: 0, byStatus: {}, byDeclaredVersion: {}, lastStatus: null, lastReason: null, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } });
});

test('an idle receiver (no traffic) returns zeroed rejections in GET /summary (parity with today — no false alarm)', async () => {
  const { handler } = wiringWithTally();
  const rej = await summaryRejections(handler);
  assert.deepEqual(rej, { total: 0, byStatus: {}, byDeclaredVersion: {}, lastStatus: null, lastReason: null, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } });
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
    byDeclaredVersion: {},
    lastStatus: null,
    lastReason: null,
    lastSeen: null,
    timeline: { buckets: [], bucketMs: 1_800_000 },
  });
});

// ── REJECTION TIMELINE (WARDEN-798) ──────────────────────────────────────────
// The bounded `timeline` on the rejections tally — mirrors the read-path
// `timeline` (summarizeTimeline, WARDEN-603) and the persistErrors timeline
// (WARDEN-777) so a maintainer can tell an ONGOING schema-drift storm (415s
// still landing in the newest bucket) from a RESOLVED one (415s clustered in
// older buckets, newest bucket empty) — the spike-vs-baseline question `total` +
// `lastSeen` provably cannot answer for the roadmap's flagship risk (schema
// drift → a 415 flood). Driven directly with an injected fake clock (mirroring
// the tally unit tests above): bucket-placement parity with summarizeTimeline,
// the rolling-window roll-off bound, redaction parity, the degenerate-config
// guard, and the ongoing-vs-resolved flagship.

test('rejections tally timeline: an empty tally carries the zeroed shape — buckets: [], bucketMs = default (no false alarm)', () => {
  // Case 1: a fresh createRejectionTally({ now }) snapshot returns the zeroed
  // timeline — shape-stable whether or not the tally is wired, mirroring
  // EMPTY_PERSIST_ERRORS. bucketMs is the default granularity (NOT 0).
  const tally = createRejectionTally({ now: () => 0 });
  assert.deepEqual(tally.snapshot().timeline, { buckets: [], bucketMs: 1_800_000 });
});

test('rejections tally timeline: a known seeded time spread — a recent drift spike vs an earlier baseline (bucket placement parity with the read-path summarizeTimeline + the persistErrors tally)', () => {
  // Mirrors the read-path /summary "known seeded time spread" test AND the
  // persistErrors tally timeline test (WARDEN-777): record 415s at known fake-clock
  // times so the snapshot emits buckets at EXACTLY those times. window
  // [0, 86_400_000], bucketMs 1_800_000; bucket 1 = [1.8M, 3.6M); bucket 47 =
  // [84.6M, 86.4M); a record at EXACTLY now (86.4M) folds into the newest bucket
  // (parity with summarizeTimeline's top-boundary fold). Covers same-bucket
  // accumulation (count: 3) and chronological sort (oldest → newest).
  let clock = 0;
  const tally = createRejectionTally({ now: () => clock });
  // baseline: one 415 in bucket 1
  clock = 1_800_000; tally.record({ status: 415, reason: 'baseline drift', declaredVersion: '3' });
  // spike: three 415s in bucket 47 (one at exactly now → folded via the top edge)
  clock = 84_600_000; tally.record({ status: 415, reason: 'spike drift', declaredVersion: '5' });
  clock = 84_600_001; tally.record({ status: 415, reason: 'spike drift', declaredVersion: '5' });
  clock = 86_400_000; tally.record({ status: 415, reason: 'spike drift', declaredVersion: '5' }); // === now → newest via boundary fold
  clock = 86_400_000; // snapshot time
  const { timeline, byDeclaredVersion, total, byStatus } = tally.snapshot();
  assert.equal(timeline.buckets.length, 2, 'two distinct buckets: baseline + spike');
  assert.deepEqual(timeline.buckets, [
    { bucketStart: 1_800_000, bucketEnd: 3_600_000, count: 1 },   // baseline (oldest)
    { bucketStart: 84_600_000, bucketEnd: 86_400_000, count: 3 }, // recent spike (newest) — three 415s accumulated into one bucket
  ]);
  assert.equal(timeline.bucketMs, 1_800_000);
  assert.equal(total, 4, 'total is cumulative across the timeline');
  assert.deepEqual(byStatus, { '415': 4 }, 'byStatus still accumulates in parallel');
  // The new timeline field does NOT disturb the existing drift axis (WARDEN-761):
  // byDeclaredVersion still populates — the timeline is an ADDITIVE temporal axis,
  // not a replacement for the version histogram.
  assert.deepEqual(byDeclaredVersion, { '3': 1, '5': 3 }, 'byDeclaredVersion still populates — the timeline is an additive axis, the drift population is unaffected');
});

test('rejections tally timeline: ROLL-OFF BOUND — buckets older than the window drop on snapshot(); bucket count never exceeds maxBuckets', () => {
  // Record one 415 per distinct 30-min slot for 60 slots (> maxBuckets=48), each a
  // bucketMs apart, so the 48-slot-wide rolling 24h window cannot hold them all.
  // Advancing the fake clock pushes the earliest 415s out of the window: the
  // snapshot must drop the rolled-off buckets, never emit more than maxBuckets,
  // and — unlike `total` — only the TIMELINE window rolls; total stays cumulative.
  let clock = 1_800_000;
  const tally = createRejectionTally({ now: () => clock });
  for (let i = 0; i < 60; i++) {
    tally.record({ status: 415, reason: `drift slot ${i}`, declaredVersion: '3' });
    clock += 1_800_000; // advance to the next distinct 30-min slot
  }
  // clock is now 1_800_000 + 60 * 1_800_000 = 109_800_000 (snapshot time)
  const snap = tally.snapshot();
  assert.equal(snap.total, 60, 'total is cumulative — it does NOT roll off (only the timeline window does)');
  assert.ok(snap.timeline.buckets.length <= 48, `bucket count bounded at maxBuckets (got ${snap.timeline.buckets.length})`);
  assert.equal(snap.timeline.bucketMs, 1_800_000);
  // Every surviving bucket sits inside the current 24h window — the earliest 415s
  // (the first ~12 slots) rolled off. windowStart = clock - windowMs = 23_400_000.
  const windowStart = clock - 86_400_000;
  for (const b of snap.timeline.buckets) {
    assert.ok(b.bucketStart >= windowStart, `bucket ${b.bucketStart} is inside the rolling window (>= ${windowStart})`);
  }
  // Emitted oldest → newest (chronological sort).
  for (let i = 1; i < snap.timeline.buckets.length; i++) {
    assert.ok(
      snap.timeline.buckets[i - 1].bucketStart < snap.timeline.buckets[i].bucketStart,
      'buckets sorted oldest → newest'
    );
  }
});

test('rejections tally timeline: redaction parity — a bucket carries COUNT + window only, never a reason / status / declared-version string', () => {
  // Mirrors the read-path /summary "never echoes raw events or extended-tier
  // names" timeline test AND the persistErrors tally redaction test (WARDEN-777):
  // the timeline adds TIMING, not content. A rejection's `reason` is the
  // receiver's diagnostic and the bucket carries only a count + time window — so
  // no reason text, status, or declared version reaches the timeline.
  let clock = 1_800_000;
  const tally = createRejectionTally({ now: () => clock });
  tally.record({ status: 415, reason: 'unsupported telemetry schema version: super secret drift diagnostic #7B', declaredVersion: '3' });
  const { timeline } = tally.snapshot();
  assert.equal(timeline.buckets.length, 1, 'the rejection fired a bucket');
  const json = JSON.stringify(timeline);
  assert.equal(json.includes('super secret drift diagnostic'), false, 'no reason text in the timeline');
  assert.equal(json.includes('7B'), false, 'no reason fragment in the timeline');
  // The bucket shape is count + window only — there is no field that could carry
  // a reason, status, or declared version.
  assert.deepEqual(Object.keys(timeline.buckets[0]).sort(), ['bucketEnd', 'bucketStart', 'count']);
});

test('rejections tally timeline: DEGENERATE config (windowMs: 0 / maxBuckets: 0) → { buckets: [], bucketMs: 0 } while the scalar tally keeps working', () => {
  // Mirrors summarizeTimeline's + the persistErrors tally's degenerate-config
  // guard: a bad windowMs/maxBuckets override collapses the timeline to
  // { buckets: [], bucketMs: 0 } — never a huge/NaN array — while the scalar
  // tally (total/byStatus/byDeclaredVersion/lastStatus/lastReason/lastSeen) keeps
  // working regardless.
  let clock = 5_000;
  const tally = createRejectionTally({ now: () => clock, maxBuckets: 0, windowMs: 0 });
  tally.record({ status: 415, reason: 'drift', declaredVersion: '3' });
  const snap = tally.snapshot();
  // scalar tally unaffected
  assert.equal(snap.total, 1);
  assert.deepEqual(snap.byStatus, { '415': 1 });
  assert.deepEqual(snap.byDeclaredVersion, { '3': 1 }, 'the drift axis still works under a degenerate timeline config');
  assert.equal(snap.lastStatus, 415);
  assert.equal(snap.lastReason, 'drift');
  assert.equal(snap.lastSeen, 5_000);
  // timeline collapsed — never a huge/NaN array
  assert.deepEqual(snap.timeline, { buckets: [], bucketMs: 0 });
});

test('rejections tally timeline: ONGOING drift vs RESOLVED drift — the newest bucket tells them apart (total + lastSeen alone cannot)', () => {
  // The flagship success criterion (WARDEN-798): two receivers with the SAME
  // total can be distinguished by whether 415 traffic is STILL landing (newest
  // bucket non-empty = ONGOING) or has gone quiet (no bucket near now, only older
  // buckets populated = RESOLVED). `total` + a single `lastSeen` provably cannot
  // answer this — a sustained drift flood and a spike that clients recovered from
  // read identical without the per-bucket distribution.
  const snapshotTime = 100_000_000;

  // ONGOING: a 415 landed moments before the snapshot — newest bucket non-empty.
  let clock = snapshotTime;
  const ongoing = createRejectionTally({ now: () => clock });
  clock = snapshotTime - 60_000; // 1 min ago — inside the newest bucket
  ongoing.record({ status: 415, reason: 'live drift', declaredVersion: '5' });
  clock = snapshotTime;
  const ongSnap = ongoing.snapshot();
  const ongBuckets = ongSnap.timeline.buckets;
  assert.ok(ongBuckets.length >= 1, 'ONGOING: at least one bucket is populated');
  const ongNewest = ongBuckets[ongBuckets.length - 1];
  assert.equal(ongNewest.bucketEnd, snapshotTime, 'ONGOING: the newest bucket reaches now — traffic is still landing');
  assert.ok(ongNewest.count >= 1, 'ONGOING: the newest bucket is non-empty');

  // RESOLVED: the only 415 landed near the START of the window, then clients were
  // updated; snapshot much later — the 415 sits in an OLD bucket, no bucket near now.
  const resolved = createRejectionTally({ now: () => clock });
  clock = snapshotTime - 80_000_000; // ~22h ago — near the window's oldest edge
  resolved.record({ status: 415, reason: 'old drift', declaredVersion: '3' });
  clock = snapshotTime; // clients updated an hour ago; 415s stopped
  const resSnap = resolved.snapshot();
  const resBuckets = resSnap.timeline.buckets;
  assert.ok(resBuckets.length >= 1, 'RESOLVED: the old 415 still has its bucket');
  const resNewest = resBuckets[resBuckets.length - 1];
  assert.ok(
    resNewest.bucketEnd < snapshotTime,
    'RESOLVED: the newest POPULATED bucket ends well before now — no bucket reaches the snapshot time (traffic went quiet)'
  );

  // Same total, opposite verdict — the temporal distribution is the deciding signal.
  assert.equal(ongSnap.total, resSnap.total, 'both saw one 415 — identical total, but the timeline tells them apart');
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
    timeline: { buckets: [], bucketMs: 1_800_000 },
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
  // the earlier snapshot is unchanged by the later record — its scalar aggregate
  // AND its timeline are both frozen. (The one record at clock=5000 is the newest
  // event in a 24h window ending at 5000, so it lands in the newest bucket, whose
  // right edge is exactly 5000 — bucketEnd === lastSeen.)
  assert.deepEqual(snap, {
    total: 1,
    lastReason: 'disk full',
    lastSeen: 5000,
    timeline: { buckets: [{ bucketStart: -1_795_000, bucketEnd: 5_000, count: 1 }], bucketMs: 1_800_000 },
  });
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
  assert.deepEqual(Object.keys(snap).sort(), ['lastReason', 'lastSeen', 'timeline', 'total'], 'shape stays bounded — no per-failure growth');
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
  // The additive timeline (WARDEN-777) surfaces the failure's TIMING: it fired one
  // bucket, and the failure sat at the top edge of the rolling window's newest
  // bucket, so bucketEnd === lastSeen (the injected now). This is the spike-vs-
  // baseline signal total+lastSeen alone could not convey.
  assert.ok(pe.timeline, 'timeline field present (additive — carried on every response)');
  assert.equal(pe.timeline.buckets.length, 1, 'the one persist failure fired one bucket');
  assert.equal(pe.timeline.buckets[0].count, 1);
  assert.equal(pe.timeline.bucketMs, 1_800_000);
  assert.equal(pe.timeline.buckets[0].bucketEnd, 12345, 'the failure is at the top edge of the newest bucket (bucketEnd === lastSeen)');
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
  assert.deepEqual(Object.keys(pe).sort(), ['lastReason', 'lastSeen', 'timeline', 'total'], 'shape stays bounded — no per-failure growth');
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
    { total: 0, byStatus: {}, byDeclaredVersion: {}, lastStatus: null, lastReason: null, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } },
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
  assert.deepEqual(pe, { total: 0, lastReason: null, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } });
});

test('an idle receiver (no traffic) returns zeroed persistErrors in GET /summary (parity with today — no false alarm)', async () => {
  const { handler } = wiringWithPersistErrors();
  const pe = await summaryPersistErrors(handler);
  assert.deepEqual(pe, { total: 0, lastReason: null, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } });
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
    timeline: { buckets: [], bucketMs: 1_800_000 },
  });
});

// ── PERSIST-ERROR TIMELINE (WARDEN-777) ──────────────────────────────────────
// The bounded `timeline` on the persistErrors tally — mirrors the read-path
// `timeline` (summarizeTimeline, WARDEN-603) so a maintainer can tell an ONGOING
// store outage (failures still landing in the newest bucket) from a RESOLVED one
// (failures clustered in older buckets, newest bucket empty). Driven directly with
// an injected fake clock (mirroring the tally unit tests above): bucket-placement
// parity with summarizeTimeline, the rolling-window roll-off bound, and redaction
// parity with the read-path timeline.

test('persistErrors tally timeline: a known seeded time spread — a recent outage spike vs an earlier baseline (bucket placement parity with the read-path summarizeTimeline)', () => {
  // Mirrors the read-path /summary "known seeded time spread" test above: record
  // failures at known fake-clock times so the snapshot emits buckets at EXACTLY those
  // times. window [0, 86_400_000], bucketMs 1_800_000; bucket 1 = [1.8M, 3.6M);
  // bucket 47 = [84.6M, 86.4M); a record at EXACTLY now (86.4M) folds into the newest
  // bucket (parity with summarizeTimeline's top-boundary fold). The expected buckets
  // are byte-identical to the read-path /summary timeline test.
  let clock = 0;
  const tally = createPersistErrorTally({ now: () => clock });
  // baseline: one failure in bucket 1
  clock = 1_800_000; tally.record({ reason: 'baseline ENOSPC' });
  // spike: three failures in bucket 47 (one at exactly now → folded via the top edge)
  clock = 84_600_000; tally.record({ reason: 'spike EACCES' });
  clock = 84_600_001; tally.record({ reason: 'spike EACCES' });
  clock = 86_400_000; tally.record({ reason: 'spike EACCES' }); // === now → newest via boundary fold
  clock = 86_400_000; // snapshot time
  const { timeline } = tally.snapshot();
  assert.equal(timeline.buckets.length, 2, 'two distinct buckets: baseline + spike');
  assert.deepEqual(timeline.buckets, [
    { bucketStart: 1_800_000, bucketEnd: 3_600_000, count: 1 },   // baseline
    { bucketStart: 84_600_000, bucketEnd: 86_400_000, count: 3 }, // recent spike (newest)
  ]);
  assert.equal(timeline.bucketMs, 1_800_000);
});

test('persistErrors tally timeline: ROLL-OFF BOUND — buckets older than the window drop on snapshot(); bucket count never exceeds maxBuckets', () => {
  // Record one failure per distinct 30-min slot for 60 slots (> maxBuckets=48), each a
  // bucketMs apart, so the 48-slot-wide rolling 24h window cannot hold them all.
  // Advancing the fake clock pushes the earliest failures out of the window: the
  // snapshot must drop the rolled-off buckets, never emit more than maxBuckets, and —
  // unlike `total` — only the TIMELINE window rolls; total stays cumulative.
  let clock = 1_800_000;
  const tally = createPersistErrorTally({ now: () => clock });
  for (let i = 0; i < 60; i++) {
    tally.record({ reason: `outage slot ${i}` });
    clock += 1_800_000; // advance to the next distinct 30-min slot
  }
  // clock is now 1_800_000 + 60 * 1_800_000 = 109_800_000 (snapshot time)
  const snap = tally.snapshot();
  assert.equal(snap.total, 60, 'total is cumulative — it does NOT roll off (only the timeline window does)');
  assert.ok(snap.timeline.buckets.length <= 48, `bucket count bounded at maxBuckets (got ${snap.timeline.buckets.length})`);
  assert.equal(snap.timeline.bucketMs, 1_800_000);
  // Every surviving bucket sits inside the current 24h window — the oldest failures
  // (the first ~12 slots) rolled off. windowStart = clock - windowMs = 23_400_000.
  const windowStart = clock - 86_400_000;
  for (const b of snap.timeline.buckets) {
    assert.ok(b.bucketStart >= windowStart, `bucket ${b.bucketStart} is inside the rolling window (>= ${windowStart})`);
  }
  // Emitted oldest → newest.
  for (let i = 1; i < snap.timeline.buckets.length; i++) {
    assert.ok(
      snap.timeline.buckets[i - 1].bucketStart < snap.timeline.buckets[i].bucketStart,
      'buckets sorted oldest → newest'
    );
  }
});

test('persistErrors tally timeline: redaction parity — a bucket carries COUNT + window only, never a reason / OS errno (parity with the read-path timeline)', () => {
  // Mirrors the read-path /summary "never echoes raw events or extended-tier names"
  // timeline test above: the timeline adds TIMING, not content. A persist failure's
  // `reason` is already an OS errno / sink diagnostic (the store JSON.stringified
  // each event BEFORE the sink ran, so a sink throw carries system info, never event
  // bytes), and the bucket carries only a count + time window — so no reason text
  // reaches the timeline beyond what `lastReason` already exposes.
  let clock = 1_800_000;
  const tally = createPersistErrorTally({ now: () => clock });
  tally.record({ reason: 'ENOSPC: super secret disk diagnostic #7B' });
  const { timeline } = tally.snapshot();
  assert.equal(timeline.buckets.length, 1, 'the failure fired a bucket');
  const json = JSON.stringify(timeline);
  assert.equal(json.includes('ENOSPC'), false, 'no OS errno text in the timeline');
  assert.equal(json.includes('super secret disk diagnostic'), false, 'no reason text in the timeline');
  assert.equal(json.includes('7B'), false, 'no reason fragment in the timeline');
  // The bucket shape is count + window only — there is no field that could carry a reason.
  assert.deepEqual(Object.keys(timeline.buckets[0]).sort(), ['bucketEnd', 'bucketStart', 'count']);
});

// ── RETENTION-HEALTH TALLY (WARDEN-743) ──────────────────────────────────────
// The third "silent signal-loss" path on the receiver, made legible: an event
// ingested + persisted can still leave the pipeline by being PRUNED (count cap /
// age window), and before WARDEN-743 that eviction was invisible on GET /summary.
// `store.prune()` already returned {before, after, pruned, rewrote} but
// createRetentionTrigger DISCARDED it; now a SUCCESSFUL prune records into the
// retention-health tally and /summary surfaces it. Below: the tally is driven
// directly (record/snapshot), then through the trigger's .then (the discipline
// that a FAILED prune records nothing), then end-to-end through /summary. Still
// ZERO real fs, ZERO real network — the in-memory file mirror + fake clock drive it.

// Drive GET /summary on the handler and return just the `retention` aggregate.
async function summaryRetention(handler) {
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200, 'summary read must succeed to inspect the tally');
  return JSON.parse(res.body).retention;
}

test('retention tally: a fresh tally snapshots to the zeroed shape, carrying the configured bounds (parity with an idle receiver)', () => {
  const tally = createRetentionTally({ now: () => 0, maxEvents: 7, maxAgeMs: 3600_000 });
  assert.deepEqual(tally.snapshot(), {
    configured: { maxEvents: 7, maxAgeMs: 3600_000 },
    retainedCount: 0,
    totalPruned: 0,
    last: null,
    timeline: { buckets: [], bucketMs: 1_800_000 },
  });
});

test('retention tally: record() on a prune that DROPPED events accumulates totalPruned + tracks the most-recent sample', () => {
  let clock = 1000;
  const tally = createRetentionTally({ now: () => clock, maxEvents: 3 });
  tally.record({ before: 5, after: 3, pruned: 2, rewrote: true, retainedCount: 3 });
  clock = 2000;
  tally.record({ before: 4, after: 3, pruned: 1, rewrote: true, retainedCount: 3 });

  const snap = tally.snapshot();
  assert.equal(snap.totalPruned, 3, 'running total accumulates across prunes (2 + 1)');
  assert.equal(snap.retainedCount, 3, 'retainedCount is the post-prune store size');
  assert.deepEqual(snap.last, {
    before: 4,
    after: 3,
    pruned: 1,
    rewrote: true,
    ts: 2000,
  }, 'last is the single most-recent prune sample stamped with the injected now()');
});

test('retention tally: a no-op prune (pruned:0) does NOT inflate totalPruned but DOES refresh last (so a maintainer sees when retention last ran)', () => {
  // Chosen semantics: totalPruned counts only what was actually DROPPED; `last`
  // is the most-recent prune SAMPLE regardless of whether it dropped anything, so
  // an idle-but-healthy retention (a no-op sweep on a quiet store) is still
  // observable as "retention ran, dropped nothing" rather than reading as never-run.
  let clock = 5000;
  const tally = createRetentionTally({ now: () => clock, maxEvents: 3 });
  tally.record({ before: 5, after: 3, pruned: 2, rewrote: true, retainedCount: 3 });
  clock = 6000;
  tally.record({ before: 3, after: 3, pruned: 0, rewrote: false, retainedCount: 3 });

  const snap = tally.snapshot();
  assert.equal(snap.totalPruned, 2, 'the no-op prune added 0 — totalPruned is unchanged');
  assert.equal(snap.retainedCount, 3);
  assert.deepEqual(snap.last, {
    before: 3,
    after: 3,
    pruned: 0,
    rewrote: false,
    ts: 6000,
  }, 'last refreshed to the no-op sample (retention ran, dropped nothing)');
});

test('retention tally: snapshot() is a stable point-in-time copy — a later record does not mutate it', () => {
  let clock = 7000;
  const tally = createRetentionTally({ now: () => clock, maxEvents: 2 });
  tally.record({ before: 4, after: 2, pruned: 2, rewrote: true, retainedCount: 2 });
  const snap = tally.snapshot();
  clock = 8000;
  tally.record({ before: 3, after: 2, pruned: 1, rewrote: true, retainedCount: 2 });
  // the earlier snapshot is unchanged by the later record
  assert.deepEqual(snap, {
    configured: { maxEvents: 2, maxAgeMs: 0 },
    retainedCount: 2,
    totalPruned: 2,
    last: { before: 4, after: 2, pruned: 2, rewrote: true, ts: 7000 },
    timeline: { buckets: [{ bucketStart: -1_793_000, bucketEnd: 7_000, count: 2 }], bucketMs: 1_800_000 },
  });
  // a fresh snapshot reflects the new state
  assert.equal(tally.snapshot().totalPruned, 3);
  assert.equal(tally.snapshot().last.ts, 8000);
});

test('retention tally: BOUNDED — many records never grow unbounded (one most-recent sample + a running total, not one entry per prune)', () => {
  const tally = createRetentionTally({ now: () => 0, maxEvents: 10 });
  for (let i = 0; i < 1000; i++) {
    tally.record({ before: 11, after: 10, pruned: 1, rewrote: true, retainedCount: 10 });
  }
  const snap = tally.snapshot();
  assert.equal(snap.totalPruned, 1000, 'running total accumulates');
  assert.equal(snap.retainedCount, 10);
  assert.deepEqual(snap.last, {
    before: 11,
    after: 10,
    pruned: 1,
    rewrote: true,
    ts: 0,
  }, 'only the single most-recent sample is retained — not 1000 entries');
  assert.deepEqual(Object.keys(snap).sort(), ['configured', 'last', 'retainedCount', 'timeline', 'totalPruned'], 'shape stays bounded — timeline included, no per-prune growth');
});

test('retention tally: record() on a bare/empty call is a defensive no-op that does not throw or record a spurious sample', () => {
  const tally = createRetentionTally({ now: () => 0, maxEvents: 3 });
  assert.doesNotThrow(() => tally.record());
  assert.doesNotThrow(() => tally.record({}));
  const snap = tally.snapshot();
  assert.equal(snap.totalPruned, 0, 'a bare record added nothing');
  assert.equal(snap.retainedCount, 0);
  assert.equal(snap.last, null, 'no prune was recorded');
  assert.deepEqual(snap.timeline, { buckets: [], bucketMs: 1_800_000 }, 'a bare record painted no spurious eviction bucket');
});

// ── RETENTION TALLY timeline (WARDEN-838) ────────────────────────────────────
// The bounded rolling-window timeline mirrors the three sibling tallies'
// timelines (rejections WARDEN-798 / persistErrors WARDEN-777 / deduped
// WARDEN-812), all composed from the shared createBoundedRollingTimeline helper
// (WARDEN-834). ONE divergence: a prune contributes its DROPPED event count to
// the bucket (a 5000-event compaction registers as 5000), not +1 per prune — so
// the timeline answers "how much signal was lost WHEN", distinguishing ONGOING
// eviction churn from a RESOLVED one-time compaction. Driven directly with a
// fake clock (no real Date); the bucket math is verified byte-exact below.

test('retention tally timeline: an empty tally carries the zeroed shape — buckets: [], bucketMs = default (no false alarm)', () => {
  // A fresh createRetentionTally({ now }) snapshot returns the zeroed timeline —
  // shape-stable whether or not the tally is wired, mirroring the EMPTY_* shapes
  // of the three sibling tallies. bucketMs is the default granularity (NOT 0).
  const tally = createRetentionTally({ now: () => 0 });
  assert.deepEqual(tally.snapshot().timeline, { buckets: [], bucketMs: 1_800_000 });
});

test('retention tally timeline: a known seeded time spread — a recent eviction spike vs an earlier baseline, weighted by DROPPED count (parity with the sibling timelines + summarizeTimeline)', () => {
  // Mirrors the deduped/persistErrors/rejections tally timeline tests: record
  // prunes at known fake-clock times so the snapshot emits buckets at EXACTLY
  // those times. window [0, 86_400_000], bucketMs 1_800_000; bucket 1 =
  // [1.8M, 3.6M); bucket 47 = [84.6M, 86.4M); a record at EXACTLY now (86.4M)
  // folds into the newest bucket (top-boundary fold). Covers same-bucket
  // accumulation and chronological sort. KEY DIFFERENCE from the +1 siblings:
  // three prunes in the newest bucket accumulate to count:10 (5 + 3 + 2), NOT 3
  // — each prune contributes its DROPPED count. A 5000-event compaction would
  // register as 5000.
  let clock = 0;
  const tally = createRetentionTally({ now: () => clock });
  // baseline: one eviction (dropped 1) in bucket 1
  clock = 1_800_000; tally.record({ before: 4, after: 3, pruned: 1, rewrote: true, retainedCount: 3 });
  // spike: three compactions in bucket 47, accumulating by DROPPED count
  clock = 84_600_000; tally.record({ before: 8, after: 3, pruned: 5, rewrote: true, retainedCount: 3 });
  clock = 84_600_001; tally.record({ before: 6, after: 3, pruned: 3, rewrote: true, retainedCount: 3 });
  clock = 86_400_000; tally.record({ before: 5, after: 3, pruned: 2, rewrote: true, retainedCount: 3 }); // === now → newest via boundary fold
  clock = 86_400_000; // snapshot time
  const { timeline, totalPruned } = tally.snapshot();
  assert.equal(timeline.buckets.length, 2, 'two distinct buckets: baseline + spike');
  assert.deepEqual(timeline.buckets, [
    { bucketStart: 1_800_000, bucketEnd: 3_600_000, count: 1 },    // baseline (oldest)
    { bucketStart: 84_600_000, bucketEnd: 86_400_000, count: 10 }, // recent spike — 5 + 3 + 2, weighted by DROPPED count (NOT +1 per prune)
  ]);
  assert.equal(timeline.bucketMs, 1_800_000);
  assert.equal(totalPruned, 11, 'totalPruned is cumulative across the timeline');
  // The bucket shape is count + window only — there is no field that could carry
  // a raw event byte or an extended-tier identifier (the trust model is preserved).
  assert.deepEqual(Object.keys(timeline.buckets[0]).sort(), ['bucketEnd', 'bucketStart', 'count']);
});

test('retention tally timeline: ROLL-OFF BOUND — buckets older than the window drop on snapshot(); bucket count never exceeds maxBuckets', () => {
  // Record one dropped-1 prune per distinct 30-min slot for 60 slots
  // (> maxBuckets=48), each a bucketMs apart, so the 48-slot-wide rolling 24h
  // window cannot hold them all. Advancing the fake clock pushes the earliest
  // evictions out of the window: the snapshot must drop the rolled-off buckets,
  // never emit more than maxBuckets, and — unlike `totalPruned` — only the
  // TIMELINE window rolls; totalPruned stays cumulative.
  let clock = 1_800_000;
  const tally = createRetentionTally({ now: () => clock });
  for (let i = 0; i < 60; i++) {
    tally.record({ before: 11, after: 10, pruned: 1, rewrote: true, retainedCount: 10 });
    clock += 1_800_000; // advance to the next distinct 30-min slot
  }
  // clock is now 1_800_000 + 60 * 1_800_000 = 109_800_000 (snapshot time)
  const snap = tally.snapshot();
  assert.equal(snap.totalPruned, 60, 'totalPruned is cumulative — it does NOT roll off (only the timeline window does)');
  assert.ok(snap.timeline.buckets.length <= 48, `bucket count bounded at maxBuckets (got ${snap.timeline.buckets.length})`);
  assert.equal(snap.timeline.bucketMs, 1_800_000);
  // Every surviving bucket sits inside the current 24h window — the earliest
  // evictions (the first ~12 slots) rolled off. windowStart = clock - windowMs.
  const windowStart = clock - 86_400_000;
  for (const b of snap.timeline.buckets) {
    assert.ok(b.bucketStart >= windowStart, `bucket ${b.bucketStart} is inside the rolling window (>= ${windowStart})`);
  }
  // Emitted oldest → newest (chronological sort).
  for (let i = 1; i < snap.timeline.buckets.length; i++) {
    assert.ok(
      snap.timeline.buckets[i - 1].bucketStart < snap.timeline.buckets[i].bucketStart,
      'buckets sorted oldest → newest'
    );
  }
});

test('retention tally timeline: DEGENERATE config (windowMs: 0 / maxBuckets: 0) → { buckets: [], bucketMs: 0 } while the scalar tally keeps working', () => {
  // Mirrors summarizeTimeline's + the sibling tallies' degenerate-config guard:
  // a bad windowMs/maxBuckets override collapses the timeline to
  // { buckets: [], bucketMs: 0 } — never a huge/NaN array — while the scalar
  // tally (totalPruned / last) keeps working regardless.
  let clock = 5_000;
  const tally = createRetentionTally({ now: () => clock, maxBuckets: 0, windowMs: 0 });
  tally.record({ before: 5, after: 3, pruned: 2, rewrote: true, retainedCount: 3 });
  const snap = tally.snapshot();
  // scalar tally unaffected
  assert.equal(snap.totalPruned, 2);
  assert.deepEqual(snap.last, { before: 5, after: 3, pruned: 2, rewrote: true, ts: 5_000 });
  // timeline collapsed — never a huge/NaN array
  assert.deepEqual(snap.timeline, { buckets: [], bucketMs: 0 });
});

test('retention tally timeline: a no-op prune (pruned:0) refreshes `last` but bumps NO bucket (parity with totalPruned, which also adds 0)', () => {
  // Chosen semantics: the timeline counts only what was actually DROPPED, so a
  // no-op sweep on a quiet store (retention ran, evicted nothing) paints no
  // spurious eviction bucket — exactly as it adds 0 to totalPruned. `last` still
  // refreshes so a maintainer sees retention last RAN (the WARDEN-743 discipline,
  // now timeline-aware).
  let clock = 9_000;
  const tally = createRetentionTally({ now: () => clock });
  clock = 9_000; tally.record({ before: 5, after: 3, pruned: 2, rewrote: true, retainedCount: 3 }); // a real eviction
  clock = 9_001; tally.record({ before: 3, after: 3, pruned: 0, rewrote: false, retainedCount: 3 }); // a no-op sweep
  const snap = tally.snapshot();
  assert.equal(snap.totalPruned, 2, 'the no-op prune added 0 — totalPruned is unchanged');
  assert.deepEqual(snap.last, { before: 3, after: 3, pruned: 0, rewrote: false, ts: 9_001 }, 'last refreshed to the no-op sample');
  // The real eviction fired one bucket (count: 2); the no-op added NO second
  // bucket and did not inflate the first.
  assert.equal(snap.timeline.buckets.length, 1, 'the no-op prune painted no bucket');
  assert.equal(snap.timeline.buckets[0].count, 2, 'the bucket still reflects only the real eviction');
});

test('retention tally timeline: ONGOING eviction churn vs RESOLVED compaction — the newest bucket tells them apart (totalPruned + last.ts alone cannot)', () => {
  // The flagship success criterion (WARDEN-838): two receivers with the SAME
  // totalPruned can be distinguished by whether eviction churn is STILL landing
  // (newest bucket non-empty = ONGOING — the store is at cap and /summary's
  // window is actively shrinking → raise STORE_MAX_EVENTS) or has gone quiet
  // (no bucket near now, only older buckets populated = RESOLVED — a one-time
  // compaction an hour ago, benign history → do nothing). `totalPruned` + a
  // single `last.ts` provably cannot answer this — a sustained eviction flood
  // and a spike that recovered read identical without the per-bucket distribution.
  const snapshotTime = 100_000_000;

  // ONGOING: an eviction landed moments before the snapshot — newest bucket non-empty.
  let clock = snapshotTime;
  const ongoing = createRetentionTally({ now: () => clock });
  clock = snapshotTime - 60_000; // 1 min ago — inside the newest bucket
  ongoing.record({ before: 5, after: 3, pruned: 4, rewrote: true, retainedCount: 3 });
  clock = snapshotTime;
  const ongSnap = ongoing.snapshot();
  const ongBuckets = ongSnap.timeline.buckets;
  assert.ok(ongBuckets.length >= 1, 'ONGOING: at least one bucket is populated');
  const ongNewest = ongBuckets[ongBuckets.length - 1];
  assert.equal(ongNewest.bucketEnd, snapshotTime, 'ONGOING: the newest bucket reaches now — evictions are still landing');
  assert.ok(ongNewest.count >= 1, 'ONGOING: the newest bucket is non-empty');

  // RESOLVED: the only eviction landed near the START of the window, then the
  // store drained below the cap; snapshot much later — the eviction sits in an
  // OLD bucket, no bucket near now.
  const resolved = createRetentionTally({ now: () => clock });
  clock = snapshotTime - 80_000_000; // ~22h ago — near the window's oldest edge
  resolved.record({ before: 5, after: 3, pruned: 4, rewrote: true, retainedCount: 3 });
  clock = snapshotTime; // the store drained an hour ago; evictions stopped
  const resSnap = resolved.snapshot();
  const resBuckets = resSnap.timeline.buckets;
  assert.ok(resBuckets.length >= 1, 'RESOLVED: the old eviction still has its bucket');
  const resNewest = resBuckets[resBuckets.length - 1];
  assert.ok(
    resNewest.bucketEnd < snapshotTime,
    'RESOLVED: the newest POPULATED bucket ends well before now — no bucket reaches the snapshot time (evictions went quiet)'
  );

  // Same totalPruned, opposite verdict — the temporal distribution is the deciding signal.
  assert.equal(ongSnap.totalPruned, resSnap.totalPruned, 'both pruned 4 — identical totalPruned, but the timeline tells them apart');
});

// ── RETENTION TALLY wired through createRetentionTrigger (.then discipline) ───
// The trigger records prune()'s already-computed result ONLY on success. A FAILED
// prune removed nothing and must not record a spurious sample — the .then vs
// .catch/.finally distinction. Driven with the fake clock + the spy store.

test('retention tally: createRetentionTrigger records a SUCCESSFUL prune into the tally (.then discipline)', async () => {
  const store = storeWithRetentionSpy({
    pruneImpl: async () => ({ before: 5, after: 3, pruned: 2, rewrote: true }),
  });
  const clock = fakeClock();
  clock.setNow(42_000);
  const retention = createRetentionTally({ now: clock.now, maxEvents: 3, maxAgeMs: 1000 });
  const trigger = createRetentionTrigger(store, {
    maxEvents: 3,
    maxAgeMs: 1000,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    retention,
  });
  trigger.afterAppend(3); // cross the count bound → arm
  clock.flushAll(); // fire the debounced prune
  await new Promise((r) => setTimeout(r, 0)); // let the .then settle

  const snap = retention.snapshot();
  assert.equal(store.calls.length, 1, 'the prune ran once');
  assert.equal(snap.totalPruned, 2, 'the dropped count was recorded');
  assert.equal(snap.retainedCount, 3, 'retainedCount is the post-prune store size (after)');
  assert.deepEqual(snap.last, {
    before: 5,
    after: 3,
    pruned: 2,
    rewrote: true,
    ts: 42_000,
  }, 'last carries the prune sample stamped with the injected now()');
});

test('retention tally: createRetentionTrigger records NOTHING on a FAILED prune (a failed prune removed nothing)', async () => {
  const store = { prune: async () => { throw new Error('disk full'); } };
  const clock = fakeClock();
  const retention = createRetentionTally({ now: clock.now, maxEvents: 1 });
  const trigger = createRetentionTrigger(store, {
    maxEvents: 1,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    retention,
  });
  trigger.afterAppend(1); // arm
  assert.doesNotThrow(() => clock.flushAll(), 'flushing a failed prune must not throw');
  await new Promise((r) => setTimeout(r, 0)); // let the rejected prune's .catch settle

  // The .then discipline: a rejected prune skips .then, so NOTHING is recorded —
  // the tally stays zeroed (no spurious "dropped 0" sample, no totalPruned bump).
  const snap = retention.snapshot();
  assert.equal(snap.totalPruned, 0);
  assert.equal(snap.retainedCount, 0);
  assert.equal(snap.last, null, 'a failed prune recorded no sample');
});

test('retention tally: createRetentionTrigger with NO retention tally wired is today’s behavior (optional dep — nothing throws)', async () => {
  // Omitting `retention` from the trigger opts must not change the trigger's
  // behavior: a prune still runs, the (absent) tally is never consulted, and
  // nothing throws — the optional-dep parity shared with the sibling tallies.
  const store = storeWithRetentionSpy({
    pruneImpl: async () => ({ before: 4, after: 2, pruned: 2, rewrote: true }),
  });
  const clock = fakeClock();
  const trigger = createRetentionTrigger(store, {
    maxEvents: 1,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    // no `retention` tally wired
  });
  trigger.afterAppend(1);
  assert.doesNotThrow(() => clock.flushAll());
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(store.calls.length, 1, 'the prune still ran with no tally wired');
});

// ── RETENTION AGGREGATE surfaced in GET /summary (WARDEN-743) ─────────────────
// A wired tally is read by GET /summary; an absent tally yields the zeroed
// EMPTY_RETENTION shape (backward-compatible additive field). The tally is
// UNSCOPED by the /summary event filters (receiver operational health, like
// rejections / persistErrors).

test('GET /summary carries a `retention` field; a wired-but-idle tally reads zeroed EXCEPT configured carries the active bounds', async () => {
  const store = readableStore([errorEvent]);
  const retentionHealth = createRetentionTally({ maxEvents: 9, maxAgeMs: 7200_000 });
  const handler = createRequestHandler({ store, retentionHealth });
  const ret = await summaryRetention(handler);
  assert.deepEqual(ret, {
    configured: { maxEvents: 9, maxAgeMs: 7200_000 },
    retainedCount: 0,
    totalPruned: 0,
    last: null,
    timeline: { buckets: [], bucketMs: 1_800_000 },
  }, 'no prune ran → zeroed sample, but the configured bounds are still visible');
});

test('GET /summary WITHOUT a wired retentionHealth tally returns the zeroed EMPTY_RETENTION shape (backward-compatible additive field)', async () => {
  // A caller that does not pass a retentionHealth tally still gets the field,
  // zeroed — the handler is unchanged for callers that don't wire the tally,
  // exactly like an absent persistErrors dep. configured is the unset shape.
  const store = readableStore([]);
  const handler = createRequestHandler({ store }); // no retentionHealth
  const ret = await summaryRetention(handler);
  assert.deepEqual(ret, {
    configured: { maxEvents: 0, maxAgeMs: 0 },
    retainedCount: 0,
    totalPruned: 0,
    last: null,
    timeline: { buckets: [], bucketMs: 1_800_000 },
  });
});

test('GET /summary retention reflects a real end-to-end prune (ingest past the cap → debounced prune → totalPruned + last sample)', async () => {
  // The full slice: a handler + trigger + tally wired to a live in-memory store.
  // Ingesting past maxEvents arms a debounced prune; flushing it compacts the
  // store AND records into the tally, which the next GET /summary surfaces.
  const f = inMemoryFile();
  const store = createNdjsonStore(f);
  const clock = fakeClock();
  clock.setNow(123_000);
  const retentionHealth = createRetentionTally({ now: clock.now, maxEvents: 3 });
  const retention = createRetentionTrigger(store, {
    maxEvents: 3,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    retention: retentionHealth,
  });
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    retention,
    retentionHealth,
  });

  // Ingest 4 one-event batches; maxEvents=3 → a debounced prune is armed once the
  // bound is crossed, but it has NOT fired yet (the response path does not flush).
  for (let i = 0; i < 4; i++) {
    const res = fakeRes();
    await handler(fakeReq({ headers: schemaHeaders, body: validBody }), res);
    assert.equal(res.statusCode, 202);
  }
  assert.equal(clock.pending(), 1, 'a debounced prune is armed once the bound is crossed');
  assert.equal(f.read().length, 4, 'pre-prune: all 4 events still persisted');

  clock.flushAll(); // fire the off-path, debounced prune
  await new Promise((r) => setTimeout(r, 0)); // let the async compaction + .then settle

  assert.equal(f.read().length, 3, 'post-prune: store bounded to the count cap');

  // The maintainer's view: retention pruned 1, left 3, and the overview now
  // spans only the retained window — a previously-silent eviction, surfaced.
  const ret = await summaryRetention(handler);
  assert.equal(ret.configured.maxEvents, 3, 'the configured cap is visible');
  assert.equal(ret.configured.maxAgeMs, 0);
  assert.equal(ret.totalPruned, 1, 'the one pruned event is counted');
  assert.equal(ret.retainedCount, 3, 'retainedCount is the post-prune store size');
  assert.deepEqual(ret.last, {
    before: 4,
    after: 3,
    pruned: 1,
    rewrote: true,
    ts: 123_000,
  }, 'last is the most-recent prune sample stamped with the injected now()');
});

test('GET /summary?platform=win32 leaves retention UNSCOPED (receiver operational health survives the event filter)', async () => {
  // retention tallies receiver OPERATIONAL health (what retention pruned) — a
  // platform filter scopes the EVENTS, not the retention tally, exactly like
  // rejections / persistErrors. Drive a real prune by ingesting past the cap,
  // then scope /summary to a platform and assert the retention tally survives.
  const f = inMemoryFile();
  const store = createNdjsonStore(f);
  const clock = fakeClock();
  clock.setNow(999_000);
  const retentionHealth = createRetentionTally({ now: clock.now, maxEvents: 2 });
  const retention = createRetentionTrigger(store, {
    maxEvents: 2,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    retention: retentionHealth,
  });
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    retention,
    retentionHealth,
  });

  // Ingest 3 one-event batches → crosses maxEvents:2 → arms a debounced prune.
  for (let i = 0; i < 3; i++) {
    await handler(fakeReq({ headers: schemaHeaders, body: validBody }), fakeRes());
  }
  clock.flushAll();
  await new Promise((r) => setTimeout(r, 0));

  // Scope to win32 — the retention tally must read through the event filter.
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary?platform=win32' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.ok(body.matched <= body.total, 'the platform filter scoped the events');
  assert.equal(body.retention.totalPruned, 1, 'retention survives the platform filter (unscoped)');
  assert.equal(body.retention.retainedCount, 2);
  assert.equal(body.retention.last.pruned, 1);
});

test('GET /summary.retention.timeline reaches the maintainer — shape + byte-for-byte granularity parity with the three sibling timelines, weighted by the dropped count', async () => {
  // Mirrors the deduped.timeline end-to-end parity block: wire ALL FOUR tallies
  // sharing a fake clock, drive ONE real end-to-end prune (ingest past the cap),
  // and assert body.retention.timeline is present, carries the ['bucketMs',
  // 'buckets'] shape ONLY, shares bucketMs with the three sibling timelines, and
  // its single bucket's count is the DROPPED event count (not 1).
  const f = inMemoryFile();
  const store = createNdjsonStore(f);
  const clock = fakeClock();
  clock.setNow(86_400_000); // a known snapshot time → the eviction lands in the newest bucket
  const retentionHealth = createRetentionTally({ now: clock.now, maxEvents: 3 });
  const retention = createRetentionTrigger(store, {
    maxEvents: 3,
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    retention: retentionHealth,
  });
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    retention,
    retentionHealth,
    rejections: createRejectionTally({ now: clock.now }),
    persistErrors: createPersistErrorTally({ now: clock.now }),
    deduped: createDedupTally({ now: clock.now }),
  });

  // Ingest 8 one-event batches; maxEvents=3 → ONE debounced prune compacts 8 → 3
  // (dropped 5). The coalesced debounce fires a single prune.
  for (let i = 0; i < 8; i++) {
    await handler(fakeReq({ headers: schemaHeaders, body: validBody }), fakeRes());
  }
  clock.flushAll(); // fire the off-path, debounced prune
  await new Promise((r) => setTimeout(r, 0)); // let the async compaction + .then settle

  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);

  // The additive retention.timeline (WARDEN-838) carries the SAME shape + granularity
  // as persistErrors.timeline / rejections.timeline / deduped.timeline — byte-for-
  // byte parity between the four tallies' temporal axes (all default to the read-path
  // DEFAULT_TIMELINE_* constants). retention is the last event-flow tally to carry one.
  assert.ok(body.retention.timeline, 'retention.timeline present (additive — carried on every response)');
  assert.deepEqual(Object.keys(body.retention.timeline).sort(), ['bucketMs', 'buckets'], 'retention.timeline shape: buckets + bucketMs only');
  assert.equal(body.retention.timeline.bucketMs, body.persistErrors.timeline.bucketMs, 'same granularity as persistErrors.timeline');
  assert.equal(body.retention.timeline.bucketMs, body.rejections.timeline.bucketMs, 'same granularity as rejections.timeline');
  assert.equal(body.retention.timeline.bucketMs, body.deduped.timeline.bucketMs, 'same granularity as deduped.timeline');
  assert.equal(body.retention.timeline.bucketMs, 1_800_000);
  // The single coalesced prune fired one bucket; its count is the DROPPED event
  // count (5), not 1 — the weighting that distinguishes retention from the
  // +1-per-record sibling tallies.
  assert.equal(body.retention.timeline.buckets.length, 1, 'the single prune fired one timeline bucket');
  assert.equal(body.retention.timeline.buckets[0].count, 5, 'the bucket count is the dropped event count (8 → 3 = 5), not +1');
  assert.equal(body.retention.totalPruned, 5, 'totalPruned agrees with the timeline (cumulative)');
});

// ── DEDUP TALLY (WARDEN-752) ─────────────────────────────────────────────────
// The transport-retry twin of the rejections / persistErrors tallies. WARDEN-666
// built idempotent ingest: a client that lost a 2xx retries the SAME bytes with
// the SAME idempotency-key, ingest() recognizes the key, and returns 202
// {accepted:0, deduped:true} WITHOUT re-persisting (so one crash retried ≤3× lands
// as ONE event). That correctness mechanism was invisible on the receiver — the
// handler never inspected `result.body.deduped` and GET /summary had no `deduped`
// field — so a maintainer could not tell "clients are retrying because my receiver
// is slow / the network is flaky" from "traffic is flowing cleanly". This tally
// closes that gap: it counts dedup HITS at the ONE detection site and surfaces a
// bounded `{ total, lastSeen }` on GET /summary. Below: the tally is driven
// directly with an INJECTED fake clock (mirroring the persistErrors tally unit
// tests), then the handler is proven to record a dedup ONLY when a retry is
// absorbed (seenKeys + deduped both wired), keep it a SEPARATE signal from
// rejections / persistErrors, and stay BOUNDED. Still ZERO real fs, ZERO real
// network — driven with fake req/res and a sink+source-sharing store (mirroring
// the WARDEN-666 scenario).

// Wire a handler to a sink+source-sharing store (appends become visible to later
// reads) + a seenKeys set + a deduped tally with an INJECTED clock (so lastSeen is
// deterministic). Mirrors wiringWithPersistErrors.
function wiringWithDedup({ now } = {}) {
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const seenKeys = createSeenKeys();
  const deduped = createDedupTally(now != null ? { now } : {});
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    seenKeys,
    deduped,
  });
  return { handler, deduped, seenKeys, store };
}

// Drive GET /summary on the handler and return just the `deduped` aggregate.
async function summaryDeduped(handler) {
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200, 'summary read must succeed to inspect the tally');
  return JSON.parse(res.body).deduped;
}

// Drive GET /summary on the handler and return just the `seenKeys` capacity
// aggregate (WARDEN-790 — the dedup-set fill level, complement to `deduped`).
async function summarySeenKeys(handler) {
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200, 'summary read must succeed to inspect the set');
  return JSON.parse(res.body).seenKeys;
}

test('deduped tally: a fresh tally snapshots to the zeroed shape (parity with a healthy receiver)', () => {
  const tally = createDedupTally({ now: () => 0 });
  assert.deepEqual(tally.snapshot(), {
    total: 0,
    lastSeen: null,
    timeline: { buckets: [], bucketMs: 1_800_000 },
  });
});

test('deduped tally: record() accumulates the total + tracks the most-recent occurrence', () => {
  let clock = 1000;
  const tally = createDedupTally({ now: () => clock });
  tally.record();
  clock = 2000;
  tally.record();

  const snap = tally.snapshot();
  assert.equal(snap.total, 2);
  assert.equal(snap.lastSeen, 2000, 'lastSeen is the injected now() of the most-recent record');
});

test('deduped tally: snapshot() is a stable point-in-time copy — a later record does not mutate it', () => {
  let clock = 5000;
  const tally = createDedupTally({ now: () => clock });
  tally.record();
  const snap = tally.snapshot();
  clock = 6000;
  tally.record();
  // the earlier snapshot is unchanged by the later record — its scalar aggregate
  // AND its timeline are both frozen. (The one dedup at clock=5000 is the newest
  // event in a 24h window ending at 5000, so it lands in the newest bucket, whose
  // right edge is exactly 5000 — bucketEnd === lastSeen.)
  assert.deepEqual(snap, {
    total: 1,
    lastSeen: 5000,
    timeline: { buckets: [{ bucketStart: -1_795_000, bucketEnd: 5_000, count: 1 }], bucketMs: 1_800_000 },
  });
  // a fresh snapshot reflects the new state
  assert.equal(tally.snapshot().total, 2);
  assert.equal(tally.snapshot().lastSeen, 6000);
});

test('deduped tally: BOUNDED — many records never grow unbounded (one count, one sample; no per-dedup growth)', () => {
  // Mirrors "persistErrors tally: BOUNDED". A dedup carries no diagnostic string,
  // so the shape is the persistErrors shape MINUS `lastReason` — { total, lastSeen }.
  // 1000 dedups must accumulate to a single COUNT + the single most-recent sample,
  // never one entry per dedup.
  let clock = 0;
  const tally = createDedupTally({ now: () => (clock += 1) });
  for (let i = 0; i < 1000; i++) {
    tally.record();
  }
  const snap = tally.snapshot();
  assert.equal(snap.total, 1000);
  assert.equal(snap.lastSeen, 1000, 'only the single most-recent lastSeen is retained');
  assert.deepEqual(Object.keys(snap).sort(), ['lastSeen', 'timeline', 'total'], 'shape stays bounded — no per-dedup growth, no reason field');
});

test('deduped tally: record() ignores any argument — a dedup carries no diagnostic string', () => {
  // A dedup is "a batch we'd already accepted came back" — there is no reason to
  // record, so record() takes no payload. Passing one (defensively) must not leak
  // into the snapshot as an unbounded/garbage field.
  const tally = createDedupTally({ now: () => 0 });
  tally.record('should be ignored');
  tally.record({ anything: true });
  const snap = tally.snapshot();
  assert.equal(snap.total, 2);
  assert.deepEqual(Object.keys(snap).sort(), ['lastSeen', 'timeline', 'total'], 'no reason/payload field is retained');
});

// ── DEDUP AGGREGATE surfaced in GET /summary (WARDEN-752) ─────────────────────
// A retried batch (same idempotency-key) is absorbed → recorded into the dedup
// tally whose snapshot GET /summary reads. Driven through the handler with the
// sink+source-sharing store; the tally persists across requests on the SAME
// handler closure.

test('a retried batch (same idempotency-key) increments /summary.deduped — transport-retry made visible', async () => {
  // seenKeys + deduped both wired (mirrors createReceiver). The SAME bytes posted
  // twice with the SAME key: the first accepts+persists, the second is a DEDUP.
  let clock = 4242;
  const { handler } = wiringWithDedup({ now: () => clock });
  const headers = { ...schemaHeaders, 'idempotency-key': 'batch-retry' };

  // First POST: accepted + persisted normally (NOT a dedup).
  const r1 = fakeRes();
  await handler(fakeReq({ headers, body: validBody }), r1);
  assert.deepEqual(JSON.parse(r1.body), { accepted: 1 }, 'first post is a normal accept');

  // Second POST — the retry (same key, identical bytes): a 202 DEDUP.
  const r2 = fakeRes();
  await handler(fakeReq({ headers, body: validBody }), r2);
  assert.deepEqual(JSON.parse(r2.body), { accepted: 0, deduped: true }, 'the retry was deduped');

  // The dedup tally counted exactly ONE transport-retry (the retry), not the first
  // accept, and its lastSeen is the injected now() of the retry.
  const ded = await summaryDeduped(handler);
  assert.equal(ded.total, 1, 'the single deduped retry was recorded');
  assert.equal(ded.lastSeen, 4242, 'lastSeen is the injected now() of the dedup');
  // The additive timeline (WARDEN-812) surfaces the dedup's TIMING: the single retry
  // fired one bucket, and it sat at the top edge of the rolling window's newest
  // bucket, so bucketEnd === lastSeen (the injected now). This is the spike-vs-
  // baseline signal total+lastSeen alone could not convey — same shape + granularity
  // as persistErrors.timeline / rejections.timeline (all default to the read-path
  // DEFAULT_TIMELINE_* constants).
  assert.ok(ded.timeline, 'timeline field present (additive — carried on every response)');
  assert.deepEqual(Object.keys(ded.timeline).sort(), ['bucketMs', 'buckets'], 'timeline shape: buckets + bucketMs only');
  assert.equal(ded.timeline.buckets.length, 1, 'the single deduped retry fired one bucket');
  assert.equal(ded.timeline.buckets[0].count, 1);
  assert.equal(ded.timeline.bucketMs, 1_800_000);
  assert.equal(ded.timeline.buckets[0].bucketEnd, 4242, 'the dedup is at the top edge of the newest bucket (bucketEnd === lastSeen)');
});

test('a fresh accept (distinct keys, no retry) does NOT increment deduped (healthy traffic is never a dedup)', async () => {
  // Two distinct-key batches both accept normally — neither is a retry, so the
  // dedup tally must stay at zero.
  const { handler } = wiringWithDedup({ now: () => 0 });
  await handler(fakeReq({ headers: { ...schemaHeaders, 'idempotency-key': 'A' }, body: validBody }), fakeRes());
  await handler(fakeReq({ headers: { ...schemaHeaders, 'idempotency-key': 'B' }, body: validBody }), fakeRes());

  const ded = await summaryDeduped(handler);
  assert.deepEqual(ded, { total: 0, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } }, 'no retry → no dedup recorded');
});

test('deduped stays BOUNDED across many retries — one count + single most-recent lastSeen', async () => {
  // Drive many dedups (one accept, then 49 retries of the SAME key); the tally must
  // accumulate a COUNT and retain only the single most-recent lastSeen (mirroring
  // the "persistErrors stay BOUNDED" test). lastSeen advances via the injected clock.
  let clock = 0;
  const { handler } = wiringWithDedup({ now: () => (clock += 1000) });
  const headers = { ...schemaHeaders, 'idempotency-key': 'storm' };
  // First post accepts (and does NOT touch the dedup tally's clock); the remaining
  // 49 are deduped retries, each advancing the tally's now() by 1000.
  for (let n = 0; n < 50; n++) {
    await handler(fakeReq({ headers, body: validBody }), fakeRes());
  }
  const ded = await summaryDeduped(handler);
  assert.equal(ded.total, 49, '49 retries were deduped (the first accept was not)');
  assert.equal(ded.lastSeen, 49_000, 'lastSeen is the injected now() of the most-recent dedup (49 advances × 1000)');
  assert.deepEqual(Object.keys(ded).sort(), ['lastSeen', 'timeline', 'total'], 'shape stays bounded — no per-dedup growth');
});

test('a dedup records into deduped, NOT into rejections or persistErrors (three separate signals)', async () => {
  // A dedup is a distinct "transport-retry absorbed" class — it must not overload
  // the rejection tally's HTTP-rejection-sites-only contract nor the persistErrors
  // "validated but un-storable" contract. Wire ALL THREE tallies; a dedup bumps
  // deduped and leaves rejections + persistErrors at zero.
  let clock = 9000;
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const seenKeys = createSeenKeys();
  const rejections = createRejectionTally({ now: () => clock });
  const persistErrors = createPersistErrorTally({ now: () => clock });
  const deduped = createDedupTally({ now: () => clock });
  const handler = createRequestHandler({
    store,
    schema: { SCHEMA_VERSION, validateEvent },
    seenKeys,
    rejections,
    persistErrors,
    deduped,
  });
  const headers = { ...schemaHeaders, 'idempotency-key': 'solo' };
  await handler(fakeReq({ headers, body: validBody }), fakeRes()); // accept
  await handler(fakeReq({ headers, body: validBody }), fakeRes()); // dedup

  const summaryRes = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), summaryRes);
  const body = JSON.parse(summaryRes.body);
  assert.equal(body.deduped.total, 1, 'the deduped retry was recorded in its own tally');
  assert.deepEqual(
    body.rejections,
    { total: 0, byStatus: {}, byDeclaredVersion: {}, lastStatus: null, lastReason: null, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } },
    'rejections untouched — a dedup is not an HTTP rejection'
  );
  assert.deepEqual(
    body.persistErrors,
    { total: 0, lastReason: null, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } },
    'persistErrors untouched — a dedup is not a persist failure'
  );
  // The additive deduped.timeline (WARDEN-812) carries the SAME shape + granularity
  // as persistErrors.timeline / rejections.timeline — byte-for-byte parity between
  // the three tallies' temporal axes (all default to the read-path DEFAULT_TIMELINE_*
  // constants). The dedup fired one bucket; the other two stay zeroed but shape-stable.
  assert.ok(body.deduped.timeline, 'deduped.timeline present (additive — carried on every response)');
  assert.deepEqual(Object.keys(body.deduped.timeline).sort(), ['bucketMs', 'buckets'], 'deduped.timeline shape: buckets + bucketMs only');
  assert.equal(body.deduped.timeline.bucketMs, body.persistErrors.timeline.bucketMs, 'same granularity as persistErrors.timeline');
  assert.equal(body.deduped.timeline.bucketMs, body.rejections.timeline.bucketMs, 'same granularity as rejections.timeline');
  assert.equal(body.deduped.timeline.buckets.length, 1, 'the single dedup fired one timeline bucket');
  assert.equal(body.deduped.timeline.buckets[0].count, 1);
});

test('an idle receiver (no traffic) returns zeroed deduped in GET /summary (parity with today — no false alarm)', async () => {
  const { handler } = wiringWithDedup();
  const ded = await summaryDeduped(handler);
  assert.deepEqual(ded, { total: 0, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } });
});

test('GET /summary WITHOUT a wired deduped tally still returns a zeroed deduped field (backward-compatible additive shape)', async () => {
  // A caller that does not pass a deduped tally still gets the field, zeroed — the
  // handler is unchanged for callers that don't wire the tally, exactly like an
  // absent persistErrors dep. Crucially: even when seenKeys IS wired, a dedup
  // happens but is NOT recorded without a deduped tally (the recording is the
  // single guarded switch), proving the tally is truly OPTIONAL.
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const seenKeys = createSeenKeys();
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent }, seenKeys }); // no deduped
  const headers = { ...schemaHeaders, 'idempotency-key': 'unwired' };
  await handler(fakeReq({ headers, body: validBody }), fakeRes()); // accept
  const r2 = fakeRes();
  await handler(fakeReq({ headers, body: validBody }), r2); // dedup (but not recorded)
  assert.deepEqual(JSON.parse(r2.body), { accepted: 0, deduped: true }, 'a dedup still happens at the ingest seam');

  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(JSON.parse(res.body).deduped, {
    total: 0,
    lastSeen: null,
    timeline: { buckets: [], bucketMs: 1_800_000 },
  });
});

test('a handler with NO seenKeys dep does NOT dedup, so deduped stays at zero (backward-compatible — today behavior)', async () => {
  // No seenKeys wired → ingest() never returns deduped:true → the dedup tally is
  // never bumped, regardless of whether a deduped tally is wired. An unwired
  // receiver (or an old client that sends no key header) is unchanged.
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const deduped = createDedupTally({ now: () => 0 });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent }, deduped }); // no seenKeys
  const headers = { ...schemaHeaders, 'idempotency-key': 'no-set' };
  await handler(fakeReq({ headers, body: validBody }), fakeRes());
  await handler(fakeReq({ headers, body: validBody }), fakeRes()); // both persist (no dedup)

  const ded = await summaryDeduped(handler);
  assert.deepEqual(ded, { total: 0, lastSeen: null, timeline: { buckets: [], bucketMs: 1_800_000 } }, 'no seenKeys → no dedup → tally stays zeroed');
});

// ── DEDUP TIMELINE (WARDEN-812) ──────────────────────────────────────────────
// The bounded `timeline` on the deduped tally — mirrors the read-path `timeline`
// (summarizeTimeline, WARDEN-603), the persistErrors timeline (WARDEN-777), and the
// rejections timeline (WARDEN-798) so a maintainer can tell an ONGOING retry storm
// (dedups still landing in the newest bucket — clients hammering because the
// receiver is slow / the network is flaky RIGHT NOW) from a RESOLVED blip (dedups
// clustered in older buckets, newest bucket empty — an hour-old spike that stopped)
// — the spike-vs-baseline question `total` + `lastSeen` provably cannot answer for
// an episodic retry flood that often recovers. Driven directly with an injected
// fake clock (mirroring the tally unit tests above): bucket-placement parity with
// summarizeTimeline, the rolling-window roll-off bound, the degenerate-config
// guard, and the ongoing-vs-resolved flagship.

test('deduped tally timeline: an empty tally carries the zeroed shape — buckets: [], bucketMs = default (no false alarm)', () => {
  // Case 1: a fresh createDedupTally({ now }) snapshot returns the zeroed timeline —
  // shape-stable whether or not the tally is wired, mirroring EMPTY_PERSIST_ERRORS /
  // EMPTY_REJECTIONS. bucketMs is the default granularity (NOT 0).
  const tally = createDedupTally({ now: () => 0 });
  assert.deepEqual(tally.snapshot().timeline, { buckets: [], bucketMs: 1_800_000 });
});

test('deduped tally timeline: a known seeded time spread — a recent retry spike vs an earlier baseline (bucket placement parity with the read-path summarizeTimeline + the persistErrors/rejections tallies)', () => {
  // Mirrors the read-path /summary "known seeded time spread" test AND the
  // persistErrors/rejections tally timeline tests: record dedups at known fake-clock
  // times so the snapshot emits buckets at EXACTLY those times. window
  // [0, 86_400_000], bucketMs 1_800_000; bucket 1 = [1.8M, 3.6M); bucket 47 =
  // [84.6M, 86.4M); a record at EXACTLY now (86.4M) folds into the newest bucket
  // (parity with summarizeTimeline's top-boundary fold). Covers same-bucket
  // accumulation (count: 3) and chronological sort (oldest → newest). A dedup
  // carries no diagnostic string, so the bucket shape stays count + window only.
  let clock = 0;
  const tally = createDedupTally({ now: () => clock });
  // baseline: one dedup in bucket 1
  clock = 1_800_000; tally.record();
  // spike: three dedups in bucket 47 (one at exactly now → folded via the top edge)
  clock = 84_600_000; tally.record();
  clock = 84_600_001; tally.record();
  clock = 86_400_000; tally.record(); // === now → newest via boundary fold
  clock = 86_400_000; // snapshot time
  const { timeline, total } = tally.snapshot();
  assert.equal(timeline.buckets.length, 2, 'two distinct buckets: baseline + spike');
  assert.deepEqual(timeline.buckets, [
    { bucketStart: 1_800_000, bucketEnd: 3_600_000, count: 1 },   // baseline (oldest)
    { bucketStart: 84_600_000, bucketEnd: 86_400_000, count: 3 }, // recent spike (newest) — three dedups accumulated into one bucket
  ]);
  assert.equal(timeline.bucketMs, 1_800_000);
  assert.equal(total, 4, 'total is cumulative across the timeline');
  // The bucket shape is count + window only — there is no field that could carry a
  // reason or payload (a dedup has neither; record() takes no argument).
  assert.deepEqual(Object.keys(timeline.buckets[0]).sort(), ['bucketEnd', 'bucketStart', 'count']);
});

test('deduped tally timeline: ROLL-OFF BOUND — buckets older than the window drop on snapshot(); bucket count never exceeds maxBuckets', () => {
  // Record one dedup per distinct 30-min slot for 60 slots (> maxBuckets=48), each a
  // bucketMs apart, so the 48-slot-wide rolling 24h window cannot hold them all.
  // Advancing the fake clock pushes the earliest dedups out of the window: the
  // snapshot must drop the rolled-off buckets, never emit more than maxBuckets, and
  // — unlike `total` — only the TIMELINE window rolls; total stays cumulative.
  let clock = 1_800_000;
  const tally = createDedupTally({ now: () => clock });
  for (let i = 0; i < 60; i++) {
    tally.record();
    clock += 1_800_000; // advance to the next distinct 30-min slot
  }
  // clock is now 1_800_000 + 60 * 1_800_000 = 109_800_000 (snapshot time)
  const snap = tally.snapshot();
  assert.equal(snap.total, 60, 'total is cumulative — it does NOT roll off (only the timeline window does)');
  assert.ok(snap.timeline.buckets.length <= 48, `bucket count bounded at maxBuckets (got ${snap.timeline.buckets.length})`);
  assert.equal(snap.timeline.bucketMs, 1_800_000);
  // Every surviving bucket sits inside the current 24h window — the earliest dedups
  // (the first ~12 slots) rolled off. windowStart = clock - windowMs = 23_400_000.
  const windowStart = clock - 86_400_000;
  for (const b of snap.timeline.buckets) {
    assert.ok(b.bucketStart >= windowStart, `bucket ${b.bucketStart} is inside the rolling window (>= ${windowStart})`);
  }
  // Emitted oldest → newest (chronological sort).
  for (let i = 1; i < snap.timeline.buckets.length; i++) {
    assert.ok(
      snap.timeline.buckets[i - 1].bucketStart < snap.timeline.buckets[i].bucketStart,
      'buckets sorted oldest → newest'
    );
  }
});

test('deduped tally timeline: DEGENERATE config (windowMs: 0 / maxBuckets: 0) → { buckets: [], bucketMs: 0 } while the scalar tally keeps working', () => {
  // Mirrors summarizeTimeline's + the persistErrors/rejections tallies' degenerate-
  // config guard: a bad windowMs/maxBuckets override collapses the timeline to
  // { buckets: [], bucketMs: 0 } — never a huge/NaN array — while the scalar tally
  // (total/lastSeen) keeps working regardless.
  let clock = 5_000;
  const tally = createDedupTally({ now: () => clock, maxBuckets: 0, windowMs: 0 });
  tally.record();
  const snap = tally.snapshot();
  // scalar tally unaffected
  assert.equal(snap.total, 1);
  assert.equal(snap.lastSeen, 5_000);
  // timeline collapsed — never a huge/NaN array
  assert.deepEqual(snap.timeline, { buckets: [], bucketMs: 0 });
});

test('deduped tally timeline: ONGOING retry storm vs RESOLVED blip — the newest bucket tells them apart (total + lastSeen alone cannot)', () => {
  // The flagship success criterion (WARDEN-812): two receivers with the SAME total
  // can be distinguished by whether dedup traffic is STILL landing (newest bucket
  // non-empty = ONGOING — clients still retrying because the receiver is slow / the
  // network is flaky RIGHT NOW) or has gone quiet (no bucket near now, only older
  // buckets populated = RESOLVED — an hour-old spike that stopped). `total` + a
  // single `lastSeen` provably cannot answer this — a sustained retry flood and a
  // spike that clients recovered from read identical without the per-bucket
  // distribution.
  const snapshotTime = 100_000_000;

  // ONGOING: a dedup landed moments before the snapshot — newest bucket non-empty.
  let clock = snapshotTime;
  const ongoing = createDedupTally({ now: () => clock });
  clock = snapshotTime - 60_000; // 1 min ago — inside the newest bucket
  ongoing.record();
  clock = snapshotTime;
  const ongSnap = ongoing.snapshot();
  const ongBuckets = ongSnap.timeline.buckets;
  assert.ok(ongBuckets.length >= 1, 'ONGOING: at least one bucket is populated');
  const ongNewest = ongBuckets[ongBuckets.length - 1];
  assert.equal(ongNewest.bucketEnd, snapshotTime, 'ONGOING: the newest bucket reaches now — retries are still landing');
  assert.ok(ongNewest.count >= 1, 'ONGOING: the newest bucket is non-empty');

  // RESOLVED: the only dedup landed near the START of the window, then the network
  // recovered; snapshot much later — the dedup sits in an OLD bucket, no bucket near now.
  const resolved = createDedupTally({ now: () => clock });
  clock = snapshotTime - 80_000_000; // ~22h ago — near the window's oldest edge
  resolved.record();
  clock = snapshotTime; // network recovered an hour ago; retries stopped
  const resSnap = resolved.snapshot();
  const resBuckets = resSnap.timeline.buckets;
  assert.ok(resBuckets.length >= 1, 'RESOLVED: the old dedup still has its bucket');
  const resNewest = resBuckets[resBuckets.length - 1];
  assert.ok(
    resNewest.bucketEnd < snapshotTime,
    'RESOLVED: the newest POPULATED bucket ends well before now — no bucket reaches the snapshot time (retries went quiet)'
  );

  // Same total, opposite verdict — the temporal distribution is the deciding signal.
  assert.equal(ongSnap.total, resSnap.total, 'both saw one dedup — identical total, but the timeline tells them apart');
});

// ── SEEN-KEYS CAPACITY surfaced in GET /summary (WARDEN-790) ──────────────────
// The capacity-health complement to the `deduped` tally above: `deduped` reports the
// dedup HIT COUNT, `seenKeys` reports the live FILL LEVEL of the in-memory set that
// backs the dedup decision (sourced from the existing seenKeys.snapshot()). Driven
// through the same wiringWithDedup harness (which wires seenKeys + deduped together,
// mirroring createReceiver) so the field reflects the SAME set ingest() consults.

test('seenKeys.size increments on /summary after a successful persist (the set that backs dedup is legible)', async () => {
  // A fresh accept RECORDS its key into the set; /summary.seenKeys.size must reflect
  // that one live key. This is the core capacity signal — a maintainer sees the set
  // filling as traffic flows, against the configured cap.
  const { handler } = wiringWithDedup({ now: () => 0 });
  let sk = await summarySeenKeys(handler);
  assert.equal(sk.size, 0, 'an idle receiver shows an empty set');

  await handler(fakeReq({ headers: { ...schemaHeaders, 'idempotency-key': 'A' }, body: validBody }), fakeRes());
  sk = await summarySeenKeys(handler);
  assert.equal(sk.size, 1, 'one accepted batch → one live key in the set');
});

test('seenKeys carries its configured bounds (maxKeys + ttlMs) so size is read against the cap', async () => {
  // The default-constructed set (mirrors createReceiver's createSeenKeys() call with no
  // overrides) carries DEFAULT_DEDUP_MAX_KEYS + DEFAULT_DEDUP_TTL_MS — the cap + TTL a
  // maintainer needs to interpret `size`. Mirrors how retention.snapshots carries
  // {maxEvents, maxAgeMs} beside retainedCount.
  const { handler } = wiringWithDedup({ now: () => 0 });
  const sk = await summarySeenKeys(handler);
  assert.deepEqual(sk.configured, {
    maxKeys: DEFAULT_DEDUP_MAX_KEYS,
    ttlMs: DEFAULT_DEDUP_TTL_MS,
  }, 'the field surfaces the FIFO cap + per-key TTL the set was built with');
  // and the full shape is exactly { configured, size } — nothing else leaks
  assert.deepEqual(Object.keys(sk).sort(), ['configured', 'size'], 'shape is { configured, size } only');
});

test('seenKeys.size grows with DISTINCT accepted keys (a near-unique-key flood is legible at the read surface)', async () => {
  // The signal-loss scenario the field exists to surface: a fleet / a client-side
  // idempotency-key bug emitting distinct keys per batch fills the set toward the cap.
  // Three distinct-key batches all accept normally (none dedup) → the set holds three
  // live keys. A maintainer reading size=3 against maxKeys can tell the set is filling
  // even though deduped.total is still 0 (no retries happened).
  const { handler } = wiringWithDedup({ now: () => 0 });
  await handler(fakeReq({ headers: { ...schemaHeaders, 'idempotency-key': 'A' }, body: validBody }), fakeRes());
  await handler(fakeReq({ headers: { ...schemaHeaders, 'idempotency-key': 'B' }, body: validBody }), fakeRes());
  await handler(fakeReq({ headers: { ...schemaHeaders, 'idempotency-key': 'C' }, body: validBody }), fakeRes());

  const sk = await summarySeenKeys(handler);
  assert.equal(sk.size, 3, 'three distinct accepted keys → three live keys in the set');
});

test('a retried batch (same idempotency-key) does NOT grow seenKeys.size — the key is already present (dedup is visible by NOT growing)', async () => {
  // The complement to the deduped-increments test: the SAME key retried is a HIT, so the
  // set records it ONCE (record() refreshes expiry on an existing key) and size stays at
  // 1 — while deduped.total climbs to 1. The two fields together read "1 retry absorbed
  // against a set of size 1" — exactly the hit-count + capacity pairing.
  const { handler } = wiringWithDedup({ now: () => 0 });
  const headers = { ...schemaHeaders, 'idempotency-key': 'solo' };
  await handler(fakeReq({ headers, body: validBody }), fakeRes()); // accept → records 'solo'
  await handler(fakeReq({ headers, body: validBody }), fakeRes()); // retry → dedup, does NOT add a new key

  const sk = await summarySeenKeys(handler);
  const ded = await summaryDeduped(handler);
  assert.equal(sk.size, 1, 'a retry of an already-seen key does not grow the set');
  assert.equal(ded.total, 1, 'the same retry WAS counted as a dedup hit');
});

test('GET /summary WITHOUT a wired seenKeys dep still returns a zeroed seenKeys field (backward-compatible additive shape)', async () => {
  // Parity with the deduped-absent test above: a caller that does not pass a seenKeys
  // set still gets the field, zeroed to the EMPTY_SEEN_KEYS shape — exactly like an
  // absent deduped/persistErrors/rejections dep. The {maxKeys:0, ttlMs:0} "unset"
  // configured shape mirrors EMPTY_RETENTION's {maxEvents:0, maxAgeMs:0} convention.
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  // no seenKeys wired
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const sk = await summarySeenKeys(handler);
  assert.deepEqual(sk, {
    configured: { maxKeys: 0, ttlMs: 0 },
    size: 0,
  }, 'an absent seenKeys dep yields the zeroed EMPTY_SEEN_KEYS shape');
});

// ── UNREADABLE-LINE COUNT ON /summary (WARDEN-825) ───────────────────────────
// The read-path signal-loss count: an event can clear every write-path gate
// (rejections / persistErrors / retention) and still VANISH if its line is
// unreadable on disk (a partial append left by a process killed mid-write —
// createReceiver has no SIGTERM/SIGINT handler). parseNdjson already SKIPS the
// bad line; /summary.unreadable is the observability for it. A STATE snapshot
// recomputed per read (NOT a cumulative tally), self-healing to 0 on the next
// compaction. Counts ONLY — never the corrupt bytes.

// A store whose source holds raw NDJSON text (with an OPTIONAL truncated line)
// and forwards the onSkip observer the way fileSource does in production. Reads
// parse the text; writes append to it. Still ZERO real fs.
function rawTextStore(rawText) {
  let text = rawText;
  return createNdjsonStore({
    sink: async (line) => {
      text += `${line}\n`;
    },
    source: (onSkip) => parseNdjson(text, onSkip),
  });
}

// Drive GET /summary and return the parsed body.
async function summaryBody(handler) {
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200, 'summary read must succeed to inspect the body');
  return JSON.parse(res.body);
}

test('GET /summary surfaces a non-zero `unreadable` count when the store has a truncated NDJSON line (WARDEN-825)', async () => {
  // The on-disk file has 2 good records + 1 truncated line (3 non-blank lines).
  // The good records validate enough to summarize; the truncated line is the
  // exact partial append a process killed mid-write leaves.
  const rawText = `${JSON.stringify(errorEvent)}\n{"bad": tru   ← partial\n${JSON.stringify(crashEvent)}\n`;
  const store = rawTextStore(rawText);
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const body = await summaryBody(handler);
  // The one truncated line is counted; total reflects only the 2 good records.
  assert.equal(body.unreadable, 1, 'the truncated line is counted as unreadable');
  assert.equal(body.total, 2, 'total reflects the 2 PARSED records (the bad line drops out)');
  // total + unreadable reconciles with the on-disk non-blank line count (3).
  assert.equal(body.total + body.unreadable, 3, 'total + unreadable reconciles with on-disk lines');
});

test('GET /summary on a clean store reads a zeroed `unreadable: 0` (no false alarm — parity with EMPTY_* shapes)', async () => {
  // A clean file with no corrupt lines: the field is always present (additive,
  // backward-compatible) and reads 0.
  const store = rawTextStore(`${JSON.stringify(errorEvent)}\n${JSON.stringify(crashEvent)}\n`);
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const body = await summaryBody(handler);
  assert.equal(body.unreadable, 0, 'a clean store reads a zeroed unreadable count');
  assert.equal(body.total, 2);
});

test('GET /summary on an empty store reads `unreadable: 0` (the field is ALWAYS present, additive)', async () => {
  const store = rawTextStore('');
  const handler = createRequestHandler({ store });
  const body = await summaryBody(handler);
  assert.equal(body.unreadable, 0, 'an empty store carries the zeroed unreadable field');
  assert.equal(body.total, 0);
});

test('GET /summary NEVER echoes the corrupt line bytes — `unreadable` is a count only (trust posture)', async () => {
  // A partial line could carry a payload fragment / residual identifier, so the
  // entire surface is the bare integer. The corrupt marker must not appear anywhere.
  const poison = 'SECRET-FRAGMENT-DO-NOT-LEAK';
  const rawText = `${JSON.stringify(errorEvent)}\n{"bad": ${poison}\n${JSON.stringify(crashEvent)}\n`;
  const store = rawTextStore(rawText);
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const res = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.unreadable, 1, 'the corrupt line is counted');
  assert.equal(res.body.includes(poison), false, 'the corrupt bytes are never present in the response');
});

test('GET /summary `unreadable` is a per-read STATE snapshot — two reads of the same corrupt file report the SAME count (no inflation)', async () => {
  // Every /summary request re-reads the file → re-skips the same line. A
  // cumulative tally would inflate; a state snapshot reports the same count each
  // read. (The self-heal-to-0-on-compaction is covered in test/store.test.mjs.)
  const rawText = `${JSON.stringify(errorEvent)}\n{"bad": partial\n${JSON.stringify(crashEvent)}\n`;
  const store = rawTextStore(rawText);
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });
  const first = await summaryBody(handler);
  const second = await summaryBody(handler);
  assert.equal(first.unreadable, 1);
  assert.equal(second.unreadable, 1, 'a second read reports the same count — state, not a cumulative tally');
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

test('createSeenKeys: snapshot() carries the configured maxKeys + ttlMs bounds beside size (WARDEN-790)', () => {
  // The snapshot the /summary capacity-health field is sourced from. It must carry the
  // FIFO cap + per-key TTL the set was built with so `size` is interpretable against
  // the bound — mirroring how createRetentionTally().snapshot() carries {maxEvents,
  // maxAgeMs} beside retainedCount. Defaults reflect DEFAULT_DEDUP_MAX_KEYS / _TTL_MS;
  // overrides flow through verbatim.
  const defaults = createSeenKeys({ now: () => 0 }).snapshot();
  assert.deepEqual(defaults.configured, { maxKeys: DEFAULT_DEDUP_MAX_KEYS, ttlMs: DEFAULT_DEDUP_TTL_MS });
  assert.deepEqual(Object.keys(defaults).sort(), ['configured', 'size'], 'shape is { configured, size } only');

  const custom = createSeenKeys({ ttlMs: 5000, maxKeys: 100, now: () => 0 });
  custom.record('k');
  assert.deepEqual(custom.snapshot(), { configured: { maxKeys: 100, ttlMs: 5000 }, size: 1 }, 'overrides flow through beside size');
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

test('createSeenKeys: driving past maxKeys evicts and snapshot.size stays pinned at the cap (capacity pressure is legible — WARDEN-790)', () => {
  // The signal-loss scenario the capacity field exists to surface: a sustained flood of
  // distinct keys (a fleet, or a client-side idempotency-key bug) PINS the set at the
  // cap. snapshot.size reflects exactly maxKeys (never above), so a maintainer reading
  // size === maxKeys on /summary sees the set is FULL — and knows a retried batch whose
  // key was evicted before its retry arrives would now be treated as fresh. This is the
  // capacity-health complement to the deduped hit count: it tells you the set that backs
  // dedup is LOSING keys, not just that dedup fired.
  const keys = createSeenKeys({ ttlMs: 1e9, maxKeys: 3, now: () => 0 });
  for (const k of ['a', 'b', 'c', 'd', 'e', 'f', 'g']) keys.record(k); // 7 distinct, cap 3
  const snap = keys.snapshot();
  assert.equal(snap.size, 3, 'size stays pinned at the cap regardless of how many distinct keys flowed through');
  assert.equal(snap.configured.maxKeys, 3, 'the configured cap is surfaced beside the pinned size');
  // only the NEWEST 3 survive (FIFO evicted a–d)
  assert.equal(keys.has('e'), true);
  assert.equal(keys.has('f'), true);
  assert.equal(keys.has('g'), true);
  assert.equal(keys.has('a'), false, 'the oldest keys were evicted under the cap');
});

test('createSeenKeys: a non-string/empty key is a no-op (an absent header never dedups)', () => {
  const keys = createSeenKeys({ now: () => 0 });
  keys.record('');
  keys.record(undefined);
  assert.equal(keys.has(''), false);
  assert.equal(keys.has(undefined), false);
  assert.equal(keys.snapshot().size, 0);
});
