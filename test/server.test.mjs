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
import { createRequestHandler, createRetentionTrigger, createRejectionTally, DEFAULT_MAX_EVENTS } from '../server.mjs';
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

test('all three GET read routes are served by the SAME createRequestHandler (no route drift between /summary and /capabilities)', async () => {
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => lines.map((l) => JSON.parse(l)),
  });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } });

  const capsRes = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/capabilities' }), capsRes);
  assert.equal(capsRes.statusCode, 200, 'GET /capabilities is wired through the handler');
  assert.equal(JSON.parse(capsRes.body).schemaVersion, SCHEMA_VERSION);

  const summaryRes = fakeRes();
  await handler(fakeReq({ method: 'GET', url: '/summary' }), summaryRes);
  assert.equal(summaryRes.statusCode, 200, 'GET /summary is still wired (not displaced)');

  const ingestRes = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), ingestRes);
  assert.equal(ingestRes.statusCode, 202, 'POST /ingest still routes through the handler');
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
  await handler(fakeReq({ headers: headersV1, body: validBody }), res);
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
    await handler(fakeReq({ headers: headersV1, body: validBody }), res);
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
  assert.deepEqual(body.schemaVersions, { '1': 2 });
  assert.equal(body.firstSeen, 300, 'firstSeen bounds the RETAINED window (100 was pruned)');
  assert.equal(body.lastSeen, 400);
});

test('retention: an absent retention dep leaves the handler unchanged (today behavior — no afterAppend call)', async () => {
  const captured = [];
  const store = createNdjsonStore({ sink: async (l) => void captured.push(l) });
  const handler = createRequestHandler({ store, schema: { SCHEMA_VERSION, validateEvent } }); // no retention
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), res);
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
  await handler(fakeReq({ headers: { 'x-telemetry-schema': '2' }, body: 'GARBAGE NOT JSON' }), res);
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
  const badBody = JSON.stringify({ schemaVersion: 1, events: [{ ...validError, runtime: 'worker' }] });
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: badBody }), res);
  assert.equal(res.statusCode, 422);

  const rej = await summaryRejections(handler);
  assert.ok(rej.byStatus['422'] >= 1);
  assert.equal(rej.lastStatus, 422);
});

test('a 400 rejection (malformed JSON body) is surfaced in GET /summary', async () => {
  const { handler } = wiringWithTally();
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: 'not json' }), res);
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
  await handler(fakeReq({ headers: headersV1, body: validBody }), res); // no bearer → 401 at the gate
  assert.equal(res.statusCode, 401);

  const rej = await summaryRejections(handler, { authorization: `Bearer ${TALLY_SECRET}` });
  assert.ok(rej.byStatus['401'] >= 1, 'the auth-gate 401 was recorded (not just ingest-result rejections)');
  assert.equal(rej.lastStatus, 401);
  assert.equal(rej.lastReason, 'unauthorized');
});

test('a successful ingest (202) does NOT increment rejections (accepted traffic is never counted as rejected)', async () => {
  const { handler } = wiringWithTally();
  const res = fakeRes();
  await handler(fakeReq({ headers: headersV1, body: validBody }), res);
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
  await handler(fakeReq({ headers: { 'x-telemetry-schema': '2' }, body: 'x' }), fakeRes()); // 415
  await handler(fakeReq({ headers: headersV1, body: 'not json' }), fakeRes()); // 400
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
