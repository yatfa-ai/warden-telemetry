// Events selector + GET /events handler tests (WARDEN-599). Mirrors the discipline
// of test/summary.test.mjs (the PURE helper exercised directly with plain arrays —
// ZERO real network, ZERO real filesystem) and test/server.test.mjs (the handler
// driven with fake req/res + an INJECTED capturing store — no socket, no disk).
// selectEvents is the full-fidelity sibling of summarize(): where summarize()
// reduces to aggregates, selectEvents returns the events themselves, bounded.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { selectEvents, EVENTS_LIMIT_DEFAULT, EVENTS_LIMIT_MAX } from '../events.mjs';
import { createRequestHandler } from '../server.mjs';
import { createNdjsonStore } from '../store.mjs';

// ── PURE HELPER: selectEvents(events, opts) ───────────────────────────────────

// Canonical valid events (verbatim shapes ingest persists — one per base type),
// carrying the diagnostic fields /summary drops (message+frames / reason / lagMs).
const validError = {
  schemaVersion: 1,
  type: 'error',
  runtime: 'main',
  timestamp: 100,
  name: 'TypeError',
  message: 'Cannot read properties of undefined',
  frames: [{ function: 'foo', file: 'app.js', line: 42 }],
};
const validCrash = {
  schemaVersion: 1,
  type: 'crash',
  runtime: 'renderer',
  timestamp: 200,
  reason: 'oom',
  exitCode: 133,
};
const validStall = {
  schemaVersion: 1,
  type: 'performance-stall',
  runtime: 'main',
  timestamp: 300,
  lagMs: 750,
  source: 'event-loop',
};

// ── EMPTY / DEFENSIVE ─────────────────────────────────────────────────────────

test('selectEvents: empty input → []', () => {
  assert.deepEqual(selectEvents([]), []);
});

test('selectEvents: non-array input is treated as empty (defensive — never throws)', () => {
  assert.deepEqual(selectEvents(undefined), []);
  assert.deepEqual(selectEvents(null), []);
  assert.deepEqual(selectEvents('nope'), []);
});

test('selectEvents: empty input yields [] regardless of filters/limit', () => {
  assert.deepEqual(selectEvents([], { type: 'error', limit: 5, since: 10 }), []);
});

// ── FULL FIDELITY (the whole point vs /summary) ───────────────────────────────

test('selectEvents: returns events at FULL FIDELITY — message + frames + reason + lagMs survive', () => {
  // /summary deliberately discards these; /events exists to surface them. Assert
  // the diagnostic fields are carried through verbatim, not reduced to aggregates.
  const out = selectEvents([validError, validCrash, validStall]);
  assert.equal(out.length, 3);
  assert.deepEqual(out[0], validError, 'error event keeps message + frames + name');
  assert.deepEqual(out[1], validCrash, 'crash event keeps reason');
  assert.deepEqual(out[2], validStall, 'stall event keeps lagMs + source');
});

test('selectEvents: returns event REFERENCES into the input (no defensive clone, like a pure selector)', () => {
  // A pure selector over an array returns the matching elements; it does not
  // serialize/reduce them. Pin that the returned objects ARE the input objects
  // (a maintainer drilling down reads exactly what landed, byte-for-byte).
  const out = selectEvents([validError]);
  assert.equal(out[0], validError);
});

// ── DEFAULT + HARD-CAPPED LIMIT ───────────────────────────────────────────────

test('selectEvents: default limit is the bounded EVENTS_LIMIT_DEFAULT', () => {
  assert.equal(EVENTS_LIMIT_DEFAULT, 100, 'the documented default');
  const events = Array.from({ length: 250 }, (_, i) => ({ type: 'error', timestamp: i }));
  // 250 events, no limit → newest 100 (the default).
  assert.equal(selectEvents(events).length, EVENTS_LIMIT_DEFAULT);
});

test('selectEvents: a below-cap limit is honored', () => {
  const events = Array.from({ length: 10 }, (_, i) => ({ type: 'error', timestamp: i }));
  assert.equal(selectEvents(events, { limit: 3 }).length, 3);
});

test('selectEvents: a limit above the hard cap is CLAMPED to EVENTS_LIMIT_MAX', () => {
  assert.equal(EVENTS_LIMIT_MAX, 200, 'the documented hard cap');
  const events = Array.from({ length: 500 }, (_, i) => ({ type: 'error', timestamp: i }));
  // 500 events, limit=50000 → the response is bounded to the cap, never 500.
  assert.equal(selectEvents(events, { limit: 50000 }).length, EVENTS_LIMIT_MAX);
  assert.ok(selectEvents(events, { limit: 50000 }).length <= EVENTS_LIMIT_MAX);
});

test('selectEvents: limit just over the cap clamps to the cap', () => {
  const events = Array.from({ length: 300 }, (_, i) => ({ type: 'error', timestamp: i }));
  assert.equal(selectEvents(events, { limit: 201 }).length, EVENTS_LIMIT_MAX);
});

test('selectEvents: a fractional limit is floored (then clamped)', () => {
  const events = Array.from({ length: 10 }, (_, i) => ({ type: 'error', timestamp: i }));
  assert.equal(selectEvents(events, { limit: 2.9 }).length, 2);
});

test('selectEvents: a SUB-1 fractional limit NEVER bypasses the hard cap (WARDEN-599 audit)', () => {
  // Regression for the load-bearing bound invariant — the ticket's single stated
  // risk mitigation ("an unbounded response bloating on a large store, mitigated
  // by the hard-capped limit"). A fractional limit in (0,1) floors to 0, and
  // `slice(-0)` === `slice(0)` returns the WHOLE array — which on a near-full
  // store would yield exactly the multi-MB response the cap exists to prevent.
  // The guard routes sub-1 fractions to the default instead. This test goes red
  // on the `limit > 0` guard (whole array returned) and green on `limit >= 1`.
  const events = Array.from({ length: 500 }, (_, i) => ({ type: 'error', timestamp: i }));
  for (const bad of [0.5, 0.9, 5e-1, 0.001, 0.9999]) {
    const out = selectEvents(events, { limit: bad });
    assert.ok(
      out.length <= EVENTS_LIMIT_MAX,
      `limit=${bad} must not exceed the hard cap (got ${out.length}) — slice(-0) bypass`
    );
    assert.equal(out.length, EVENTS_LIMIT_DEFAULT, `limit=${bad} falls back to the default`);
  }
});

test('selectEvents: a missing / non-finite / non-positive limit falls back to the default', () => {
  const events = Array.from({ length: 250 }, (_, i) => ({ type: 'error', timestamp: i }));
  assert.equal(selectEvents(events, { limit: undefined }).length, EVENTS_LIMIT_DEFAULT);
  assert.equal(selectEvents(events, { limit: NaN }).length, EVENTS_LIMIT_DEFAULT);
  assert.equal(selectEvents(events, { limit: 0 }).length, EVENTS_LIMIT_DEFAULT, 'limit=0 → default, not an empty page');
  assert.equal(selectEvents(events, { limit: -5 }).length, EVENTS_LIMIT_DEFAULT);
  assert.equal(selectEvents(events, { limit: 'huge' }).length, EVENTS_LIMIT_DEFAULT);
});

test('selectEvents: limit larger than the event count returns everything (no over-slicing)', () => {
  const events = [validError, validCrash];
  assert.equal(selectEvents(events, { limit: 999 }).length, 2);
});

// ── NEWEST-N ORDERING (store is append-ordered newest-last) ────────────────────

test('selectEvents: returns the NEWEST N (the last N in arrival order) — newest last', () => {
  // Arrival order is oldest→newest (append-ordered newest-last). The newest N are
  // the LAST N, returned in arrival order — so the newest event is the final one.
  const events = [
    { type: 'error', timestamp: 1, name: 'old' },
    { type: 'error', timestamp: 2, name: 'mid' },
    { type: 'error', timestamp: 3, name: 'new' },
  ];
  const out = selectEvents(events, { limit: 2 });
  assert.deepEqual(
    out.map((e) => e.name),
    ['mid', 'new'],
    'the 2 newest, in arrival order (newest last)'
  );
});

test('selectEvents: default window is also newest-N (the tail), not the head', () => {
  const events = Array.from({ length: EVENTS_LIMIT_DEFAULT + 50 }, (_, i) => ({
    type: 'error',
    timestamp: i,
  }));
  const out = selectEvents(events); // default limit
  assert.equal(out.length, EVENTS_LIMIT_DEFAULT);
  // newest-N = the tail; the oldest retained is the one just past the dropped head.
  assert.equal(out[0].timestamp, 50, 'the dropped head is the 50 oldest events');
  assert.equal(out[out.length - 1].timestamp, events.length - 1, 'newest is last');
});

// ── TYPE FILTER ───────────────────────────────────────────────────────────────

test('selectEvents: ?type= filters to a single base type', () => {
  const out = selectEvents([validError, validCrash, validStall], { type: 'performance-stall' });
  assert.equal(out.length, 1);
  assert.equal(out[0].type, 'performance-stall');
});

test('selectEvents: ?type=error keeps only errors', () => {
  const out = selectEvents([validError, validCrash, validStall, { ...validError, name: 'X' }], {
    type: 'error',
  });
  assert.equal(out.length, 2);
  assert.ok(out.every((e) => e.type === 'error'));
});

test('selectEvents: a non-matching type yields [] (the filter is exact, not "contains")', () => {
  assert.deepEqual(selectEvents([validError, validCrash], { type: 'performance-stall' }), []);
});

test('selectEvents: a non-string / empty type applies NO type filter', () => {
  const all = [validError, validCrash, validStall];
  assert.equal(selectEvents(all, { type: undefined }).length, 3);
  assert.equal(selectEvents(all, { type: '' }).length, 3);
  assert.equal(selectEvents(all, { type: 123 }).length, 3);
});

// ── SINCE FILTER (absolute epoch-ms cutoff) ───────────────────────────────────

test('selectEvents: ?since= keeps events with timestamp >= since (inclusive lower bound)', () => {
  const events = [
    { type: 'error', timestamp: 100 },
    { type: 'error', timestamp: 200 },
    { type: 'error', timestamp: 300 },
  ];
  const out = selectEvents(events, { since: 200 });
  assert.deepEqual(
    out.map((e) => e.timestamp),
    [200, 300],
    '200 is included (>=), 100 is dropped'
  );
});

test('selectEvents: ?since= later than every event yields []', () => {
  const events = [
    { type: 'error', timestamp: 100 },
    { type: 'error', timestamp: 200 },
  ];
  assert.deepEqual(selectEvents(events, { since: 201 }), []);
});

test('selectEvents: a non-finite since applies NO time filter', () => {
  const all = [validError, validCrash, validStall];
  assert.equal(selectEvents(all, { since: NaN }).length, 3);
  assert.equal(selectEvents(all, { since: undefined }).length, 3);
  assert.equal(selectEvents(all, { since: 'nope' }).length, 3);
});

test('selectEvents: since drops events without a finite timestamp (they do not satisfy the window)', () => {
  const events = [
    { type: 'error', timestamp: 300 },
    { type: 'error', timestamp: undefined }, // no timestamp
    { type: 'error', timestamp: Infinity }, // non-finite
  ];
  const out = selectEvents(events, { since: 0 });
  assert.equal(out.length, 1);
  assert.equal(out[0].timestamp, 300);
});

// ── SINCE FILTER — clock-skew robustness via receivedAt (WARDEN-692) ──────────
// The ?since cutoff keys off the RECEIVER's `receivedAt` (with a `timestamp`
// fallback), so "show me events since the deploy" is robust to skewed client
// clocks: an event the receiver saw after the cutoff is returned even if the
// client's own timestamp predates it.

test('selectEvents: ?since= keys off receivedAt — a skewed-old timestamp with a fresh receivedAt is KEPT', () => {
  // Client timestamp = 100 (before the cutoff); receivedAt = 300 (the receiver saw
  // it after the cutoff). Under timestamp-only keying this event would be DROPPED
  // (timestamp < since); receivedAt wins → it is kept.
  const events = [{ type: 'error', timestamp: 100, receivedAt: 300 }];
  const out = selectEvents(events, { since: 200 });
  assert.equal(out.length, 1);
  assert.equal(out[0].receivedAt, 300);
});

test('selectEvents: ?since= prefers receivedAt over an out-of-window timestamp', () => {
  // since=250: timestamp=100 is below it, receivedAt=300 is above it → kept.
  const out = selectEvents([{ type: 'error', timestamp: 100, receivedAt: 300 }], { since: 250 });
  assert.equal(out.length, 1);
  // ...but when receivedAt is below the cutoff too, the event is dropped (the
  // effective time — receivedAt — does not satisfy the window).
  assert.equal(selectEvents([{ type: 'error', timestamp: 100, receivedAt: 200 }], { since: 250 }).length, 0);
});

test('selectEvents: ?since= on events lacking receivedAt reads via the timestamp fallback (graceful backfill)', () => {
  // Pre-annotation events (no receivedAt): the client timestamp governs, unchanged.
  const events = [
    { type: 'error', timestamp: 100 },
    { type: 'error', timestamp: 300 },
  ];
  assert.deepEqual(
    selectEvents(events, { since: 200 }).map((e) => e.timestamp),
    [300]
  );
});

// ── COMBINED FILTERS (conjunctive) + newest-N ─────────────────────────────────

test('selectEvents: type + since are conjunctive, then newest-N is taken from the matches', () => {
  // Of the matches, the newest N are returned — not "newest N overall then filtered".
  const events = [
    { type: 'crash', timestamp: 50, reason: 'old-crash' },
    { type: 'error', timestamp: 100, name: 'e1' },
    { type: 'error', timestamp: 200, name: 'e2' },
    { type: 'error', timestamp: 300, name: 'e3' },
    { type: 'crash', timestamp: 400, reason: 'new-crash' },
  ];
  // type=error AND since=150 → [e2, e3]; newest 1 → [e3].
  const out = selectEvents(events, { type: 'error', since: 150, limit: 1 });
  assert.deepEqual(
    out.map((e) => e.name),
    ['e3']
  );
});

// ── SKIP-ROBUST (mirror summary.mjs) ──────────────────────────────────────────

test('selectEvents: malformed entries (null / primitives / non-objects) are skipped, not fatal', () => {
  const out = selectEvents([null, 'not-an-object', 42, undefined, validError, validCrash]);
  assert.equal(out.length, 2);
  assert.deepEqual(out[0], validError);
  assert.deepEqual(out[1], validCrash);
});

test('selectEvents: skip-robust applies even with no filters (non-objects never leak into the window)', () => {
  const out = selectEvents([null, 7, validError, 'x']);
  assert.equal(out.length, 1);
  assert.deepEqual(out[0], validError);
});

// ═══════════════════════════════════════════════════════════════════════════════
// ── HANDLER-LEVEL: GET /events via createRequestHandler ───────────────────────
// Drives the handler directly with fake req/res + an INJECTED capturing store
// (readableStore: an INJECTED source returns seeded events; writes go nowhere).
// Still ZERO real fs, ZERO real network — the same discipline as test/server.test.
// ═══════════════════════════════════════════════════════════════════════════════

// A fake IncomingMessage whose body chunks emit on nextTick (mimics a readable
// stream). GET reads no body, so the default empty body is fine.
function fakeReq({ method = 'GET', url = '/events', headers = {} } = {}) {
  const req = {
    method,
    url,
    headers,
    on() {
      return this;
    },
  };
  // readBody is never called on a GET, but keep the EventEmitter shape for safety.
  return req;
}

// A fake ServerResponse: captures status, headers, and the JSON payload passed to
// .end(), instead of writing to a real socket.
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

// A store pre-seeded with a known event mix via an INJECTED source (reads return
// the seeded events; writes go nowhere) — the readableStore pattern from
// server.test.mjs. structuredClone so a test can't mutate the shared fixtures.
function readableStore(events) {
  return createNdjsonStore({
    sink: async () => {},
    source: () => events.map((e) => structuredClone(e)),
  });
}

// ── DONE criterion: a persisted error event /summary only counted is returnable
// with its message + frames via GET /events. ───────────────────────────────────

test('GET /events: a persisted error event is returned with its message + frames (the DONE criterion)', async () => {
  // /summary would COUNT this error (1x TypeError) but DROP message+frames.
  // /events returns them — the drill-down that turns /summary's count into an
  // inspectable, act-on-able event.
  const store = readableStore([validError]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events' }), res);
  assert.equal(res.statusCode, 200);
  assert.equal(res.headers['content-type'], 'application/json');
  const body = JSON.parse(res.body);
  assert.equal(body.events.length, 1);
  assert.equal(body.events[0].name, 'TypeError');
  assert.equal(body.events[0].message, 'Cannot read properties of undefined', 'message survives (dropped by /summary)');
  assert.deepEqual(body.events[0].frames, [{ function: 'foo', file: 'app.js', line: 42 }], 'frames survive (dropped by /summary)');
});

// ── 200 + bounded shape ───────────────────────────────────────────────────────

test('GET /events: returns 200 + { events, total } over a pre-seeded store', async () => {
  const store = readableStore([validError, validCrash, validStall]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(Array.isArray(body.events), true);
  assert.equal(body.events.length, 3);
  assert.equal(body.total, 3, 'total is the full persisted count');
});

test('GET /events: total reflects the FULL persisted count, independent of the bound window', async () => {
  // Seed more than the default window; total is the whole set, events is the cap.
  const events = Array.from({ length: 250 }, (_, i) => ({ type: 'error', timestamp: i }));
  const store = readableStore(events);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events' }), res);
  const body = JSON.parse(res.body);
  assert.equal(body.total, 250, 'total = full persisted count (pre-bound)');
  assert.equal(body.events.length, EVENTS_LIMIT_DEFAULT, 'events = bounded window');
});

test('GET /events on an empty/missing store → 200 { events: [], total: 0 } (parity with /summary)', async () => {
  const store = readableStore([]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events' }), res);
  assert.equal(res.statusCode, 200);
  assert.deepEqual(JSON.parse(res.body), { events: [], total: 0 });
});

// ── ?limit= honored + capped at 200 ───────────────────────────────────────────

test('GET /events: ?limit= is honored', async () => {
  const events = Array.from({ length: 10 }, (_, i) => ({ type: 'error', timestamp: i }));
  const store = readableStore(events);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events?limit=3' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.events.length, 3, 'limit=3 bounds the window');
  assert.equal(body.total, 10, 'total still reflects the full persisted set');
  // newest 3 by arrival order: timestamps 7,8,9 (newest last).
  assert.deepEqual(
    body.events.map((e) => e.timestamp),
    [7, 8, 9]
  );
});

test('GET /events: ?limit=50000 is CAPPED at 200 (a near-full store cannot yield a multi-MB response)', async () => {
  const events = Array.from({ length: 500 }, (_, i) => ({ type: 'error', timestamp: i }));
  const store = readableStore(events);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events?limit=50000' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.events.length, EVENTS_LIMIT_MAX, 'hard-capped at 200');
  assert.ok(body.events.length <= EVENTS_LIMIT_MAX);
  assert.equal(body.total, 500);
});

test('GET /events: ?limit=0.5 NEVER bypasses the cap (WARDEN-599 audit — sub-1 fraction end-to-end)', async () => {
  // The handler parses ?limit= via Number(), so ?limit=0.5 reaches selectEvents
  // as the number 0.5 — the bypass class. The bound must hold end-to-end, not
  // just in the pure helper: a sub-1 fraction cannot return the whole store.
  const events = Array.from({ length: 500 }, (_, i) => ({ type: 'error', timestamp: i }));
  const store = readableStore(events);
  const handler = createRequestHandler({ store });
  for (const bad of ['0.5', '0.9', '5e-1', '0.001']) {
    const res = fakeRes();
    await handler(fakeReq({ url: `/events?limit=${bad}` }), res);
    assert.equal(res.statusCode, 200);
    const body = JSON.parse(res.body);
    assert.ok(
      body.events.length <= EVENTS_LIMIT_MAX,
      `?limit=${bad} must not exceed the hard cap (got ${body.events.length})`
    );
    assert.equal(body.events.length, EVENTS_LIMIT_DEFAULT, `?limit=${bad} falls back to the default`);
    assert.equal(body.total, 500, 'total is still the full persisted count');
  }
});

test('GET /events: ?limit=abc (non-numeric) falls back to the default window', async () => {
  const events = Array.from({ length: 250 }, (_, i) => ({ type: 'error', timestamp: i }));
  const store = readableStore(events);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events?limit=abc' }), res);
  assert.equal(JSON.parse(res.body).events.length, EVENTS_LIMIT_DEFAULT);
});

// ── ?type= filter ─────────────────────────────────────────────────────────────

test('GET /events: ?type=performance-stall filters to that type', async () => {
  const store = readableStore([validError, validCrash, validStall, { ...validStall, lagMs: 999 }]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events?type=performance-stall' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.events.length, 2);
  assert.ok(body.events.every((e) => e.type === 'performance-stall'));
  assert.equal(body.total, 4, 'total is the full set across all types');
});

// ── ?since= filter ────────────────────────────────────────────────────────────

test('GET /events: ?since= filters to events at/after the cutoff', async () => {
  const store = readableStore([
    { ...validError, timestamp: 1000 },
    { ...validCrash, timestamp: 2000 },
    { ...validStall, timestamp: 3000 },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events?since=2000' }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.deepEqual(
    body.events.map((e) => e.timestamp),
    [2000, 3000]
  );
});

test('GET /events: ?since= + ?type= compose (conjunctive)', async () => {
  const store = readableStore([
    { type: 'error', timestamp: 100 },
    { type: 'crash', timestamp: 200 },
    { type: 'error', timestamp: 300 },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events?type=error&since=200' }), res);
  const body = JSON.parse(res.body);
  assert.deepEqual(
    body.events.map((e) => e.timestamp),
    [300]
  );
});

// ── newest-N by arrival order (end-to-end through the handler) ────────────────

test('GET /events: returns the newest N by arrival order (newest last)', async () => {
  const store = readableStore([
    { type: 'error', timestamp: 1, name: 'old' },
    { type: 'error', timestamp: 2, name: 'mid' },
    { type: 'error', timestamp: 3, name: 'new' },
  ]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events?limit=2' }), res);
  const body = JSON.parse(res.body);
  assert.deepEqual(
    body.events.map((e) => e.name),
    ['mid', 'new'],
    'newest 2 in arrival order'
  );
});

// ── 404 fall-through preserved ────────────────────────────────────────────────

test('GET /events: an unknown path still 404s (the new branch does not swallow the fall-through)', async () => {
  const store = readableStore([validError]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/no-such-route' }), res);
  assert.equal(res.statusCode, 404);
});

test('GET /events: POST /events → 404 (only GET /events is wired; POST is not ingest)', async () => {
  const store = readableStore([validError]);
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ method: 'POST', url: '/events' }), res);
  assert.equal(res.statusCode, 404);
});

// ── AUTH GATE INHERITED (mirror auth.test.mjs:252-259) ─────────────────────────
// The auth gate runs BEFORE routing, so GET /events inherits it with zero new
// auth code. A no-token request with AUTH_TOKEN set → 401, and readEvents() NEVER
// runs (no full-fidelity path is exercised on a reject). With a valid token → 200.

const SECRET = 'events-shared-token';

test('GET /events: AUTH_TOKEN set + no token → 401, readEvents NEVER runs (the gate is inherited)', async () => {
  let reads = 0;
  const store = { readEvents: () => { reads += 1; return []; } };
  const handler = createRequestHandler({ store, authToken: SECRET });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events' }), res);
  assert.equal(res.statusCode, 401, 'the read surface is rejected before routing');
  assert.equal(reads, 0, 'readEvents never ran → no full-fidelity path exercised on a reject');
});

test('GET /events: AUTH_TOKEN set + valid token → 200 (authenticated read proceeds past the gate)', async () => {
  const store = { readEvents: () => [validError] };
  const handler = createRequestHandler({ store, authToken: SECRET });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events', headers: { authorization: `Bearer ${SECRET}` } }), res);
  assert.equal(res.statusCode, 200);
  const body = JSON.parse(res.body);
  assert.equal(body.events.length, 1);
  assert.equal(body.events[0].message, 'Cannot read properties of undefined');
});

test('GET /events: with no AUTH_TOKEN, the route is OPEN (parity with /summary)', async () => {
  const store = readableStore([validError]);
  const handler = createRequestHandler({ store }); // no authToken
  const res = fakeRes();
  await handler(fakeReq({ url: '/events' }), res);
  assert.equal(res.statusCode, 200);
});

// ── write-only (source-less) store → 500, not a crash (parity with /summary) ──

test('GET /events on a write-only (source-less) store → 500, not a crash', async () => {
  // A store wired without a source has a readEvents() that throws loud. The handler
  // must surface that as a clean 500, never let it kill the server process.
  const store = createNdjsonStore({ sink: async () => {} }); // no source
  const handler = createRequestHandler({ store });
  const res = fakeRes();
  await handler(fakeReq({ url: '/events' }), res);
  assert.equal(res.statusCode, 500);
  assert.match(JSON.parse(res.body).error, /source|events/);
});
