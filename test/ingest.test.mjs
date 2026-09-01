// Ingest pipeline tests (WARDEN-547). Exercises the pure `ingest({ headers, body }, deps)`
// seam directly — ZERO real network, a capturing in-memory store that touches ZERO
// real filesystem. Covers every success criterion: accept+persist a valid batch,
// reject an unknown schema version without parsing the body, and hard-reject an
// out-of-schema event with nothing persisted.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ingest } from '../ingest.mjs';
import { SCHEMA_VERSION, validateEvent } from '../schema.ts';

// A capturing in-memory store: records exactly what would have been persisted.
// Clones each event so the test asserts on a snapshot, not a live reference.
function memoryStore() {
  const appended = [];
  return {
    appended,
    async appendEvents(events) {
      for (const event of events) appended.push(structuredClone(event));
    },
  };
}

// A fixed server clock so the receivedAt stamp (WARDEN-692) is deterministic and
// assertable — ingest() stamps every accepted event with `now()` once per batch
// (the batch arrived at one instant). Mirrors the fake-clock discipline of
// summarizeTimeline({ now }) and applyRetention({ now }).
const RECEIVED_AT = 5_000_000;
const fakeNow = () => RECEIVED_AT;

const deps = (store) => ({ SCHEMA_VERSION, validateEvent, store, now: fakeNow });

// A capturing seen-key set double for the ingest dedup dep (WARDEN-666). Mirrors
// createSeenKeys' {has, record} contract so ingest() is tested in isolation from
// server.mjs — the same discipline as memoryStore() standing in for the file
// store. `recorded` exposes what was recorded, in order, for assertions.
function seenKeysSet() {
  const recorded = [];
  const set = new Set();
  return {
    recorded,
    has(key) {
      return set.has(key);
    },
    record(key) {
      set.add(key);
      recorded.push(key);
    },
  };
}

// Canonical valid events (one per base-tier type).
const validError = {
  schemaVersion: SCHEMA_VERSION,
  type: 'error',
  runtime: 'main',
  timestamp: 123,
  name: 'TypeError',
  message: 'boom',
  frames: [],
};
const validCrash = {
  schemaVersion: SCHEMA_VERSION,
  type: 'crash',
  runtime: 'renderer',
  timestamp: 9,
  reason: 'oom',
  exitCode: 133,
};
const validStall = {
  schemaVersion: SCHEMA_VERSION,
  type: 'performance-stall',
  runtime: 'main',
  timestamp: 3,
  lagMs: 750,
  source: 'event-loop',
};

const bodyOf = (events) => JSON.stringify({ schemaVersion: SCHEMA_VERSION, events });
// WARDEN-1258 — the aggregate usage event. Valid shape per the v5 schema:
// folded counts / ok-fail / min-avg-max / boundary-keyed histograms, with
// kebab-case operation literals (a path or hostname riding the aggregate key
// is out-of-schema by construction).
const validMetrics = {
  schemaVersion: SCHEMA_VERSION,
  type: 'operational-metrics',
  runtime: 'main',
  timestamp: 4,
  windowStartedAt: 1,
  windowEndedAt: 4,
  boundaries: [50, 100, 250, 500, 1000, 2500, 5000, 10000],
  operations: [
    { operation: 'file-exists-local', count: 2, okCount: 1, failCount: 1, min: 0.5, avg: 1, max: 1.5, buckets: [2, 0, 0, 0, 0, 0, 0, 0, 0] },
    { operation: 'file-exists-remote', count: 1, okCount: 1, failCount: 0, min: 300, avg: 300, max: 300, buckets: [0, 0, 1, 0, 0, 0, 0, 0, 0] },
    { operation: 'file-exists-cache-hit', count: 3, okCount: 3, failCount: 0, min: 0, avg: 0, max: 0, buckets: [3, 0, 0, 0, 0, 0, 0, 0, 0] },
  ],
  rejected: 0,
};

const schemaHeaders = { 'x-telemetry-schema': String(SCHEMA_VERSION) };

// ── SUCCESS CRITERION 1: accept a schemaVersion-matched batch + durably persist ─

test('accepts a schemaVersion-matched batch of valid events → 2xx and durably persists them', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([validError, validCrash, validStall]) },
    deps(store)
  );
  assert.equal(res.ok, true);
  assert.ok(res.status >= 200 && res.status <= 299, `status is 2xx (got ${res.status})`);
  assert.equal(res.body.accepted, 3);
  // Readable back via the store, in order. Each carries the receiver's receivedAt
  // stamp (WARDEN-692) alongside the verbatim client payload.
  assert.equal(store.appended.length, 3);
  assert.deepEqual(store.appended[0], { ...validError, receivedAt: RECEIVED_AT });
  assert.deepEqual(store.appended[1], { ...validCrash, receivedAt: RECEIVED_AT });
  assert.deepEqual(store.appended[2], { ...validStall, receivedAt: RECEIVED_AT });
});

test('accepts an operational-metrics event (WARDEN-1258 aggregate usage window)', async () => {
  const store = memoryStore();
  const res = await ingest({ headers: schemaHeaders, body: bodyOf([validMetrics]) }, deps(store));
  assert.equal(res.ok, true);
  assert.equal(res.body.accepted, 1);
  assert.deepEqual(store.appended[0], { ...validMetrics, receivedAt: RECEIVED_AT });
});

test('hard-rejects an operational-metrics event whose operation key carries a path (hard exclusion is structural)', async () => {
  const store = memoryStore();
  const hostile = structuredClone(validMetrics);
  hostile.operations[0].operation = '/etc/passwd';
  const res = await ingest({ headers: schemaHeaders, body: bodyOf([hostile]) }, deps(store));
  assert.equal(res.ok, false);
  assert.ok(res.status === 422 || res.status === 400, 'non-retryable 4xx');
  assert.equal(store.appended.length, 0, 'nothing persisted');
});

test('persists each event as a separate record (one per accepted event)', async () => {
  const store = memoryStore();
  await ingest({ headers: schemaHeaders, body: bodyOf([validError, validStall]) }, deps(store));
  assert.equal(store.appended.length, 2);
});

test('accepts an empty events batch with 2xx and persists nothing', async () => {
  const store = memoryStore();
  const res = await ingest({ headers: schemaHeaders, body: bodyOf([]) }, deps(store));
  assert.equal(res.ok, true);
  assert.equal(res.body.accepted, 0);
  assert.equal(store.appended.length, 0);
});

// ── SUCCESS CRITERION 2: unknown x-telemetry-schema → reject WITHOUT body parse ─

test('rejects an unknown x-telemetry-schema version WITHOUT parsing the body or persisting', async () => {
  const store = memoryStore();
  // A body that is not even valid JSON — proving we never parsed it.
  const res = await ingest(
    { headers: { 'x-telemetry-schema': String(SCHEMA_VERSION + 1) }, body: '}{ not json at all' },
    deps(store)
  );
  assert.equal(res.ok, false);
  assert.ok(res.status >= 400 && res.status <= 499, 'non-retryable 4xx');
  assert.notEqual(res.status, 429, 'never 429 (retryable)');
  assert.equal(store.appended.length, 0, 'nothing persisted');
});

test('rejects a MISSING x-telemetry-schema header the same way (no parse, no persist)', async () => {
  const store = memoryStore();
  const res = await ingest({ headers: {}, body: bodyOf([validError]) }, deps(store));
  assert.equal(res.ok, false);
  assert.ok(res.status >= 400 && res.status <= 499);
  assert.equal(store.appended.length, 0);
});

// ── 415 carries the DECLARED version structurally (WARDEN-761) ────────────────
// The declared version rides on `body.declaredVersion` — NOT embedded only in the
// reason string — so the receiver buckets drift without parsing the diagnostic.

test('a 415 carries body.declaredVersion equal to the raw header value (structural, not string-parsed)', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: { 'x-telemetry-schema': String(SCHEMA_VERSION + 1) }, body: '}{ not json at all' },
    deps(store)
  );
  assert.equal(res.ok, false);
  assert.equal(res.status, 415);
  assert.equal(res.body.declaredVersion, String(SCHEMA_VERSION + 1), 'declaredVersion is the raw header value');
});

test('a 415 for a non-numeric/scanner header value carries it verbatim (no validation drops real drift signal)', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: { 'x-telemetry-schema': 'abc' }, body: '}{ not json' },
    deps(store)
  );
  assert.equal(res.status, 415);
  assert.equal(res.body.declaredVersion, 'abc', 'a scanner value is carried through untouched');
});

test('a 415 for a MISSING header carries declaredVersion undefined (absent → no bucket downstream)', async () => {
  const store = memoryStore();
  const res = await ingest({ headers: {}, body: bodyOf([validError]) }, deps(store));
  assert.equal(res.status, 415);
  assert.equal(res.body.declaredVersion, undefined, 'a missing header yields no declaredVersion');
});

test('a NON-415 rejection does NOT carry declaredVersion (the field is 415-handshake-only)', async () => {
  // 400 — malformed JSON (passes the handshake because the header matches).
  const storeA = memoryStore();
  const r400 = await ingest({ headers: schemaHeaders, body: 'not json' }, deps(storeA));
  assert.equal(r400.status, 400);
  assert.equal(r400.body.declaredVersion, undefined, 'a 400 carries no declaredVersion');

  // 422 — an out-of-schema event (passes the handshake + parse, fails validation).
  const storeB = memoryStore();
  const r422 = await ingest(
    { headers: schemaHeaders, body: bodyOf([{ ...validError, runtime: 'worker' }]) },
    deps(storeB)
  );
  assert.equal(r422.status, 422);
  assert.equal(r422.body.declaredVersion, undefined, 'a 422 carries no declaredVersion');
});

// ── SUCCESS CRITERION 3: out-of-schema event → hard-reject, nothing persisted ───

test('hard-rejects a batch containing an out-of-schema event (bad runtime) and persists NOTHING', async () => {
  const store = memoryStore();
  const badRuntime = { ...validError, runtime: 'worker' };
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([validError, badRuntime, validStall]) },
    deps(store)
  );
  assert.equal(res.ok, false);
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0, 'whole batch rejected — nothing persisted');
});

test('hard-rejects an out-of-schema event with a non-finite timestamp', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([{ ...validError, timestamp: NaN }]) },
    deps(store)
  );
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0);
});

test('hard-rejects an out-of-schema event with a wrong type', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([{ ...validError, type: 'bogus' }]) },
    deps(store)
  );
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0);
});

test('hard-rejects an out-of-schema event with a non-string extended field', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([{ ...validError, chatName: 42 }]) },
    deps(store)
  );
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0);
});

test('hard-rejects an event whose own schemaVersion disagrees with the header', async () => {
  const store = memoryStore();
  // header declares the current version, but the event carries schemaVersion 999 — validateEvent catches it
  const res = await ingest(
    {
      headers: schemaHeaders,
      body: JSON.stringify({
        schemaVersion: SCHEMA_VERSION,
        events: [{ schemaVersion: 999, type: 'error', runtime: 'main', timestamp: 1, name: 'E', message: 'm', frames: [] }],
      }),
    },
    deps(store)
  );
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0);
});

// ── Body shape / parse rejections ─────────────────────────────────────────────

test('rejects a malformed JSON body with a non-retryable 4xx', async () => {
  const store = memoryStore();
  const res = await ingest({ headers: schemaHeaders, body: 'not json' }, deps(store));
  assert.equal(res.status, 400);
  assert.equal(store.appended.length, 0);
});

test('rejects a body whose `events` is not an array', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: schemaHeaders, body: JSON.stringify({ schemaVersion: SCHEMA_VERSION, events: 'nope' }) },
    deps(store)
  );
  assert.equal(res.status, 400);
  assert.equal(store.appended.length, 0);
});

test('rejects a non-object JSON body', async () => {
  const store = memoryStore();
  const res = await ingest({ headers: schemaHeaders, body: '42' }, deps(store));
  assert.equal(res.status, 400);
});

// ── Atomicity + discipline ────────────────────────────────────────────────────

test('validates EVERY event before persisting any (a bad event never reaches the store)', async () => {
  let persisted = 0;
  const countingStore = {
    async appendEvents(events) {
      persisted += events.length;
    },
  };
  // first event valid, second invalid → store must never be called
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([validError, { ...validStall, source: 'gpu' }]) },
    { SCHEMA_VERSION, validateEvent, store: countingStore }
  );
  assert.equal(res.ok, false);
  assert.equal(persisted, 0, 'store never reached when any event is invalid');
});

test('header lookup is case-insensitive (robust to proxy/transport casing)', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: { 'X-Telemetry-Schema': String(SCHEMA_VERSION) }, body: bodyOf([validError]) },
    deps(store)
  );
  assert.equal(res.ok, true);
  assert.equal(store.appended.length, 1);
});

test('EVERY rejection is a non-retryable 4xx — the client drops, never retries', async () => {
  const store = memoryStore();
  const badCases = [
    { headers: { 'x-telemetry-schema': String(SCHEMA_VERSION + 1) }, body: 'x' }, // unknown schema
    { headers: {}, body: bodyOf([validError]) }, // missing schema header
    { headers: schemaHeaders, body: 'x' }, // malformed JSON
    { headers: schemaHeaders, body: '{"events":1}' }, // bad shape
    { headers: schemaHeaders, body: bodyOf([{ ...validError, type: 'bogus' }]) }, // invalid event
  ];
  for (const c of badCases) {
    const res = await ingest(c, deps(store));
    assert.ok(res.status >= 400 && res.status <= 499, `status ${res.status} is 4xx`);
    assert.notEqual(res.status, 429, 'never 429 (the client would retry that)');
    assert.ok(res.status < 500, 'never 5xx (the client would retry that)');
  }
});

// ── IDEMPOTENT INGEST / DEDUP (WARDEN-666) ────────────────────────────────────
// A retried batch whose 2xx was lost would otherwise be persisted AGAIN (the
// client reuses the identical bytes). With a seenKeys dep wired, ingest() remembers
// a batch's idempotency-key after accepting it and short-circuits a retry to 202
// {accepted:0, deduped:true} WITHOUT re-persisting. These exercise the pure
// ingest() seam with a capturing seenKeys double + memoryStore.

test('with seenKeys: the SAME idempotency-key posted twice persists ONE copy; the second is a dedup 202 that never reaches the store', async () => {
  const store = memoryStore();
  const keys = seenKeysSet();
  const headers = { ...schemaHeaders, 'idempotency-key': 'dup-key-1' };

  const first = await ingest({ headers, body: bodyOf([validError, validCrash]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys, now: fakeNow });
  const second = await ingest({ headers, body: bodyOf([validError, validCrash]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys, now: fakeNow });

  // First: accepted + persisted normally.
  assert.equal(first.ok, true);
  assert.equal(first.status, 202);
  assert.equal(first.body.accepted, 2);
  // Second: a 202 SUCCESS (so the client stops retrying) but deduped — nothing appended.
  assert.equal(second.ok, true);
  assert.equal(second.status, 202);
  assert.equal(second.body.accepted, 0);
  assert.equal(second.body.deduped, true, 'the retry is marked deduped');
  // The store holds ONE copy — exactly the events of the first (accepted) batch.
  assert.equal(store.appended.length, 2, 'appendEvents ran once — the retry did not double-count');
  assert.deepEqual(store.appended[0], { ...validError, receivedAt: RECEIVED_AT });
  assert.deepEqual(store.appended[1], { ...validCrash, receivedAt: RECEIVED_AT });
  // The key was recorded exactly once (on the first accept), proving the retry did
  // not re-record (which would be harmless but confirms the HIT path skips record).
  assert.deepEqual(keys.recorded, ['dup-key-1']);
});

test('with seenKeys: DIFFERENT idempotency-keys both persist (two distinct batches)', async () => {
  const store = memoryStore();
  const keys = seenKeysSet();

  const a = await ingest({ headers: { ...schemaHeaders, 'idempotency-key': 'key-A' }, body: bodyOf([validError]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys });
  const b = await ingest({ headers: { ...schemaHeaders, 'idempotency-key': 'key-B' }, body: bodyOf([validCrash]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys });

  assert.equal(a.body.accepted, 1);
  assert.equal(b.body.accepted, 1);
  assert.equal(b.body.deduped, undefined, 'a distinct key is never deduped');
  assert.equal(store.appended.length, 2, 'both batches persisted — distinct keys are distinct batches');
  assert.deepEqual(keys.recorded, ['key-A', 'key-B']);
});

test('a missing idempotency-key header is NEVER deduped (old client — unchanged) even with seenKeys wired', async () => {
  const store = memoryStore();
  const keys = seenKeysSet();
  // Two posts with NO idempotency-key header: both persist, neither deduped.
  const first = await ingest({ headers: schemaHeaders, body: bodyOf([validError]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys });
  const second = await ingest({ headers: schemaHeaders, body: bodyOf([validError]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys });
  assert.equal(first.body.accepted, 1);
  assert.equal(second.body.accepted, 1);
  assert.equal(second.body.deduped, undefined);
  assert.equal(store.appended.length, 2, 'no key header → no dedup → both stored (today behavior)');
  assert.equal(keys.recorded.length, 0, 'nothing recorded when there is no key');
});

test('seenKeys omitted entirely → repeated key is NOT deduped (backward-compatible with an unwired receiver)', async () => {
  const store = memoryStore();
  const headers = { ...schemaHeaders, 'idempotency-key': 'dup-key-2' };
  // No seenKeys in deps at all — exactly today's behavior: the key header is ignored.
  const first = await ingest({ headers, body: bodyOf([validError]) }, deps(store));
  const second = await ingest({ headers, body: bodyOf([validError]) }, deps(store));
  assert.equal(first.body.accepted, 1);
  assert.equal(second.body.accepted, 1);
  assert.equal(second.body.deduped, undefined);
  assert.equal(store.appended.length, 2, 'no seenKeys dep → no dedup → the batch is stored twice');
});

test('a REJECTED batch does not record its key — only an accepted batch is cached (record-on-success)', async () => {
  const store = memoryStore();
  const keys = seenKeysSet();
  const headers = { ...schemaHeaders, 'idempotency-key': 'rejected-key' };
  // An out-of-schema event → 422. The key must NOT be recorded (the batch was not
  // accepted), so a second post with the same key is RE-PROCESSED (re-rejected),
  // never silently deduped. This proves dedup only ever short-circuits batches the
  // receiver actually persisted.
  const first = await ingest({ headers, body: bodyOf([{ ...validError, type: 'bogus' }]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys });
  const second = await ingest({ headers, body: bodyOf([{ ...validError, type: 'bogus' }]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys });
  assert.equal(first.status, 422);
  assert.equal(second.status, 422, 'the retry is re-validated + re-rejected, not deduped');
  assert.equal(keys.recorded.length, 0, 'a rejected batch never poisons the seen-key set');
  assert.equal(store.appended.length, 0);
});

test('dedup never relaxes validation: an out-of-schema event with an already-seen key is still 422 (validation runs before the dedup HIT)', async () => {
  const store = memoryStore();
  const keys = seenKeysSet();
  // Prime the set with a key via a VALID batch (so the key is "seen")...
  await ingest({ headers: { ...schemaHeaders, 'idempotency-key': 'prime' }, body: bodyOf([validError]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys });
  assert.equal(keys.has('prime'), true);
  // ...then POST a body that is INVALID under the SAME key. Validation must run and
  // 422 it — the dedup HIT (which would skip persist) sits AFTER validate, so a bad
  // body is never accepted just because its key was seen.
  const res = await ingest({ headers: { ...schemaHeaders, 'idempotency-key': 'prime' }, body: bodyOf([{ ...validError, runtime: 'worker' }]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys });
  assert.equal(res.status, 422, 'validate runs before dedup — an invalid body is rejected regardless of the key');
  assert.equal(store.appended.length, 1, 'only the valid prime batch is stored');
});

// ── SERVER RECEIPT STAMP — receivedAt (WARDEN-692) ────────────────────────────
// ingest() stamps each ACCEPTED event with the receiver's own `receivedAt`
// (when IT saw the batch) AFTER the dedup-HIT early-return and BEFORE persist, so
// the three time-sensitive read surfaces can prefer the receiver's clock over the
// client's (robust to skewed client clocks). The stamp is receiver-owned metadata
// added after the client's redacted payload lands — it changes nothing about what
// the client sends or what consent covers.

test('stamps every accepted event with a finite receivedAt epoch-ms (one `now()` read shared across the batch)', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([validError, validCrash, validStall]) },
    deps(store)
  );
  assert.equal(res.ok, true);
  assert.equal(store.appended.length, 3);
  // Every persisted event carries the stamp...
  for (const e of store.appended) {
    assert.equal(Number.isFinite(e.receivedAt), true, 'receivedAt is a finite epoch-ms');
  }
  // ...and the batch arrived at one instant, so one `now()` stamps them identically.
  assert.deepEqual(
    store.appended.map((e) => e.receivedAt),
    [RECEIVED_AT, RECEIVED_AT, RECEIVED_AT],
    'a single now() read stamps the whole batch'
  );
});

test('receivedAt is stamped AFTER validate — a 422 rejection persists (and stamps) NOTHING', async () => {
  const store = memoryStore();
  // An out-of-schema event → 422 before the stamp loop runs; nothing is persisted,
  // so nothing carries a receivedAt.
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([{ ...validError, type: 'bogus' }]) },
    deps(store)
  );
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0, 'a rejected batch stamps + persists nothing');
});

test('a default `now` (Date.now) stamps a finite receivedAt when no clock is injected', async () => {
  const store = memoryStore();
  // No `now` in the deps object — ingest() falls back to its Date.now default, so a
  // production wiring (or any caller) that omits the clock still stamps every event.
  const res = await ingest(
    { headers: schemaHeaders, body: bodyOf([validError]) },
    { SCHEMA_VERSION, validateEvent, store }
  );
  assert.equal(res.ok, true);
  assert.equal(store.appended.length, 1);
  assert.equal(Number.isFinite(store.appended[0].receivedAt), true, 'the default clock still stamps receivedAt');
});

test('a dedup HIT stamps NOTHING — the 202 {accepted:0,deduped:true} path persists and stamps no receivedAt (ordering refinement)', async () => {
  // The stamp loop sits AFTER the dedup-HIT early-return (WARDEN-692 ordering
  // refinement): a retried batch whose 2xx was lost returns 202 {accepted:0,
  // deduped:true} WITHOUT persisting, so it stamps nothing. Pass a DISTINCT now to
  // the retry to prove its clock is never read — the persisted event keeps the
  // FIRST (accepted) call's receivedAt.
  const store = memoryStore();
  const keys = seenKeysSet();
  const headers = { ...schemaHeaders, 'idempotency-key': 'stamp-dedup' };
  const first = await ingest({ headers, body: bodyOf([validError]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys, now: fakeNow });
  const second = await ingest({ headers, body: bodyOf([validError]) }, { SCHEMA_VERSION, validateEvent, store, seenKeys: keys, now: () => RECEIVED_AT + 999 });
  assert.equal(first.body.accepted, 1);
  assert.equal(second.body.accepted, 0);
  assert.equal(second.body.deduped, true);
  assert.equal(store.appended.length, 1, 'the retry did not double-count');
  assert.deepEqual(store.appended[0], { ...validError, receivedAt: RECEIVED_AT }, 'only the first call stamped; the dedup HIT stamped nothing');
});
