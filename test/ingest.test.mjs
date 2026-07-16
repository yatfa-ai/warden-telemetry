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

const deps = (store) => ({ SCHEMA_VERSION, validateEvent, store });

// Canonical valid events (one per base-tier type).
const validError = {
  schemaVersion: 1,
  type: 'error',
  runtime: 'main',
  timestamp: 123,
  name: 'TypeError',
  message: 'boom',
  frames: [],
};
const validCrash = {
  schemaVersion: 1,
  type: 'crash',
  runtime: 'renderer',
  timestamp: 9,
  reason: 'oom',
  exitCode: 133,
};
const validStall = {
  schemaVersion: 1,
  type: 'performance-stall',
  runtime: 'main',
  timestamp: 3,
  lagMs: 750,
  source: 'event-loop',
};

const bodyOf = (events) => JSON.stringify({ schemaVersion: 1, events });
const headersV1 = { 'x-telemetry-schema': '1' };

// ── SUCCESS CRITERION 1: accept a schemaVersion:1 batch + durably persist ─────

test('accepts a schemaVersion:1 batch of valid events → 2xx and durably persists them', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: headersV1, body: bodyOf([validError, validCrash, validStall]) },
    deps(store)
  );
  assert.equal(res.ok, true);
  assert.ok(res.status >= 200 && res.status <= 299, `status is 2xx (got ${res.status})`);
  assert.equal(res.body.accepted, 3);
  // Readable back via the store, verbatim, in order.
  assert.equal(store.appended.length, 3);
  assert.deepEqual(store.appended[0], validError);
  assert.deepEqual(store.appended[1], validCrash);
  assert.deepEqual(store.appended[2], validStall);
});

test('persists each event as a separate record (one per accepted event)', async () => {
  const store = memoryStore();
  await ingest({ headers: headersV1, body: bodyOf([validError, validStall]) }, deps(store));
  assert.equal(store.appended.length, 2);
});

test('accepts an empty events batch with 2xx and persists nothing', async () => {
  const store = memoryStore();
  const res = await ingest({ headers: headersV1, body: bodyOf([]) }, deps(store));
  assert.equal(res.ok, true);
  assert.equal(res.body.accepted, 0);
  assert.equal(store.appended.length, 0);
});

// ── SUCCESS CRITERION 2: unknown x-telemetry-schema → reject WITHOUT body parse ─

test('rejects an unknown x-telemetry-schema version WITHOUT parsing the body or persisting', async () => {
  const store = memoryStore();
  // A body that is not even valid JSON — proving we never parsed it.
  const res = await ingest(
    { headers: { 'x-telemetry-schema': '2' }, body: '}{ not json at all' },
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

// ── SUCCESS CRITERION 3: out-of-schema event → hard-reject, nothing persisted ───

test('hard-rejects a batch containing an out-of-schema event (bad runtime) and persists NOTHING', async () => {
  const store = memoryStore();
  const badRuntime = { ...validError, runtime: 'worker' };
  const res = await ingest(
    { headers: headersV1, body: bodyOf([validError, badRuntime, validStall]) },
    deps(store)
  );
  assert.equal(res.ok, false);
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0, 'whole batch rejected — nothing persisted');
});

test('hard-rejects an out-of-schema event with a non-finite timestamp', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: headersV1, body: bodyOf([{ ...validError, timestamp: NaN }]) },
    deps(store)
  );
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0);
});

test('hard-rejects an out-of-schema event with a wrong type', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: headersV1, body: bodyOf([{ ...validError, type: 'bogus' }]) },
    deps(store)
  );
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0);
});

test('hard-rejects an out-of-schema event with a non-string extended field', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: headersV1, body: bodyOf([{ ...validError, chatName: 42 }]) },
    deps(store)
  );
  assert.equal(res.status, 422);
  assert.equal(store.appended.length, 0);
});

test('hard-rejects an event whose own schemaVersion disagrees with the header', async () => {
  const store = memoryStore();
  // header says 1, but the event carries schemaVersion 999 — validateEvent catches it
  const res = await ingest(
    {
      headers: headersV1,
      body: JSON.stringify({
        schemaVersion: 1,
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
  const res = await ingest({ headers: headersV1, body: 'not json' }, deps(store));
  assert.equal(res.status, 400);
  assert.equal(store.appended.length, 0);
});

test('rejects a body whose `events` is not an array', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: headersV1, body: JSON.stringify({ schemaVersion: 1, events: 'nope' }) },
    deps(store)
  );
  assert.equal(res.status, 400);
  assert.equal(store.appended.length, 0);
});

test('rejects a non-object JSON body', async () => {
  const store = memoryStore();
  const res = await ingest({ headers: headersV1, body: '42' }, deps(store));
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
    { headers: headersV1, body: bodyOf([validError, { ...validStall, source: 'gpu' }]) },
    { SCHEMA_VERSION, validateEvent, store: countingStore }
  );
  assert.equal(res.ok, false);
  assert.equal(persisted, 0, 'store never reached when any event is invalid');
});

test('header lookup is case-insensitive (robust to proxy/transport casing)', async () => {
  const store = memoryStore();
  const res = await ingest(
    { headers: { 'X-Telemetry-Schema': '1' }, body: bodyOf([validError]) },
    deps(store)
  );
  assert.equal(res.ok, true);
  assert.equal(store.appended.length, 1);
});

test('EVERY rejection is a non-retryable 4xx — the client drops, never retries', async () => {
  const store = memoryStore();
  const badCases = [
    { headers: { 'x-telemetry-schema': '2' }, body: 'x' }, // unknown schema
    { headers: {}, body: bodyOf([validError]) }, // missing schema header
    { headers: headersV1, body: 'x' }, // malformed JSON
    { headers: headersV1, body: '{"events":1}' }, // bad shape
    { headers: headersV1, body: bodyOf([{ ...validError, type: 'bogus' }]) }, // invalid event
  ];
  for (const c of badCases) {
    const res = await ingest(c, deps(store));
    assert.ok(res.status >= 400 && res.status <= 499, `status ${res.status} is 4xx`);
    assert.notEqual(res.status, 429, 'never 429 (the client would retry that)');
    assert.ok(res.status < 500, 'never 5xx (the client would retry that)');
  }
});
