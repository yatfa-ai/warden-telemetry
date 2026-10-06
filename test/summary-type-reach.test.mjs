// typeReach axis tests (WARDEN-1594). Pure `summarize()` seam plus one handler
// test for filter-consistency with `schemaVersions`.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { summarize, TYPE_INTRODUCED_IN_SCHEMA_VERSION } from '../summary.mjs';
import { BASE_EVENT_TYPES } from '../schema.ts';
import { createRequestHandler } from '../server.mjs';
import { createNdjsonStore } from '../store.mjs';

const ev = (type, schemaVersion, extra = {}) => ({ schemaVersion, type, runtime: 'main', timestamp: 1, ...extra });
const many = (n, make) => Array.from({ length: n }, make);

test('GUARD: every BASE_EVENT_TYPES member has an introduced-in entry (and no stale extras)', () => {
  for (const t of BASE_EVENT_TYPES) {
    assert.equal(typeof TYPE_INTRODUCED_IN_SCHEMA_VERSION[t], 'number', `${t} lacks an introduced-in schema version — add it to TYPE_INTRODUCED_IN_SCHEMA_VERSION in summary.mjs`);
    assert.ok(Number.isInteger(TYPE_INTRODUCED_IN_SCHEMA_VERSION[t]) && TYPE_INTRODUCED_IN_SCHEMA_VERSION[t] >= 1);
  }
  assert.deepEqual(Object.keys(TYPE_INTRODUCED_IN_SCHEMA_VERSION).sort(), [...BASE_EVENT_TYPES].sort());
});

test('typeReach has exactly one row per BASE_EVENT_TYPES member and emitted === byType', () => {
  const s = summarize([ev('error', 1), ev('operational-metrics', 9), ev('operational-metrics', 9)]);
  assert.deepEqual(Object.keys(s.typeReach).sort(), [...BASE_EVENT_TYPES].sort());
  for (const t of BASE_EVENT_TYPES) {
    assert.equal(s.typeReach[t].emitted, s.byType[t]);
    assert.equal(s.typeReach[t].introducedInSchemaVersion, TYPE_INTRODUCED_IN_SCHEMA_VERSION[t]);
  }
});

test('empty input: every type is no-eligible-builds with zero eligible', () => {
  const s = summarize([]);
  for (const t of BASE_EVENT_TYPES) {
    assert.equal(s.typeReach[t].eligibleEvents, 0);
    assert.equal(s.typeReach[t].emitted, 0);
    assert.equal(s.typeReach[t].verdict, 'no-eligible-builds');
  }
});

test('live shape: v6/v8/v9 events, no feature-usage, no v10+ → feature-usage eligible-silent, process-memory no-eligible-builds', () => {
  const events = [
    ...many(3, () => ev('operational-metrics', 6)),
    ...many(2, () => ev('workspace-shape', 8)),
    ...many(5, () => ev('operational-metrics', 9)),
    ev('server-stall', 9, { lagMs: 1, source: 'x' }),
    ev('crash', 9),
  ];
  const s = summarize(events);
  const v9 = 7;
  assert.equal(s.typeReach['feature-usage'].verdict, 'eligible-silent');
  assert.equal(s.typeReach['feature-usage'].eligibleEvents, v9);
  assert.equal(s.typeReach['feature-usage'].emitted, 0);
  assert.equal(s.typeReach['process-memory'].verdict, 'no-eligible-builds');
  assert.equal(s.typeReach['process-memory'].eligibleEvents, 0);
  assert.equal(s.typeReach['operational-metrics'].verdict, 'emitting');
  assert.equal(s.typeReach['error'].verdict, 'eligible-silent', 'error introduced v1: every event is eligible, none emitted');
  assert.equal(s.typeReach['error'].eligibleEvents, events.length);
  assert.equal(s.typeReach['workspace-shape'].eligibleEvents, 2 + v9, 'v8 + v9 events are eligible for a v8 type; v6 ones are not');
});

test('eligibility is monotonic and inclusive at exactly the introducing version', () => {
  const s9 = summarize([ev('crash', 9)]);
  assert.equal(s9.typeReach['feature-usage'].eligibleEvents, 1, 'event at exactly the introducing version counts (>=, not >)');
  assert.equal(s9.typeReach['process-memory'].eligibleEvents, 0, 'a v9 event is NOT eligible for a v10 type');
  assert.equal(s9.typeReach['workspace-names'].eligibleEvents, 1, 'and is eligible for lower-introduced types');
  const s8 = summarize([ev('crash', 8)]);
  assert.equal(s8.typeReach['feature-usage'].eligibleEvents, 0, 'v8 is one below the v9 introduction');
  const s10 = summarize([ev('crash', 10)]);
  assert.equal(s10.typeReach['process-memory'].eligibleEvents, 1);
  assert.equal(s10.typeReach['process-memory'].verdict, 'eligible-silent');
});

test('a type with emitted>0 is emitting even if eligible count equals emitted', () => {
  const s = summarize([ev('feature-usage', 9)]);
  assert.equal(s.typeReach['feature-usage'].verdict, 'emitting');
  assert.equal(s.typeReach['feature-usage'].eligibleEvents, 1);
});

test('absent / null / non-numeric schemaVersion never throws and counts toward no type', () => {
  const bad = [
    { type: 'crash', runtime: 'main', timestamp: 1 },
    { schemaVersion: null, type: 'crash' },
    { schemaVersion: '11', type: 'crash' },
    { schemaVersion: NaN, type: 'crash' },
    { schemaVersion: {}, type: 'crash' },
    null,
    'junk',
  ];
  let s;
  assert.doesNotThrow(() => { s = summarize(bad); });
  for (const t of BASE_EVENT_TYPES) assert.equal(s.typeReach[t].eligibleEvents, 0, t);
  assert.equal(s.typeReach['crash'].emitted, 5);
  assert.equal(s.typeReach['crash'].verdict, 'emitting');
});

test('GET /summary carries typeReach and a filtered read scopes it consistently with schemaVersions', async () => {
  const events = [
    ev('operational-metrics', 9, { appVersion: '0.1.86' }),
    ev('operational-metrics', 9, { appVersion: '0.1.86' }),
    ev('operational-metrics', 6, { appVersion: '0.1.50' }),
  ];
  const store = createNdjsonStore({ sink: async () => {}, source: () => events.map((e) => structuredClone(e)) });
  const handler = createRequestHandler({ store, now: () => 86_400_000 });
  const get = async (url) => {
    const res = { headers: {}, setHeader(k, v) { this.headers[k] = v; }, end(p) { this.body = p; } };
    const req = new EventEmitter();
    Object.assign(req, { method: 'GET', url, headers: {} });
    process.nextTick(() => req.emit('end'));
    await handler(req, res);
    return JSON.parse(res.body);
  };
  const all = await get('/summary');
  assert.equal(all.typeReach['feature-usage'].eligibleEvents, 2);
  assert.equal(all.typeReach['operational-metrics'].eligibleEvents, 3);
  assert.deepEqual(all.schemaVersions, { '6': 1, '9': 2 });
  const filtered = await get('/summary?appVersion=0.1.50');
  assert.deepEqual(filtered.schemaVersions, { '6': 1 });
  assert.equal(filtered.typeReach['feature-usage'].eligibleEvents, 0, 'filtered read scopes eligibility exactly as schemaVersions');
  assert.equal(filtered.typeReach['feature-usage'].verdict, 'no-eligible-builds');
  assert.equal(filtered.typeReach['operational-metrics'].eligibleEvents, 1);
  for (const t of BASE_EVENT_TYPES) assert.equal(filtered.typeReach[t].emitted, filtered.byType[t]);
});
