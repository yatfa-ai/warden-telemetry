// Summary aggregator tests (WARDEN-567). Exercises the PURE `summarize(events)`
// seam directly — ZERO real network, ZERO real filesystem (it takes an event
// array and returns an aggregate object; no deps). Mirrors test/ingest.test.mjs's
// canonical fixtures (one valid event per base-tier type).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { summarize, summarizeTimeline } from '../summary.mjs';

// Canonical valid events (verbatim shapes ingest persists — one per base type).
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

const ZEROED_BY_TYPE = { error: 0, crash: 0, 'performance-stall': 0 };

// ── EMPTY / ZEROED ────────────────────────────────────────────────────────────

test('empty input → total 0, zeroed byType, empty histograms, null time window', () => {
  const s = summarize([]);
  assert.equal(s.total, 0);
  assert.deepEqual(s.byType, ZEROED_BY_TYPE);
  assert.deepEqual(s.topErrorNames, []);
  assert.deepEqual(s.schemaVersions, {});
  assert.equal(s.firstSeen, null);
  assert.equal(s.lastSeen, null);
});

test('non-array input is treated as empty (defensive — never throws)', () => {
  assert.deepEqual(summarize(undefined), summarize([]));
  assert.deepEqual(summarize(null), summarize([]));
  assert.deepEqual(summarize('nope'), summarize([]));
});

// ── TOTAL + PER-TYPE ──────────────────────────────────────────────────────────

test('counts total + per-type across a mixed batch', () => {
  const s = summarize([validError, validCrash, validStall]);
  assert.equal(s.total, 3);
  assert.deepEqual(s.byType, { error: 1, crash: 1, 'performance-stall': 1 });
});

test('byType shape is stable — every base-type key is present even at 0', () => {
  const s = summarize([validError, validError]);
  assert.deepEqual(s.byType, { error: 2, crash: 0, 'performance-stall': 0 });
});

test('repeats accumulate per type', () => {
  const s = summarize([validCrash, validCrash, validStall]);
  assert.equal(s.total, 3);
  assert.deepEqual(s.byType, { error: 0, crash: 2, 'performance-stall': 1 });
});

// ── TOP ERROR NAMES ───────────────────────────────────────────────────────────

test('topErrorNames counts error names, sorted by count desc', () => {
  const events = [
    { ...validError, name: 'TypeError' },
    { ...validError, name: 'RangeError' },
    { ...validError, name: 'TypeError' },
    { ...validError, name: 'SyntaxError' },
    { ...validError, name: 'TypeError' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.topErrorNames, [
    { name: 'TypeError', count: 3 },
    { name: 'RangeError', count: 1 },
    { name: 'SyntaxError', count: 1 },
  ]);
});

test('topErrorNames tie-break is alphabetical (stable, deterministic order)', () => {
  const events = [
    { ...validError, name: 'Zeta' },
    { ...validError, name: 'Alpha' },
    { ...validError, name: 'Mu' },
  ];
  const s = summarize(events);
  assert.deepEqual(
    s.topErrorNames.map((e) => e.name),
    ['Alpha', 'Mu', 'Zeta']
  );
});

test('topErrorNames caps at 10 distinct names', () => {
  const events = [];
  for (let i = 0; i < 15; i++) events.push({ ...validError, name: `Err${i}` });
  const s = summarize(events);
  assert.equal(s.topErrorNames.length, 10);
  // every entry count is 1 (all distinct), so the cap is the only limiter
  assert.equal(s.topErrorNames.every((e) => e.count === 1), true);
});

test('non-error events contribute nothing to topErrorNames', () => {
  const s = summarize([validCrash, validStall]);
  assert.deepEqual(s.topErrorNames, []);
});

// ── SCHEMA-VERSION HISTOGRAM ──────────────────────────────────────────────────

test('schemaVersions is a histogram keyed by stringified schemaVersion', () => {
  const events = [
    { ...validError, schemaVersion: 1 },
    { ...validCrash, schemaVersion: 1 },
    { ...validStall, schemaVersion: 2 },
  ];
  const s = summarize(events);
  assert.deepEqual(s.schemaVersions, { '1': 2, '2': 1 });
});

test('schemaVersions is empty when no events carry a version', () => {
  const s = summarize([{ type: 'error' }]);
  assert.deepEqual(s.schemaVersions, {});
});

// ── TIME WINDOW ───────────────────────────────────────────────────────────────

test('firstSeen/lastSeen are the min/max of finite timestamps', () => {
  const s = summarize([
    { ...validError, timestamp: 300 },
    { ...validCrash, timestamp: 10 },
    { ...validStall, timestamp: 200 },
  ]);
  assert.equal(s.firstSeen, 10);
  assert.equal(s.lastSeen, 300);
});

test('non-finite timestamps are ignored by the time window', () => {
  const s = summarize([{ ...validError, timestamp: Infinity }, { ...validError, timestamp: 50 }]);
  assert.equal(s.firstSeen, 50);
  assert.equal(s.lastSeen, 50);
});

// ── TRUST MODEL — aggregates only, never raw events or identifiers ───────────

test('the summary never echoes raw events or extended-tier identifiers (aggregates only)', () => {
  const events = [
    {
      ...validError,
      message: 'super secret stack detail',
      chatName: 'Refactor auth',
      sessionName: 'claude-7b3a2f1',
    },
  ];
  const s = summarize(events);
  const json = JSON.stringify(s);
  assert.equal(json.includes('Refactor auth'), false, 'no chatName in summary');
  assert.equal(json.includes('claude-7b3a2f1'), false, 'no sessionName in summary');
  assert.equal(json.includes('super secret stack detail'), false, 'no message in summary');
  // the non-identifying error name IS the only string carried through
  assert.equal(json.includes('TypeError'), true);
});

// ── SKIP-ROBUST ───────────────────────────────────────────────────────────────

test('malformed entries (null / primitives / non-objects) are skipped, not fatal', () => {
  const s = summarize([null, 'not-an-object', 42, undefined, validError, validCrash]);
  assert.equal(s.total, 2);
  assert.deepEqual(s.byType, { error: 1, crash: 1, 'performance-stall': 0 });
  assert.equal(s.topErrorNames.length, 1);
});

// ── TIMELINE — bounded temporal distribution (WARDEN-603) ─────────────────────
// The sibling of summarize(): a PURE function of an event array + an injected
// `now` (no fs, no network, no deps) — event counts per time bucket over a
// rolling recent window. Driven directly with a FAKE `now`, mirroring
// createRejectionTally({ now: () => 0 }) in test/server.test.mjs:578. Small
// windowMs/maxBuckets make the bucket arithmetic exact and legible.

test('timeline: empty input → zeroed shape (no false alarm on a quiet store)', () => {
  const t = summarizeTimeline([], { now: () => 10_000_000 });
  assert.deepEqual(t.buckets, []);
  assert.ok(t.bucketMs > 0, 'bucketMs conveys the granularity even when empty');
});

test('timeline: non-array input is treated as empty (defensive — never throws)', () => {
  const opts = { now: () => 0 };
  assert.deepEqual(summarizeTimeline(undefined, opts), summarizeTimeline([], opts));
  assert.deepEqual(summarizeTimeline(null, opts), summarizeTimeline([], opts));
  assert.deepEqual(summarizeTimeline('nope', opts), summarizeTimeline([], opts));
});

test('timeline: events within the window land in the correct time bucket', () => {
  // window [0, 100], 10 buckets of width 10 → bucket 0 = [0,10), bucket 9 = [90,100).
  const t = summarizeTimeline(
    [{ timestamp: 5 }, { timestamp: 95 }, { timestamp: 50 }],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    { bucketStart: 0, bucketEnd: 10, count: 1 },
    { bucketStart: 50, bucketEnd: 60, count: 1 },
    { bucketStart: 90, bucketEnd: 100, count: 1 },
  ]);
  assert.equal(t.bucketMs, 10);
});

test('timeline: multiple events in the same bucket accumulate into one count', () => {
  const t = summarizeTimeline(
    [{ timestamp: 1 }, { timestamp: 2 }, { timestamp: 9 }],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [{ bucketStart: 0, bucketEnd: 10, count: 3 }]);
});

test('timeline: buckets are sorted chronologically (oldest → newest)', () => {
  // feed events out of chronological order
  const t = summarizeTimeline(
    [{ timestamp: 95 }, { timestamp: 5 }, { timestamp: 50 }],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(
    t.buckets.map((b) => b.bucketStart),
    [0, 50, 90]
  );
});

test('timeline: an event timestamped exactly `now` lands in the newest bucket', () => {
  const t = summarizeTimeline([{ timestamp: 100 }], {
    now: () => 100,
    windowMs: 100,
    maxBuckets: 10,
  });
  assert.deepEqual(t.buckets, [{ bucketStart: 90, bucketEnd: 100, count: 1 }]);
});

test('timeline: events older than the rolling window are EXCLUDED from the distribution', () => {
  // window [100, 200]; an event at 50 (before windowStart) is excluded — it is
  // still counted by summarize()'s total/firstSeen (the full retained set), just
  // not in the recent-shape distribution.
  const t = summarizeTimeline(
    [{ timestamp: 50 }, { timestamp: 150 }],
    { now: () => 200, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [{ bucketStart: 150, bucketEnd: 160, count: 1 }]);
});

test('timeline: future timestamps (client/server clock skew) are excluded from the distribution', () => {
  const t = summarizeTimeline(
    [{ timestamp: 250 }, { timestamp: 150 }],
    { now: () => 200, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [{ bucketStart: 150, bucketEnd: 160, count: 1 }]);
});

test('timeline: the window ROLLS with `now` — the same event ages out as now advances', () => {
  // at now=100, ts=50 is in [0,100]; at now=1_000_000 it is far outside the window.
  const events = [{ timestamp: 50 }];
  assert.equal(summarizeTimeline(events, { now: () => 100, windowMs: 100, maxBuckets: 10 }).buckets.length, 1);
  assert.deepEqual(
    summarizeTimeline(events, { now: () => 1_000_000, windowMs: 100, maxBuckets: 10 }).buckets,
    []
  );
});

test('timeline: bucket count is capped at maxBuckets however many events span the window', () => {
  // 200 distinct timestamps across the window would naively make 200 buckets;
  // they collapse into at most maxBuckets grid slots (the 10k-over-months bound).
  const events = [];
  for (let i = 0; i < 200; i++) events.push({ timestamp: i }); // timestamps 0..199
  const t = summarizeTimeline(events, { now: () => 200, windowMs: 200, maxBuckets: 20 });
  assert.ok(t.buckets.length <= 20, 'never more than maxBuckets buckets');
  assert.equal(t.buckets.length, 20, 'every grid slot is hit → exactly maxBuckets');
  // no event is lost: the bucket counts sum to the in-window total
  assert.equal(
    t.buckets.reduce((sum, b) => sum + b.count, 0),
    200
  );
});

test('timeline: non-finite / malformed timestamps are ignored (skip-robust, never fatal)', () => {
  const t = summarizeTimeline(
    [{ timestamp: Infinity }, { timestamp: NaN }, { timestamp: 'nope' }, { timestamp: 50 }, null, 42, 'str'],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [{ bucketStart: 50, bucketEnd: 60, count: 1 }]);
});

test('timeline: never echoes raw events or extended-tier identifiers (timestamps only)', () => {
  const t = summarizeTimeline(
    [
      {
        ...validError,
        timestamp: 50,
        message: 'super secret stack detail',
        chatName: 'Refactor auth',
        sessionName: 'claude-7b3a2f1',
      },
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  const json = JSON.stringify(t);
  assert.equal(json.includes('Refactor auth'), false, 'no chatName in timeline');
  assert.equal(json.includes('claude-7b3a2f1'), false, 'no sessionName in timeline');
  assert.equal(json.includes('super secret stack detail'), false, 'no message in timeline');
  assert.equal(json.includes('TypeError'), false, 'no error name either — timestamps are the only field read');
});

test('timeline: a degenerate config (non-positive window / maxBuckets) collapses to a zeroed shape', () => {
  assert.deepEqual(
    summarizeTimeline([{ timestamp: 5 }], { now: () => 100, windowMs: 0, maxBuckets: 10 }),
    { buckets: [], bucketMs: 0 }
  );
  assert.deepEqual(
    summarizeTimeline([{ timestamp: 5 }], { now: () => 100, windowMs: 100, maxBuckets: 0 }),
    { buckets: [], bucketMs: 0 }
  );
});
