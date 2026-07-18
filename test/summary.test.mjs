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
  assert.deepEqual(s.topSignatures, []);
  assert.deepEqual(s.schemaVersions, {});
  assert.deepEqual(s.appVersions, {});
  assert.deepEqual(s.platforms, {});
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

// ── TOP SIGNATURES — distinct-failure ranking (WARDEN-707) ────────────────────
// `topSignatures` ranks DISTINCT failures via a per-type `signature` so a
// maintainer can tell ONE regression × N from N distinct bugs — the axis
// `topErrorNames` (Error#name only) cannot show. Mirrors the topErrorNames
// section above: same sort (count desc, then key asc), same cap (10).

test('topSignatures collapses N errors sharing name + frames[0] into ONE entry', () => {
  // The actionable case: ONE failure copied N×. Same name AND same top frame →
  // a single bucket with count N (not N entries, and not flattened to just the name).
  const frame = { function: 'renderChat', file: 'App.tsx', line: 142 };
  const events = [
    { ...validError, name: 'TypeError', frames: [frame] },
    { ...validError, name: 'TypeError', frames: [frame] },
    { ...validError, name: 'TypeError', frames: [frame] },
  ];
  const s = summarize(events);
  assert.deepEqual(s.topSignatures, [
    { signature: 'TypeError @ App.tsx:142 (renderChat)', type: 'error', count: 3 },
  ]);
});

test('topSignatures keeps DISTINCT signatures separate (same name, different frame)', () => {
  // Same error name but a different top frame is a DIFFERENT failure — two
  // entries, not one merged `TypeError` bucket (which is all topErrorNames sees).
  const events = [
    { ...validError, name: 'TypeError', frames: [{ file: 'App.tsx', line: 142 }] },
    { ...validError, name: 'TypeError', frames: [{ file: 'Other.tsx', line: 9 }] },
  ];
  const s = summarize(events);
  assert.deepEqual(s.topSignatures, [
    { signature: 'TypeError @ App.tsx:142', type: 'error', count: 1 },
    { signature: 'TypeError @ Other.tsx:9', type: 'error', count: 1 },
  ]);
});

test('topSignatures degrades to name-only when frames are empty / lack the location fields', () => {
  // An error with NO frames, or whose frames[0] lacks function/file/line, falls
  // back to the bare `name` — exactly today's topErrorNames bucket. Graceful
  // superset: nothing regresses.
  const s = summarize([
    { ...validError, name: 'TypeError', frames: [] }, // empty frames
    { ...validError, name: 'TypeError', frames: [{ column: 9 }] }, // frame has no fn/file/line
    { ...validError, name: 'TypeError' }, // no frames field at all
  ]);
  assert.deepEqual(s.topSignatures, [{ signature: 'TypeError', type: 'error', count: 3 }]);
});

test('topSignatures buckets crash by reason+exitCode and stall by source, ranked across types', () => {
  // validCrash carries reason:'oom' + exitCode:133 → `crash:oom:exit=133`; a crash
  // with NO exitCode omits the `:exit=N` segment. Stalls bucket by `source`. Each
  // entry carries its `type` for a mixed-type ranking.
  const events = [
    validCrash, // crash:oom:exit=133
    { ...validCrash }, // crash:oom:exit=133 → count 2
    { type: 'crash', reason: 'killed' }, // no exitCode → `crash:killed`
    { ...validStall, source: 'event-loop' },
    { ...validStall, source: 'unresponsive' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.topSignatures, [
    { signature: 'crash:oom:exit=133', type: 'crash', count: 2 },
    // the three count=1 entries tie-break by signature asc: 'c' < 's', so the
    // crash precedes the stalls, and 'stall:event-loop' < 'stall:unresponsive'.
    { signature: 'crash:killed', type: 'crash', count: 1 },
    { signature: 'stall:event-loop', type: 'performance-stall', count: 1 },
    { signature: 'stall:unresponsive', type: 'performance-stall', count: 1 },
  ]);
});

test('topSignatures ranks a mixed error/crash/stall batch together in one list', () => {
  // Cross-type ranking: a high-count error outranks low-count crashes/stalls, so
  // the maintainer sees the single biggest failure first regardless of its type.
  const frame = { function: 'renderChat', file: 'App.tsx', line: 142 };
  const events = [
    { ...validError, name: 'TypeError', frames: [frame] },
    { ...validError, name: 'TypeError', frames: [frame] },
    validCrash, // crash:oom:exit=133 (validCrash carries exitCode)
    { ...validStall }, // stall:event-loop
  ];
  const s = summarize(events);
  assert.deepEqual(s.topSignatures, [
    { signature: 'TypeError @ App.tsx:142 (renderChat)', type: 'error', count: 2 },
    { signature: 'crash:oom:exit=133', type: 'crash', count: 1 },
    { signature: 'stall:event-loop', type: 'performance-stall', count: 1 },
  ]);
});

test('topSignatures caps at 10 distinct signatures', () => {
  // 15 distinct failure signatures collapse to the top 10 by count (here every
  // count is 1, so the cap is the only limiter — mirrors topErrorNames' cap test).
  const events = [];
  for (let i = 0; i < 15; i++) {
    events.push({ ...validError, name: 'TypeError', frames: [{ file: `F${i}.ts`, line: i }] });
  }
  const s = summarize(events);
  assert.equal(s.topSignatures.length, 10);
  assert.equal(s.topSignatures.every((e) => e.count === 1), true);
});

test('topSignatures tie-break is signature asc (stable, deterministic order)', () => {
  // All count 1 → the rank is purely the signature ascending. Distinct names so
  // the names themselves order the list (Zeta < … is false; Alpha < Mu < Zeta).
  const events = [
    { ...validError, name: 'Zeta', frames: [] },
    { ...validError, name: 'Alpha', frames: [] },
    { ...validError, name: 'Mu', frames: [] },
  ];
  const s = summarize(events);
  assert.deepEqual(
    s.topSignatures.map((e) => e.signature),
    ['Alpha', 'Mu', 'Zeta']
  );
});

test('topSignatures ignores events that yield no signature (skip-robust, never fatal)', () => {
  // A nameless error, a reasonless crash, a sourceless stall, and a non-object
  // all yield null from signatureOf → no bucket, and the good records still rank.
  const s = summarize([
    { type: 'error', frames: [] }, // no name
    { type: 'crash' }, // no reason
    { type: 'performance-stall' }, // no source
    null,
    { ...validError, name: 'TypeError', frames: [{ file: 'App.tsx', line: 1 }] },
  ]);
  assert.deepEqual(s.topSignatures, [
    { signature: 'TypeError @ App.tsx:1', type: 'error', count: 1 },
  ]);
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

// ── APP-VERSION HISTOGRAM (WARDEN-665) ────────────────────────────────────────
// Mirrors the schemaVersions histogram: bucket event counts by the non-identifying
// `appVersion` release label. Only a PRESENT non-empty string is bucketed — absent
// / null / non-string / empty is skipped (a v2 source that cannot read the version
// emits no field, and a malformed value must never crash or make a junk bucket).

test('appVersions is a histogram keyed by the appVersion release label', () => {
  const events = [
    { ...validError, appVersion: '0.1.19' },
    { ...validCrash, appVersion: '0.1.19' },
    { ...validStall, appVersion: '0.1.20' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.appVersions, { '0.1.19': 2, '0.1.20': 1 });
});

test('appVersions accumulates counts for events sharing a release', () => {
  const events = [
    { ...validError, appVersion: '0.1.19' },
    { ...validError, appVersion: '0.1.19' },
    { ...validError, appVersion: '0.1.19' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.appVersions, { '0.1.19': 3 });
});

test('appVersions skips absent / null / non-string / empty values (skip-robust, never a bucket)', () => {
  const events = [
    { ...validError }, // no appVersion field
    { ...validError, appVersion: null },
    { ...validError, appVersion: 2 },
    { ...validError, appVersion: '' },
    { ...validError, appVersion: { x: 1 } },
    { ...validError, appVersion: '0.1.19' }, // the only bucketable one
  ];
  const s = summarize(events);
  assert.deepEqual(s.appVersions, { '0.1.19': 1 });
});

test('appVersions is empty when no events carry a release label', () => {
  const s = summarize([{ ...validError }, { ...validCrash }]);
  assert.deepEqual(s.appVersions, {});
});

// ── PLATFORM HISTOGRAM (WARDEN-684) ───────────────────────────────────────────
// Mirrors the appVersions histogram: bucket event counts by the non-identifying
// `platform` OS label (darwin/win32/linux). Only a PRESENT non-empty string is
// bucketed — absent / null / non-string / empty is skipped (a v3 source that
// cannot read process.platform emits no field, and a malformed value must never
// crash or make a junk bucket).

test('platforms is a histogram keyed by the platform OS label', () => {
  const events = [
    { ...validError, platform: 'darwin' },
    { ...validCrash, platform: 'darwin' },
    { ...validStall, platform: 'win32' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.platforms, { darwin: 2, win32: 1 });
});

test('platforms accumulates counts for events sharing an OS', () => {
  const events = [
    { ...validError, platform: 'linux' },
    { ...validError, platform: 'linux' },
    { ...validError, platform: 'linux' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.platforms, { linux: 3 });
});

test('platforms skips absent / null / non-string / empty values (skip-robust, never a bucket)', () => {
  const events = [
    { ...validError }, // no platform field
    { ...validError, platform: null },
    { ...validError, platform: 2 },
    { ...validError, platform: '' },
    { ...validError, platform: { x: 1 } },
    { ...validError, platform: 'darwin' }, // the only bucketable one
  ];
  const s = summarize(events);
  assert.deepEqual(s.platforms, { darwin: 1 });
});

test('platforms is empty when no events carry an OS label', () => {
  const s = summarize([{ ...validError }, { ...validCrash }]);
  assert.deepEqual(s.platforms, {});
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
      // A real top frame so the failure signature carries the non-identifying
      // location fields (file/function/line) — asserted present below.
      frames: [{ function: 'renderChat', file: 'App.tsx', line: 142 }],
      message: 'super secret stack detail',
      chatName: 'Refactor auth',
      sessionName: 'claude-7b3a2f1',
    },
  ];
  const s = summarize(events);
  const json = JSON.stringify(s);
  // The redacted free-text `message` and the extended-tier identifiers MUST NOT
  // reach the summary — the signature is built ONLY from non-identifying fields.
  assert.equal(json.includes('Refactor auth'), false, 'no chatName in summary');
  assert.equal(json.includes('claude-7b3a2f1'), false, 'no sessionName in summary');
  assert.equal(json.includes('super secret stack detail'), false, 'no message in summary');
  // the non-identifying error name IS carried through…
  assert.equal(json.includes('TypeError'), true);
  // …and so are the non-identifying frame fields, INSIDE a signature bucket —
  // this is the trust model for topSignatures (WARDEN-707): only structured
  // non-identifying fields, never message/identifiers.
  assert.equal(json.includes('App.tsx:142'), true, 'non-identifying frame file:line is in a signature');
  assert.equal(json.includes('renderChat'), true, 'non-identifying frame function is in a signature');
  assert.deepEqual(s.topSignatures, [
    { signature: 'TypeError @ App.tsx:142 (renderChat)', type: 'error', count: 1 },
  ]);
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

test('timeline: future CLIENT timestamps with no receivedAt are excluded via the timestamp fallback', () => {
  // These events predate the receivedAt annotation (WARDEN-692), so the effective
  // time falls back to the client `timestamp`; a future one is still excluded
  // (when > currentTime). The receivedAt-present skew case is covered below.
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

// ── TIMELINE — clock-skew robustness via receivedAt (WARDEN-692) ──────────────
// The effective time PREFERS the receiver's `receivedAt` and falls back to the
// client's `timestamp`. The headline fix: a fast-clock client whose `timestamp`
// is minutes in the future no longer VANISHES from the "did this just spike?"
// window — the receiver saw the batch in-window, so receivedAt places it there.

test('timeline: a fast-clock client (future timestamp) still appears in the recent window via receivedAt', () => {
  // Server now = 200, window [100, 200]. The client's `timestamp` is 5 MINUTES in
  // the future (a fast clock) — under the timestamp-only keying that EXCLUDES it
  // (when > currentTime), so a regression spike vanishes at the moment it matters.
  // receivedAt = 150 (the receiver saw it in-window) → it still lands in the
  // recent window keyed off the RECEIVER's clock.
  const t = summarizeTimeline(
    [{ timestamp: 200 + 300_000, receivedAt: 150 }],
    { now: () => 200, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [{ bucketStart: 150, bucketEnd: 160, count: 1 }]);
});

test('timeline: receivedAt is PREFERRED — an in-window receivedAt wins over an out-of-window timestamp', () => {
  // timestamp = 50 (before windowStart = 100 → would be excluded); receivedAt = 150
  // (in window). receivedAt wins, so the event is included.
  const t = summarizeTimeline(
    [{ timestamp: 50, receivedAt: 150 }],
    { now: () => 200, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [{ bucketStart: 150, bucketEnd: 160, count: 1 }]);
});

test('timeline: an event lacking receivedAt still reads via the timestamp fallback (graceful backfill, no migration)', () => {
  // A pre-annotation persisted event (no receivedAt): the client timestamp
  // governs, unchanged from before — old surfaces never go blank.
  const t = summarizeTimeline(
    [{ timestamp: 150 }],
    { now: () => 200, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [{ bucketStart: 150, bucketEnd: 160, count: 1 }]);
});
