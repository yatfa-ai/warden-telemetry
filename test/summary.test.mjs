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
  assert.deepEqual(s.byRuntime, {});
  assert.deepEqual(s.crashReasons, {});
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

// ── RUNTIME HISTOGRAM (WARDEN-869) ────────────────────────────────────────────
// Mirrors the appVersions / platforms histograms: bucket event counts by the
// non-identifying `runtime` PROCESS label (main / renderer) — the process-axis
// sibling of the release (appVersions) and OS (platforms) axes. `runtime` is
// MANDATORY on every receiver-accepted event, but a partial read / shape drift
// can still omit it, so the same skip-robust guard applies: only a PRESENT
// non-empty string is bucketed — absent / null / non-string / empty is skipped
// (a malformed value must never crash or make a junk bucket). The canonical
// fixtures already carry varied runtimes (validError / validStall = 'main',
// validCrash = 'renderer'), so cases override / drop it as needed.

test('byRuntime is a histogram keyed by the runtime process label', () => {
  const events = [
    { ...validError, runtime: 'main' },
    { ...validCrash, runtime: 'renderer' },
    { ...validStall, runtime: 'main' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.byRuntime, { main: 2, renderer: 1 });
});

test('byRuntime accumulates counts for events sharing a runtime', () => {
  const events = [
    { ...validError, runtime: 'main' },
    { ...validError, runtime: 'main' },
    { ...validError, runtime: 'main' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.byRuntime, { main: 3 });
});

test('byRuntime skips absent / null / non-string / empty values (skip-robust, never a bucket)', () => {
  // Drop `runtime` entirely for the first entry (validError carries 'main') so the
  // truly-absent-field case is covered; the rest override it with a bad value.
  const { runtime: _omit, ...noRuntime } = validError;
  const events = [
    noRuntime, // no runtime field at all (a partial read omits it)
    { ...validError, runtime: null },
    { ...validError, runtime: 2 },
    { ...validError, runtime: '' },
    { ...validError, runtime: { x: 1 } },
    { ...validError, runtime: 'renderer' }, // the only bucketable one
  ];
  const s = summarize(events);
  assert.deepEqual(s.byRuntime, { renderer: 1 });
});

test('byRuntime is empty when no events carry a runtime label', () => {
  const { runtime: _omit, ...noRuntime } = validError;
  const s = summarize([noRuntime, { ...validCrash, runtime: undefined }]);
  assert.deepEqual(s.byRuntime, {});
});
// ── CRASH REASON HISTOGRAM (WARDEN-872) ───────────────────────────────────────
// `crashReasons` is the crash-CAUSE axis `byType.crash` (a bare count) and
// `topSignatures` (capped at 10 across ALL types, split by exitCode, ranked)
// both obscure: it buckets crash counts by the non-identifying `reason` string
// (Electron's fixed enum — oom / crashed / killed — plus the main-process
// 'unexpected-termination' sentinel, WARDEN-687). Mirrors the appVersions /
// platforms discipline: COUNTS-only, skip-robust on an absent / null / non-string
// / empty reason, a clean empty {} shape, and it never echoes raw events or
// identifiers (`reason` is a redaction no-op, the same tier as `platform`).

test('crashReasons is a histogram keyed by the crash reason', () => {
  // validCrash carries reason:'oom'; a second validCrash → oom:2; a reason:'killed'
  // crash → killed:1. Non-crash events contribute nothing.
  const events = [
    validCrash,
    { ...validCrash },
    { type: 'crash', reason: 'killed' },
    validError,
    validStall,
  ];
  const s = summarize(events);
  assert.deepEqual(s.crashReasons, { oom: 2, killed: 1 });
});

test('crashReasons is keyed by reason ALONE — exitCode does NOT split the bucket', () => {
  // topSignatures splits a crash into reason+exitCode (`crash:oom:exit=133` vs
  // `crash:oom:exit=1`), so the marginal "total OOM crashes" is NOT derivable from
  // it. crashReasons rolls that up — the blind spot this histogram closes.
  const events = [
    { ...validCrash, reason: 'oom', exitCode: 133 },
    { ...validCrash, reason: 'oom', exitCode: 1 },
    { ...validCrash, reason: 'oom' }, // no exitCode
  ];
  const s = summarize(events);
  assert.deepEqual(s.crashReasons, { oom: 3 });
});

test('crashReasons skips absent / null / non-string / empty reason (skip-robust, never a bucket)', () => {
  // A reasonless crash is still counted by byType.crash but NOT bucketed here.
  const events = [
    { type: 'crash' }, // no reason
    { ...validCrash, reason: null },
    { ...validCrash, reason: 2 },
    { ...validCrash, reason: '' },
    { ...validCrash, reason: { x: 1 } },
    { ...validCrash, reason: 'oom' }, // the only bucketable one
  ];
  const s = summarize(events);
  assert.deepEqual(s.crashReasons, { oom: 1 });
});

test('crashReasons values sum to ≤ byType.crash (equality iff every crash has a reason)', () => {
  // The invariant the proposal's "sum === byType.crash" DONE criterion is really
  // expressing: a reasonless crash is counted by byType.crash but NOT bucketed, so
  // the histogram sum can only ever be ≤ — never >. Asserting a strict === across a
  // reasonless-crash fixture would fail (the skip-robust gap the proposer flagged).
  const events = [
    { type: 'crash' }, // reasonless → counted by byType.crash, NOT bucketed
    { ...validCrash, reason: 'oom' },
    { ...validCrash, reason: 'killed' },
  ];
  const s = summarize(events);
  assert.equal(s.byType.crash, 3);
  const sum = Object.values(s.crashReasons).reduce((a, b) => a + b, 0);
  assert.ok(sum <= s.byType.crash, 'histogram sum never exceeds the crash count');
  assert.equal(sum, 2, 'the reasonless crash is the gap (3 counted, 2 bucketed)');
  // Equality holds ONLY when every crash carries a present non-empty reason:
  const allReasoned = summarize([
    { ...validCrash, reason: 'oom' },
    { ...validCrash, reason: 'killed' },
  ]);
  const sumAll = Object.values(allReasoned.crashReasons).reduce((a, b) => a + b, 0);
  assert.equal(sumAll, allReasoned.byType.crash, 'equality when every crash has a reason');
});

test('crashReasons is empty when no events are crashes (no false alarm)', () => {
  const s = summarize([validError, validStall]);
  assert.deepEqual(s.crashReasons, {});
});

test('crashReasons reflects only a platform/appVersion-filtered slice', () => {
  // summarize() is a PURE function of the event array it is handed — the /summary
  // handler filters the array BEFORE calling summarize() (server.mjs:
  // summarize(filtered)), so the histogram honors ?platform / ?appVersion / ?since
  // / ?type for free. Here we emulate that filter by handing summarize() only the
  // filtered slice directly (the primary testable seam; the wiring is asserted in
  // server.test.mjs's filter-scoping tests).
  const events = [
    { ...validCrash, reason: 'oom', platform: 'darwin', appVersion: '0.1.19' },
    { ...validCrash, reason: 'oom', platform: 'darwin', appVersion: '0.1.19' },
    { ...validCrash, reason: 'killed', platform: 'win32', appVersion: '0.1.20' },
  ];
  // emulate ?platform=darwin: only the two darwin oom crashes survive the slice
  const filtered = events.filter((e) => e.platform === 'darwin');
  const s = summarize(filtered);
  assert.deepEqual(s.crashReasons, { oom: 2 });
});

// ── STALL SEVERITY — magnitude aggregate split by source (WARDEN-854) ──────────
// `stalls` is the MAGNITUDE axis the stall COUNT (`byType` / `topSignatures`)
// cannot show: 500 × 50ms micro-hitches and 500 × 5s hard freezes read byte-
// identically on every other surface. It captures the `lagMs` distribution
// (min / avg / max — the real user-perceived freeze duration) of performance-stall
// events, split by `source`. Mirrors the appVersions / platforms discipline:
// `count === byType['performance-stall']` (the magnitude + count surfaces agree),
// min/avg/max over the finite-lagMs subset, a clean zeroed empty shape, skip-robust
// on a non-finite / absent lagMs (skipped from stats, STILL counted), and it never
// echoes raw events or identifiers.

test('stalls.count matches byType[performance-stall] and max is the headline freeze duration', () => {
  // 50 / 200 / 5000 across both sources → max = 5000 (the freeze a user actually
  // felt, not buried in the average), avg = 1750, min = 50.
  const events = [
    { ...validStall, lagMs: 50, source: 'event-loop' },
    { ...validStall, lagMs: 5000, source: 'unresponsive' },
    { ...validStall, lagMs: 200, source: 'event-loop' },
  ];
  const s = summarize(events);
  assert.equal(s.stalls.count, 3);
  assert.equal(
    s.stalls.count, s.byType['performance-stall'],
    'the magnitude count agrees with the count surface'
  );
  assert.equal(s.stalls.min, 50);
  assert.equal(s.stalls.avg, 1750);
  assert.equal(s.stalls.max, 5000, 'max is the worst freeze a user actually felt');
});

test('stalls.bySource splits event-loop jank from unresponsive renderer hangs', () => {
  const events = [
    { ...validStall, lagMs: 50, source: 'event-loop' },
    { ...validStall, lagMs: 100, source: 'event-loop' },
    { ...validStall, lagMs: 5000, source: 'unresponsive' },
  ];
  const s = summarize(events);
  assert.deepEqual(s.stalls.bySource, {
    'event-loop': { count: 2, min: 50, avg: 75, max: 100 },
    unresponsive: { count: 1, min: 5000, avg: 5000, max: 5000 },
  });
});

test('stalls is a clean zeroed shape on a stall-free store (no false alarm)', () => {
  // Non-stall events contribute nothing — stalls reads only performance-stall.
  const s = summarize([validError, validCrash]);
  assert.deepEqual(s.stalls, { count: 0, min: null, avg: 0, max: null, bySource: {} });
  // empty input too
  assert.deepEqual(summarize([]).stalls, { count: 0, min: null, avg: 0, max: null, bySource: {} });
});

test('stalls: a non-finite lagMs (NaN / Infinity) is skipped from stats but STILL counted', () => {
  // The load-bearing guard: validateBaseEvent only typeof-checks lagMs (schema.ts),
  // so NaN / Infinity can reach summarize(). An unguarded Math.min/max or running
  // average would poison the whole aggregate from a single bad record.
  const events = [
    { ...validStall, lagMs: NaN, source: 'event-loop' },
    { ...validStall, lagMs: Infinity, source: 'event-loop' },
    { ...validStall, lagMs: 500, source: 'event-loop' },
  ];
  const s = summarize(events);
  assert.equal(s.stalls.count, 3, 'the bad records are still counted');
  assert.equal(s.stalls.count, s.byType['performance-stall'], 'count-match invariant holds');
  assert.equal(s.stalls.min, 500);
  assert.equal(s.stalls.avg, 500);
  assert.equal(s.stalls.max, 500, 'min/avg/max reflect ONLY the finite record');
  // the per-source bucket counts the bad records too, but its stats stay finite
  assert.deepEqual(s.stalls.bySource, {
    'event-loop': { count: 3, min: 500, avg: 500, max: 500 },
  });
});

test('stalls: an absent lagMs is skipped from stats but STILL counted', () => {
  const events = [
    { ...validStall, lagMs: undefined, source: 'event-loop' },
    { ...validStall, lagMs: 500, source: 'event-loop' },
  ];
  const s = summarize(events);
  assert.equal(s.stalls.count, 2);
  assert.equal(s.stalls.count, s.byType['performance-stall']);
  assert.equal(s.stalls.avg, 500);
  assert.equal(s.stalls.max, 500);
});

test('stalls: a sourceless stall is counted overall but not bucketed in bySource', () => {
  // A stall with no `source` is malformed; it is counted (count-match invariant)
  // and its magnitude still feeds the overall rollup, but it yields no per-source
  // bucket (mirrors signatureOf, which returns null for a sourceless stall).
  const events = [
    { ...validStall, lagMs: 500, source: 'event-loop' },
    { type: 'performance-stall', lagMs: 9000 }, // no source
  ];
  const s = summarize(events);
  assert.equal(s.stalls.count, 2);
  assert.equal(s.stalls.max, 9000, 'overall max still reflects the sourceless stall');
  assert.deepEqual(s.stalls.bySource, {
    'event-loop': { count: 1, min: 500, avg: 500, max: 500 },
  });
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
    // A stall carrying the SAME redacted free-text + extended-tier identifiers, to
    // extend the trust model over the new `stalls` magnitude aggregate (WARDEN-854):
    // its `lagMs` (a non-identifying magnitude) + `source` ARE aggregated, but the
    // message / chatName / sessionName MUST NOT reach the summary any more than the
    // error's do.
    {
      ...validStall,
      lagMs: 5000,
      source: 'unresponsive',
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
    // the added stall yields a `stall:unresponsive` signature (count 1, tie-broken
    // after the error by signature asc — 'T' < 's').
    { signature: 'stall:unresponsive', type: 'performance-stall', count: 1 },
  ]);
  // The stalls aggregate reads ONLY the non-identifying `lagMs` magnitude + `source`
  // — the freeze duration is aggregated (max = the 5000ms a user felt) and the
  // source is a bucket key, but the stall's message / identifiers never appear.
  assert.equal(s.stalls.count, 1);
  assert.equal(s.stalls.max, 5000);
  assert.deepEqual(s.stalls.bySource, {
    unresponsive: { count: 1, min: 5000, avg: 5000, max: 5000 },
  });
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

// ── TIME WINDOW — clock-skew robustness via receivedAt (WARDEN-692 / WARDEN-738) ─
// The `firstSeen`/`lastSeen` overview bounds key off the RECEIVER's `receivedAt`
// (falling back to the client `timestamp`), the SAME effective time the timeline
// / retention / ?since surfaces already use — completing the WARDEN-692 cutover
// for the last skew-broken /summary signal. A skewed client clock can no longer
// push `lastSeen` into the future or drag `firstSeen` into the past, so the
// overview bounds now agree with the receivedAt-keyed `timeline` on the SAME
// response (no more "timeline spikes now, lastSeen points to 2099" contradiction).

test('firstSeen/lastSeen: a future client timestamp does NOT push lastSeen beyond receivedAt', () => {
  // A fast-clock client stamped `timestamp` far in the future, but the receiver
  // saw the batch at receivedAt=200. Keyed off receivedAt, lastSeen tracks the
  // receiver's clock (200) — NOT the client's skewed future (the prior
  // timestamp-only keying would have set lastSeen to the bogus future value).
  const s = summarize([
    { ...validError, timestamp: 100, receivedAt: 100 },
    { ...validError, timestamp: 4_000_000_000, receivedAt: 200 },
  ]);
  assert.equal(s.firstSeen, 100);
  assert.equal(
    s.lastSeen, 200,
    'lastSeen is the receiver receipt time, not the future client timestamp'
  );
});

test('firstSeen/lastSeen: a past client timestamp does NOT drag firstSeen below receivedAt', () => {
  // A slow-clock client stamped `timestamp` in the distant past (0), but the
  // receiver saw the batch at receivedAt=6000. Keyed off receivedAt, firstSeen
  // tracks the receiver's clock — NOT the client's skewed past (the prior
  // timestamp-only keying would have dragged firstSeen down to 0).
  const s = summarize([
    { ...validError, timestamp: 5_000, receivedAt: 5_000 },
    { ...validError, timestamp: 0, receivedAt: 6_000 },
  ]);
  assert.equal(
    s.firstSeen, 5_000,
    'firstSeen is the receiver receipt time, not the past client timestamp'
  );
  assert.equal(s.lastSeen, 6_000);
});

test('firstSeen/lastSeen: an event lacking receivedAt still reads via the timestamp fallback (graceful backfill, no migration)', () => {
  // A pre-annotation persisted event (no receivedAt) alongside a receivedAt-
  // annotated one: the legacy event falls back to its client `timestamp`, so it
  // still contributes to the bounds unchanged — old surfaces never go blank.
  const s = summarize([
    { ...validError, timestamp: 10 }, // no receivedAt → effective time = 10
    { ...validError, timestamp: 90, receivedAt: 90 },
  ]);
  assert.equal(s.firstSeen, 10);
  assert.equal(s.lastSeen, 90);
});
