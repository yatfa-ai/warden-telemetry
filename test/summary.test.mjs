// Summary aggregator tests (WARDEN-567). Exercises the PURE `summarize(events)`
// seam directly — ZERO real network, ZERO real filesystem (it takes an event
// array and returns an aggregate object; no deps). Mirrors test/ingest.test.mjs's
// canonical fixtures (one valid event per base-tier type).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { summarize, summarizeTimeline, summarizeStallsTimeline, lastAcceptedInstant, CLIENT_KEY_MAX_LENGTH, CLIENT_HISTOGRAM_CAP, OPERATIONS_SUMMARY_CAP, WORKSPACE_NAMES_SUMMARY_CAP, GENERATED_NAME_SHAPE, FEATURE_USAGE_SUMMARY_CAP, RELEASES_SUMMARY_CAP } from '../summary.mjs';
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

const ZEROED_BY_TYPE = { error: 0, crash: 0, 'performance-stall': 0, 'operational-metrics': 0, 'server-stall': 0, 'workspace-names': 0, 'workspace-shape': 0,
    'feature-usage': 0, 'process-memory': 0 };

// ── EMPTY / ZEROED ────────────────────────────────────────────────────────────

test('empty input → total 0, zeroed byType, empty histograms, null time window', () => {
  const s = summarize([]);
  assert.equal(s.total, 0);
  assert.deepEqual(s.byType, ZEROED_BY_TYPE);
  assert.deepEqual(s.topErrorNames, []);
  assert.deepEqual(s.topSignatures, []);
  assert.deepEqual(s.schemaVersions, {});
  assert.deepEqual(s.appVersions, {});
  assert.deepEqual(s.releases, { distinctCount: 0, byVersion: {}, newestSeen: null }, 'WARDEN-1540: stable zeroed shape');
  assert.deepEqual(s.platforms, {});
  assert.deepEqual(s.byRuntime, {});
  assert.deepEqual(s.crashReasons, {});
  assert.deepEqual(s.operations, {});
  assert.deepEqual(s.operationLatency, {}, 'WARDEN-1500: stable empty shape');
  // WARDEN-1473 — the workspace axes read a STABLE zeroed shape on an empty
  // store: every schema count is present with `windowsSeen: 0` and `null`
  // min/max, never a fabricated `0` (a `0` panesOpen is a REAL measurement).
  assert.deepEqual(s.workspaceShape, {
    windowsSeen: 0,
    lastSnapshotAt: null,
    counts: {
      workspaces: { windowsSeen: 0, min: null, avg: 0, max: null },
      panesOpen: { windowsSeen: 0, min: null, avg: 0, max: null },
      panesActive: { windowsSeen: 0, min: null, avg: 0, max: null },
      chats: { windowsSeen: 0, min: null, avg: 0, max: null },
      peakPanesOpen: { windowsSeen: 0, min: null, avg: 0, max: null },
      peakChats: { windowsSeen: 0, min: null, avg: 0, max: null },
    },
  });
  assert.deepEqual(s.workspaceNames, {
    windowsSeen: 0,
    names: {},
    distinctCount: 0,
    maxChatCount: null,
    lastChatCount: null,
    truncatedEver: false,
    generatedShaped: { distinctCount: 0, considered: 0, share: null },
    lastSnapshotAt: null,
  });
  // WARDEN-1488 — the feature-usage axis reads a STABLE zeroed shape too.
  assert.deepEqual(s.featureUsage, { windowsSeen: 0, lastWindowAt: null, features: {}, distinctCount: 0 });
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
  assert.deepEqual(s.byType, { error: 1, crash: 1, 'performance-stall': 1, 'operational-metrics': 0, 'server-stall': 0, 'workspace-names': 0, 'workspace-shape': 0,
    'feature-usage': 0, 'process-memory': 0 });
});

test('byType shape is stable — every base-type key is present even at 0', () => {
  const s = summarize([validError, validError]);
  assert.deepEqual(s.byType, { error: 2, crash: 0, 'performance-stall': 0, 'operational-metrics': 0, 'server-stall': 0, 'workspace-names': 0, 'workspace-shape': 0,
    'feature-usage': 0, 'process-memory': 0 });
});

test('repeats accumulate per type', () => {
  const s = summarize([validCrash, validCrash, validStall]);
  assert.equal(s.total, 3);
  assert.deepEqual(s.byType, { error: 0, crash: 2, 'performance-stall': 1, 'operational-metrics': 0, 'server-stall': 0, 'workspace-names': 0, 'workspace-shape': 0,
    'feature-usage': 0, 'process-memory': 0 });
});

// WARDEN-1278 — the SERVER child's folded stall window is a first-class base
// type on the count axis. The ticket deliberately scoped the MAGNITUDE axis
// (`stalls`) to the per-stall `performance-stall` type only: a server-stall
// carries a folded window, not a single lagMs, so feeding it into a min/avg/max
// built from per-event durations would produce a number that means neither.
const validServerStall = {
  schemaVersion: validError.schemaVersion,
  type: 'server-stall',
  runtime: 'server',
  timestamp: 7,
  windowStartedAt: 1,
  windowEndedAt: 7,
  count: 2,
  totalMs: 7400,
  maxMs: 6000,
  boundaries: [1000, 2000, 5000, 10000, 30000],
  buckets: [0, 1, 0, 1, 0, 0],
  culprits: [{ culprit: 'get-api-claude-sessions', count: 2, totalOverlapMs: 7300 }],
};

test('byType counts server-stall — the backend child is visible in the aggregate (WARDEN-1278)', () => {
  const s = summarize([validError, validServerStall, validServerStall]);
  assert.equal(s.total, 3);
  assert.deepEqual(s.byType, {
    error: 1, crash: 0, 'performance-stall': 0, 'operational-metrics': 0, 'server-stall': 2,
    'workspace-names': 0,
    'workspace-shape': 0,
    'feature-usage': 0, 'process-memory': 0,
  });
});

// WARDEN-1416 — the `names` category's own carrying event. The receiver counts
// it like any other type; the point of pinning it here is that the type is
// VISIBLE in the aggregate at all — a maintainer asking "are names arriving?"
// reads it off byType, and a silently-uncounted type would answer "no".
const validWorkspaceNames = {
  schemaVersion: 1,
  type: 'workspace-names',
  runtime: 'server',
  timestamp: 1735689600000,
  windowStartedAt: 1735689300000,
  windowEndedAt: 1735689600000,
  chats: ['demo', 'Refactor auth'],
  chatCount: 2,
  truncated: false,
};

test('byType counts workspace-names — the names category is visible in the aggregate (WARDEN-1416)', () => {
  const s = summarize([validError, validWorkspaceNames, validWorkspaceNames]);
  assert.equal(s.total, 3);
  assert.deepEqual(s.byType, {
    error: 1, crash: 0, 'performance-stall': 0, 'operational-metrics': 0, 'server-stall': 0,
    'workspace-names': 2,
    'workspace-shape': 0,
    'feature-usage': 0, 'process-memory': 0,
  });
});

test('a workspace-names does NOT enter the per-stall MAGNITUDE axis', () => {
  // Same separation the server-stall test below asserts: the magnitude axis is
  // per-event lagMs, and a names window has none.
  const s = summarize([validWorkspaceNames, { ...validStall, lagMs: 400 }]);
  assert.equal(s.stalls.count, 1, 'only the per-stall event is in the magnitude axis');
  assert.equal(s.byType['workspace-names'], 1, 'and the names window is still counted on the count axis');
});

// WARDEN-1424 — the renderer's workspace-shape COUNT snapshot. Same visibility
// pin as the names type above: a maintainer asking "is the shape snapshot
// arriving?" reads it off byType, and a silently-uncounted type would answer
// "no".
const validWorkspaceShape = {
  schemaVersion: 1,
  type: 'workspace-shape',
  runtime: 'renderer',
  timestamp: 1735689600000,
  windowStartedAt: 1735689300000,
  windowEndedAt: 1735689600000,
  workspaces: 2,
  panesOpen: 5,
  panesActive: 3,
  chats: 7,
  peakPanesOpen: 6,
  peakChats: 9,
};

test('byType counts workspace-shape — the shape snapshot is visible in the aggregate (WARDEN-1424)', () => {
  const s = summarize([validError, validWorkspaceShape, validWorkspaceShape]);
  assert.equal(s.total, 3);
  assert.deepEqual(s.byType, {
    error: 1, crash: 0, 'performance-stall': 0, 'operational-metrics': 0, 'server-stall': 0,
    'workspace-names': 0,
    'workspace-shape': 2,
    'feature-usage': 0, 'process-memory': 0,
  });
});

test('a workspace-shape does NOT enter the per-stall MAGNITUDE axis', () => {
  // A counts-only window has no lagMs — the magnitude axis is per-event lag
  // and the shape snapshot must not fold into it (the same separation the
  // names and server-stall types assert above).
  const s = summarize([validWorkspaceShape, { ...validStall, lagMs: 400 }]);
  assert.equal(s.stalls.count, 1, 'only the per-stall event is in the magnitude axis');
  assert.equal(s.byType['workspace-shape'], 1, 'and the shape window is still counted on the count axis');
});

test('a server-stall does NOT enter the per-stall MAGNITUDE axis', () => {
  // The two axes answer different questions and must not be conflated: `stalls`
  // is min/avg/max over PER-EVENT lagMs, and a server-stall has no lagMs — it
  // has a whole folded window. Counting it here would silently mix a window's
  // aggregate into an event-level percentile.
  const s = summarize([validServerStall, { ...validStall, lagMs: 400 }]);
  assert.equal(s.stalls.count, 1, 'only the per-stall event is in the magnitude axis');
  assert.equal(
    s.stalls.count, s.byType['performance-stall'],
    'the count-match invariant is unaffected by the new type',
  );
  assert.equal(s.byType['server-stall'], 1, 'and the server-stall is still counted on the count axis');
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

// ── PER-OPERATION AGGREGATE (WARDEN-1435) ─────────────────────────────────────
// `operations` is the per-operation axis the `operational-metrics` COUNT
// (`byType`) discards: a day of 5-minute windows carrying every /api route's
// latency reduces on `byType` to `"operational-metrics": 288`. It folds every
// window's `operations[]` across the retained set into bounded per-NAME
// buckets carrying count / okCount / failCount / min / avg / max — the
// crashReasons-style histogram discipline with a stall-style magnitude inside
// each bucket. NO histogram axis is projected (see the mixed-scales test
// below for why); the numbers are scale-free and always comparable.

// Copyable fixtures. The server-side scale (8 boundaries / 9 buckets — the
// DEFAULT_BUCKET_BOUNDARIES_MS shape requestTelemetry ships) and the renderer
// scale (12 boundaries / 13 buckets — PANE_LATENCY_BOUNDARIES_MS, reaching the
// wire through buildOperationalMetricsEvent) are the two REAL scales the
// receiver's store holds for this ONE event type.
const SERVER_BOUNDARIES = [50, 100, 250, 500, 1000, 2500, 5000, 10000];
const RENDERER_BOUNDARIES = [25, 50, 75, 100, 150, 200, 300, 500, 800, 1200, 2000, 5000];
const op = (operation, count, okCount, failCount, min, avg, max, buckets) => ({
  operation, count, okCount, failCount, min, avg, max, buckets,
});
const metricsWindow = (operations, overrides = {}) => ({
  schemaVersion: 8,
  type: 'operational-metrics',
  runtime: 'server',
  timestamp: 4,
  windowStartedAt: 1,
  windowEndedAt: 4,
  boundaries: SERVER_BOUNDARIES,
  operations,
  rejected: 0,
  ...overrides,
});

test('operations is a stable empty shape on a metrics-free store (no false alarm)', () => {
  // Non-metrics events contribute nothing — operations reads only
  // operational-metrics windows.
  assert.deepEqual(summarize([validError, validCrash, validStall]).operations, {});
  assert.deepEqual(summarize([]).operations, {});
});

test('operations folds a window into one bucket per operation name', () => {
  const s = summarize([metricsWindow([
    op('file-exists-local', 2, 1, 1, 0.5, 1, 1.5, [2, 0, 0, 0, 0, 0, 0, 0, 0]),
    op('file-exists-remote', 1, 1, 0, 300, 300, 300, [0, 0, 1, 0, 0, 0, 0, 0, 0]),
  ])]);
  assert.deepEqual(s.operations, {
    'file-exists-local': { count: 2, okCount: 1, failCount: 1, min: 0.5, avg: 1, max: 1.5 },
    'file-exists-remote': { count: 1, okCount: 1, failCount: 0, min: 300, avg: 300, max: 300 },
  });
});

test('operations folds across windows — Σ bucket.count equals Σ per-entry count (no count loss)', () => {
  // The crashReasons-style equality invariant, held through the cross-window
  // fold: every seeded observation is represented exactly once.
  const windows = [
    metricsWindow([op('get-api-claude-sessions', 10, 9, 1, 80, 100, 4000, [1, 2, 3, 2, 1, 1, 0, 0, 0])]),
    metricsWindow([op('get-api-claude-sessions', 1, 1, 0, 950, 1000, 1000, [0, 0, 0, 0, 1, 0, 0, 0, 0])]),
    metricsWindow([op('fs-read-file-sync', 4, 4, 0, 5, 6, 9, [4, 0, 0, 0, 0, 0, 0, 0, 0])]),
  ];
  const s = summarize(windows);
  assert.equal(s.operations['get-api-claude-sessions'].count, 11, 'same name across windows folds into ONE bucket');
  const sumOfBuckets = Object.values(s.operations).reduce((a, b) => a + b.count, 0);
  const sumOfEntries = windows.flatMap((w) => w.operations).reduce((a, e) => a + e.count, 0);
  assert.equal(sumOfBuckets, sumOfEntries, 'Σ buckets == Σ entries');
});

test('a slow route surfaces its true max beside fast ones — no /events paging needed', () => {
  // The read the count axis cannot give: one 4s route in a sea of 30ms ones is
  // a single bucket away, not N raw windows to re-fold by hand.
  const s = summarize([
    metricsWindow([
      op('get-api-fast', 20, 20, 0, 10, 12, 30, [20, 0, 0, 0, 0, 0, 0, 0, 0]),
      op('get-api-slow-route', 5, 5, 0, 3000, 3500, 4000, [0, 0, 0, 0, 0, 5, 0, 0, 0]),
    ]),
    metricsWindow([op('get-api-fast', 20, 20, 0, 10, 12, 30, [20, 0, 0, 0, 0, 0, 0, 0, 0])]),
  ]);
  assert.equal(s.operations['get-api-slow-route'].max, 4000);
  assert.equal(s.operations['get-api-fast'].max, 30);
  assert.equal(s.operations['get-api-fast'].count, 40, 'the fast route folded across both windows');
});

test('avg is WEIGHTED — Σ(avg×count)/Σcount, never a mean of window means', () => {
  // 10 observations @ avg 100 + 1 observation @ avg 1000: weighted reads
  // 2000/11 ≈ 181.8; a mean of means would read 550 — nearly 3× wrong, and it
  // would get WORSE the more windows carried the slow tail.
  const s = summarize([
    metricsWindow([op('get-api-claude-sessions', 10, 10, 0, 80, 100, 200, [10, 0, 0, 0, 0, 0, 0, 0, 0])]),
    metricsWindow([op('get-api-claude-sessions', 1, 1, 0, 950, 1000, 1100, [0, 0, 0, 0, 1, 0, 0, 0, 0])]),
  ]);
  const b = s.operations['get-api-claude-sessions'];
  assert.equal(b.count, 11);
  assert.ok(Math.abs(b.avg - 2000 / 11) < 1e-9, `weighted avg ≈ 181.8 (got ${b.avg})`);
  assert.ok(Math.abs(b.avg - 550) > 1, 'explicitly NOT the mean of the two window means');
});

test('okCount + failCount == count holds per bucket for producer-satisfying inputs, and a fail-heavy operation is identifiable by ratio alone', () => {
  // The producer maintains the identity but the validator range-checks the
  // three integers INDEPENDENTLY, so the receiver's fold must PRESERVE it (Σok
  // + Σfail == Σcount) without ever ASSUMING it.
  const s = summarize([
    metricsWindow([
      op('get-api-healthy', 9, 9, 0, 10, 12, 30, [9, 0, 0, 0, 0, 0, 0, 0, 0]),
      op('get-api-degraded', 4, 1, 3, 500, 800, 2000, [0, 0, 0, 1, 1, 1, 1, 0, 0]),
    ]),
    metricsWindow([op('get-api-degraded', 6, 2, 4, 400, 700, 1500, [0, 0, 0, 2, 2, 1, 1, 0, 0])]),
  ]);
  for (const [name, b] of Object.entries(s.operations)) {
    assert.equal(b.okCount + b.failCount, b.count, `${name}: the identity survives the fold`);
  }
  const degraded = s.operations['get-api-degraded'];
  assert.equal(degraded.count, 10);
  assert.equal(degraded.failCount, 7, 'failures fold across windows');
  assert.equal(degraded.failCount / degraded.count, 0.7, 'a 70% failure ratio is readable from the bucket alone');
});

test('a zero-count placeholder entry does not corrupt the fold (idle renderer windows)', () => {
  // paneLatency.ts resets its accumulators but KEEPS the keys, so idle windows
  // carry { count: 0, min: 0, avg: 0, max: 0 } placeholders beside real
  // observations in other windows. Folding a placeholder's extrema would
  // report a false `min: 0` for an operation that was simply idle — the most
  // likely silent defect in this fold.
  const s = summarize([
    metricsWindow(
      [op('pane-echo-e2e', 0, 0, 0, 0, 0, 0, new Array(13).fill(0))],
      { boundaries: RENDERER_BOUNDARIES, runtime: 'renderer' }
    ),
    metricsWindow(
      [op('pane-echo-e2e', 1, 1, 0, 250, 250, 250, new Array(13).fill(0).map((v, i) => (i === 3 ? 1 : v)))],
      { boundaries: RENDERER_BOUNDARIES, runtime: 'renderer' }
    ),
  ]);
  const b = s.operations['pane-echo-e2e'];
  assert.equal(b.count, 1, 'the placeholder contributes no count');
  assert.equal(b.min, 250, 'min is the real observation, NOT the placeholder 0');
  assert.equal(b.max, 250);
  assert.equal(b.avg, 250);
  assert.equal(JSON.stringify(s.operations).includes('null'), false, 'no null/NaN anywhere in the payload');
});

test('a non-finite min/max/avg is skipped from the stats but its count still folds (defence over a NON-validated row)', () => {
  // Defence-in-depth, NOT a wire case: the ingest validator rejects non-finite
  // values, so this reaches summarize() only off a hand-written / partial-read
  // row. The honesty posture mirrors `stalls`: skip the value, keep the
  // observation. min/max read null (never 0) when nothing finite was folded.
  const s = summarize([metricsWindow([
    { operation: 'hand-written-row', count: 2, okCount: 1, failCount: 1, min: NaN, avg: Infinity, max: NaN, buckets: [] },
    op('hand-written-row', 1, 1, 0, 500, 500, 500, [1, 0, 0, 0, 0, 0, 0, 0, 0]),
  ])]);
  const b = s.operations['hand-written-row'];
  assert.equal(b.count, 3, 'every admitted observation is still counted');
  assert.equal(b.okCount + b.failCount, b.count);
  assert.equal(b.min, 500);
  assert.equal(b.max, 500, 'extrema reflect ONLY the finite record');
  assert.equal(b.avg, 500, 'the weighted avg reflects ONLY the finite record');
});

test('a count-bearing row with NO finite stats reads count with null extrema and avg 0 — absence is not 0', () => {
  // The `min`/`max` are `null` rather than `0` honesty clause: `0` is a REAL
  // measured duration here (a cache hit), so it can never double as the empty
  // sentinel.
  const s = summarize([metricsWindow([
    { operation: 'stats-less-row', count: 5, okCount: 5, failCount: 0, buckets: [] },
  ])]);
  assert.deepEqual(s.operations['stats-less-row'], { count: 5, okCount: 5, failCount: 0, min: null, avg: 0, max: null });
});

test('a malformed or partial operations entry is skipped whole — never fatal, event still counted', () => {
  const s = summarize([
    metricsWindow([
      null, // non-object entry
      'garbage', // primitive entry
      {}, // missing operation AND count
      { operation: 'no-count' }, // partial: no count
      { operation: 42, count: 5 }, // non-string operation
      { operation: '', count: 5 }, // empty operation
      { operation: 'negative-count', count: -1, okCount: 0, failCount: 0, min: 1, avg: 1, max: 1, buckets: [] },
      { operation: 'nan-count', count: NaN, okCount: 0, failCount: 0, min: 1, avg: 1, max: 1, buckets: [] },
      op('real-op', 3, 2, 1, 10, 20, 30, [3, 0, 0, 0, 0, 0, 0, 0, 0]),
    ]),
    metricsWindow('not-an-array'), // a non-array operations field is skipped, not fatal
  ]);
  assert.deepEqual(
    s.operations,
    { 'real-op': { count: 3, okCount: 2, failCount: 1, min: 10, avg: 20, max: 30 } },
    'only the well-formed entry is bucketed; nothing throws, no NaN poisons the bucket'
  );
  assert.equal(s.byType['operational-metrics'], 2, 'both events still count in byType');
});

test('over-cap distinct names fold into ONE counted __overflow__ bucket with no count loss', () => {
  // The cap is anchored to the wire's own structural bound
  // (MAX_OPERATIONS_PER_EVENT = 129), NOT the free-text histograms' 10: the
  // live name space is ~104 names, and a cap of 10 would answer "which route
  // is slow?" with __overflow__.
  const names = [];
  for (let i = 0; i < OPERATIONS_SUMMARY_CAP + 10; i += 1) names.push(`op-${String(i).padStart(3, '0')}`);
  const s = summarize([metricsWindow(names.map((n, i) => op(n, 1, 1, 0, i, i, i, [1, 0, 0, 0, 0, 0, 0, 0, 0])))]);
  const keys = Object.keys(s.operations);
  assert.equal(keys.length, OPERATIONS_SUMMARY_CAP + 1, 'bounded: cap distinct names + one overflow bucket');
  assert.ok(keys.includes('__overflow__'), 'the overflow bucket exists');
  assert.equal(s.operations.__overflow__.count, 10, 'the over-cap observations are REPRESENTED, not dropped');
  const sumOfBuckets = Object.values(s.operations).reduce((a, b) => a + b.count, 0);
  assert.equal(sumOfBuckets, names.length, 'no count loss in the overflow fold');
});

test('mixed boundary scales (9-bucket server + 13-bucket renderer) fold coherently — and NO histogram axis is projected', () => {
  // The two live producers ship INCOMPATIBLE bucket scales into this ONE event
  // type. The fold reads none of `buckets[]` / `boundaries`, so it can never
  // hit an array-length error or silently index-sum two different x-axes into
  // one meaningless array — the refusal is structural.
  const s = summarize([
    metricsWindow(
      [op('get-api-claude-sessions', 2, 2, 0, 60, 75, 90, [2, 0, 0, 0, 0, 0, 0, 0, 0])],
      { boundaries: SERVER_BOUNDARIES }
    ),
    metricsWindow(
      [op('get-api-claude-sessions', 3, 3, 0, 40, 60, 150, [0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])],
      { boundaries: RENDERER_BOUNDARIES, runtime: 'renderer' }
    ),
  ]);
  assert.deepEqual(
    s.operations['get-api-claude-sessions'],
    { count: 5, okCount: 5, failCount: 0, min: 40, avg: 66, max: 150 },
    'one coherent scale-free bucket (weighted avg = (75×2 + 60×3)/5 = 66)'
  );
  assert.equal('buckets' in s.operations['get-api-claude-sessions'], false, 'no histogram axis: the two x-axes cannot merge');
  assert.equal(Object.keys(s.operations).length, 1, 'the shared name is ONE bucket');
});

test('an over-128-char operation name is truncated — defence-in-depth over a NON-validated store row', () => {
  // NOT a wire need: OPERATION_NAME_RE (schema.ts) caps a validator-accepted
  // operation name at 64 chars, so truncation can never fire on an accepted
  // event (there is no fixture for it — manufacturing one would fake a wire
  // gap). This pins the bound that defends a hand-written / partial-read row,
  // the same posture as the client-keyed histograms.
  const s = summarize([metricsWindow([op('a'.repeat(200), 1, 1, 0, 5, 5, 5, [1, 0, 0, 0, 0, 0, 0, 0, 0])])]);
  const keys = Object.keys(s.operations);
  assert.equal(keys.length, 1);
  assert.equal(keys[0].length, CLIENT_KEY_MAX_LENGTH, 'truncated at the shared client-key bound');
});

// ── operationLatency (WARDEN-1500) ────────────────────────────────────────────
// Per-SCALE latency distribution + nearest-rank bucket-upper-bound percentiles.

const lat = (windows, name) => summarize(windows).operationLatency[name];
const zeros9 = () => [0, 0, 0, 0, 0, 0, 0, 0, 0];

test('operationLatency: the live bimodal route reads p50 ≤50 / p95 ≤10000 while operations.avg is unchanged', () => {
  const buckets = [1696, 4, 36, 8, 2, 38, 49, 340, 0];
  const w = metricsWindow([op('get-api-agent-states', 2173, 2173, 0, 1, 1212, 8837, buckets)]);
  const s = summarize([w]);
  const l = s.operationLatency['get-api-agent-states'];
  assert.deepEqual(l.boundaries, SERVER_BOUNDARIES);
  assert.deepEqual(l.buckets, buckets);
  assert.equal(l.histogramCount, 2173);
  assert.equal(l.excludedCount, 0);
  assert.equal(l.p50, 50);
  assert.equal(l.p95, 10000);
  assert.equal(l.p99, 10000);
  assert.deepEqual(s.operations['get-api-agent-states'], { count: 2173, okCount: 2173, failCount: 0, min: 1, avg: 1212, max: 8837 });
});

test('operationLatency: nearest-rank uses ceil — 95 in bucket 0, 5 in bucket 3 → p95 bucket-0 bound, p99 bucket-3 bound', () => {
  const b = zeros9();
  b[0] = 95;
  b[3] = 5;
  const l = lat([metricsWindow([op('x-op', 100, 100, 0, 1, 1, 1, b)])], 'x-op');
  assert.equal(l.p50, 50);
  assert.equal(l.p95, 50);
  assert.equal(l.p99, 500);
});

test('operationLatency: folds the SAME scale across windows', () => {
  const a = zeros9(); a[0] = 3;
  const b = zeros9(); b[2] = 1;
  const l = lat([metricsWindow([op('x-op', 3, 3, 0, 1, 1, 1, a)]), metricsWindow([op('x-op', 1, 1, 0, 1, 1, 1, b)])], 'x-op');
  assert.deepEqual(l.buckets, [3, 0, 1, 0, 0, 0, 0, 0, 0]);
  assert.equal(l.histogramCount, 4);
});

test('operationLatency: overflow-only observations → null percentiles, buckets[last] > 0', () => {
  const b = zeros9(); b[8] = 4;
  const l = lat([metricsWindow([op('x-op', 4, 4, 0, 20000, 20000, 20000, b)])], 'x-op');
  assert.equal(l.p50, null);
  assert.equal(l.p95, null);
  assert.equal(l.p99, null);
  assert.equal(l.histogramCount, 4);
  assert.ok(l.buckets[l.buckets.length - 1] > 0);
});

test('operationLatency: empty histogram (zero-count placeholder) → null percentiles, histogramCount 0, no NaN', () => {
  const s = summarize([metricsWindow([op('idle-op', 0, 0, 0, 0, 0, 0, zeros9())])]);
  const l = s.operationLatency['idle-op'];
  assert.deepEqual(l, { boundaries: [], buckets: [], histogramCount: 0, excludedCount: 0, p50: null, p95: null, p99: null });
  assert.equal(JSON.stringify(s.operationLatency).includes('NaN'), false);
});

test('operationLatency: mixed scales — larger-count scale reported, other in excludedCount, never index-summed', () => {
  const server = zeros9(); server[0] = 2;
  const renderer = new Array(13).fill(0); renderer[2] = 3;
  const l = lat([
    metricsWindow([op('get-api-claude-sessions', 2, 2, 0, 60, 75, 90, server)], { boundaries: SERVER_BOUNDARIES }),
    metricsWindow([op('get-api-claude-sessions', 3, 3, 0, 40, 60, 150, renderer)], { boundaries: RENDERER_BOUNDARIES, runtime: 'renderer' }),
  ], 'get-api-claude-sessions');
  assert.deepEqual(l.boundaries, RENDERER_BOUNDARIES);
  assert.deepEqual(l.buckets, renderer);
  assert.equal(l.histogramCount, 3);
  assert.equal(l.excludedCount, 2);
  assert.equal(l.p50, 75);
});

test('operationLatency: distinct scales past the cap fold into excludedCount (bounded, no loss)', () => {
  const wins = [];
  for (let i = 0; i < 7; i += 1) {
    // Six distinct 1-boundary scales, i-th carries i+1 observations; scale 0..3 tracked.
    wins.push(metricsWindow([op('x-op', i + 1, i + 1, 0, 1, 1, 1, [i + 1, 0])], { boundaries: [100 + i] }));
  }
  const l = lat(wins, 'x-op');
  const total = 1 + 2 + 3 + 4 + 5 + 6 + 7;
  // Only the first OPERATION_LATENCY_SCALE_CAP (4) scales are tracked; the largest of them (4 obs) is reported.
  assert.equal(l.histogramCount, 4);
  assert.equal(l.histogramCount + l.excludedCount, total);
});

test('operationLatency: unusable buckets contribute nothing to the histogram but still fold into operations and byType', () => {
  const good = zeros9(); good[1] = 2;
  const s = summarize([
    metricsWindow([
      op('x-op', 2, 2, 0, 80, 90, 100, good),
      op('x-op', 5, 5, 0, 10, 10, 10, [1, 2, 3]), // wrong length
      op('x-op', 5, 5, 0, 10, 10, 10, [NaN, 0, 0, 0, 0, 0, 0, 0, 0]),
      op('x-op', 5, 5, 0, 10, 10, 10, [-1, 0, 0, 0, 0, 0, 0, 0, 0]),
      op('x-op', 5, 5, 0, 10, 10, 10, [1.5, 0, 0, 0, 0, 0, 0, 0, 0]),
      op('x-op', 5, 5, 0, 10, 10, 10, 'nope'),
      op('x-op', 5, 5, 0, 10, 10, 10, undefined),
    ]),
    metricsWindow([op('x-op', 1, 1, 0, 1, 1, 1, zeros9())], { boundaries: 'bad' }),
  ]);
  const l = s.operationLatency['x-op'];
  assert.equal(l.histogramCount, 2);
  assert.equal(l.excludedCount, 0);
  assert.deepEqual(l.buckets, [0, 2, 0, 0, 0, 0, 0, 0, 0]);
  assert.equal(s.operations['x-op'].count, 33, 'operations still folds every entry');
  assert.equal(s.byType['operational-metrics'], 2);
  assert.equal(JSON.stringify(s.operationLatency).includes('NaN'), false);
});

test('operationLatency: >129 names fold to __overflow__ with no observation loss, sharing operations\' key set', () => {
  const names = [];
  for (let i = 0; i < OPERATIONS_SUMMARY_CAP + 10; i += 1) names.push(`op-${String(i).padStart(3, '0')}`);
  const s = summarize([metricsWindow(names.map((n) => op(n, 1, 1, 0, 1, 1, 1, [1, 0, 0, 0, 0, 0, 0, 0, 0])))]);
  assert.deepEqual(Object.keys(s.operationLatency).sort(), Object.keys(s.operations).sort());
  assert.equal(s.operationLatency.__overflow__.histogramCount, 10);
  const total = Object.values(s.operationLatency).reduce((a, b) => a + b.histogramCount, 0);
  assert.equal(total, names.length);
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
  assert.deepEqual(s.byType, { error: 1, crash: 1, 'performance-stall': 0, 'operational-metrics': 0, 'server-stall': 0, 'workspace-names': 0, 'workspace-shape': 0,
    'feature-usage': 0, 'process-memory': 0 });
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

// ── STALLS TIMELINE — bounded stall-SEVERITY distribution (WARDEN-886) ─────────
// `summarizeStallsTimeline` is the TEMPORAL twin of the `stalls` magnitude snapshot
// (WARDEN-854): a per-bucket `max` freeze `lagMs` (overall + split by `source`)
// over the SAME rolling window / granularity as `summarizeTimeline` (the two SHARE
// the pure bucket-assignment helper, so they can never drift). It exists because
// `stalls.max` collapses the whole window into one number with no time axis: a 5s
// freeze minutes ago (ACTIVE) and a 5s freeze hours ago (RESOLVED) read byte-
// identically on `stalls`. The timeline places the worst freeze in TIME. Mirrors
// the timeline-test pattern: fake `now`, small `windowMs`/`maxBuckets` for exact
// bucket arithmetic (window [0,100], 10 buckets of width 10 here). `validStall`
// carries lagMs:750 + source:'event-loop'.

test('stallsTimeline: empty input → zeroed shape (no false alarm on a quiet store)', () => {
  const t = summarizeStallsTimeline([], { now: () => 10_000_000 });
  assert.deepEqual(t.buckets, []);
  assert.ok(t.bucketMs > 0, 'bucketMs conveys the granularity even when empty');
});

test('stallsTimeline: non-array input is treated as empty (defensive — never throws)', () => {
  const opts = { now: () => 0 };
  assert.deepEqual(summarizeStallsTimeline(undefined, opts), summarizeStallsTimeline([], opts));
  assert.deepEqual(summarizeStallsTimeline(null, opts), summarizeStallsTimeline([], opts));
  assert.deepEqual(summarizeStallsTimeline('nope', opts), summarizeStallsTimeline([], opts));
});

test('stallsTimeline: stalls within the window land in the correct bucket with count + max + bySource', () => {
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: 95, lagMs: 5000, source: 'unresponsive' },
      { ...validStall, timestamp: 5, lagMs: 50, source: 'event-loop' },
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    { bucketStart: 0, bucketEnd: 10, count: 1, max: 50, bySource: { 'event-loop': { count: 1, max: 50 } } },
    { bucketStart: 90, bucketEnd: 100, count: 1, max: 5000, bySource: { unresponsive: { count: 1, max: 5000 } } },
  ]);
  assert.equal(t.bucketMs, 10);
});

test('stallsTimeline: multiple stalls in the same bucket accumulate count + track the worst max', () => {
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: 1, lagMs: 50, source: 'event-loop' },
      { ...validStall, timestamp: 2, lagMs: 5000, source: 'event-loop' },
      { ...validStall, timestamp: 9, lagMs: 200, source: 'unresponsive' },
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    {
      bucketStart: 0, bucketEnd: 10, count: 3, max: 5000,
      bySource: {
        'event-loop': { count: 2, max: 5000 },
        unresponsive: { count: 1, max: 200 },
      },
    },
  ]);
});

test('stallsTimeline: a non-finite lagMs (NaN / Infinity) is skipped from max but STILL counted', () => {
  // The load-bearing WARDEN-854 guard, inherited by the timeline: validateBaseEvent
  // only typeof-checks lagMs (schema.ts), so NaN / Infinity can reach here. An
  // unguarded per-bucket Math.max would poison the bucket from one bad record.
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: 1, lagMs: NaN, source: 'event-loop' },
      { ...validStall, timestamp: 2, lagMs: Infinity, source: 'event-loop' },
      { ...validStall, timestamp: 3, lagMs: 500, source: 'event-loop' },
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    {
      bucketStart: 0, bucketEnd: 10, count: 3, max: 500,
      bySource: { 'event-loop': { count: 3, max: 500 } },
    },
  ]);
});

test('stallsTimeline: an absent lagMs is skipped from max but STILL counted', () => {
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: 1, lagMs: undefined, source: 'event-loop' },
      { ...validStall, timestamp: 2, lagMs: 500, source: 'event-loop' },
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    { bucketStart: 0, bucketEnd: 10, count: 2, max: 500, bySource: { 'event-loop': { count: 2, max: 500 } } },
  ]);
});

test('stallsTimeline: max is null when NO finite lagMs reached the bucket (but the stalls are still counted)', () => {
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: 1, lagMs: NaN, source: 'event-loop' },
      { ...validStall, timestamp: 2, lagMs: undefined, source: 'unresponsive' },
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    {
      bucketStart: 0, bucketEnd: 10, count: 2, max: null,
      bySource: {
        'event-loop': { count: 1, max: null },
        unresponsive: { count: 1, max: null },
      },
    },
  ]);
});

test('stallsTimeline: a sourceless stall is counted + feeds the overall max but yields no bySource entry', () => {
  // A stall with no `source` is malformed; it is counted and its magnitude feeds the
  // bucket's overall `max`, but it yields no per-source bucket (mirrors signatureOf,
  // which returns null for a sourceless stall).
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: 1, lagMs: 500, source: 'event-loop' },
      { type: 'performance-stall', timestamp: 2, lagMs: 9000 }, // no source
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    {
      bucketStart: 0, bucketEnd: 10, count: 2, max: 9000,
      bySource: { 'event-loop': { count: 1, max: 500 } },
    },
  ]);
});

test('stallsTimeline: non-stall events (errors / crashes) never fire a bucket (reads only performance-stall)', () => {
  // A bucket that WOULD fire on the COUNT timeline (errors land here) fires NOTHING
  // on the stall timeline — non-stalls are dropped before bucketing, so a bucket
  // appears ONLY when a stall lands in it (no false "0 stalls here" noise).
  const t = summarizeStallsTimeline([validError, validCrash], {
    now: () => 100,
    windowMs: 100,
    maxBuckets: 10,
  });
  assert.deepEqual(t.buckets, []);
  assert.ok(t.bucketMs > 0, 'bucketMs conveys the granularity even when no stall fired');
});

test('stallsTimeline: buckets are sorted chronologically (oldest → newest)', () => {
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: 95 },
      { ...validStall, timestamp: 5 },
      { ...validStall, timestamp: 50 },
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(
    t.buckets.map((b) => b.bucketStart),
    [0, 50, 90]
  );
});

test('stallsTimeline: a stall timestamped exactly `now` lands in the newest bucket (top-boundary fold)', () => {
  const t = summarizeStallsTimeline([{ ...validStall, timestamp: 100 }], {
    now: () => 100,
    windowMs: 100,
    maxBuckets: 10,
  });
  assert.deepEqual(t.buckets, [
    { bucketStart: 90, bucketEnd: 100, count: 1, max: 750, bySource: { 'event-loop': { count: 1, max: 750 } } },
  ]);
});

test('stallsTimeline: stalls older than the rolling window are EXCLUDED from the timeline', () => {
  // window [100, 200]; a stall at 50 (before windowStart) is excluded — it is still
  // counted by summarize()'s stalls snapshot (the full retained set), just not in the
  // recent per-bucket distribution.
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: 50, lagMs: 5000 },
      { ...validStall, timestamp: 150, lagMs: 100 },
    ],
    { now: () => 200, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    { bucketStart: 150, bucketEnd: 160, count: 1, max: 100, bySource: { 'event-loop': { count: 1, max: 100 } } },
  ]);
});

test('stallsTimeline: receivedAt is PREFERRED over timestamp (clock-skew robust, parity with timeline)', () => {
  // A fast-clock client whose `timestamp` is far in the future still lands in-window
  // via the receiver's receivedAt — the spike-vs-baseline read is skew-robust (the
  // WARDEN-692 cutover the COUNT timeline already has).
  const t = summarizeStallsTimeline(
    [{ ...validStall, timestamp: 200 + 300_000, receivedAt: 150, lagMs: 5000, source: 'unresponsive' }],
    { now: () => 200, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    { bucketStart: 150, bucketEnd: 160, count: 1, max: 5000, bySource: { unresponsive: { count: 1, max: 5000 } } },
  ]);
});

test('stallsTimeline: the window ROLLS with `now` — the same stall ages out as now advances', () => {
  const events = [{ ...validStall, timestamp: 50 }];
  assert.equal(
    summarizeStallsTimeline(events, { now: () => 100, windowMs: 100, maxBuckets: 10 }).buckets.length,
    1
  );
  assert.deepEqual(
    summarizeStallsTimeline(events, { now: () => 1_000_000, windowMs: 100, maxBuckets: 10 }).buckets,
    []
  );
});

test('stallsTimeline: bucket count is capped at maxBuckets however many stalls span the window', () => {
  const events = [];
  for (let i = 0; i < 200; i++) events.push({ ...validStall, timestamp: i });
  const t = summarizeStallsTimeline(events, { now: () => 200, windowMs: 200, maxBuckets: 20 });
  assert.ok(t.buckets.length <= 20, 'never more than maxBuckets buckets');
  assert.equal(t.buckets.length, 20, 'every grid slot is hit → exactly maxBuckets');
  assert.equal(t.buckets.reduce((sum, b) => sum + b.count, 0), 200, 'no stall lost');
});

test('stallsTimeline: non-finite / malformed timestamps are ignored (skip-robust, never fatal)', () => {
  const t = summarizeStallsTimeline(
    [
      { ...validStall, timestamp: Infinity },
      { ...validStall, timestamp: NaN },
      { ...validStall, timestamp: 'nope' },
      { ...validStall, timestamp: 50 },
      null,
      42,
      'str',
    ],
    { now: () => 100, windowMs: 100, maxBuckets: 10 }
  );
  assert.deepEqual(t.buckets, [
    { bucketStart: 50, bucketEnd: 60, count: 1, max: 750, bySource: { 'event-loop': { count: 1, max: 750 } } },
  ]);
});

test('stallsTimeline: never echoes raw events or extended-tier identifiers (lagMs / source / time only)', () => {
  const t = summarizeStallsTimeline(
    [
      {
        ...validStall,
        timestamp: 50,
        lagMs: 5000,
        source: 'unresponsive',
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
  // the non-identifying magnitude + source ARE aggregated
  assert.equal(json.includes('5000'), true, 'lagMs magnitude is in the bucket max');
  assert.equal(json.includes('unresponsive'), true, 'source is a bucket key');
});

test('stallsTimeline: a degenerate config (non-positive window / maxBuckets) collapses to a zeroed shape', () => {
  const events = [{ ...validStall, timestamp: 5 }];
  assert.deepEqual(
    summarizeStallsTimeline(events, { now: () => 100, windowMs: 0, maxBuckets: 10 }),
    { buckets: [], bucketMs: 0 }
  );
  assert.deepEqual(
    summarizeStallsTimeline(events, { now: () => 100, windowMs: 100, maxBuckets: 0 }),
    { buckets: [], bucketMs: 0 }
  );
});

test('stallsTimeline: bucket boundaries are byte-identical to summarizeTimeline for the same events (never drift)', () => {
  // The shared _assignTimelineBuckets helper guarantees the COUNT timeline and the
  // stall timeline can NEVER drift on window, granularity, or bucket boundary.
  const events = [
    { ...validStall, timestamp: 5 },
    { ...validStall, timestamp: 50 },
    { ...validStall, timestamp: 95 },
  ];
  const opts = { now: () => 100, windowMs: 100, maxBuckets: 10 };
  const count = summarizeTimeline(events, opts);
  const stall = summarizeStallsTimeline(events, opts);
  assert.deepEqual(
    stall.buckets.map((b) => ({ bucketStart: b.bucketStart, bucketEnd: b.bucketEnd })),
    count.buckets.map((b) => ({ bucketStart: b.bucketStart, bucketEnd: b.bucketEnd })),
    'byte-identical bucket boundaries (never drift)'
  );
  assert.equal(stall.bucketMs, count.bucketMs);
  // the per-bucket stall COUNT on the severity timeline matches the COUNT timeline
  assert.deepEqual(
    stall.buckets.map((b) => b.count),
    count.buckets.map((b) => b.count)
  );
});

// THE HEADLINE — the spike-vs-baseline property the `stalls` SNAPSHOT provably lacks.
test('stallsTimeline: the HEADLINE — two stores with byte-identical stalls.max but different temporal placement read differently', () => {
  // `stalls.max` collapses the whole window into one number, so a 5s freeze minutes
  // ago (ACTIVE — users feeling it now) and a 5s freeze hours ago (RESOLVED — already
  // gone) read byte-identical on the snapshot. The timeline places the worst freeze
  // in TIME, so the maintainer's "is this still happening?" becomes answerable.
  // window [0, 100], 10 buckets of width 10.
  const opts = { now: () => 100, windowMs: 100, maxBuckets: 10 };
  // Store A: the 5000ms freeze is in the NEWEST bucket (bucket 9, [90,100)) — ACTIVE.
  const storeA = [{ ...validStall, timestamp: 95, lagMs: 5000, source: 'unresponsive' }];
  // Store B: the 5000ms freeze is in an OLDER bucket (bucket 0, [0,10)) — RESOLVED.
  const storeB = [{ ...validStall, timestamp: 5, lagMs: 5000, source: 'unresponsive' }];

  // Both read byte-identically on the `stalls` SNAPSHOT — it cannot tell them apart.
  assert.equal(summarize(storeA).stalls.max, 5000);
  assert.equal(summarize(storeB).stalls.max, 5000);
  assert.equal(
    summarize(storeA).stalls.max,
    summarize(storeB).stalls.max,
    'the snapshot collapses both into the same max — it cannot answer "is this recent?"'
  );

  // ...but the TIMELINE places the worst freeze in DIFFERENT buckets.
  const tlA = summarizeStallsTimeline(storeA, opts);
  const tlB = summarizeStallsTimeline(storeB, opts);
  assert.deepEqual(tlA.buckets, [
    {
      bucketStart: 90, bucketEnd: 100, count: 1, max: 5000,
      bySource: { unresponsive: { count: 1, max: 5000 } },
    },
  ], 'ACTIVE regression — the worst freeze is in the NEWEST bucket');
  assert.deepEqual(tlB.buckets, [
    {
      bucketStart: 0, bucketEnd: 10, count: 1, max: 5000,
      bySource: { unresponsive: { count: 1, max: 5000 } },
    },
  ], 'RESOLVED blip — the same worst freeze is in an OLDER bucket');
  assert.notEqual(
    tlA.buckets[0].bucketStart, tlB.buckets[0].bucketStart,
    'the active regression and the resolved blip now read APART in time'
  );
});

// ── CLIENT-KEYED HISTOGRAM BOUNDS (WARDEN-1246) ───────────────────────────────
// No single accepted event may permanently inflate every /summary response:
// the client-keyed breakdowns are bounded in BOTH key length and distinct-key
// cardinality, with overflow REPRESENTED via the `__overflow__` sentinel (the
// same shape as createRejectionTally's byDeclaredVersion, WARDEN-829).

// ── NULL-PROTOTYPE HISTOGRAMS (WARDEN-1524) ───────────────────────────────────
// The client-keyed plain-object histograms must treat hostile keys
// ("__proto__" / "constructor" / "toString") as ORDINARY own keys, exactly like
// the Map-backed featureUsage sibling. On a plain `{}` "__proto__" writes were
// silent no-ops and "constructor" counts became inherited-function STRINGS.

test('topErrorNames treats hostile error names as ordinary own keys with NUMERIC counts', () => {
  const ev = (name, t) => ({ type: 'error', timestamp: t, name });
  const s = summarize([
    ev('TypeError', 1), ev('TypeError', 2), ev('TypeError', 3),
    ev('constructor', 4), ev('constructor', 5),
    ev('toString', 6),
    ev('__proto__', 7), ev('__proto__', 8),
  ]);
  const byName = Object.fromEntries(s.topErrorNames.map((e) => [e.name, e.count]));
  assert.equal(byName.TypeError, 3);
  assert.strictEqual(byName.constructor, 2, 'constructor counts as a NUMBER, not an inherited-function string');
  assert.strictEqual(byName.toString, 1);
  assert.strictEqual(byName.__proto__, 2, '__proto__ events are bucketed, not silently dropped');
  assert.ok(s.topErrorNames.every((e) => typeof e.count === 'number'));
  assert.equal(s.topErrorNames.reduce((n, e) => n + e.count, 0), s.byType.error, 'histogram total equals the error count');
});

test('a "__proto__" client key buckets as an ordinary own key and does not trigger the overflow fold', () => {
  const events = [];
  for (let i = 0; i < CLIENT_HISTOGRAM_CAP + 2; i += 1) {
    events.push({ type: 'error', timestamp: i + 1, name: 'E', platform: '__proto__' });
  }
  events.push({ type: 'error', timestamp: 100, name: 'E', platform: 'darwin' });
  events.push({ type: 'crash', timestamp: 101, reason: 'constructor' });
  events.push({ type: 'crash', timestamp: 102, reason: '__proto__' });
  const s = summarize(events);
  assert.strictEqual(s.platforms.__proto__, CLIENT_HISTOGRAM_CAP + 2, '__proto__ events are counted');
  assert.strictEqual(s.platforms.darwin, 1, 'a legitimate key is not mis-folded');
  assert.equal(Object.prototype.hasOwnProperty.call(s.platforms, '__overflow__'), false, 'no spurious overflow');
  assert.strictEqual(s.crashReasons.constructor, 1, 'constructor reason counts as a number');
  assert.strictEqual(s.crashReasons.__proto__, 1);
  // The key survives JSON as an ordinary own key.
  assert.equal(JSON.parse(JSON.stringify(s.platforms)).darwin, 1);
});

test('an oversized client key is truncated in every client-keyed histogram', () => {
  const huge = 'x'.repeat(CLIENT_KEY_MAX_LENGTH * 40); // far over the cap
  const s = summarize([
    { type: 'crash', timestamp: 1, reason: huge },
    { type: 'error', timestamp: 2, name: 'TypeError', platform: huge, appVersion: huge, runtime: huge },
  ]);
  // Each histogram holds ONE key of exactly CLIENT_KEY_MAX_LENGTH chars — the
  // response stays proportionate no matter what the client sent.
  for (const [hist, label] of [
    [s.crashReasons, 'crashReasons'],
    [s.platforms, 'platforms'],
    [s.appVersions, 'appVersions'],
    [s.byRuntime, 'byRuntime'],
  ]) {
    const keys = Object.keys(hist);
    assert.equal(keys.length, 1, `${label} holds one key`);
    assert.equal(keys[0].length, CLIENT_KEY_MAX_LENGTH, `${label} key is truncated`);
    assert.equal(hist[keys[0]], 1, `${label} count preserved`);
  }
});

test('distinct-key cardinality is capped with a counted __overflow__ bucket', () => {
  // CAP distinct values each with its own count, then two MORE past the cap.
  const crashes = [];
  for (let i = 0; i < CLIENT_HISTOGRAM_CAP + 2; i++) {
    crashes.push({ ...validCrash, reason: `distinct-reason-${i}` });
  }
  const s = summarize(crashes);
  const keys = Object.keys(s.crashReasons);
  assert.equal(keys.length, CLIENT_HISTOGRAM_CAP + 1, 'capped keys + ONE overflow bucket');
  assert.equal(keys.filter((k) => k === '__overflow__').length, 1, 'the overflow sentinel is present');
  assert.equal(s.crashReasons['__overflow__'], 2, 'overflow is COUNTED, not dropped');
  assert.equal(
    Object.values(s.crashReasons).reduce((a, b) => a + b, 0),
    crashes.length,
    'no count loss: every crash is bucketed'
  );
  assert.ok(s.crashReasons['distinct-reason-0'] === 1, 'an under-cap key keeps its own bucket');
});

test('ordinary small-cardinality traffic produces the same breakdowns as before', () => {
  const s = summarize([
    { ...validCrash, reason: 'oom' },
    { ...validCrash, reason: 'oom' },
    { ...validCrash, reason: 'killed' },
    { ...validError, platform: 'darwin', appVersion: '1.2.3', runtime: 'main' },
  ]);
  assert.deepEqual(s.crashReasons, { oom: 2, killed: 1 });
  assert.deepEqual(s.platforms, { darwin: 1 });
  assert.deepEqual(s.appVersions, { '1.2.3': 1 });
  assert.deepEqual(s.byRuntime, { main: 1, renderer: 3 });
});

test('topErrorNames and topSignatures keys are length-bounded at read time', () => {
  const s = summarize([
    { ...validError, name: 'E'.repeat(CLIENT_KEY_MAX_LENGTH * 30) },
    { ...validCrash, reason: 'r'.repeat(CLIENT_KEY_MAX_LENGTH * 30) },
  ]);
  assert.ok(s.topErrorNames.length === 1);
  assert.equal(s.topErrorNames[0].name.length, CLIENT_KEY_MAX_LENGTH);
  const crashSig = s.topSignatures.find((t) => t.type === 'crash');
  assert.ok(crashSig, 'the crash signature is ranked');
  assert.ok(crashSig.signature.length <= 'crash:'.length + CLIENT_KEY_MAX_LENGTH + ':exit=133'.length);
  assert.equal(crashSig.signature.length, CLIENT_KEY_MAX_LENGTH, 'the signature is truncated to the bound');
});

test('stalls.bySource folds overflow sources into one shared __overflow__ accumulator', () => {
  const stalls = [];
  for (let i = 0; i < CLIENT_HISTOGRAM_CAP + 2; i++) {
    stalls.push({ ...validStall, source: `src-${i}`, lagMs: 100 + i });
  }
  const s = summarize(stalls);
  assert.equal(s.stalls.count, stalls.length, 'every stall is still counted overall');
  const sources = Object.keys(s.stalls.bySource);
  assert.equal(sources.length, CLIENT_HISTOGRAM_CAP + 1, 'capped sources + ONE overflow bucket');
  assert.equal(s.stalls.bySource['__overflow__'].count, 2, 'overflow sources MERGED, not dropped');
});

test('summarizeStallsTimeline bySource is bounded too (same class of client key)', () => {
  const now = 1000;
  const stalls = [];
  for (let i = 0; i < CLIENT_HISTOGRAM_CAP + 3; i++) {
    stalls.push({ ...validStall, timestamp: now - 1, source: `src-${'a'.repeat(100)}-${i}` });
  }
  const tl = summarizeStallsTimeline(stalls, { now: () => now, windowMs: 100, maxBuckets: 2 });
  assert.equal(tl.buckets.length, 1);
  const sources = Object.keys(tl.buckets[0].bySource);
  assert.equal(sources.length, CLIENT_HISTOGRAM_CAP + 1, 'capped + overflow');
  assert.ok(sources.every((k) => k.length <= CLIENT_KEY_MAX_LENGTH), 'every source key is length-bounded');
  assert.equal(tl.buckets[0].bySource['__overflow__'].count, 3, 'overflow counted');
});

// ── lastAcceptedInstant (WARDEN-1428) ────────────────────────────────────────
// The newest effective instant across a batch — i.e. WHEN the newest ACCEPTED
// event landed. PURE, single-arg, no clock, exactly like summarize(). It backs
// the `/summary` `liveness` verdict, which RESTATES that instant so the verdict
// is self-contained; the equal-to-lastSeen test below is the contract that keeps
// the two from ever disagreeing FOR THE SAME ARRAY. (The /summary handler calls
// this on the UNSCOPED array, so on a filtered read the top-level `lastSeen` is
// scoped and this instant is not — they may legitimately differ there.)

test('lastAcceptedInstant returns the NEWEST effective instant across the batch', () => {
  const events = [
    { ...validError, timestamp: 100, receivedAt: 100 },
    { ...validCrash, timestamp: 900, receivedAt: 900 },
    { ...validStall, timestamp: 400, receivedAt: 400 },
  ];
  assert.equal(lastAcceptedInstant(events), 900, 'the maximum, regardless of array order');
});

test('lastAcceptedInstant PREFERS receivedAt over a skewed client timestamp', () => {
  // Same WARDEN-692 skew-robustness summarize()/summarizeTimeline already have: a
  // client clock far in the future must not push the apparent last-accepted instant.
  const events = [{ ...validError, timestamp: 9_999_999, receivedAt: 500 }];
  assert.equal(lastAcceptedInstant(events), 500, 'the RECEIVER stamp wins when present');
});

test('lastAcceptedInstant falls back to timestamp when receivedAt is absent (old persisted events)', () => {
  const events = [{ ...validError, timestamp: 700 }]; // pre-annotation shape, no receivedAt
  assert.equal(lastAcceptedInstant(events), 700, 'the client timestamp is the documented fallback');
});

test('lastAcceptedInstant is null on an empty / non-array input — ABSENCE, never 0', () => {
  // `0` would read as "an event just arrived at the epoch" — the precise false
  // reassurance the liveness block exists to prevent. Mirrors firstSeen/lastSeen.
  assert.equal(lastAcceptedInstant([]), null, 'empty store → null');
  assert.equal(lastAcceptedInstant(undefined), null, 'non-array → null (total, like summarize)');
  assert.equal(lastAcceptedInstant(null), null);
  assert.equal(lastAcceptedInstant('nope'), null);
});

test('lastAcceptedInstant is skip-robust: malformed entries and non-finite instants never crash or count', () => {
  const events = [
    null,
    'garbage',
    42,
    { ...validError, timestamp: undefined, receivedAt: undefined },
    { ...validError, timestamp: NaN, receivedAt: NaN },
    { ...validError, timestamp: 'later', receivedAt: 'later' },
    { ...validError, timestamp: Infinity, receivedAt: Infinity },
    { ...validError, timestamp: 250, receivedAt: 250 },
  ];
  assert.equal(lastAcceptedInstant(events), 250, 'only the one FINITE instant counts; nothing throws');
});

test('lastAcceptedInstant returns exactly summarize().lastSeen for the SAME array (cannot drift)', () => {
  // The load-bearing contract: /summary serves both in one body, so a divergence
  // would make the response contradict itself. Both read the SAME shared
  // _effectiveInstant rule, and this pins that they agree over a mixed batch.
  const events = [
    { ...validError, timestamp: 9_999_999, receivedAt: 100 }, // skewed client clock
    { ...validCrash, timestamp: 880 },                        // no receivedAt → fallback
    { ...validStall, timestamp: NaN, receivedAt: NaN },       // contributes nothing
    null,                                                     // skipped
  ];
  assert.equal(lastAcceptedInstant(events), summarize(events).lastSeen, 'identical by construction');
  assert.equal(lastAcceptedInstant(events), 880, 'and the value is the honest maximum');
});

// ── WORKSPACE AGGREGATES (WARDEN-1473) ────────────────────────────────────────
// The read-side completion of the vein `stalls` (WARDEN-854) and `operations`
// (WARDEN-1435) cut: both workspace event types reduced to a bare `byType`
// integer, which made roadmap WARDEN-1265's founding defect ILLEGIBLE on the
// read surface — "twenty-five chats and twenty-five identically-named chats are
// the same number". `workspaceShape` projects the count snapshot's min/avg/max
// per axis; `workspaceNames` projects the bounded distinct-name set beside the
// TRUE catalog size, so `distinctCount < maxChatCount` IS the collision verdict.

// Copyable fixtures — the verbatim shapes the validator accepts (schema.ts
// isWorkspaceShapeShape / isWorkspaceNamesShape).
const shapeWindow = (overrides = {}) => ({
  schemaVersion: 8,
  type: 'workspace-shape',
  runtime: 'renderer',
  timestamp: 1735689600000,
  windowStartedAt: 1735689300000,
  windowEndedAt: 1735689600000,
  workspaces: 1,
  panesOpen: 2,
  panesActive: 2,
  chats: 3,
  peakPanesOpen: 2,
  peakChats: 3,
  ...overrides,
});
const namesWindow = (overrides = {}) => ({
  schemaVersion: 8,
  type: 'workspace-names',
  runtime: 'server',
  timestamp: 1735689600000,
  windowStartedAt: 1735689300000,
  windowEndedAt: 1735689600000,
  chats: ['demo'],
  chatCount: 1,
  truncated: false,
  ...overrides,
});

// The empty (never-populated) per-count snapshot, for the stable-shape asserts.
const EMPTY_COUNT = { windowsSeen: 0, min: null, avg: 0, max: null };

test('workspaceShape/workspaceNames are stable zeroed shapes on a workspace-free store (no false alarm)', () => {
  // Non-workspace events contribute nothing — each axis reads only its own type.
  const s = summarize([validError, validCrash, validStall, validServerStall]);
  assert.equal(s.workspaceShape.windowsSeen, 0);
  assert.equal(s.workspaceShape.lastSnapshotAt, null);
  assert.deepEqual(s.workspaceShape.counts, {
    workspaces: EMPTY_COUNT, panesOpen: EMPTY_COUNT, panesActive: EMPTY_COUNT,
    chats: EMPTY_COUNT, peakPanesOpen: EMPTY_COUNT, peakChats: EMPTY_COUNT,
  });
  assert.deepEqual(s.workspaceNames, {
    windowsSeen: 0, names: {}, distinctCount: 0, maxChatCount: null,
    lastChatCount: null, truncatedEver: false,
    generatedShaped: { distinctCount: 0, considered: 0, share: null },
    lastSnapshotAt: null,
  });
});

test('workspaceShape.counts key set is STABLE — every schema count is present even when never populated', () => {
  // The `byType` zeroed-shape posture one level down: a maintainer always reads
  // every count axis, and an axis nothing carried says so with windowsSeen 0 +
  // null extrema rather than vanishing from the response.
  const s = summarize([shapeWindow()]);
  assert.deepEqual(
    Object.keys(s.workspaceShape.counts).sort(),
    ['chats', 'panesActive', 'panesOpen', 'peakChats', 'peakPanesOpen', 'workspaces'],
  );
});

test('workspaceShape.windowsSeen equals byType[workspace-shape] and max is the true max per count', () => {
  // Criterion 2's seeded-store equality: for each count, windowsSeen == number
  // of seeded shape events and max == the true max across them.
  const windows = [
    shapeWindow({ workspaces: 1, panesOpen: 2, panesActive: 1, chats: 4, peakPanesOpen: 3, peakChats: 5 }),
    shapeWindow({ workspaces: 3, panesOpen: 8, panesActive: 6, chats: 4, peakPanesOpen: 9, peakChats: 7 }),
    shapeWindow({ workspaces: 2, panesOpen: 5, panesActive: 2, chats: 1, peakPanesOpen: 5, peakChats: 4 }),
  ];
  const s = summarize(windows);
  assert.equal(s.workspaceShape.windowsSeen, 3);
  assert.equal(s.workspaceShape.windowsSeen, s.byType['workspace-shape'], 'the payload and count axes agree');
  for (const key of ['workspaces', 'panesOpen', 'panesActive', 'chats', 'peakPanesOpen', 'peakChats']) {
    const snap = s.workspaceShape.counts[key];
    assert.equal(snap.windowsSeen, windows.length, `${key}: every window contributed`);
    assert.equal(snap.max, Math.max(...windows.map((w) => w[key])), `${key}: max is the true max`);
    assert.equal(snap.min, Math.min(...windows.map((w) => w[key])), `${key}: min is the true min`);
    const mean = windows.reduce((a, w) => a + w[key], 0) / windows.length;
    assert.equal(snap.avg, mean, `${key}: avg is the PLAIN mean — each window is one equal-weight snapshot`);
  }
});

test('a 9-panes-then-1-pane window surfaces max.peakPanesOpen 9 without touching /events (WARDEN-1473 criterion 2)', () => {
  // The per-window peak is exactly the open-then-close burst the shape event
  // exists to keep visible: the window CLOSED on 1 pane, and the burst is still
  // readable off the summary alone. The validator guarantees peak >= closing
  // INSIDE each event, so the peaks compose under `max` with no special case.
  const s = summarize([shapeWindow({ panesOpen: 1, peakPanesOpen: 9 })]);
  assert.equal(s.workspaceShape.counts.peakPanesOpen.max, 9, 'the burst is visible');
  assert.equal(s.workspaceShape.counts.panesOpen.max, 1, 'and the closing count still reads its own value');
});

test('workspaceShape.lastSnapshotAt is the newest windowEndedAt — the PRODUCER clock, not arrival order', () => {
  // Persisted out of order on purpose: freshness must key off the window's own
  // close instant, so a late-persisted old window cannot make the axis look stale.
  const s = summarize([
    shapeWindow({ windowEndedAt: 5_000 }),
    shapeWindow({ windowEndedAt: 9_000 }),
    shapeWindow({ windowEndedAt: 7_000 }),
  ]);
  assert.equal(s.workspaceShape.lastSnapshotAt, 9_000);
});

test('workspaceShape counts a 0 as a REAL measurement, never as the empty sentinel', () => {
  // A workspace with no panes open is a genuine observation; `null` is reserved
  // for "nothing finite was folded" (the _stallSnapshot honesty posture).
  const s = summarize([shapeWindow({ panesOpen: 0, panesActive: 0, peakPanesOpen: 0 })]);
  assert.deepEqual(s.workspaceShape.counts.panesOpen, { windowsSeen: 1, min: 0, avg: 0, max: 0 });
});

test('workspaceShape is skip-robust: a malformed count degrades ONLY its own axis, never to NaN', () => {
  // Criterion 5, per-field grain: the event still counts in byType AND in
  // windowsSeen (parity), the good counts fold normally, and the bad ones read
  // the honest empty snapshot instead of NaN.
  const s = summarize([
    shapeWindow({ panesOpen: NaN, panesActive: 'three', chats: -1, peakChats: undefined, workspaces: 4, peakPanesOpen: 6 }),
  ]);
  assert.equal(s.byType['workspace-shape'], 1, 'still counted on the count axis');
  assert.equal(s.workspaceShape.windowsSeen, 1, 'and still counted as a window seen (byType parity)');
  assert.deepEqual(s.workspaceShape.counts.workspaces, { windowsSeen: 1, min: 4, avg: 4, max: 4 });
  assert.deepEqual(s.workspaceShape.counts.peakPanesOpen, { windowsSeen: 1, min: 6, avg: 6, max: 6 });
  for (const key of ['panesOpen', 'panesActive', 'chats', 'peakChats']) {
    assert.deepEqual(s.workspaceShape.counts[key], EMPTY_COUNT, `${key}: unusable → honest empty, not NaN`);
    assert.ok(!Number.isNaN(s.workspaceShape.counts[key].avg), `${key}: avg is never NaN`);
  }
});

test('workspaceShape skips a wholly-malformed entry without poisoning the aggregate', () => {
  const s = summarize([null, 'garbage', 42, shapeWindow({ panesOpen: 7, peakPanesOpen: 7 })]);
  assert.equal(s.workspaceShape.windowsSeen, 1, 'only the real window counted');
  assert.equal(s.workspaceShape.counts.panesOpen.max, 7);
});

// ── workspaceNames — the identically-named-chats verdict ──────────────────────

test('workspaceNames makes the collision legible: distinctCount 5 vs maxChatCount 25 (WARDEN-1473 criterion 3)', () => {
  // The founding defect, readable in ONE query: a 25-chat catalog carrying only
  // 5 DISTINCT names. `distinctCount < maxChatCount` is the verdict.
  const five = ['Untitled', 'demo', 'Refactor auth', 'scratch', 'test'];
  const s = summarize([
    namesWindow({ chats: five, chatCount: 25, truncated: true, windowEndedAt: 10_000 }),
    namesWindow({ chats: five, chatCount: 25, truncated: true, windowEndedAt: 20_000 }),
  ]);
  assert.equal(s.workspaceNames.windowsSeen, 2);
  assert.equal(s.workspaceNames.distinctCount, 5, 'the catalog holds 5 distinct names');
  assert.equal(s.workspaceNames.maxChatCount, 25, 'against a TRUE catalog size of 25');
  assert.ok(
    s.workspaceNames.distinctCount < s.workspaceNames.maxChatCount,
    'the identically-named-chats verdict is readable off /summary alone',
  );
  assert.deepEqual(s.workspaceNames.names, {
    Untitled: 2, demo: 2, 'Refactor auth': 2, scratch: 2, test: 2,
  }, 'each name is counted once per window it appeared in');
});

test('workspaceNames.truncatedEver flips on a seeded truncated window (and is false otherwise)', () => {
  assert.equal(summarize([namesWindow()]).workspaceNames.truncatedEver, false, 'an honest full list');
  const s = summarize([namesWindow(), namesWindow({ truncated: true })]);
  assert.equal(s.workspaceNames.truncatedEver, true, 'ANY truncated window flips it');
});

test('workspaceNames.windowsSeen equals byType[workspace-names]', () => {
  const s = summarize([validError, namesWindow(), namesWindow(), namesWindow()]);
  assert.equal(s.workspaceNames.windowsSeen, 3);
  assert.equal(s.workspaceNames.windowsSeen, s.byType['workspace-names'], 'the payload and count axes agree');
});

test('workspaceNames.maxChatCount is the largest TRUE catalog size; lastChatCount is the most RECENT window', () => {
  // Persisted out of order on purpose: "most recent" keys off the producer's own
  // windowEndedAt, so a late-persisted old window cannot rewrite the latest size.
  const s = summarize([
    namesWindow({ chatCount: 40, chats: ['a'], truncated: true, windowEndedAt: 5_000 }),
    namesWindow({ chatCount: 7, chats: ['a'], truncated: true, windowEndedAt: 9_000 }),
    namesWindow({ chatCount: 12, chats: ['a'], truncated: true, windowEndedAt: 7_000 }),
  ]);
  assert.equal(s.workspaceNames.maxChatCount, 40, 'the peak catalog size across every window');
  assert.equal(s.workspaceNames.lastChatCount, 7, 'the NEWEST window by windowEndedAt, not by array order');
  assert.equal(s.workspaceNames.lastSnapshotAt, 9_000);
});

test('workspaceNames treats chatCount 0 as a REAL size, never as the empty sentinel', () => {
  const s = summarize([namesWindow({ chats: [], chatCount: 0 })]);
  assert.equal(s.workspaceNames.maxChatCount, 0, 'a measured empty catalog');
  assert.equal(s.workspaceNames.lastChatCount, 0);
  assert.deepEqual(s.workspaceNames.names, {});
  assert.equal(s.workspaceNames.distinctCount, 0);
});

test('workspaceNames is skip-robust: a malformed entry is skipped, still counted, never NaN (criterion 5)', () => {
  const s = summarize([
    namesWindow({ chats: 'not-an-array', chatCount: NaN, truncated: 'yes', windowEndedAt: 'soon' }),
    namesWindow({ chats: [null, 42, '', 'real'], chatCount: 9, windowEndedAt: 1_000 }),
  ]);
  assert.equal(s.byType['workspace-names'], 2, 'both events counted on the count axis');
  assert.equal(s.workspaceNames.windowsSeen, 2, 'and both counted as windows seen (byType parity)');
  assert.deepEqual(s.workspaceNames.names, { real: 1 }, 'only the usable name bucketed');
  assert.equal(s.workspaceNames.distinctCount, 1);
  assert.equal(s.workspaceNames.maxChatCount, 9, 'the non-finite chatCount contributed nothing');
  assert.ok(!Number.isNaN(s.workspaceNames.maxChatCount));
  assert.equal(s.workspaceNames.truncatedEver, false, 'a non-boolean `truncated` is not a truncation claim');
  assert.equal(s.workspaceNames.lastSnapshotAt, 1_000, 'the non-finite windowEndedAt contributed nothing');
});

test('workspaceNames records a last-known chatCount even when every windowEndedAt is unusable', () => {
  // The degradation is deliberate: a window whose producer clock is unreadable
  // still contributes a last-known catalog size (arrival order) rather than
  // leaving lastChatCount null while maxChatCount plainly reports a number.
  const s = summarize([namesWindow({ chatCount: 3, chats: ['a'], truncated: true, windowEndedAt: NaN })]);
  assert.equal(s.workspaceNames.maxChatCount, 3);
  assert.equal(s.workspaceNames.lastChatCount, 3);
  assert.equal(s.workspaceNames.lastSnapshotAt, null, 'freshness stays honestly absent');
});

// ── BOUNDING (criterion 4, WARDEN-1246 posture) ───────────────────────────────

test('workspaceNames truncates a name longer than CLIENT_KEY_MAX_LENGTH', () => {
  // Chat names are FREE client strings (the validator carries no name-pattern
  // constraint by design), so one multi-KB name must never be reproduced in full
  // inside EVERY subsequent /summary response.
  const long = 'n'.repeat(CLIENT_KEY_MAX_LENGTH + 500);
  const s = summarize([namesWindow({ chats: [long], chatCount: 1 })]);
  const keys = Object.keys(s.workspaceNames.names);
  assert.equal(keys.length, 1);
  assert.equal(keys[0].length, CLIENT_KEY_MAX_LENGTH, 'the key is bounded at the shared cap');
  assert.equal(keys[0], 'n'.repeat(CLIENT_KEY_MAX_LENGTH));
});

test('workspaceNames folds past-cap distinct names into ONE counted __overflow__ bucket', () => {
  // Bounded cardinality, no count loss — the createRejectionTally / operations
  // shape. The cap is anchored to the PRODUCER's own per-window cap so a
  // realistic catalog keeps every name readable; only a hostile / runaway
  // cardinality reaches the fold.
  const N = WORKSPACE_NAMES_SUMMARY_CAP;
  const overflowing = Array.from({ length: N + 25 }, (_, i) => `chat-${i}`);
  const s = summarize([namesWindow({ chats: overflowing, chatCount: overflowing.length })]);
  const names = s.workspaceNames.names;
  assert.equal(Object.keys(names).length, N + 1, `collapses to ${N} + 1 keys regardless of input cardinality`);
  assert.equal(names.__overflow__, 25, 'and the folded count is STATED, never dropped');
  assert.equal(
    Object.values(names).reduce((a, b) => a + b, 0),
    overflowing.length,
    'Σ counts == Σ seeded names (no count loss)',
  );
  assert.equal(s.workspaceNames.distinctCount, N + 1, 'distinctCount counts the overflow bucket — the cap is LOUD');
});

test('a realistic catalog well under the cap keeps every name in its own readable bucket', () => {
  // The reason the cap is NOT CLIENT_HISTOGRAM_CAP (10): folding a 25-name
  // catalog into __overflow__ would answer "which chats exist?" with
  // `__overflow__` and destroy the capability this axis adds.
  const twentyFive = Array.from({ length: 25 }, (_, i) => `chat-${i}`);
  const s = summarize([namesWindow({ chats: twentyFive, chatCount: 25 })]);
  assert.equal(Object.keys(s.workspaceNames.names).length, 25);
  assert.equal(s.workspaceNames.names.__overflow__, undefined, 'no overflow at realistic cardinality');
  assert.ok(WORKSPACE_NAMES_SUMMARY_CAP > CLIENT_HISTOGRAM_CAP, 'the name axis is deliberately wider than the free-text axes');
});

// ── CROSS-AXIS ISOLATION ──────────────────────────────────────────────────────

test('the two workspace axes never cross-contaminate: a shape window is not a names window', () => {
  // `chats` is a COUNT on the shape event and a LIST on the names event — the
  // one field name the two types share, and the obvious place to get it wrong.
  const s = summarize([shapeWindow({ chats: 7, peakChats: 7 }), namesWindow({ chats: ['a', 'b'], chatCount: 2 })]);
  assert.equal(s.workspaceShape.counts.chats.max, 7, 'the shape axis read the COUNT');
  assert.deepEqual(s.workspaceNames.names, { a: 1, b: 1 }, 'the names axis read the LIST');
  assert.equal(s.workspaceShape.windowsSeen, 1);
  assert.equal(s.workspaceNames.windowsSeen, 1);
});

test('a workspace window does NOT enter the operations or stall axes', () => {
  const s = summarize([shapeWindow(), namesWindow(), { ...validStall, lagMs: 400 }]);
  assert.equal(s.stalls.count, 1, 'only the per-stall event is in the magnitude axis');
  assert.deepEqual(s.operations, {}, 'and neither workspace type folds into the per-operation axis');
});

test('summarize() stays a PURE single-arg function over the workspace axes (scoping lives in the handler)', () => {
  // Criterion 7's pure half: the same array in yields the same aggregate out,
  // twice, with no second parameter — which is what lets the /summary handler
  // pre-filter via the SHARED filterEvents (WARDEN-727) and get scoped workspace
  // aggregates for free.
  const events = [shapeWindow({ panesOpen: 4, peakPanesOpen: 6 }), namesWindow({ chats: ['a'], chatCount: 3, truncated: true })];
  assert.equal(summarize.length, 1, 'one declared parameter');
  assert.deepEqual(summarize(events), summarize(events), 'deterministic — no hidden state across calls');
  const scopedToShape = summarize(events.filter((e) => e.type === 'workspace-shape'));
  assert.equal(scopedToShape.workspaceNames.windowsSeen, 0, 'a pre-filtered array scopes the OTHER axis to its zeroed shape');
  assert.equal(scopedToShape.workspaceShape.counts.peakPanesOpen.max, 6);
});

// ── FEATURE USAGE (WARDEN-1488) ───────────────────────────────────────────────

const featureWindow = (features, overrides = {}) => ({
  schemaVersion: 9,
  type: 'feature-usage',
  runtime: 'renderer',
  timestamp: 1735689600000,
  windowStartedAt: 1735689300000,
  windowEndedAt: 1735689600000,
  features,
  ...overrides,
});

test('featureUsage is a stable zeroed shape on a feature-free store (no false alarm)', () => {
  const s = summarize([validError, validCrash, validStall, validServerStall]);
  assert.deepEqual(s.featureUsage, { windowsSeen: 0, lastWindowAt: null, features: {}, distinctCount: 0 });
});

test('positive control: without the fold, the capability names are invisible on the summary', () => {
  // Pre-change, summarize() reduced the event to byType's bare integer. Model the
  // pre-change surface as the summary MINUS the new key: the name must appear
  // nowhere else, and must appear with the key (so the probe can fire).
  const s = summarize([featureWindow([{ name: 'global-search', count: 7 }])]);
  assert.equal(s.byType['feature-usage'], 1);
  const { featureUsage, ...preChange } = s;
  assert.equal(JSON.stringify(preChange).includes('global-search'), false, 'names discarded by every pre-existing key');
  assert.equal(JSON.stringify(s).includes('global-search'), true, 'and present once featureUsage exists');
  assert.equal(featureUsage.features['global-search'].count, 7);
});

test('featureUsage folds count as a SUM of uses and windowsSeen as windows-per-capability', () => {
  const s = summarize([
    featureWindow([{ name: 'global-search', count: 7 }, { name: 'chat-create', count: 3 }], { windowEndedAt: 100 }),
    featureWindow([{ name: 'global-search', count: 2 }], { windowEndedAt: 300 }),
  ]);
  assert.deepEqual(s.featureUsage.features, {
    'global-search': { count: 9, windowsSeen: 2 },
    'chat-create': { count: 3, windowsSeen: 1 },
  });
  assert.equal(s.featureUsage.windowsSeen, 2);
  assert.equal(s.featureUsage.windowsSeen, s.byType['feature-usage'], 'parity with the count axis');
  assert.equal(s.featureUsage.distinctCount, 2);
});

test('featureUsage.lastWindowAt is the newest windowEndedAt — producer clock, not arrival order', () => {
  const s = summarize([
    featureWindow([{ name: 'a-b', count: 1 }], { windowEndedAt: 500 }),
    featureWindow([{ name: 'a-b', count: 1 }], { windowEndedAt: 200 }),
  ]);
  assert.equal(s.featureUsage.lastWindowAt, 500);
});

test('featureUsage is skip-robust: malformed rows are skipped, the event still counts, never NaN', () => {
  const s = summarize([
    featureWindow([
      null, 'x', { name: '', count: 1 }, { name: 7, count: 1 }, { name: 'ok-name', count: 'many' },
      { name: 'nan-name', count: NaN }, { name: 'zero-name', count: 0 }, { name: 'neg-name', count: -2 },
      { name: 'good-one', count: 4 },
    ]),
    featureWindow('not-an-array', { windowEndedAt: 'nope' }),
    featureWindow(undefined),
  ]);
  assert.equal(s.featureUsage.windowsSeen, 3, 'every event counts, however unusable its payload');
  assert.equal(s.featureUsage.windowsSeen, s.byType['feature-usage']);
  assert.deepEqual(s.featureUsage.features, { 'good-one': { count: 4, windowsSeen: 1 } });
  assert.equal(s.featureUsage.lastWindowAt, 1735689600000, 'an unreadable clock is skipped, not folded');
  assert.equal(JSON.stringify(s.featureUsage).includes('NaN'), false);
});

test('featureUsage truncates a name longer than CLIENT_KEY_MAX_LENGTH', () => {
  const long = 'a'.repeat(CLIENT_KEY_MAX_LENGTH + 40);
  const s = summarize([featureWindow([{ name: long, count: 2 }])]);
  assert.deepEqual(Object.keys(s.featureUsage.features), ['a'.repeat(CLIENT_KEY_MAX_LENGTH)]);
});

test('featureUsage folds past-cap distinct names into ONE counted __overflow__ bucket', () => {
  const N = FEATURE_USAGE_SUMMARY_CAP;
  assert.equal(N, 64, 'anchored by comment to schema.ts MAX_FEATURES_PER_EVENT');
  const rows = Array.from({ length: N + 10 }, (_, i) => ({ name: `feat-${i}`, count: 2 }));
  const s = summarize([featureWindow(rows.slice(0, N)), featureWindow(rows.slice(N))]);
  const f = s.featureUsage.features;
  assert.equal(Object.keys(f).length, N + 1, `collapses to ${N} + 1 keys`);
  assert.deepEqual(f.__overflow__, { count: 20, windowsSeen: 1 }, 'the folded uses are STATED, never dropped');
  assert.equal(Object.values(f).reduce((a, b) => a + b.count, 0), rows.length * 2, 'no count loss');
  assert.equal(s.featureUsage.distinctCount, N + 1, 'distinctCount counts the overflow bucket — the cap is LOUD');
});

test('featureUsage treats hostile keys as ordinary own keys', () => {
  const s = summarize([featureWindow([{ name: 'constructor', count: 1 }, { name: 'toString', count: 2 }])]);
  assert.deepEqual(s.featureUsage.features, { constructor: { count: 1, windowsSeen: 1 }, toString: { count: 2, windowsSeen: 1 } });
});

// ── PROCESS MEMORY (WARDEN-1514) ──────────────────────────────────────────────

const memWindow = (overrides = {}) => ({
  schemaVersion: 10,
  type: 'process-memory',
  runtime: 'main',
  timestamp: 1000,
  windowStartedAt: 900,
  windowEndedAt: 1000,
  samples: 10,
  rssMinBytes: 100,
  rssAvgBytes: 150,
  rssMaxBytes: 200,
  processAgeMs: 5000,
  ...overrides,
});

const ZEROED_RUNTIME = {
  windowsSeen: 0, samples: 0, rssMinBytes: null, rssAvgBytes: null, rssMaxBytes: null,
  heapUsedMaxBytes: null, peak: null, latest: null,
};

test('processMemory is a stable zeroed shape: silence is null, never fabricated zero', () => {
  const s = summarize([validError, validCrash, validStall, validServerStall]);
  assert.deepEqual(s.processMemory, {
    windowsSeen: 0,
    lastWindowAt: null,
    byRuntime: { main: ZEROED_RUNTIME, renderer: ZEROED_RUNTIME, server: ZEROED_RUNTIME },
  });
  assert.deepEqual(Object.keys(summarize([]).processMemory.byRuntime), ['main', 'renderer', 'server']);
});

test('processMemory folds min/max/samples across windows and runtimes; unreported runtimes stay null', () => {
  const s = summarize([
    memWindow({ rssMinBytes: 120, rssMaxBytes: 300, samples: 4, windowEndedAt: 1000 }),
    memWindow({ rssMinBytes: 90, rssMaxBytes: 250, samples: 6, windowEndedAt: 2000 }),
    memWindow({ runtime: 'renderer', rssMinBytes: 500, rssAvgBytes: 600, rssMaxBytes: 700, samples: 3, windowEndedAt: 1500 }),
  ]);
  const pm = s.processMemory;
  assert.equal(pm.windowsSeen, 3);
  assert.equal(pm.lastWindowAt, 2000);
  assert.equal(pm.byRuntime.main.windowsSeen, 2);
  assert.equal(pm.byRuntime.main.samples, 10);
  assert.equal(pm.byRuntime.main.rssMinBytes, 90);
  assert.equal(pm.byRuntime.main.rssMaxBytes, 300);
  assert.equal(pm.byRuntime.renderer.windowsSeen, 1);
  assert.equal(pm.byRuntime.renderer.rssMinBytes, 500);
  assert.deepEqual(pm.byRuntime.server, ZEROED_RUNTIME);
  assert.equal(s.byType['process-memory'], 3);
});

test('processMemory rssAvgBytes is SAMPLE-WEIGHTED, not a mean of means', () => {
  const s = summarize([
    memWindow({ rssAvgBytes: 100, samples: 1, windowEndedAt: 1000 }),
    memWindow({ rssAvgBytes: 400, samples: 9, windowEndedAt: 2000 }),
  ]);
  // weighted = (100*1 + 400*9)/10 = 370; mean of means would be 250.
  assert.equal(s.processMemory.byRuntime.main.rssAvgBytes, 370);
  // integer-rounded: (100*1 + 101*2)/3 = 100.67 → 101
  const r = summarize([memWindow({ rssAvgBytes: 100, samples: 1 }), memWindow({ rssAvgBytes: 101, samples: 2, windowEndedAt: 2000 })]);
  assert.equal(r.processMemory.byRuntime.main.rssAvgBytes, 101);
});

test('processMemory heapUsedMaxBytes is max where present, null where no window carried one', () => {
  const none = summarize([memWindow(), memWindow({ windowEndedAt: 2000 })]);
  assert.equal(none.processMemory.byRuntime.main.heapUsedMaxBytes, null);
  const some = summarize([
    memWindow({ heapUsedMaxBytes: 50 }),
    memWindow({ windowEndedAt: 2000 }),
    memWindow({ windowEndedAt: 3000, heapUsedMaxBytes: 80 }),
    memWindow({ runtime: 'server', windowEndedAt: 3000 }),
  ]);
  assert.equal(some.processMemory.byRuntime.main.heapUsedMaxBytes, 80);
  assert.equal(some.processMemory.byRuntime.server.heapUsedMaxBytes, null);
});

test('processMemory peak and latest diverge; peak carries THAT window\'s processAgeMs', () => {
  const s = summarize([
    memWindow({ rssMaxBytes: 900, rssAvgBytes: 800, processAgeMs: 1234, windowEndedAt: 1000 }),
    memWindow({ rssMaxBytes: 300, rssAvgBytes: 250, processAgeMs: 99999, windowEndedAt: 2000 }),
  ]);
  const m = s.processMemory.byRuntime.main;
  assert.deepEqual(m.peak, { rssMaxBytes: 900, processAgeMs: 1234, windowEndedAt: 1000 });
  assert.deepEqual(m.latest, { rssAvgBytes: 250, rssMaxBytes: 300, processAgeMs: 99999, windowEndedAt: 2000 });
});

test('processMemory latest follows windowEndedAt (producer clock), not arrival order; ties are order-independent', () => {
  const newer = memWindow({ windowEndedAt: 5000, rssMaxBytes: 111, rssAvgBytes: 100, processAgeMs: 7 });
  const older = memWindow({ windowEndedAt: 1000, rssMaxBytes: 222, rssAvgBytes: 200, processAgeMs: 3 });
  const a = summarize([newer, older]).processMemory.byRuntime.main;
  const b = summarize([older, newer]).processMemory.byRuntime.main;
  assert.deepEqual(a, b);
  assert.equal(a.latest.windowEndedAt, 5000);
  assert.equal(a.latest.rssMaxBytes, 111);
  assert.equal(a.peak.rssMaxBytes, 222);

  const t1 = memWindow({ windowEndedAt: 5000, rssMaxBytes: 300, processAgeMs: 10 });
  const t2 = memWindow({ windowEndedAt: 5000, rssMaxBytes: 300, processAgeMs: 20 });
  assert.deepEqual(summarize([t1, t2]).processMemory.byRuntime.main, summarize([t2, t1]).processMemory.byRuntime.main);
});

// Each tie-break rung is pinned by WHICH window wins (and which values are
// reported), in BOTH arrival orders. Order-independence alone would also be
// satisfied by a wrong-but-deterministic ladder (WARDEN-1523).
const bothOrders = (a, b) => [summarize([a, b]).processMemory.byRuntime.main, summarize([b, a]).processMemory.byRuntime.main];

test('processMemory peak tie on rssMax: the LATER windowEndedAt wins and its processAgeMs is reported', () => {
  // The earlier window carries the LARGER age so the age rung cannot mask the endedAt rung.
  const early = memWindow({ rssMaxBytes: 500, windowEndedAt: 1000, processAgeMs: 9000 });
  const late = memWindow({ rssMaxBytes: 500, windowEndedAt: 2000, processAgeMs: 100 });
  for (const m of bothOrders(early, late)) {
    assert.deepEqual(m.peak, { rssMaxBytes: 500, processAgeMs: 100, windowEndedAt: 2000 });
  }
});

test('processMemory peak tie on rssMax and windowEndedAt: the LARGER processAgeMs wins', () => {
  const young = memWindow({ rssMaxBytes: 500, windowEndedAt: 2000, processAgeMs: 10 });
  const old = memWindow({ rssMaxBytes: 500, windowEndedAt: 2000, processAgeMs: 20 });
  for (const m of bothOrders(young, old)) {
    assert.deepEqual(m.peak, { rssMaxBytes: 500, processAgeMs: 20, windowEndedAt: 2000 });
  }
});

test('processMemory latest tie on windowEndedAt: larger rssMax wins, then larger processAgeMs, then larger rssAvgBytes', () => {
  // Rung 1: rssMax decides even though the loser is ahead on age and avg.
  const bigMax = memWindow({ windowEndedAt: 5000, rssMaxBytes: 300, processAgeMs: 1, rssAvgBytes: 1 });
  const smallMax = memWindow({ windowEndedAt: 5000, rssMaxBytes: 200, processAgeMs: 99, rssAvgBytes: 99 });
  for (const m of bothOrders(bigMax, smallMax)) {
    assert.deepEqual(m.latest, { rssAvgBytes: 1, rssMaxBytes: 300, processAgeMs: 1, windowEndedAt: 5000 });
  }

  // Rung 2: equal rssMax → processAgeMs decides even though the loser has the larger avg.
  const olderProc = memWindow({ windowEndedAt: 5000, rssMaxBytes: 300, processAgeMs: 20, rssAvgBytes: 1 });
  const youngerProc = memWindow({ windowEndedAt: 5000, rssMaxBytes: 300, processAgeMs: 10, rssAvgBytes: 99 });
  for (const m of bothOrders(olderProc, youngerProc)) {
    assert.deepEqual(m.latest, { rssAvgBytes: 1, rssMaxBytes: 300, processAgeMs: 20, windowEndedAt: 5000 });
  }

  // Rung 3: equal rssMax and processAgeMs → larger rssAvgBytes decides.
  const highAvg = memWindow({ windowEndedAt: 5000, rssMaxBytes: 300, processAgeMs: 10, rssAvgBytes: 250 });
  const lowAvg = memWindow({ windowEndedAt: 5000, rssMaxBytes: 300, processAgeMs: 10, rssAvgBytes: 150 });
  for (const m of bothOrders(highAvg, lowAvg)) {
    assert.deepEqual(m.latest, { rssAvgBytes: 250, rssMaxBytes: 300, processAgeMs: 10, windowEndedAt: 5000 });
  }
});

test('processMemory is skip-robust: bad numbers, missing/unknown runtime never throw or poison a sum', () => {
  const s = summarize([
    memWindow({ rssMinBytes: 'x', rssAvgBytes: NaN, rssMaxBytes: Infinity, samples: 'many', processAgeMs: null, windowEndedAt: 1000 }),
    memWindow({ runtime: 'toString' }),
    memWindow({ runtime: undefined }),
    memWindow({ runtime: 42 }),
    memWindow({ windowEndedAt: 'soon', rssAvgBytes: 100, samples: 2, rssMinBytes: 10, rssMaxBytes: 20 }),
  ]);
  const pm = s.processMemory;
  assert.equal(pm.windowsSeen, 5, 'every event counts, parity with byType');
  assert.equal(s.byType['process-memory'], 5);
  const m = pm.byRuntime.main;
  assert.equal(m.windowsSeen, 2);
  assert.equal(m.samples, 2);
  assert.equal(m.rssMinBytes, 10);
  assert.equal(m.rssMaxBytes, 20);
  assert.equal(m.rssAvgBytes, 100);
  assert.equal(Number.isNaN(m.rssAvgBytes), false);
  assert.deepEqual(m.peak, { rssMaxBytes: 20, processAgeMs: 5000, windowEndedAt: null });
  assert.equal(m.latest.windowEndedAt, 1000);
  assert.equal(JSON.stringify(pm).includes('NaN'), false);
  assert.deepEqual(Object.keys(pm.byRuntime), ['main', 'renderer', 'server'], 'no foreign runtime key leaked');
  assert.doesNotThrow(() => summarize([{ type: 'process-memory' }]));
  assert.equal(summarize([{ type: 'process-memory' }]).processMemory.windowsSeen, 1);
});

test('processMemory positive control: values appear only on the new key, pre-existing keys unchanged', () => {
  const s = summarize([memWindow({ rssMaxBytes: 987654321 })]);
  const { processMemory, ...preChange } = s;
  assert.equal(JSON.stringify(preChange).includes('987654321'), false, 'absent from every pre-existing key');
  assert.equal(JSON.stringify(s).includes('987654321'), true, 'present once processMemory exists');
  assert.equal(processMemory.byRuntime.main.rssMaxBytes, 987654321);
});


// ── OPERATION REJECTIONS (WARDEN-1519) ────────────────────────────────────────

const rejWindow = (runtime, rejected, windowEndedAt) =>
  metricsWindow([], { runtime, rejected, windowEndedAt, timestamp: windowEndedAt });

test('operationRejections is a stable shape on an empty store: silence is null, never zero', () => {
  const r = summarize([]).operationRejections;
  assert.equal(r.windowsSeen, 0);
  assert.deepEqual(Object.keys(r.byRuntime), ['main', 'renderer', 'server']);
  for (const rt of ['main', 'renderer', 'server']) {
    assert.deepEqual(r.byRuntime[rt], { windowsSeen: 0, windowsWithRejections: 0, rejectedTotal: null, rejectedStaleTotal: null, rejectedInvalidTotal: null, lastRejectedAt: null });
  }
});

test('operationRejections folds rejected per runtime; malformed counts as a window but never poisons the sum', () => {
  const s = summarize([
    rejWindow('renderer', 1, 100),
    rejWindow('renderer', 2, 300), // latest rejected window
    rejWindow('renderer', 0, 500), // later window, nothing refused → must NOT move lastRejectedAt
    rejWindow('renderer', '3', 600), // malformed (string)
    rejWindow('main', 0, 200),
  ]);
  const r = s.operationRejections;
  assert.equal(r.windowsSeen, 5);
  assert.deepEqual(r.byRuntime.renderer, { windowsSeen: 4, windowsWithRejections: 2, rejectedTotal: 3, rejectedStaleTotal: null, rejectedInvalidTotal: null, lastRejectedAt: 300 });
  assert.deepEqual(r.byRuntime.main, { windowsSeen: 1, windowsWithRejections: 0, rejectedTotal: 0, rejectedStaleTotal: null, rejectedInvalidTotal: null, lastRejectedAt: null }, 'measured zero');
  assert.deepEqual(r.byRuntime.server, { windowsSeen: 0, windowsWithRejections: 0, rejectedTotal: null, rejectedStaleTotal: null, rejectedInvalidTotal: null, lastRejectedAt: null }, 'silence');
});

test('operationRejections skip-robust: negative / fractional / NaN / missing rejected and unknown runtime never throw', () => {
  const evs = [
    rejWindow('renderer', -1, 10),
    rejWindow('renderer', 1.5, 20),
    rejWindow('renderer', NaN, 30),
    rejWindow('renderer', undefined, 40),
    rejWindow('renderer', 4, 50),
    rejWindow('bogus', 9, 60), // unknown runtime: top-level only
  ];
  const r = summarize(evs).operationRejections;
  assert.equal(r.windowsSeen, 6);
  assert.deepEqual(r.byRuntime.renderer, { windowsSeen: 5, windowsWithRejections: 1, rejectedTotal: 4, rejectedStaleTotal: null, rejectedInvalidTotal: null, lastRejectedAt: 50 });
  assert.deepEqual(Object.keys(r.byRuntime), ['main', 'renderer', 'server']);
});

// ── THE STALE/INVALID SPLIT (WARDEN-1528, schema v11) ─────────────────────────

const splitWindow = (runtime, split, windowEndedAt) =>
  metricsWindow([], { runtime, ...split, windowEndedAt, timestamp: windowEndedAt });

test('operationRejections projects the v11 split: stale=5/invalid=2 → rejectedStaleTotal 5, rejectedInvalidTotal 2, rejectedTotal 7', () => {
  const r = summarize([splitWindow('renderer', { rejected: 7, rejectedStale: 5, rejectedInvalid: 2 }, 100)]).operationRejections;
  assert.deepEqual(r.byRuntime.renderer, { windowsSeen: 1, windowsWithRejections: 1, rejectedTotal: 7, rejectedStaleTotal: 5, rejectedInvalidTotal: 2, lastRejectedAt: 100 });
});

test('operationRejections: a v9/v10-era window (no split fields) projects NULL split totals, never zeros', () => {
  const r = summarize([rejWindow('renderer', 4, 100)]).operationRejections;
  assert.equal(r.byRuntime.renderer.rejectedTotal, 4);
  assert.equal(r.byRuntime.renderer.rejectedStaleTotal, null, 'unclassified, not zero');
  assert.equal(r.byRuntime.renderer.rejectedInvalidTotal, null, 'unclassified, not zero');
  // A split window that MEASURED zero is a real zero, distinct from the null above.
  const z = summarize([splitWindow('main', { rejected: 0, rejectedStale: 0, rejectedInvalid: 0 }, 50)]).operationRejections;
  assert.equal(z.byRuntime.main.rejectedStaleTotal, 0);
  assert.equal(z.byRuntime.main.rejectedInvalidTotal, 0);
});

test('operationRejections: mixed pre-split + split windows sum the split only over windows that carry it; malformed split values are skipped', () => {
  const r = summarize([
    rejWindow('renderer', 4, 100), // pre-split
    splitWindow('renderer', { rejected: 7, rejectedStale: 5, rejectedInvalid: 2 }, 200),
    splitWindow('renderer', { rejected: 1, rejectedStale: -1, rejectedInvalid: '1' }, 300), // malformed split
    splitWindow('renderer', { rejected: 2, rejectedStale: 1.5, rejectedInvalid: 2 }, 400),
  ]).operationRejections.byRuntime.renderer;
  assert.equal(r.rejectedTotal, 14);
  assert.equal(r.rejectedStaleTotal, 5);
  assert.equal(r.rejectedInvalidTotal, 4);
});

test('operationRejections lastRejectedAt follows windowEndedAt (producer clock), not arrival order', () => {
  const a = rejWindow('renderer', 1, 900);
  const b = rejWindow('renderer', 1, 100);
  assert.equal(summarize([a, b]).operationRejections.byRuntime.renderer.lastRejectedAt, 900);
  assert.equal(summarize([b, a]).operationRejections.byRuntime.renderer.lastRejectedAt, 900);
});

test('operationRejections reads only operational-metrics windows; existing keys are unaffected', () => {
  const s = summarize([validError, memWindow(), rejWindow('renderer', 2, 100)]);
  assert.equal(s.operationRejections.windowsSeen, 1);
  const { operationRejections, ...rest } = s;
  assert.equal(rest.byType['operational-metrics'], 1);
  assert.equal(rest.total, 3);
});

// ── workspaceNames.generatedShaped — distinct-but-machine-named verdict ───────

test('workspaceNames.generatedShaped counts distinct names matching the retired generator shapes', () => {
  const s = summarize([namesWindow({
    chats: ['chat-0r8s2s', 'split-33jgqgxp', 'shell-cr00af', 'demo', 'Refactor auth'], chatCount: 5,
  })]);
  assert.deepEqual(s.workspaceNames.generatedShaped, { distinctCount: 3, considered: 5, share: 0.6 });
});

test('workspaceNames.generatedShaped does NOT match near-miss shapes (negative controls)', () => {
  const near = ['chat-planner', 'split-abc', 'chatabcdef', 'Chat-0r8s2s'];
  for (const n of near) assert.equal(GENERATED_NAME_SHAPE.test(n), false, `${n} must not match`);
  const s = summarize([namesWindow({ chats: near, chatCount: 4 })]);
  assert.deepEqual(s.workspaceNames.generatedShaped, { distinctCount: 0, considered: 4, share: 0 });
});

test('workspaceNames.generatedShaped excludes the __overflow__ bucket from both counts', () => {
  const N = WORKSPACE_NAMES_SUMMARY_CAP;
  const chats = Array.from({ length: N + 5 }, (_, i) => `chat-${String(i).padStart(6, '0')}`);
  const s = summarize([namesWindow({ chats, chatCount: chats.length })]);
  assert.ok(s.workspaceNames.names.__overflow__ > 0, 'precondition: overflow bucket exists');
  assert.deepEqual(s.workspaceNames.generatedShaped, { distinctCount: N, considered: N, share: 1 });
});

test('workspaceNames.generatedShaped share is null (never 0) when nothing was considered', () => {
  assert.deepEqual(summarize([]).workspaceNames.generatedShaped, { distinctCount: 0, considered: 0, share: null });
});

// ── RELEASES adoption axis (WARDEN-1540) ──────────────────────────────────────

const relEv = (appVersion, extra = {}) => ({ ...validError, appVersion, ...extra });

test('releases: per-version count + firstSeenAt/lastSeenAt, receivedAt preferred over timestamp fallback', () => {
  const s = summarize([
    relEv('0.1.86', { timestamp: 10, receivedAt: 1000 }),
    relEv('0.1.86', { timestamp: 20 }), // timestamp-only fallback (older, pre-annotation)
    relEv('0.1.86', { timestamp: 9_999_999, receivedAt: 3000 }), // skewed client clock ignored
    relEv('0.1.90', { timestamp: 5000, receivedAt: 4000 }),
  ]);
  assert.deepEqual(s.releases.byVersion['0.1.86'], { count: 3, firstSeenAt: 20, lastSeenAt: 3000 });
  assert.deepEqual(s.releases.byVersion['0.1.90'], { count: 1, firstSeenAt: 4000, lastSeenAt: 4000 });
  assert.equal(s.releases.distinctCount, 2);
  assert.equal(s.releases.newestSeen, '0.1.90');
});

test('releases: an event with no effective instant counts but leaves first/last null (never 0)', () => {
  const e = { ...validError, appVersion: '0.1.5' };
  delete e.timestamp;
  const s = summarize([e]);
  assert.deepEqual(s.releases.byVersion['0.1.5'], { count: 1, firstSeenAt: null, lastSeenAt: null });
  // a later event with an instant fills them in
  const s2 = summarize([e, relEv('0.1.5', { timestamp: 7 })]);
  assert.deepEqual(s2.releases.byVersion['0.1.5'], { count: 2, firstSeenAt: 7, lastSeenAt: 7 });
});

test('releases: 12 sequential labels are ALL named while appVersions is unchanged (folds at 10)', () => {
  const labels = ['0.1.19', '0.1.56', '0.1.60', '0.1.66', '0.1.70', '0.1.72', '0.1.74', '0.1.82', '0.1.86', '0.1.88', '0.1.89', '0.1.90'];
  const s = summarize(labels.map((l, i) => relEv(l, { timestamp: 100 + i })));
  assert.equal(labels.length, 12);
  // appVersions byte-identical to the pre-existing first-seen-wins cap-10 behaviour.
  const expected = {};
  for (const l of labels.slice(0, CLIENT_HISTOGRAM_CAP)) expected[l] = 1;
  expected.__overflow__ = 2;
  assert.deepEqual(s.appVersions, expected);
  // releases names every one.
  assert.deepEqual(Object.keys(s.releases.byVersion).sort(), [...labels].sort());
  assert.equal(s.releases.distinctCount, 12);
  assert.equal(s.releases.newestSeen, '0.1.90');
  assert.equal(s.releases.byVersion['0.1.90'].lastSeenAt, 111);
});

test('releases: numeric (not lexical) ordering — 0.1.9 < 0.1.10, and shorter/longer components compare right', () => {
  assert.equal(summarize([relEv('0.1.10'), relEv('0.1.9')]).releases.newestSeen, '0.1.10');
  assert.equal(summarize([relEv('0.1.9'), relEv('0.1.10')]).releases.newestSeen, '0.1.10');
  assert.equal(summarize([relEv('0.2'), relEv('0.1.99')]).releases.newestSeen, '0.2');
  assert.equal(summarize([relEv('1.0'), relEv('1.0.0')]).releases.distinctCount, 2);
});

test('releases: cap → ONE __overflow__ bucket, no count loss, newestSeen/distinctCount stay correct past the cap', () => {
  const events = [];
  for (let i = 0; i < RELEASES_SUMMARY_CAP; i += 1) events.push(relEv(`0.1.${i}`, { timestamp: 10 + i }));
  // Past the cap: two NEW labels (the newest build among them), plus a repeat of a tracked one.
  events.push(relEv('0.1.500', { timestamp: 900 }));
  events.push(relEv('0.1.600', { timestamp: 950 }));
  events.push(relEv('0.1.0', { timestamp: 999 }));
  const s = summarize(events);
  const keys = Object.keys(s.releases.byVersion);
  assert.equal(keys.length, RELEASES_SUMMARY_CAP + 1, 'capped labels + ONE overflow bucket');
  assert.deepEqual(s.releases.byVersion.__overflow__, { count: 2, firstSeenAt: 900, lastSeenAt: 950 });
  assert.equal(s.releases.byVersion['0.1.500'], undefined, 'folded label has no named bucket');
  assert.equal(s.releases.byVersion['0.1.0'].count, 2, 'tracked label keeps bumping its own bucket');
  const total = Object.values(s.releases.byVersion).reduce((a, b) => a + b.count, 0);
  assert.equal(total, events.length, 'no count loss');
  assert.equal(s.releases.distinctCount, RELEASES_SUMMARY_CAP + 2, 'distinctCount is cap-independent; overflow bucket is not a label');
  assert.equal(s.releases.newestSeen, '0.1.600', 'newestSeen is correct even though it landed past the cap');
});

test('releases: a __proto__ / constructor label buckets as an ordinary own key', () => {
  const s = summarize([relEv('__proto__', { timestamp: 1 }), relEv('__proto__', { timestamp: 2 }), relEv('constructor', { timestamp: 3 }), relEv('0.1.1', { timestamp: 4 })]);
  assert.ok(Object.prototype.hasOwnProperty.call(s.releases.byVersion, '__proto__'));
  assert.deepEqual(s.releases.byVersion.__proto__, { count: 2, firstSeenAt: 1, lastSeenAt: 2 });
  assert.deepEqual(s.releases.byVersion.constructor, { count: 1, firstSeenAt: 3, lastSeenAt: 3 });
  assert.equal(s.releases.distinctCount, 3);
  assert.equal(s.releases.newestSeen, '0.1.1', 'non-orderable labels never win newestSeen');
});

test('releases: skip-robust — absent / null / non-string / empty ignored; non-orderable label ignored for newestSeen', () => {
  const s = summarize([
    { ...validError },
    relEv(null),
    relEv(42),
    relEv(''),
    relEv(['0.1.1']),
    relEv('nightly', { timestamp: 5 }),
    relEv('0.1.3-beta', { timestamp: 6 }),
  ]);
  assert.deepEqual(Object.keys(s.releases.byVersion).sort(), ['0.1.3-beta', 'nightly']);
  assert.equal(s.releases.distinctCount, 2);
  assert.equal(s.releases.newestSeen, null, 'no orderable label → null');
  assert.equal(summarize([relEv('nightly'), relEv('0.0.1')]).releases.newestSeen, '0.0.1');
});

test('releases: over-long label is truncated at CLIENT_KEY_MAX_LENGTH', () => {
  const s = summarize([relEv('x'.repeat(CLIENT_KEY_MAX_LENGTH + 50))]);
  assert.deepEqual(Object.keys(s.releases.byVersion), ['x'.repeat(CLIENT_KEY_MAX_LENGTH)]);
});
