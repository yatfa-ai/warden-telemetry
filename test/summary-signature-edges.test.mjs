// Guard-branch pins for the failure-signature, timeline-grid and stall-timeline
// code in summary.mjs plus the typeReach schemaVersion input gate (WARDEN-1608).
// Each test kills one-token mutants that the sibling suites leave green. Pure
// seams only (signatureOf / summarizeTimeline / summarizeStallsTimeline /
// summarize): no network, no filesystem. Assertions are strict equality on exact
// strings/shapes so a gutted implementation cannot satisfy them.
//
// Mutants killed (summary.mjs; NB the file contains a NUL byte — use `grep -a`):
//   1. signatureOf crash arm: `typeof exitCode === 'number' && Number.isFinite(...)` `&&`->`||`
//   2. _frameSegment: `f.file.length > 0`     -> `>= 0`
//   3. _frameSegment: `f.function.length > 0` -> `>= 0`
//   4. _frameSegment: `hasLine` `&&`->`||`
//   5/6. _frameSegment: `!hasFile && !hasFn && !hasLine` — each `&&`->`||`
//   7. _assignTimelineBuckets: `windowMs <= 0`   -> `< 0`
//   8. _assignTimelineBuckets: `maxBuckets < 1`  -> `<= 1`
//   9. summarizeStallsTimeline: `source.length > 0` -> `>= 0`
//  10. summarizeStallsTimeline: per-source `acc.max === null` -> `!== null`
//  11. typeReach tally: dropping `Number.isFinite(schemaVersion)`

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { summarize, summarizeTimeline, summarizeStallsTimeline, signatureOf, TYPE_INTRODUCED_IN_SCHEMA_VERSION } from '../summary.mjs';

const err = (frames) => ({ type: 'error', name: 'E', frames });
const crash = (exitCode) => ({ type: 'crash', reason: 'oom', exitCode });

// ── 1: crash exitCode must be a FINITE number ───────────────────────────────

test('signatureOf crash: a non-finite or non-number exitCode yields no :exit segment; a finite one does', () => {
  assert.equal(signatureOf(crash(NaN)), 'crash:oom');
  assert.equal(signatureOf(crash(Infinity)), 'crash:oom');
  assert.equal(signatureOf(crash(-Infinity)), 'crash:oom');
  assert.equal(signatureOf(crash('9')), 'crash:oom');
  assert.equal(signatureOf(crash(undefined)), 'crash:oom');
  assert.equal(signatureOf(crash(9)), 'crash:oom:exit=9');
  assert.equal(signatureOf(crash(0)), 'crash:oom:exit=0');
});

// ── 2-6: _frameSegment identifying-field rules ──────────────────────────────

test('signatureOf error: an EMPTY-string file / function is not identifying', () => {
  // all-empty frame carries nothing identifying → bare name
  assert.equal(signatureOf(err([{ file: '', function: '' }])), 'E');
  // empty file + real function → function only, no leading file text
  assert.equal(signatureOf(err([{ file: '', function: 'f' }])), 'E @ (f)');
  // empty function + real line → line only, no empty parens
  assert.equal(signatureOf(err([{ function: '', line: 5 }])), 'E @ :5');
  // empty function + real file → file only, no empty parens
  assert.equal(signatureOf(err([{ file: 'a.ts', function: '' }])), 'E @ a.ts');
});

test('signatureOf error: line must be a FINITE number to count', () => {
  assert.equal(signatureOf(err([{ line: NaN }])), 'E');
  assert.equal(signatureOf(err([{ line: Infinity }])), 'E');
  assert.equal(signatureOf(err([{ line: '7' }])), 'E');
  assert.equal(signatureOf(err([{ file: 'a.ts', line: NaN }])), 'E @ a.ts');
  assert.equal(signatureOf(err([{ file: 'a.ts', line: '7' }])), 'E @ a.ts');
  assert.equal(signatureOf(err([{ line: 7 }])), 'E @ :7');
  assert.equal(signatureOf(err([{ file: 'a.ts', line: 7 }])), 'E @ a.ts:7');
});

test('signatureOf error: a frame with exactly ONE identifying field still yields a segment; none degrades to the bare name', () => {
  assert.equal(signatureOf(err([{ file: 'a.ts' }])), 'E @ a.ts');
  assert.equal(signatureOf(err([{ function: 'f' }])), 'E @ (f)');
  assert.equal(signatureOf(err([{ line: 3 }])), 'E @ :3');
  assert.equal(signatureOf(err([{ file: 'a.ts', function: 'f', line: 3 }])), 'E @ a.ts:3 (f)');
  assert.equal(signatureOf(err([{}])), 'E');
  assert.equal(signatureOf(err([{ foo: 'bar' }])), 'E');
  assert.equal(signatureOf(err([])), 'E');
  assert.equal(signatureOf(err([null])), 'E');
});

// ── 7-8: timeline grid degenerate-config bounds ─────────────────────────────

test('summarizeTimeline: windowMs of exactly 0 is a degenerate config (empty), even with an event at now', () => {
  const t = summarizeTimeline([{ timestamp: 100, type: 'crash' }], { now: () => 100, windowMs: 0, maxBuckets: 10 });
  assert.deepEqual(t, { buckets: [], bucketMs: 0 });
});

test('summarizeTimeline: maxBuckets of exactly 1 is a valid single-bucket grid', () => {
  const t = summarizeTimeline([{ timestamp: 100, type: 'crash' }], { now: () => 100, windowMs: 10, maxBuckets: 1 });
  assert.equal(t.buckets.length, 1);
  assert.equal(t.buckets[0].count, 1);
  assert.equal(t.bucketMs, 10);
});

// ── 9-10: stall timeline source / per-source max ────────────────────────────

const stall = (lagMs, source) => ({ type: 'performance-stall', timestamp: 100, lagMs, source });

test('summarizeStallsTimeline: an EMPTY-string source is counted in the bucket but gets no bySource entry', () => {
  const t = summarizeStallsTimeline([stall(5, '')], { now: () => 100, windowMs: 10, maxBuckets: 1 });
  assert.equal(t.buckets.length, 1);
  assert.equal(t.buckets[0].count, 1);
  assert.equal(t.buckets[0].max, 5);
  assert.deepEqual(t.buckets[0].bySource, {});
});

test('summarizeStallsTimeline: per-source max tracks the largest FINITE lag and skips NaN', () => {
  const evs = [stall(5, 'a'), stall(9, 'a'), stall(2, 'a'), stall(NaN, 'a'), stall(7, 'b')];
  const t = summarizeStallsTimeline(evs, { now: () => 100, windowMs: 10, maxBuckets: 1 });
  assert.equal(t.buckets.length, 1);
  assert.equal(t.buckets[0].count, 5);
  assert.equal(t.buckets[0].max, 9);
  assert.deepEqual(t.buckets[0].bySource, { a: { count: 4, max: 9 }, b: { count: 1, max: 7 } });
});

// ── 11: typeReach schemaVersion gate must reject ±Infinity ──────────────────

test('typeReach: Infinity / -Infinity schemaVersions are not eligible for any type', () => {
  const ev = (type, schemaVersion) => ({ schemaVersion, type, runtime: 'main', timestamp: 1 });
  const s = summarize([ev('crash', Infinity), ev('crash', -Infinity)]);
  for (const t of Object.keys(TYPE_INTRODUCED_IN_SCHEMA_VERSION)) {
    assert.equal(s.typeReach[t].eligibleEvents, 0, `${t} eligibleEvents`);
  }
  assert.equal(s.typeReach.crash.emitted, 2);
  assert.equal(s.typeReach.crash.verdict, 'emitting');
  assert.equal(s.typeReach['feature-usage'].verdict, 'no-eligible-builds');
});
