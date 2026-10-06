// Edge-branch pins for the operationLatency / operationRejections / featureUsage /
// workspaceNames folds in summary.mjs (WARDEN-1542). Each test below kills one
// single-token mutant of summary.mjs that the sibling suites in summary.test.mjs
// leave green. Kept in its own file so it cannot collide with appends to
// summary.test.mjs. Pure `summarize(events)` seam: no network, no filesystem.
//
// Items 1, 2, 4 and 6 are defence-in-depth: the schema validator rejects those
// inputs at ingest, so they are reachable only through a direct summarize() call
// or an unvalidated stored row. These tests assert the FOLD's behaviour, not a
// schema rule.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { summarize } from '../summary.mjs';

const BOUNDS_A = [50, 100]; // 3 buckets
const BOUNDS_B = [10, 20]; // 3 buckets, a different scale

const metricsWindow = (operations, overrides = {}) => ({
  schemaVersion: 8,
  type: 'operational-metrics',
  runtime: 'server',
  timestamp: 4,
  windowStartedAt: 1,
  windowEndedAt: 4,
  boundaries: BOUNDS_A,
  operations,
  rejected: 0,
  ...overrides,
});
const op = (operation, count, buckets) => ({ operation, count, okCount: count, failCount: 0, min: 1, avg: 5, max: 9, buckets });
const latOf = (events, name) => summarize(events).operationLatency[name];
const EMPTY_LATENCY = { boundaries: [], buckets: [], histogramCount: 0, excludedCount: 0, p50: null, p95: null, p99: null };

// ── _foldLatency guards ───────────────────────────────────────────────────────

test('_foldLatency refuses an OVER-length buckets array (boundaries.length + 2), not only a short one', () => {
  const s = summarize([metricsWindow([op('route', 4, [1, 1, 1, 1])])]);
  assert.deepEqual(s.operationLatency.route, EMPTY_LATENCY, 'a 4-slot histogram against 2 boundaries (3 expected) contributes nothing');
  assert.equal(s.operations.route.count, 4, 'the count/min/avg/max fold is independent of the histogram refusal');
});

test('_foldLatency refuses a window whose boundaries contain NaN or Infinity (finite element guard)', () => {
  const nan = latOf([metricsWindow([op('route', 3, [1, 1, 1])], { boundaries: [NaN, 100] })], 'route');
  assert.equal(nan.histogramCount, 0, 'NaN boundary → no scale registered');
  assert.deepEqual(nan, EMPTY_LATENCY);
  const inf = latOf([metricsWindow([op('route', 3, [1, 1, 1])], { boundaries: [50, Infinity] })], 'route');
  assert.equal(inf.histogramCount, 0, 'Infinity boundary → no scale registered');
  assert.deepEqual(inf, EMPTY_LATENCY);
});

// ── _latencySnapshot tie ──────────────────────────────────────────────────────

test('_latencySnapshot: two scales with EQUAL totals → the FIRST-seen scale wins, in either arrival order', () => {
  const a = metricsWindow([op('route', 5, [5, 0, 0])], { boundaries: BOUNDS_A });
  const b = metricsWindow([op('route', 5, [0, 5, 0])], { boundaries: BOUNDS_B });

  const ab = latOf([a, b], 'route');
  assert.deepEqual(ab.boundaries, BOUNDS_A, 'A arrived first → A wins the tie');
  assert.deepEqual(ab.buckets, [5, 0, 0]);
  assert.equal(ab.histogramCount, 5);
  assert.equal(ab.excludedCount, 5, "the loser's whole total is excluded");

  const ba = latOf([b, a], 'route');
  assert.deepEqual(ba.boundaries, BOUNDS_B, 'B arrived first → B wins the tie');
  assert.deepEqual(ba.buckets, [0, 5, 0]);
  assert.equal(ba.histogramCount, 5);
  assert.equal(ba.excludedCount, 5);
});

// ── operationRejections ───────────────────────────────────────────────────────

const rejSplit = (runtime, fields, windowEndedAt) =>
  metricsWindow([], { runtime, ...fields, windowEndedAt, timestamp: windowEndedAt });

test('operationRejections: a FRACTIONAL rejectedInvalid is skipped (integer arm), while a valid rejectedStale still folds', () => {
  const r = summarize([rejSplit('renderer', { rejected: 3, rejectedStale: 1, rejectedInvalid: 1.5 }, 100)]).operationRejections.byRuntime.renderer;
  assert.equal(r.rejectedInvalidTotal, null, '1.5 is not an integer count → unclassified, not 1.5');
  assert.equal(r.rejectedStaleTotal, 1);
  assert.equal(r.rejectedTotal, 3);
});

test('operationRejections: a first rejecting window at the legitimate instant 0 seeds lastRejectedAt = 0 (null-seed)', () => {
  const r = summarize([rejSplit('renderer', { rejected: 2 }, 0)]).operationRejections.byRuntime.renderer;
  assert.equal(r.windowsWithRejections, 1);
  assert.equal(r.lastRejectedAt, 0, '0 is a measured instant, not "never rejected"');
});

// ── featureUsage lastWindowAt ─────────────────────────────────────────────────

const featureWindow = (windowEndedAt) => ({
  schemaVersion: 9,
  type: 'feature-usage',
  runtime: 'renderer',
  timestamp: 1,
  windowStartedAt: 0,
  windowEndedAt,
  features: [{ name: 'chat', count: 1 }],
});

test('featureUsage.lastWindowAt ignores a non-finite windowEndedAt (NaN / Infinity) and a later finite one still lands', () => {
  assert.equal(summarize([featureWindow(NaN)]).featureUsage.lastWindowAt, null, 'NaN never seeds lastWindowAt');
  assert.equal(summarize([featureWindow(Infinity)]).featureUsage.lastWindowAt, null, 'Infinity never seeds lastWindowAt');
  const s = summarize([featureWindow(NaN), featureWindow(50)]).featureUsage;
  assert.equal(s.lastWindowAt, 50, 'NaN must not poison the comparison for a later finite window');
  assert.equal(s.windowsSeen, 2, 'the NaN window still counts as a window seen');
});

// ── workspaceNames lastChatCount tie ──────────────────────────────────────────

const namesWindow = (chatCount, windowEndedAt) => ({
  schemaVersion: 1,
  type: 'workspace-names',
  runtime: 'server',
  timestamp: windowEndedAt,
  windowStartedAt: 0,
  windowEndedAt,
  chats: ['demo'],
  chatCount,
  truncated: false,
});

test('workspaceNames.lastChatCount: equal windowEndedAt → the LATER-arriving window wins (>= is deliberate)', () => {
  const s = summarize([namesWindow(3, 100), namesWindow(9, 100)]).workspaceNames;
  assert.equal(s.lastChatCount, 9, 'tie on the clock → arrival order decides, newest arrival wins');
  assert.equal(s.maxChatCount, 9);
  const r = summarize([namesWindow(9, 100), namesWindow(3, 100)]).workspaceNames;
  assert.equal(r.lastChatCount, 3, 'and the reverse arrival order flips it');
  assert.equal(r.maxChatCount, 9);
});
