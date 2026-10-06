// Guard-branch pins for the main event loop and the workspace folds in
// summary.mjs (WARDEN-1563). Each test below kills one single-token mutant of
// summary.mjs that the sibling suites (summary.test.mjs, summary-fold-edges.test.mjs)
// leave green. Kept in its own file so it cannot collide with appends to those
// files. Pure `summarize(events)` seam: no network, no filesystem.
//
// Items 1 and 2 are reachable through ingest (the validator only requires
// `typeof name/reason === 'string'`). Items 3-7 are defence-in-depth: reachable
// only through a direct summarize() call or an unvalidated stored row. These
// tests assert the FOLD's behaviour, not a schema rule.
//
// Mutants killed (summary.mjs):
//   1. `type === 'error' && typeof name === 'string' && name.length > 0` — drop `name.length > 0`
//   2. `type === 'crash' && typeof reason === 'string' && reason.length > 0` — drop `reason.length > 0`
//   3. same error gate — drop `type === 'error'`
//   4. same crash gate — drop `type === 'crash'`
//   5. `schemaVersion !== undefined && schemaVersion !== null` — drop `!== null`
//   6. workspace-shape `lastSnapshotAt` — drop `Number.isFinite(endedAt)`
//   7. workspaceNames `else if (lastChatCountAt === null)` — replace with `else`

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { summarize } from '../summary.mjs';

const errorEvent = (extra = {}) => ({ schemaVersion: 1, type: 'error', runtime: 'server', timestamp: 1, name: 'TypeError', ...extra });
const crashEvent = (extra = {}) => ({ schemaVersion: 1, type: 'crash', runtime: 'server', timestamp: 1, reason: 'oom', ...extra });

// ── 1 / 2: empty-string name / reason yield no junk bucket ───────────────────

test('an error with an EMPTY name is counted in byType but creates no topErrorNames bucket', () => {
  const s = summarize([errorEvent({ name: '' })]);
  assert.equal(s.byType.error, 1, 'the event itself still counts');
  assert.deepEqual(s.topErrorNames, [], 'an empty name must not become a junk "" bucket');
  const mixed = summarize([errorEvent({ name: '' }), errorEvent({ name: 'RangeError' })]);
  assert.deepEqual(mixed.topErrorNames, [{ name: 'RangeError', count: 1 }]);
});

test('a crash with an EMPTY reason is counted in byType but creates no crashReasons bucket', () => {
  const s = summarize([crashEvent({ reason: '' })]);
  assert.equal(s.byType.crash, 1, 'the event itself still counts');
  assert.deepEqual(s.crashReasons, {}, 'an empty reason must not become a junk "" key');
  const mixed = summarize([crashEvent({ reason: '' }), crashEvent({ reason: 'killed' })]);
  assert.deepEqual(mixed.crashReasons, { killed: 1 });
});

// ── 3 / 4: the type gates ────────────────────────────────────────────────────

test('topErrorNames counts ONLY error events: a crash carrying a `name` adds no bucket', () => {
  const s = summarize([crashEvent({ name: 'TypeError' })]);
  assert.equal(s.byType.crash, 1);
  assert.deepEqual(s.topErrorNames, []);
});

test('crashReasons counts ONLY crash events: an error carrying a `reason` adds no key', () => {
  const s = summarize([errorEvent({ reason: 'oom' })]);
  assert.equal(s.byType.error, 1);
  assert.deepEqual(s.crashReasons, {});
});

// ── 5: null schemaVersion ────────────────────────────────────────────────────

test('a null schemaVersion yields no schemaVersions bucket (no "null" key)', () => {
  const s = summarize([errorEvent({ schemaVersion: null })]);
  assert.equal(s.byType.error, 1);
  assert.deepEqual(s.schemaVersions, {});
  const mixed = summarize([errorEvent({ schemaVersion: null }), errorEvent({ schemaVersion: 3 })]);
  assert.deepEqual(mixed.schemaVersions, { 3: 1 });
});

// ── 6: workspace-shape lastSnapshotAt isFinite ───────────────────────────────

const shapeWindow = (windowEndedAt) => ({
  schemaVersion: 1,
  type: 'workspace-shape',
  runtime: 'server',
  timestamp: 1,
  windowStartedAt: 0,
  windowEndedAt,
});

test('workspaceShape.lastSnapshotAt ignores an Infinity windowEndedAt and keeps the greatest FINITE one', () => {
  const s = summarize([shapeWindow(9), shapeWindow(Infinity)]).workspaceShape;
  assert.equal(s.lastSnapshotAt, 9, 'Infinity must not pin the freshness stamp forever');
  assert.equal(s.windowsSeen, 2, 'the Infinity window still counts as a window seen');
  assert.equal(summarize([shapeWindow(Infinity)]).workspaceShape.lastSnapshotAt, null, 'Infinity never seeds it either');
});

// ── 7: workspaceNames lastChatCount arrival-order fallback ───────────────────

const namesWindow = (chatCount, windowEndedAt) => ({
  schemaVersion: 1,
  type: 'workspace-names',
  runtime: 'server',
  timestamp: 1,
  windowStartedAt: 0,
  windowEndedAt,
  chats: ['demo'],
  chatCount,
  truncated: false,
});

test('workspaceNames.lastChatCount: an UNCLOCKED window never overwrites a clocked one (arrival order only when no usable clock seen)', () => {
  const s = summarize([namesWindow(7, 9), namesWindow(3, NaN)]).workspaceNames;
  assert.equal(s.lastChatCount, 7, 'a later window with an unusable clock must not displace the clocked 7');
  assert.equal(s.maxChatCount, 7);
  const unclocked = summarize([namesWindow(3, NaN), namesWindow(5, NaN)]).workspaceNames;
  assert.equal(unclocked.lastChatCount, 5, 'with no usable clock ever seen, arrival order decides: the latest arrival wins');
});
