// Drift guard (WARDEN-547). Imports the VENDORED ../schema.ts — loaded via
// Node's native type-stripping (no Vite, no transpile step) — and asserts it still
// matches the PINNED cross-repo contract the warden client emits, and that the
// vendored validateEvent still accepts the canonical client fixtures.
//
// WHY: the shared schema is the single source of truth; schema drift across repos
// is this roadmap's chief risk. If the client's schema.ts changes, re-vendoring it
// here will change the derived constants — and this guard will FAIL until the
// pinned values below are consciously updated to match. That forces a schema bump
// to be a coordinated, visible change across both repos (not a silent mis-store).
// A hand-rolled parallel validator could not give this guarantee.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  SCHEMA_VERSION,
  BASE_EVENT_TYPES,
  RUNTIME,
  validateEvent,
  validateBaseEvent,
} from '../schema.ts';

// The PINNED contract the warden client emits. Sourced from
// warden/web/src/lib/telemetry/schema.ts. If you re-vendor schema.ts after a
// client schema bump, update THESE pinned assertions in the same change.
const PINNED = {
  SCHEMA_VERSION: 2,
  BASE_EVENT_TYPES: ['error', 'crash', 'performance-stall'],
  RUNTIME: { MAIN: 'main', RENDERER: 'renderer' },
};

test('vendored SCHEMA_VERSION matches the pinned client contract', () => {
  assert.equal(typeof SCHEMA_VERSION, 'number');
  assert.equal(SCHEMA_VERSION, PINNED.SCHEMA_VERSION);
});

test('vendored BASE_EVENT_TYPES is exactly the pinned set and frozen', () => {
  assert.deepEqual([...BASE_EVENT_TYPES], PINNED.BASE_EVENT_TYPES);
  assert.equal(Object.isFrozen(BASE_EVENT_TYPES), true, 'BASE_EVENT_TYPES must be frozen');
});

test('vendored RUNTIME is exactly the pinned { main, renderer } and frozen', () => {
  assert.equal(RUNTIME.MAIN, PINNED.RUNTIME.MAIN);
  assert.equal(RUNTIME.RENDERER, PINNED.RUNTIME.RENDERER);
  assert.equal(Object.isFrozen(RUNTIME), true, 'RUNTIME must be frozen');
});

// Canonical fixtures — verbatim from warden/web/telemetry-schema.test.mjs:52-76,
// the EXACT shapes the client's slice-4 event builders emit. validateEvent MUST
// accept every one; if it ever rejects one, the vendored validator has drifted
// from the client and the receiver would wrongly drop good telemetry.
const errorFixture = {
  schemaVersion: SCHEMA_VERSION,
  type: 'error',
  runtime: RUNTIME.MAIN,
  timestamp: 12345,
  name: 'Error',
  message: 'failed to load module',
  frames: [{ function: 'loadKey', file: 'key.pem', line: 42, column: 7 }],
};
const crashFixture = {
  schemaVersion: SCHEMA_VERSION,
  type: 'crash',
  runtime: RUNTIME.RENDERER,
  timestamp: 9,
  reason: 'oom',
  exitCode: 133,
};
const stallFixture = {
  schemaVersion: SCHEMA_VERSION,
  type: 'performance-stall',
  runtime: RUNTIME.MAIN,
  timestamp: 3,
  lagMs: 750,
  source: 'event-loop',
};

test('vendored validateEvent accepts every canonical client fixture (no drift)', () => {
  assert.equal(validateEvent(errorFixture), true, 'error fixture validates');
  assert.equal(validateEvent(crashFixture), true, 'crash fixture validates');
  assert.equal(validateEvent(stallFixture), true, 'stall fixture validates');
});

test('vendored validateEvent accepts extended-tier fixtures (chat/session names)', () => {
  assert.equal(
    validateEvent({ ...errorFixture, chatName: 'Refactor auth', sessionName: 'claude-7b3a2f1' }),
    true,
    'extended-tier names are well-typed'
  );
});

test('the vendored validator is the REAL one — it still rejects out-of-schema events', () => {
  // Guards against a future edit that stubs validateEvent to `() => true`.
  assert.equal(validateEvent(null), false);
  assert.equal(validateEvent({ ...errorFixture, runtime: 'worker' }), false, 'bad runtime');
  assert.equal(validateEvent({ ...errorFixture, timestamp: NaN }), false, 'non-finite timestamp');
  assert.equal(validateEvent({ ...errorFixture, timestamp: 'soon' }), false, 'non-numeric timestamp');
  assert.equal(validateEvent({ ...errorFixture, chatName: 42 }), false, 'non-string extended field');
  assert.equal(
    validateEvent({ schemaVersion: 999, type: 'error', runtime: 'main', timestamp: 1, name: 'E', message: 'm', frames: [] }),
    false,
    'wrong schemaVersion rejected'
  );
  assert.equal(
    validateBaseEvent({ ...crashFixture, runtime: 'main' }),
    false,
    'crash must be renderer'
  );
});
