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
//
// BYTE-IDENTITY (WARDEN-1248): the pinned assertions below prove the vendored file
// still matches the contract on everything that was ever enumerated — and are
// structurally blind to everything that was not. When canonical retired
// ConsentTier / resolveConsentTier, the vendored copy kept them and NO pin could
// see the stale extras; only a byte-for-byte comparison with canonical can. The
// test at the bottom of this file performs exactly that comparison whenever a
// canonical checkout is reachable.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
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
  SCHEMA_VERSION: 4,
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

test('vendored validateEvent accepts identifier-bearing fixtures (chat/session names)', () => {
  assert.equal(
    validateEvent({ ...errorFixture, chatName: 'Refactor auth', sessionName: 'claude-7b3a2f1' }),
    true,
    'the optional identifier fields (the `names` category) are well-typed'
  );
});

test('vendored validateEvent accepts platform-bearing fixtures (WARDEN-684 OS label)', () => {
  // platform is a v3 base-tier OS label (darwin/win32/linux) the client stamps on
  // every event via process.platform; the vendored validator MUST accept it — and
  // accept its ABSENCE (a v3 event without platform still validates). Pinning this
  // here guards the cross-repo contract: if the client adds platform and
  // re-vendors schema.ts, the receiver's validator is proven to accept it.
  assert.equal(
    validateEvent({ ...errorFixture, platform: 'darwin' }),
    true,
    'platform-bearing error fixture validates'
  );
  assert.equal(
    validateEvent({ ...crashFixture, platform: 'win32' }),
    true,
    'platform-bearing crash fixture validates'
  );
  assert.equal(validateEvent(errorFixture), true, 'absent platform still validates (optional)');
  assert.equal(
    validateEvent({ ...errorFixture, platform: 42 }),
    false,
    'non-string platform rejected'
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
  // WARDEN-687: a main-runtime crash is now ACCEPTED (was rejected pre-v4). A
  // main-process hard kill is detected on the next launch by the crash sentinel
  // and emitted as { type:'crash', runtime:'main', reason:'unexpected-termination' };
  // the relaxation is a shape change (runtime was already a non-identifying enum),
  // not new data collection. Proven on BOTH validators the receiver uses
  // (validateBaseEvent is the core shape check ingest runs; validateEvent wraps it).
  assert.equal(
    validateBaseEvent({ ...crashFixture, runtime: 'main', reason: 'unexpected-termination' }),
    true,
    'validateBaseEvent accepts a main-runtime crash (hard kill)'
  );
  assert.equal(
    validateEvent({ ...crashFixture, runtime: 'main', reason: 'unexpected-termination' }),
    true,
    'validateEvent accepts a main-runtime crash (hard kill)'
  );
  assert.equal(
    validateEvent(crashFixture),
    true,
    'a renderer-runtime crash still validates'
  );
  assert.equal(
    validateEvent({ ...crashFixture, reason: 7 }),
    false,
    'a crash with a non-string reason is still rejected'
  );
});

// ── BYTE-IDENTITY with the canonical client copy (WARDEN-1248) ────────────────
//
// The enumerated pins above can only ever catch drift on the axes someone thought
// to pin. The WARDEN-1248 drift proved the blind spot: canonical (in the warden
// repo) retired ConsentTier + resolveConsentTier and reworked its consent
// comments, while this vendored copy kept the retired exports and the stale
// two-tier prose — and every pin above stayed green, because none of them could
// see an EXTRA export or stale comments. The one check that catches every
// divergence — stale extras, stale comments, a partial re-vendor — is comparing
// the bytes.
//
// This repo is standalone on its own CI (only warden-telemetry is checked out),
// so the canonical file is located, in priority order, at:
//   1. $WARDEN_CANONICAL_SCHEMA   — explicit override (absolute path)
//   2. <sibling checkout>/warden/web/src/lib/telemetry/schema.ts — the
//      side-by-side layout of a dev machine or workbench, where the cross-repo
//      work actually happens and where this guard earns its keep
// With neither present (standalone CI), the test SKIPS — visibly, in the
// runner's `skipped` count — it never silently passes.
const CANONICAL_SCHEMA_CANDIDATES = [
  process.env.WARDEN_CANONICAL_SCHEMA,
  fileURLToPath(new URL('../../warden/web/src/lib/telemetry/schema.ts', import.meta.url)),
].filter(Boolean);
const CANONICAL_SCHEMA_PATH = CANONICAL_SCHEMA_CANDIDATES.find((p) => existsSync(p));

test('vendored schema.ts is byte-identical to the canonical client copy', {
  skip: CANONICAL_SCHEMA_PATH
    ? false
    : `canonical warden schema not found (tried: ${CANONICAL_SCHEMA_CANDIDATES.join(', ')}) — ` +
      'set WARDEN_CANONICAL_SCHEMA or place a warden checkout beside warden-telemetry to enable this guard',
}, () => {
  const vendored = readFileSync(new URL('../schema.ts', import.meta.url));
  const canonical = readFileSync(CANONICAL_SCHEMA_PATH);
  const shortSha = (buf) => createHash('sha256').update(buf).digest('hex').slice(0, 16);
  assert.ok(
    vendored.equals(canonical),
    `vendored schema.ts has DRIFTED from canonical (${CANONICAL_SCHEMA_PATH}): ` +
      `vendored sha256:${shortSha(vendored)} vs canonical sha256:${shortSha(canonical)}. ` +
      'Re-vendor with `cp ../warden/web/src/lib/telemetry/schema.ts schema.ts` from the ' +
      'warden-telemetry root (never hand-edit the vendored copy), then update the PINNED ' +
      'constants above in the same change if the contract values moved.'
  );
});
