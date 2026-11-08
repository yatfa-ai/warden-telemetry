// Seen-key durability FILE-SEAM tests. The real-filesystem helpers
// `fileSeenKeysSource` / `fileSeenKeysSink` (store.mjs — the production-wired
// seen-key persistence seams at server.mjs: load: fileSeenKeysSource(...) /
// persist: fileSeenKeysSink(...)) were never executed by any test:
// test/seenkeys-persistence.test.mjs proves the restart-mid-retry-window
// contract through INJECTED load/persist fns and its own header concedes the
// real helpers are "thin fs wrappers, NOT exercised here". Two things inside
// them are not thin, and no injected-fn test can catch a regression in either:
//   - the SOURCE's shape-validation loop: the corrupt-file boot guard (a
//     corrupt file can never crash boot) and the drop of entries that never
//     validly recorded a key+expiry;
//   - the SINK's field projection: an entry is persisted as {key, expiresAt}
//     ONLY — never an event payload, tier identifier, or any other field.
// Plus the one `createSeenKeys.boot()` branch the injected-fn suite never
// drives: the FIFO cap applied to an oversized persisted set (a stale file
// holding more than `maxKeys` entries must evict the OLDEST, keeping the
// newest, so the freshest retry window survives a restart).
//
// Zero network; a real mkdtemp tempdir per test, removed in afterEach. Nothing
// outside this file is edited; the sibling persisted suite's header stays
// accurate about what THAT file exercises.

import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileSeenKeysSource, fileSeenKeysSink } from '../store.mjs';
import { createSeenKeys } from '../server.mjs';

let dir = null;

// A fresh real tempdir per test (removed in afterEach).
function freshDir() {
  dir = mkdtempSync(join(tmpdir(), 'seenkeys-file-seams-'));
  return dir;
}

afterEach(() => {
  if (dir) {
    rmSync(dir, { recursive: true, force: true });
    dir = null;
  }
});

// ── SOURCE: fileSeenKeysSource ────────────────────────────────────────────────

test('fileSeenKeysSource: a missing seen-keys file reads back as [] (ENOENT tolerated)', async () => {
  const dirPath = freshDir();
  const src = fileSeenKeysSource(join(dirPath, 'absent-seenkeys.ndjson'));
  assert.deepEqual(await src(), []);
});

test('fileSeenKeysSource: a DIRECTORY at the path rejects with EISDIR (only ENOENT is tolerated)', async () => {
  const dirPath = freshDir();
  const notAFile = join(dirPath, 'seenkeys-is-a-dir');
  mkdirSync(notAFile);
  const src = fileSeenKeysSource(notAFile);
  await assert.rejects(src(), /EISDIR/);
});

test('fileSeenKeysSource: corrupt lines skipped, shape-invalid entries dropped, valid pairs survive in order', async () => {
  const dirPath = freshDir();
  const file = join(dirPath, 'seenkeys.ndjson');
  // One corrupt NDJSON line (unparseable), then every shape-invalid variant the
  // filter must drop: null, a non-object, a missing/non-string/empty key, and a
  // missing/non-numeric/non-finite expiresAt. Exactly one valid pair survives.
  writeFileSync(
    file,
    [
      '{"key": "broken', // corrupt line — skipped by parseNdjson, never throws
      'null', // parses to null — dropped
      '42', // parses to a number — dropped
      '{"nope": 1}', // no key at all — dropped
      '{"key": 7, "expiresAt": 123}', // non-string key — dropped
      '{"key": "", "expiresAt": 123}', // empty key — dropped
      '{"key": "k1"}', // missing expiresAt — dropped
      '{"key": "k1", "expiresAt": "soon"}', // string expiresAt — dropped
      '{"key": "k1", "expiresAt": 123}', // VALID
      '{"key": "k2", "expiresAt": 456.5}', // VALID (finite non-integer expiry)
      '',
    ].join('\n'),
    'utf8',
  );
  const entries = await fileSeenKeysSource(file)();
  assert.deepEqual(entries, [
    { key: 'k1', expiresAt: 123 },
    { key: 'k2', expiresAt: 456.5 },
  ]);
});

// ── SINK: fileSeenKeysSink ───────────────────────────────────────────────────

test('fileSeenKeysSink: an entry carrying payload fields persists {key, expiresAt} ONLY, and the source reads it back', async () => {
  const dirPath = freshDir();
  const file = join(dirPath, 'seenkeys.ndjson');
  const sink = fileSeenKeysSink(file);
  const src = fileSeenKeysSource(file);
  // A payload-shaped entry: the seen-key set stores ONLY the opaque client
  // idempotency-key string + its expiry — the sink must never serialize the
  // extra fields, whatever an upstream caller attaches.
  await sink([
    { key: 'client-key-1', expiresAt: 111, payload: { tier: 'gold', frames: [1, 2, 3] }, tier: 'gold', extra: 'no' },
  ]);
  // Asserted on the RAW file lines: the persisted bytes are the pair and nothing else.
  assert.equal(readFileSync(file, 'utf8'), '{"key":"client-key-1","expiresAt":111}\n');
  // And the round-trip through the real source yields exactly the pair.
  assert.deepEqual(await src(), [{ key: 'client-key-1', expiresAt: 111 }]);
});

test('fileSeenKeysSink: a non-array snapshot and [] write an empty file without throwing, and read back as []', async () => {
  const dirPath = freshDir();
  const file = join(dirPath, 'seenkeys.ndjson');
  const sink = fileSeenKeysSink(file);
  const src = fileSeenKeysSource(file);
  await sink(undefined); // non-array snapshot — tolerated, writes an empty file
  assert.equal(readFileSync(file, 'utf8'), '');
  await sink([]); // an empty live set — same empty file
  assert.equal(readFileSync(file, 'utf8'), '');
  assert.deepEqual(await src(), []);
});

// ── BOOT FIFO CAP: createSeenKeys.boot over an oversized persisted set ───────

test('createSeenKeys.boot: a persisted set larger than maxKeys keeps the newest maxKeys entries (oldest evicted)', async () => {
  const NOW = 1_000_000;
  const TTL = 600_000;
  // 15 live (unexpired) entries land in the file in insertion order; the cap is 10.
  const loaded = Array.from({ length: 15 }, (_, i) => ({
    key: `sk-${String(i + 1).padStart(2, '0')}`,
    expiresAt: NOW + TTL,
  }));
  const set = createSeenKeys({
    ttlMs: TTL,
    maxKeys: 10,
    now: () => NOW,
    load: async () => loaded,
  });
  await set.boot();
  assert.equal(set.snapshot().size, 10);
  // The five OLDEST are evicted: a stale oversized file cannot crowd out the
  // freshest retry window.
  assert.equal(set.has('sk-01'), false);
  assert.equal(set.has('sk-05'), false);
  // The newest ten survive.
  for (let i = 6; i <= 15; i++) {
    assert.equal(set.has(`sk-${String(i).padStart(2, '0')}`), true, `sk-${String(i).padStart(2, '0')} must survive`);
  }
});
