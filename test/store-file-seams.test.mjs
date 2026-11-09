// Event-store FILE-SEAM tests. The real-filesystem helpers `fileSink`,
// `fileSource` and `fileRewrite` (store.mjs — wired in production at server.mjs:
// sink: fileSink(storePath) / source: fileSource(storePath) / rewrite:
// fileRewrite(storePath)) are the byte contract of telemetry.ndjson, yet every
// other suite injects capturing fns (store.test.mjs and
// seenkeys-persistence.test.mjs concede these are untested-by-design). What no
// injected-fn test can catch:
//   - the SINK's newline termination, append-not-overwrite semantics and
//     cwd-at-creation path resolution;
//   - the SOURCE's ENOENT-only tolerance (swallowing any other read error would
//     let a prune see an empty store and rewrite it empty) and its forwarding
//     of `onSkip` to the parser;
//   - the REWRITE's whole-file replace and atomic tmp+rename (no `.tmp` left).
//
// Zero network; a real mkdtemp tempdir per test, removed in afterEach. Only
// node:test / node:fs are used. Nothing outside this file is edited.

import { test, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  mkdtempSync,
  mkdirSync,
  rmSync,
  writeFileSync,
  readFileSync,
  readdirSync,
  existsSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileSink, fileSource, fileRewrite, createNdjsonStore } from '../store.mjs';

let dir = null;
let origCwd = null;

function freshDir() {
  dir = mkdtempSync(join(tmpdir(), 'store-file-seams-'));
  return dir;
}

afterEach(() => {
  if (origCwd) {
    process.chdir(origCwd);
    origCwd = null;
  }
  if (dir) {
    rmSync(dir, { recursive: true, force: true });
    dir = null;
  }
});

// ── SINK: fileSink ────────────────────────────────────────────────────────────

test('fileSink: creates the file on first write and newline-terminates every record', async () => {
  const path = join(freshDir(), 'telemetry.ndjson');
  const sink = fileSink(path);
  await sink('{"a":1}');
  await sink('{"b":2}');
  assert.equal(readFileSync(path, 'utf8'), '{"a":1}\n{"b":2}\n');
});

test('fileSink: APPENDS to existing content rather than overwriting it', async () => {
  const path = join(freshDir(), 'telemetry.ndjson');
  writeFileSync(path, '{"old":true}\n', 'utf8');
  await fileSink(path)('{"new":true}');
  assert.equal(readFileSync(path, 'utf8'), '{"old":true}\n{"new":true}\n');
});

test('fileSink: the destination is resolved against cwd when the sink is CREATED, not at write time', async () => {
  const root = freshDir();
  const first = join(root, 'first');
  const second = join(root, 'second');
  mkdirSync(first);
  mkdirSync(second);
  origCwd = process.cwd();
  process.chdir(first);
  const sink = fileSink('rel.ndjson');
  process.chdir(second);
  await sink('{"x":1}');
  assert.ok(existsSync(join(first, 'rel.ndjson')), 'written under the cwd at creation time');
  assert.ok(!existsSync(join(second, 'rel.ndjson')), 'not written under the later cwd');
});

test('fileSink: a missing parent directory rejects with ENOENT', async () => {
  const path = join(freshDir(), 'no-such-dir', 'telemetry.ndjson');
  await assert.rejects(fileSink(path)('{"a":1}'), (e) => e && e.code === 'ENOENT');
});

// ── SOURCE: fileSource ────────────────────────────────────────────────────────

test('fileSource: a missing file reads back as [] (ENOENT tolerated)', async () => {
  const path = join(freshDir(), 'absent.ndjson');
  assert.deepEqual(await fileSource(path)(), []);
});

test('fileSource: a non-ENOENT read error rethrows (a directory at the path -> EISDIR)', async () => {
  const path = join(freshDir(), 'is-a-dir');
  mkdirSync(path);
  await assert.rejects(fileSource(path)(), (e) => e && e.code === 'EISDIR');
});

test('fileSource: a torn/corrupt line is skipped and reported TRIMMED to onSkip', async () => {
  const path = join(freshDir(), 'telemetry.ndjson');
  writeFileSync(path, '{"a":1}\n   {"torn":  \n{"b":2}\n', 'utf8');
  const skipped = [];
  const events = await fileSource(path)((line) => skipped.push(line));
  assert.deepEqual(events, [{ a: 1 }, { b: 2 }]);
  assert.deepEqual(skipped, ['{"torn":']);
});

test('fileSource: omitting onSkip still skips the bad line without throwing', async () => {
  const path = join(freshDir(), 'telemetry.ndjson');
  writeFileSync(path, '{"a":1}\nnot json\n{"b":2}\n', 'utf8');
  assert.deepEqual(await fileSource(path)(), [{ a: 1 }, { b: 2 }]);
});

// ── REWRITE: fileRewrite ──────────────────────────────────────────────────────

test('fileRewrite: replaces the whole file (no stale tail) and leaves no .tmp sibling', async () => {
  const root = freshDir();
  const path = join(root, 'telemetry.ndjson');
  writeFileSync(path, '{"a":1}\n{"b":2}\n{"c":3}\n', 'utf8');
  await fileRewrite(path)('{"z":9}\n');
  assert.equal(readFileSync(path, 'utf8'), '{"z":9}\n');
  assert.deepEqual(readdirSync(root), ['telemetry.ndjson']);
});

test('fileRewrite: creates the file when absent', async () => {
  const path = join(freshDir(), 'telemetry.ndjson');
  await fileRewrite(path)('{"a":1}\n');
  assert.equal(readFileSync(path, 'utf8'), '{"a":1}\n');
});

test('fileRewrite: an empty set reads back as []', async () => {
  const path = join(freshDir(), 'telemetry.ndjson');
  writeFileSync(path, '{"a":1}\n', 'utf8');
  await fileRewrite(path)('');
  assert.deepEqual(await fileSource(path)(), []);
});

// ── createNdjsonStore composed over the REAL seams ────────────────────────────

test('createNdjsonStore over real seams: append -> read -> prune rewrites to the retained set; later append lands after it', async () => {
  const path = join(freshDir(), 'telemetry.ndjson');
  const store = createNdjsonStore({
    sink: fileSink(path),
    source: fileSource(path),
    rewrite: fileRewrite(path),
  });
  await store.appendEvents([{ n: 1 }, { n: 2 }, { n: 3 }]);
  assert.deepEqual(await store.readEvents(), [{ n: 1 }, { n: 2 }, { n: 3 }]);

  const result = await store.prune({ maxEvents: 2 });
  assert.equal(result.rewrote, true);
  assert.equal(result.pruned, 1);
  assert.equal(readFileSync(path, 'utf8'), '{"n":2}\n{"n":3}\n');

  await store.appendEvents([{ n: 4 }]);
  assert.equal(readFileSync(path, 'utf8'), '{"n":2}\n{"n":3}\n{"n":4}\n');
});

test('createNdjsonStore over real seams: a no-op prune performs no rewrite', async () => {
  const path = join(freshDir(), 'telemetry.ndjson');
  const store = createNdjsonStore({
    sink: fileSink(path),
    source: fileSource(path),
    rewrite: fileRewrite(path),
  });
  await store.appendEvents([{ n: 1 }, { n: 2 }]);
  const before = readFileSync(path, 'utf8');
  const result = await store.prune({ maxEvents: 5 });
  assert.equal(result.rewrote, false);
  assert.equal(readFileSync(path, 'utf8'), before);
  assert.ok(!existsSync(`${path}.tmp`), 'no rewrite -> no tmp file was ever produced');
});
