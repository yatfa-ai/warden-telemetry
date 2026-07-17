// Store tests (WARDEN-547). Exercises createNdjsonStore with an INJECTED
// capturing sink — ZERO real filesystem touched (fileSink, the real-fs wiring,
// is deliberately NOT tested here; it's a thin appendFile wrapper, and testing
// it would write a real file, which the testing discipline forbids).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createNdjsonStore, parseNdjson } from '../store.mjs';

test('createNdjsonStore requires an injected sink (no implicit real-fs default — hermetic by construction)', () => {
  assert.throws(() => createNdjsonStore(), /sink/);
  assert.throws(() => createNdjsonStore({}), /sink/);
});

test('appendEvents writes one NDJSON line per event via the injected sink', async () => {
  const lines = [];
  const store = createNdjsonStore({ sink: async (line) => void lines.push(line) });
  await store.appendEvents([{ a: 1 }, { b: 2 }]);
  assert.equal(lines.length, 2);
  assert.deepEqual(JSON.parse(lines[0]), { a: 1 });
  assert.deepEqual(JSON.parse(lines[1]), { b: 2 });
});

test('appendEvents preserves order across async sinks (serial append)', async () => {
  const lines = [];
  const slowSink = async (line) => {
    await new Promise((r) => setTimeout(r, 0));
    lines.push(line);
  };
  const store = createNdjsonStore({ sink: slowSink });
  await store.appendEvents([{ i: 1 }, { i: 2 }, { i: 3 }]);
  assert.deepEqual(
    lines.map((l) => JSON.parse(l).i),
    [1, 2, 3]
  );
});

test('appendEvents with an empty array writes nothing', async () => {
  const lines = [];
  const store = createNdjsonStore({ sink: (l) => void lines.push(l) });
  await store.appendEvents([]);
  assert.equal(lines.length, 0);
});

test('each persisted line is a self-contained JSON record (NDJSON-safe, no bare fragments)', async () => {
  const lines = [];
  const store = createNdjsonStore({ sink: (l) => void lines.push(l) });
  await store.appendEvents([{ type: 'error', name: 'E', message: 'm' }]);
  assert.equal(lines.length, 1);
  // A single NDJSON line must round-trip through JSON.parse on its own.
  assert.deepEqual(JSON.parse(lines[0]), { type: 'error', name: 'E', message: 'm' });
});

// ── READ SEAM (WARDEN-567) ────────────────────────────────────────────────────
// Exercises the read surface with INJECTED capturing sink/source pairs — still
// ZERO real filesystem. fileSink/fileSource (the real-fs wiring) are NOT tested
// here for the same reason as before: they are thin fs wrappers, and exercising
// them would touch real disk. parseNdjson (the pure fs-free core of the read) IS
// unit-tested below.

test('parseNdjson parses one object per non-blank line', () => {
  const text = '{"a":1}\n{"b":2}\n';
  assert.deepEqual(parseNdjson(text), [{ a: 1 }, { b: 2 }]);
});

test('parseNdjson drops blank lines (including the trailing one)', () => {
  assert.deepEqual(parseNdjson('{"a":1}\n\n{"b":2}\n'), [{ a: 1 }, { b: 2 }]);
  assert.deepEqual(parseNdjson(''), []);
  assert.deepEqual(parseNdjson('\n\n'), []);
});

test('parseNdjson skips an unparseable line (a partial append) instead of throwing', () => {
  // line 2 is a half-written record (a process killed mid-append); it must not
  // take down the whole read — the good records on either side survive.
  const text = '{"good":1}\n{"bad": tru   ← partial\n{"good":2}\n';
  assert.deepEqual(parseNdjson(text), [{ good: 1 }, { good: 2 }]);
});

test('readEvents() round-trips appended events through an injected in-memory source (zero real fs)', async () => {
  // An in-memory store: the sink captures NDJSON lines into `lines`; the source
  // parses them back — exactly the contract the production fileSink/fileSource
  // pair satisfies, but with no disk involved.
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => parseNdjson(lines.join('\n')),
  });
  await store.appendEvents([{ a: 1 }, { b: 2 }]);
  const read = await store.readEvents();
  assert.deepEqual(read, [{ a: 1 }, { b: 2 }]);
});

test('readEvents() reflects appends that happen after the store is built (live read)', async () => {
  const lines = [];
  const store = createNdjsonStore({
    sink: async (line) => void lines.push(line),
    source: () => parseNdjson(lines.join('\n')),
  });
  assert.deepEqual(await store.readEvents(), []); // empty before any append
  await store.appendEvents([{ x: 9 }]);
  assert.deepEqual(await store.readEvents(), [{ x: 9 }]); // visible after
});

test('readEvents() on a source-less store throws a loud TypeError (fail loud, not a silent [])', () => {
  const store = createNdjsonStore({ sink: () => {} }); // write-only — no source
  // Mirrors createRequestHandler's synchronous "requires a store" throw: the
  // error surfaces at the call, not as a silent [] or a swallowed rejection.
  assert.throws(() => store.readEvents(), /source/);
});
