// Store tests (WARDEN-547). Exercises createNdjsonStore with an INJECTED
// capturing sink — ZERO real filesystem touched (fileSink, the real-fs wiring,
// is deliberately NOT tested here; it's a thin appendFile wrapper, and testing
// it would write a real file, which the testing discipline forbids).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createNdjsonStore } from '../store.mjs';

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
