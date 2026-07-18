// Store tests (WARDEN-547). Exercises createNdjsonStore with an INJECTED
// capturing sink — ZERO real filesystem touched (fileSink, the real-fs wiring,
// is deliberately NOT tested here; it's a thin appendFile wrapper, and testing
// it would write a real file, which the testing discipline forbids).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createNdjsonStore, parseNdjson, applyRetention } from '../store.mjs';

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

// ── RETENTION (WARDEN-579) ────────────────────────────────────────────────────
// Exercises the retention policy + prune with INJECTED capturing sink/source/
// rewrite fns — still ZERO real filesystem. `fileRewrite` (the real-fs wiring)
// is NOT tested here for the same reason as fileSink/fileSource: it is a thin fs
// wrapper, and exercising it would touch real disk. `applyRetention` (the pure
// fs-free policy core) and `prune` (driven through an in-memory rewrite seam)
// ARE unit-tested below.
//
// An in-memory FILE MIRROR: sink appends a line, source parses it back, rewrite
// replaces the whole content — exactly the contract the production
// fileSink/fileSource/fileRewrite trio satisfies, with no disk involved.
function inMemoryFile() {
  let text = '';
  return {
    sink: async (line) => {
      text += `${line}\n`;
    },
    source: () => parseNdjson(text),
    rewrite: async (newText) => {
      text = newText;
    },
    read: () => parseNdjson(text),
    snapshot: () => text,
  };
}

// ── applyRetention — the PURE policy core (plain arrays, no seams) ───────────

test('applyRetention count cap keeps the LAST N (newest by arrival order), drops the older excess', () => {
  const events = [{ i: 1 }, { i: 2 }, { i: 3 }, { i: 4 }, { i: 5 }];
  assert.deepEqual(applyRetention(events, { maxEvents: 2 }), [{ i: 4 }, { i: 5 }]);
});

test('applyRetention count cap is a no-op when the store is already under the cap', () => {
  const events = [{ i: 1 }, { i: 2 }];
  assert.deepEqual(applyRetention(events, { maxEvents: 10 }), [{ i: 1 }, { i: 2 }]);
  // Exactly at the cap → still no drop.
  assert.deepEqual(applyRetention(events, { maxEvents: 2 }), [{ i: 1 }, { i: 2 }]);
});

test('applyRetention age window drops events older than now - maxAgeMs and keeps the rest', () => {
  const NOW = 10_000;
  const events = [
    { i: 'old', timestamp: NOW - 4000 },
    { i: 'fresh', timestamp: NOW - 500 },
    { i: 'edge', timestamp: NOW - 1000 }, // exactly at the cutoff boundary → kept (>=)
  ];
  // maxAgeMs = 1000 → cutoff = 9000 → drop timestamp < 9000.
  assert.deepEqual(applyRetention(events, { maxAgeMs: 1000, now: NOW }), [
    { i: 'fresh', timestamp: NOW - 500 },
    { i: 'edge', timestamp: NOW - 1000 },
  ]);
});

test('applyRetention age window KEEPS events with no finite timestamp (age unknowable → never silently dropped)', () => {
  const NOW = 10_000;
  const events = [
    { i: 'noTs' },
    { i: 'nullTs', timestamp: null },
    { i: 'strTs', timestamp: 'oops' },
    { i: 'fresh', timestamp: NOW },
  ];
  const kept = applyRetention(events, { maxAgeMs: 1000, now: NOW });
  assert.deepEqual(kept, events.slice(0, 3).concat(events[3])); // all four kept
});

test('applyRetention applies BOTH policies (an event is retained iff it survives both)', () => {
  const NOW = 10_000;
  const events = [
    { i: 'old-and-excess', timestamp: NOW - 9999 },
    { i: 'fresh-1', timestamp: NOW },
    { i: 'fresh-2', timestamp: NOW },
  ];
  // Age drops the old one; count cap 1 then keeps the LAST 1 of what survived.
  assert.deepEqual(applyRetention(events, { maxEvents: 1, maxAgeMs: 1000, now: NOW }), [
    { i: 'fresh-2', timestamp: NOW },
  ]);
});

test('applyRetention with both policies disabled retains everything (the opt-out)', () => {
  const events = [{ i: 1 }, { i: 2 }, { i: 3 }];
  assert.deepEqual(applyRetention(events, { maxEvents: 0, maxAgeMs: 0 }), events);
  // Defaults are also disabled (calling with no opts keeps all).
  assert.deepEqual(applyRetention(events), events);
});

test('applyRetention on a non-array yields [] (robust to a bad read)', () => {
  assert.deepEqual(applyRetention(null, { maxEvents: 1 }), []);
  assert.deepEqual(applyRetention(undefined, { maxEvents: 1 }), []);
});

test('applyRetention never mutates its input', () => {
  const events = [{ i: 1 }, { i: 2 }, { i: 3 }];
  applyRetention(events, { maxEvents: 1 });
  assert.deepEqual(events, [{ i: 1 }, { i: 2 }, { i: 3 }]); // untouched
});

// ── applyRetention — clock-skew robustness via receivedAt (WARDEN-692) ────────
// The age-prune keys off the RECEIVER's `receivedAt` (with a `timestamp` fallback)
// so the retention bound is deterministic per-receiver, not governed by each
// client's skewed clock.

test('applyRetention age-prune keys off receivedAt: a slow-clock client (stale timestamp) survives via a fresh receivedAt', () => {
  // Server now = 10000, maxAgeMs = 1000 → cutoff = 9000. The client's `timestamp`
  // is far in the past (a slow clock) — under timestamp-only keying that PRUNES it
  // prematurely (timestamp < cutoff). receivedAt = NOW-50 (the receiver saw it
  // recently) → it survives, judged off the receiver's clock.
  const NOW = 10_000;
  const events = [{ i: 'slow-clock', timestamp: NOW - 9999, receivedAt: NOW - 50 }];
  assert.deepEqual(applyRetention(events, { maxAgeMs: 1000, now: NOW }), [
    { i: 'slow-clock', timestamp: NOW - 9999, receivedAt: NOW - 50 },
  ]);
});

test('applyRetention: a fast-clock event with a fresh receivedAt survives the age-prune (success criterion #2)', () => {
  // The fast-clock spike from the timeline test must ALSO survive retention: its
  // receivedAt is recent, so it is retained (consistent with a real recent event).
  const NOW = 10_000;
  const events = [{ i: 'fast-clock', timestamp: NOW + 300_000, receivedAt: NOW - 50 }];
  assert.deepEqual(applyRetention(events, { maxAgeMs: 1000, now: NOW }), [
    { i: 'fast-clock', timestamp: NOW + 300_000, receivedAt: NOW - 50 },
  ]);
});

test('applyRetention: an event lacking receivedAt is age-pruned on the timestamp fallback (graceful backfill)', () => {
  // Pre-annotation events (no receivedAt): the client timestamp governs, unchanged.
  const NOW = 10_000;
  const events = [
    { i: 'old-no-receivedAt', timestamp: NOW - 4000 },
    { i: 'fresh-no-receivedAt', timestamp: NOW - 100 },
  ];
  assert.deepEqual(applyRetention(events, { maxAgeMs: 1000, now: NOW }), [
    { i: 'fresh-no-receivedAt', timestamp: NOW - 100 },
  ]);
});

test('applyRetention: receivedAt is PREFERRED — an aged-out timestamp with no receivedAt is pruned, with one is kept', () => {
  // Same stale client timestamp; only the event WITH a fresh receivedAt survives.
  const NOW = 10_000;
  const events = [
    { i: 'no-stamp', timestamp: NOW - 9999 }, // no receivedAt → pruned (timestamp < cutoff)
    { i: 'stamped', timestamp: NOW - 9999, receivedAt: NOW - 50 }, // receivedAt → kept
  ];
  assert.deepEqual(applyRetention(events, { maxAgeMs: 1000, now: NOW }), [
    { i: 'stamped', timestamp: NOW - 9999, receivedAt: NOW - 50 },
  ]);
});

// ── prune — the persisted-seam compaction (in-memory rewrite) ────────────────

test('prune with a count cap rewrites the file to the newest N retained events', async () => {
  const f = inMemoryFile();
  const store = createNdjsonStore(f);
  await store.appendEvents([{ i: 1 }, { i: 2 }, { i: 3 }, { i: 4 }, { i: 5 }]);
  const res = await store.prune({ maxEvents: 2 });
  assert.equal(res.before, 5);
  assert.equal(res.after, 2);
  assert.equal(res.pruned, 3);
  assert.equal(res.rewrote, true);
  // Newest 2 (append order) retained; the file now holds exactly them.
  assert.deepEqual(f.read(), [{ i: 4 }, { i: 5 }]);
});

test('prune with an age window drops aged-out events and rewrites the rest', async () => {
  const f = inMemoryFile();
  const store = createNdjsonStore(f);
  const NOW = 10_000;
  await store.appendEvents([
    { i: 'old', timestamp: NOW - 4000 },
    { i: 'fresh', timestamp: NOW - 500 },
  ]);
  const res = await store.prune({ maxAgeMs: 1000, now: NOW });
  assert.equal(res.pruned, 1);
  assert.deepEqual(f.read(), [{ i: 'fresh', timestamp: NOW - 500 }]);
});

test('prune performs NO rewrite when nothing exceeds the bound (no churn, no disk write)', async () => {
  const f = inMemoryFile();
  let rewriteCalls = 0;
  const store = createNdjsonStore({
    sink: f.sink,
    source: f.source,
    rewrite: async (t) => {
      rewriteCalls += 1;
      await f.rewrite(t);
    },
  });
  await store.appendEvents([{ i: 1 }, { i: 2 }]);
  const res = await store.prune({ maxEvents: 10 }); // bound not exceeded
  assert.equal(rewriteCalls, 0, 'rewrite seam was not invoked');
  assert.equal(res.rewrote, false);
  assert.equal(res.pruned, 0);
  assert.equal(res.after, 2);
});

test('prune rewrites the file to EMPTY NDJSON when every event is aged out', async () => {
  const f = inMemoryFile();
  let writtenText = 'UNSET';
  const store = createNdjsonStore({
    sink: f.sink,
    source: f.source,
    rewrite: async (t) => {
      writtenText = t;
      await f.rewrite(t);
    },
  });
  const NOW = 10_000;
  await store.appendEvents([{ timestamp: 1 }, { timestamp: 2 }]);
  const res = await store.prune({ maxAgeMs: 1000, now: NOW });
  assert.equal(res.pruned, 2);
  assert.equal(res.rewrote, true);
  assert.equal(writtenText, '', 'an emptied store is rewritten as empty NDJSON');
  assert.deepEqual(f.read(), []);
});

test('prune on a store WITHOUT a rewrite seam rejects loud (fail loud, not a silent no-op)', async () => {
  const store = createNdjsonStore({ sink: async () => {}, source: () => [] }); // no rewrite
  // Async rejection (not sync throw) so the server trigger's Promise.catch recovers.
  await assert.rejects(() => store.prune({ maxEvents: 1 }), /rewrite/);
});

test('prune on a store WITHOUT a source seam rejects loud', async () => {
  const store = createNdjsonStore({ sink: async () => {} }); // write-only
  await assert.rejects(() => store.prune({ maxEvents: 1 }), /source/);
});

test('after prune, readEvents reflects the retained set (post-prune round-trip)', async () => {
  const f = inMemoryFile();
  const store = createNdjsonStore(f);
  await store.appendEvents([{ i: 1 }, { i: 2 }, { i: 3 }, { i: 4 }]);
  await store.prune({ maxEvents: 2 });
  // readEvents is the exact surface /summary uses — it must see the post-prune set.
  assert.deepEqual(await store.readEvents(), [{ i: 3 }, { i: 4 }]);
});

test('prune serializes against a concurrent append — an appended-during-compaction event is never lost', async () => {
  // A SLOW rewrite: it stalls until we release a gate, so we can start a
  // concurrent append WHILE the compaction is mid-flight and prove serialization
  // keeps the new event (it is not clobbered by the in-place rewrite).
  const f = inMemoryFile();
  let release;
  const rewriteGate = new Promise((r) => {
    release = r;
  });
  const store = createNdjsonStore({
    sink: f.sink,
    source: f.source,
    rewrite: async (t) => {
      await rewriteGate;
      await f.rewrite(t);
    },
  });

  // Seed 3 events, then compact to a count cap of 2 (drops the oldest). The
  // prune is started WITHOUT awaiting — it stalls inside the slow rewrite.
  await store.appendEvents([{ i: 1 }, { i: 2 }, { i: 3 }]);
  const pruneP = store.prune({ maxEvents: 2 });
  // Yield so prune reaches the rewrite gate (reads, computes, stalls).
  await new Promise((r) => setTimeout(r, 0));

  // Append a 4th event WHILE the compaction is mid-flight (it was NOT in the
  // snapshot prune read).
  const appendP = store.appendEvents([{ i: 4 }]);

  release(); // let the rewrite finish; both ops now complete in serialized order
  await Promise.all([pruneP, appendP]);

  // The 4th event must survive — serialization queued the append AFTER the
  // rewrite instead of letting the rename drop it.
  const finalIds = f.read().map((e) => e.i);
  assert.ok(finalIds.includes(4), `appended-during-prune event not lost (got ${JSON.stringify(finalIds)})`);
});
