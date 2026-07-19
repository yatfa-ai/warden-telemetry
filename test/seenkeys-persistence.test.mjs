// Seen-key dedup PERSISTENCE tests (WARDEN-803). Closes the documented restart-
// mid-retry-window residual edge: a retried batch whose original 202 was lost,
// retried AFTER a receiver restart, must still land as ONE event — not double-
// persisted. Drives the REAL `createSeenKeys` (server.mjs) with INJECTED capturing
// load/persist fns + an injected fake clock/scheduler, mirroring the store's
// capturing-sink discipline and the retention trigger's fake-clock debounce suite
// (test/server.test.mjs). The real-fs file helpers (fileSeenKeysSource /
// fileSeenKeysSink) are thin fs wrappers, NOT exercised here — same discipline as
// the store suite, which leaves fileSink/fileSource/fileRewrite untested-by-design.
//
// The "restart" is simulated WITHOUT real disk: the first set's `persist` sink
// captures the entries it would have written; the second set's `load` source
// returns those captured entries. That is the exact byte-for-byte contract the
// production file helpers satisfy (persist serializes {key, expiresAt}, load
// parses it back), driven through the real createSeenKeys boot/record/has path.
// ZERO real network, ZERO real filesystem.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createSeenKeys } from '../server.mjs';
import { ingest } from '../ingest.mjs';
import { SCHEMA_VERSION, validateEvent } from '../schema.ts';

// A fake scheduler + clock: setTimer/clearTimer manage an in-memory queue; now()
// is a controllable value. Mirrors fakeClock() in test/server.test.mjs so the
// debounce/re-entrancy logic is exercised deterministically with NO real timer.
function fakeClock() {
  const timers = [];
  let nowVal = 0;
  return {
    setTimer: (fn) => {
      const id = { fn };
      timers.push(id);
      return id;
    },
    clearTimer: (id) => {
      const i = timers.indexOf(id);
      if (i >= 0) timers.splice(i, 1);
    },
    now: () => nowVal,
    setNow: (v) => {
      nowVal = v;
    },
    pending: () => timers.length,
    flushAll: () => {
      while (timers.length) timers.shift().fn();
    },
  };
}

// A capturing in-memory store for the ingest() dedup-HIT assertion: records what
// would have been persisted, so the test can assert the retried batch reached ZERO
// writes. Mirrors memoryStore() in test/ingest.test.mjs.
function memoryStore() {
  const appended = [];
  return {
    appended,
    async appendEvents(events) {
      for (const event of events) appended.push(event);
    },
  };
}

const validEvent = {
  schemaVersion: SCHEMA_VERSION,
  type: 'error',
  runtime: 'main',
  timestamp: 1,
  name: 'E',
  message: 'm',
  frames: [],
};
const validBody = JSON.stringify({ schemaVersion: SCHEMA_VERSION, events: [validEvent] });
const schemaHeaders = { 'x-telemetry-schema': String(SCHEMA_VERSION) };

const TTL = 600_000; // 10 min — the production default; ample headroom over a restart.

// ── DEBOUNCED / OFF-PATH PERSIST WRITE (mirrors the retention debounce suite) ──

test('record() arms a DEBOUNCED persist with { key, expiresAt } — never synchronous on the hot path', async () => {
  const clock = fakeClock();
  clock.setNow(1000);
  const persisted = [];
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: clock.now,
    load: async () => [],
    persist: async (entries) => {
      persisted.push(entries);
    },
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  await seen.boot();

  seen.record('batch-1');
  // The hot path pays NO synchronous disk write: persist is deferred to the timer.
  assert.equal(clock.pending(), 1, 'record armed one debounce timer');
  assert.equal(persisted.length, 0, 'persist was NOT called synchronously on record()');

  clock.flushAll(); // fire the debounced flush
  await Promise.resolve(); // let the persist promise's sync body run
  assert.equal(persisted.length, 1, 'flush fired exactly one persist');
  assert.deepEqual(
    persisted[0],
    [{ key: 'batch-1', expiresAt: 1000 + TTL }],
    'persist received the { key, expiresAt } pair (expiry = now + ttl)'
  );
});

test('a BURST of record() calls coalesces into ONE persist (debounce, not per-key writes)', async () => {
  const clock = fakeClock();
  clock.setNow(2000);
  const persisted = [];
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: clock.now,
    persist: async (entries) => {
      persisted.push(entries);
    },
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  seen.record('a');
  assert.equal(clock.pending(), 1, 'first record armed a timer');
  seen.record('b');
  seen.record('c');
  assert.equal(clock.pending(), 1, 'the burst coalesced into the SAME single timer');

  clock.flushAll();
  await Promise.resolve();
  assert.equal(persisted.length, 1, 'one persist for the whole burst');
  assert.equal(persisted[0].length, 3, 'all three keys reached disk in that one write');
});

test('a persist REJECTION is swallowed — record() and the flush never throw (telemetry is best-effort)', async () => {
  const clock = fakeClock();
  clock.setNow(3000);
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: clock.now,
    persist: async () => {
      throw new Error('disk full');
    },
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  // record() is synchronous and arms only — it can never throw from the persist path.
  assert.doesNotThrow(() => seen.record('x'), 'record() does not throw when persist will reject');
  // Flushing the rejected persist must not throw (the .catch swallows).
  assert.doesNotThrow(() => clock.flushAll(), 'flushing a rejected persist does not throw');
  await new Promise((r) => setTimeout(r, 0)); // let the rejected persist's .catch settle

  // The receiver stays healthy: a subsequent record + flush still works, and the
  // in-memory set remains correct for this process even though disk never accepted it.
  assert.doesNotThrow(() => seen.record('y'));
  assert.doesNotThrow(() => clock.flushAll());
  assert.equal(seen.has('x'), true, 'in-memory dedup still holds despite the persist failure');
});

test('cancel() clears a pending debounced persist before it fires (shutdown cleanup)', async () => {
  const clock = fakeClock();
  clock.setNow(4000);
  const persisted = [];
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: clock.now,
    persist: async (entries) => {
      persisted.push(entries);
    },
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  seen.record('doomed');
  assert.equal(clock.pending(), 1, 'armed');
  seen.cancel();
  assert.equal(clock.pending(), 0, 'pending timer cleared by cancel()');
  clock.flushAll(); // nothing to flush
  assert.equal(persisted.length, 0, 'no persist ran after cancel');
});

// ── THE RESTART WINDOW (the ticket's central done criterion) ──────────────────

test('RESTART: a key recorded before a restart SURVIVES — has() is true post-restart and ingest() dedups WITHOUT appendEvents', async () => {
  // ── "first" receiver process: accept a batch, record the key, persist the set ──
  let clock1 = 1_000;
  const diskWrites = []; // the bytes the sink would have written to seen-keys.ndjson
  const clock1api = fakeClock();
  clock1api.setNow(clock1);
  const seen1 = createSeenKeys({
    ttlMs: TTL,
    now: clock1api.now,
    load: async () => [], // a fresh receiver — empty file on boot
    persist: async (entries) => {
      diskWrites.push(entries);
    },
    setTimer: clock1api.setTimer,
    clearTimer: clock1api.clearTimer,
  });
  await seen1.boot();
  // A successful persist of the batch records its idempotency key (ingest.mjs:176).
  seen1.record('batch-retry');
  clock1api.flushAll(); // the debounced persist writes the live set to "disk"
  await Promise.resolve();
  assert.equal(diskWrites.length, 1, 'first process persisted the set once');
  const onDisk = diskWrites[diskWrites.length - 1];
  assert.deepEqual(onDisk, [{ key: 'batch-retry', expiresAt: 1_000 + TTL }]);

  // ── the receiver RESTARTS here (crash recovery / deploy / bounce) ──

  // ── "second" receiver process: boot, RELOAD the persisted set from disk ──
  let clock2 = 5_000; // advanced past the restart, still WELL within the TTL window
  const clock2api = fakeClock();
  clock2api.setNow(clock2);
  const seen2 = createSeenKeys({
    ttlMs: TTL,
    now: clock2api.now,
    load: async () => onDisk, // the receiver reads back exactly what the sink wrote
    persist: async () => {}, // not under test here
  });
  await seen2.boot();

  // The pre-restart key survived the restart — the dedup HIT still fires.
  assert.equal(
    seen2.has('batch-retry'),
    true,
    'the pre-restart key is still a dedup HIT after reload (the restart window is closed)'
  );

  // The EXACT case that double-persists today: the client (which lost its 2xx
  // before the restart) retries the SAME bytes within its bounded ≤3 window. The
  // reloaded set recognizes the key and answers 202 {accepted:0, deduped:true}
  // WITHOUT calling store.appendEvents — asserted via a capturing store.
  const store = memoryStore();
  const headers = { ...schemaHeaders, 'idempotency-key': 'batch-retry' };
  const res = await ingest(
    { headers, body: validBody },
    { SCHEMA_VERSION, validateEvent, store, seenKeys: seen2, now: clock2api.now }
  );
  assert.deepEqual(
    res,
    { ok: true, status: 202, body: { accepted: 0, deduped: true } },
    'the retried batch was deduped, not accepted'
  );
  assert.equal(
    store.appended.length,
    0,
    'the retried batch was NOT re-persisted — restart dedup held (one event, not two)'
  );
});

test('load DROPS already-expired entries — a stale file cannot resurrect a dead key', async () => {
  let now = 10_000;
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: () => now,
    load: async () => [
      { key: 'live', expiresAt: now + 60_000 }, // still within TTL — kept
      { key: 'dead', expiresAt: now - 1 }, // already expired (stale file) — dropped
      { key: 'boundary', expiresAt: now }, // expiresAt <= now — dropped (lazy TTL uses <=)
      { key: 'bad-expiry', expiresAt: 'oops' }, // malformed — skipped
      { key: '', expiresAt: now + 60_000 }, // empty key — skipped
    ],
    persist: async () => {},
  });
  await seen.boot();
  assert.equal(seen.has('live'), true, 'a live key loads');
  assert.equal(seen.has('dead'), false, 'an already-expired key is dropped on load');
  assert.equal(
    seen.has('boundary'),
    false,
    'expiresAt === now is treated as expired (<=), matching the lazy TTL purge'
  );
  assert.equal(seen.has('bad-expiry'), false, 'a malformed expiry is skipped');
  assert.equal(seen.snapshot().size, 1, 'only the one live key was seeded');
});

test('load dropping entries never throws on a corrupt/garbage source — the receiver always boots', async () => {
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: () => 1_000,
    load: async () => {
      throw new Error('corrupt file');
    },
    persist: async () => {},
  });
  await assert.doesNotReject(() => seen.boot(), 'a throwing load() never rejects boot');
  assert.equal(seen.snapshot().size, 0, 'the set starts empty when the file is unreadable');
});

test('load returns non-array / malformed entries gracefully (defensive — receiver boots empty)', async () => {
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: () => 1_000,
    load: async () => null,
    persist: async () => {},
  });
  await assert.doesNotReject(() => seen.boot());
  assert.equal(seen.snapshot().size, 0, 'a non-array load result seeds nothing');
});

// ── UNWIRED = TODAY'S BEHAVIOR (the optional-seam contract) ───────────────────

test('UNWIRED (no load/persist): record() arms nothing, boot() is a no-op — exactly today behavior', async () => {
  let armed = 0;
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: () => 1_000,
    setTimer: () => {
      armed += 1;
      return null;
    },
    clearTimer: () => {},
  });
  await assert.doesNotReject(() => seen.boot(), 'boot with no load is a no-op');
  seen.record('x');
  assert.equal(armed, 0, 'no persist wired → no debounce timer armed');
  assert.equal(seen.has('x'), true, 'in-memory dedup still works exactly as before');
});

test('persist-only wiring (no load) starts empty but still persists on change — a valid asymmetric wiring', async () => {
  const clock = fakeClock();
  clock.setNow(7_000);
  const persisted = [];
  const seen = createSeenKeys({
    ttlMs: TTL,
    now: clock.now,
    // no `load` — the set starts empty on every boot, but changes still persist
    persist: async (entries) => {
      persisted.push(entries);
    },
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
  });
  await seen.boot(); // no-op (no load)
  assert.equal(seen.snapshot().size, 0);
  seen.record('asym');
  clock.flushAll();
  await Promise.resolve();
  assert.equal(persisted.length, 1, 'a persist-only wiring still writes on change');
  assert.deepEqual(persisted[0], [{ key: 'asym', expiresAt: 7_000 + TTL }]);
});
