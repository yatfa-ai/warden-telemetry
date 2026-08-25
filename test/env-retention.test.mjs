// envRetentionInt env-parsing tests (WARDEN-1182).
//
// `envRetentionInt` (server.mjs) is the SOLE parser for all three of the
// receiver's operational bounds — STORE_MAX_EVENTS, STORE_MAX_AGE_HOURS and
// INGEST_MAX_BODY_BYTES. Its contract, from its own comment: unset/empty → the
// bounded default; an explicit "0" → the documented opt-out; malformed/negative
// → fall back to the bounded default "so a typo can never silently unbound the
// store".
//
// It was entirely unexercised. The CONSUMER semantics are exhaustively covered
// (createRequestHandler({ maxBodyBytes }) has ~130 references in this suite) but
// always with a numeric literal passed straight in — the env→number translation
// that PRODUCES that number in production ran in no test. Mutation-proven at
// filing: deleting the `raw === ''` guard, deleting the
// `!Number.isFinite(n) || n < 0` guard, or gutting the whole body to
// `return Number(process.env[name])` each left the full suite green.
//
// Why that matters: `Number('') === 0`, and `0` is this function's documented
// OPT-OUT. So the empty-string guard is the only thing between `STORE_MAX_EVENTS=`
// (an empty assignment — a very common Docker/compose env-file shape) and
// "retention disabled" on a listener that is OPEN BY DEFAULT (AUTH_TOKEN unset).
// The failure mode is unbounded NDJSON growth on the maintainer's disk plus an
// unbounded request-body read, with no error raised anywhere.
//
// SEAM — no production change was needed for any of this:
//   1. `createReceiver({ port: null })` constructs the ENTIRE receiver (env
//      parsing included) without binding a socket: server.mjs's listen is
//      `if (port != null) server.listen(port)`. So the zero-real-network
//      discipline of server.test.mjs / auth.test.mjs is preserved exactly —
//      NO socket is bound here either. The handler is reached by emitting
//      'request' on the constructed server with the same fake req/res doubles
//      the rest of the suite uses.
//   2. The effective config is ALREADY on the wire: createRetentionTally's
//      snapshot() returns `configured: { maxEvents, maxAgeMs }`, which
//      GET /summary serves as `retention.configured`.
//
// Every test SAVES AND RESTORES the env var it sets in a `finally`, so ordering
// can never leak between the tests in this file. The hazard being defended
// against is strictly INTRA-file: Node's test runner isolates one process per
// test FILE by default (this repo sets no --test-isolation override), so
// cross-file env leakage is not possible. Probed with the repo's exact command
// (`node --experimental-strip-types --test`): two tests in one file report the
// same pid and DO see each other's `process.env` writes, while a test in a
// second file reports a different pid and sees the var as `undefined`.
//
// DELIBERATELY NOT ASSERTED: a whitespace-only value (`" "`, `"\t"`) currently
// yields 0 and thereby DISABLES the cap, because `Number(' ') === 0` and the
// guard tests `raw === ''` rather than emptiness-after-trim. That diverges from
// the stated "a typo can never silently unbound the store" contract — it is a
// defect, tracked separately as a FIX. Asserting today's behaviour here would
// cement the defect as the contract, so these tests cover only the six
// documented cases. (`"0x10"` → 16 and `"1e2"` → 100 are likewise left
// unasserted: Number() accepts hex/exponent, which is benign and arguably
// correct, but is not part of the documented contract.)

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  createReceiver,
  DEFAULT_MAX_EVENTS,
  DEFAULT_MAX_AGE_HOURS,
  DEFAULT_MAX_BODY_BYTES,
  RETENTION_SWEEP_MS,
} from '../server.mjs';
import { SCHEMA_VERSION } from '../schema.ts';

const HOUR_MS = 60 * 60 * 1000;

// A fake IncomingMessage: an EventEmitter whose body chunks emit on nextTick
// (mimicking a real readable stream) — the same double server.test.mjs uses.
// `resume` is present because the cap-aware readBody drains (never destroys)
// an over-cap stream so the socket survives to carry the 413.
function fakeReq({ method = 'GET', url = '/summary', headers = {}, body = '' } = {}) {
  const req = new EventEmitter();
  req.method = method;
  req.url = url;
  req.headers = headers;
  req.resume = () => {};
  process.nextTick(() => {
    if (body) req.emit('data', body);
    req.emit('end');
  });
  return req;
}

// A fake ServerResponse: captures status, headers and the JSON payload passed
// to .end(), instead of writing to a real socket.
function fakeRes() {
  const res = { statusCode: null, headers: {}, body: null, ended: false };
  res.setHeader = (k, v) => {
    res.headers[k] = v;
  };
  res.end = (payload) => {
    res.body = payload;
    res.ended = true;
  };
  return res;
}

/**
 * Set env vars, run `fn`, and ALWAYS restore the previous values — including
 * restoring "was not set at all" by deleting rather than assigning ''
 * (assigning '' would be a different documented case entirely). A value of
 * `undefined` in `vars` means "ensure this var is UNSET for the body".
 */
async function withEnv(vars, fn) {
  const saved = new Map();
  for (const [name, value] of Object.entries(vars)) {
    saved.set(name, Object.prototype.hasOwnProperty.call(process.env, name) ? process.env[name] : undefined);
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  try {
    return await fn();
  } finally {
    for (const [name, prev] of saved) {
      if (prev === undefined) delete process.env[name];
      else process.env[name] = prev;
    }
  }
}

// A fresh temp store dir per test file run — the receiver only ever touches
// these paths, never the repo's real telemetry.ndjson.
const TMP_DIR = mkdtempSync(join(tmpdir(), 'warden-env-retention-'));
let storeSeq = 0;
function tmpStorePath() {
  storeSeq += 1;
  return join(TMP_DIR, `store-${storeSeq}.ndjson`);
}

/** Drive one request against a constructed (never-listening) receiver. */
async function request(server, reqOpts) {
  const req = fakeReq(reqOpts);
  const res = fakeRes();
  server.emit('request', req, res);
  const deadline = Date.now() + 5000;
  while (!res.ended) {
    if (Date.now() > deadline) throw new Error('response never ended');
    await new Promise((r) => setImmediate(r));
  }
  return res;
}

/**
 * Construct a receiver with `env` applied, read GET /summary, and return the
 * `retention.configured` the receiver actually computed. `port: null` means NO
 * socket is bound.
 */
async function configuredUnderEnv(env) {
  return withEnv(env, async () => {
    const server = createReceiver({ port: null, storePath: tmpStorePath() });
    try {
      const res = await request(server, { method: 'GET', url: '/summary' });
      assert.equal(res.statusCode, 200, 'GET /summary answers 200');
      return JSON.parse(res.body).retention.configured;
    } finally {
      server.close();
    }
  });
}

// ── STORE_MAX_EVENTS: the count cap ──────────────────────────────────────────
// This is the table that PINS BOTH GUARDS. With the `raw === ''` guard deleted,
// the `""` row reads 0 (cap disabled) instead of 10000. With the
// `!Number.isFinite(n) || n < 0` guard deleted, the `"abc"` row reads NaN and
// the `"-5"` row reads -5, instead of 10000 each.

test('STORE_MAX_EVENTS unset → the bounded DEFAULT (the receiver is never unbounded by omission)', async () => {
  const configured = await configuredUnderEnv({ STORE_MAX_EVENTS: undefined });
  assert.equal(configured.maxEvents, DEFAULT_MAX_EVENTS);
  assert.equal(DEFAULT_MAX_EVENTS, 10000, 'the documented default');
});

test('STORE_MAX_EVENTS="" (empty assignment) → the DEFAULT, NOT 0 — an empty env var must never disable the cap', async () => {
  // THE HEADLINE. `Number('') === 0` and `0` is this parser's documented opt-out,
  // so without the `raw === ''` early return an empty assignment — `STORE_MAX_EVENTS=`
  // in a compose/env file, or an unset variable interpolated into one — would read
  // as "disable retention" and silently unbound the store on an open-by-default
  // listener. This asserts the empty case lands on the BOUNDED default instead.
  const configured = await configuredUnderEnv({ STORE_MAX_EVENTS: '' });
  assert.equal(configured.maxEvents, DEFAULT_MAX_EVENTS, 'empty → default, never the 0 opt-out');
  assert.notEqual(configured.maxEvents, 0, 'an empty env var did NOT disable the count cap');
});

test('STORE_MAX_EVENTS="0" → 0, the DOCUMENTED OPT-OUT (an explicit disable is still honoured)', async () => {
  // The counterweight to the test above: the empty-string guard must not swallow
  // an EXPLICIT "0". A maintainer who deliberately disables the count cap gets
  // exactly that — otherwise the documented opt-out would be unreachable.
  const configured = await configuredUnderEnv({ STORE_MAX_EVENTS: '0' });
  assert.equal(configured.maxEvents, 0, 'explicit "0" disables the count cap, as documented');
});

test('STORE_MAX_EVENTS="abc" (malformed) → the DEFAULT — a typo can never silently unbound the store', async () => {
  // `Number('abc')` is NaN. Un-guarded, maxEvents becomes NaN, every `count > cap`
  // comparison is false, and the count cap silently vanishes with no error raised.
  const configured = await configuredUnderEnv({ STORE_MAX_EVENTS: 'abc' });
  assert.equal(configured.maxEvents, DEFAULT_MAX_EVENTS);
  assert.ok(Number.isFinite(configured.maxEvents), 'never NaN — a NaN cap bounds nothing');
});

test('STORE_MAX_EVENTS="-5" (negative) → the DEFAULT — a negative cap is a misconfiguration, not an opt-out', async () => {
  const configured = await configuredUnderEnv({ STORE_MAX_EVENTS: '-5' });
  assert.equal(configured.maxEvents, DEFAULT_MAX_EVENTS);
  assert.ok(configured.maxEvents > 0, 'stays a positive bound');
});

test('STORE_MAX_EVENTS="500" (valid override) → 500 — a legitimate override is honoured verbatim', async () => {
  // The positive control for the whole table: the parser is not merely always
  // returning the fallback. A real value passes through to the real cap.
  const configured = await configuredUnderEnv({ STORE_MAX_EVENTS: '500' });
  assert.equal(configured.maxEvents, 500);
});

// ── STORE_MAX_AGE_HOURS: the age window (hours → ms) ─────────────────────────
// NOTE, stated honestly: DEFAULT_MAX_AGE_HOURS is 0 (age expiry is OFF by
// default; the count cap carries the bound). So for this var the fallback and
// the opt-out COINCIDE at 0, and `maxAgeHours > 0 ? hours * HOUR_MS : 0`
// collapses NaN to 0 as well. These rows therefore document the contract and
// pin the hours→ms conversion; they do NOT independently detect the guard
// mutants — the STORE_MAX_EVENTS table above is what does that.

test('STORE_MAX_AGE_HOURS unset → DEFAULT_MAX_AGE_HOURS (0 = age expiry off by default)', async () => {
  const configured = await configuredUnderEnv({ STORE_MAX_AGE_HOURS: undefined });
  assert.equal(configured.maxAgeMs, DEFAULT_MAX_AGE_HOURS * HOUR_MS);
  assert.equal(configured.maxAgeMs, 0);
});

test('STORE_MAX_AGE_HOURS="" / "abc" / "-5" → the default (0) — malformed input never invents an age window', async () => {
  for (const raw of ['', 'abc', '-5']) {
    const configured = await configuredUnderEnv({ STORE_MAX_AGE_HOURS: raw });
    assert.equal(configured.maxAgeMs, DEFAULT_MAX_AGE_HOURS * HOUR_MS, `raw=${JSON.stringify(raw)}`);
    assert.ok(Number.isFinite(configured.maxAgeMs), `raw=${JSON.stringify(raw)} → never NaN`);
  }
});

test('STORE_MAX_AGE_HOURS="2" → 2h in MILLISECONDS (the hours→ms conversion is applied, not the raw hours)', async () => {
  // The load-bearing unit conversion: maxAgeMs = hours * HOUR_MS. Passing the raw
  // hour count through as ms would expire events 3.6-million-fold too aggressively.
  const configured = await configuredUnderEnv({ STORE_MAX_AGE_HOURS: '2' });
  assert.equal(configured.maxAgeMs, 2 * HOUR_MS);
  assert.equal(configured.maxAgeMs, 7_200_000);
});

test('STORE_MAX_AGE_HOURS="0" → maxAgeMs 0 AND no sweep interval is armed (the opt-out costs no timer)', async () => {
  // server.mjs arms the periodic age-expiry sweep ONLY when maxAgeMs > 0. The
  // interval is unref'd, so it is invisible to getActiveResourcesInfo — the
  // observable seam is the setInterval CALL itself, spied for the construction
  // window only and restored immediately (in a finally, so a throw cannot leak
  // a patched global into the rest of the suite).
  await withEnv({ STORE_MAX_AGE_HOURS: '0' }, async () => {
    const realSetInterval = globalThis.setInterval;
    const intervals = [];
    let server;
    try {
      globalThis.setInterval = (fn, ms, ...rest) => {
        intervals.push(ms);
        return realSetInterval(fn, ms, ...rest);
      };
      server = createReceiver({ port: null, storePath: tmpStorePath() });
    } finally {
      globalThis.setInterval = realSetInterval;
    }
    try {
      const res = await request(server, { method: 'GET', url: '/summary' });
      assert.equal(JSON.parse(res.body).retention.configured.maxAgeMs, 0);
      assert.deepEqual(intervals, [], 'no sweep timer armed when the age window is disabled');
    } finally {
      server.close();
    }
  });
});

test('STORE_MAX_AGE_HOURS="2" ARMS the sweep interval at RETENTION_SWEEP_MS (the positive control for the test above)', async () => {
  // Without this control, "no interval armed" would also pass if createReceiver
  // never armed one under ANY config — i.e. if the assertion were vacuous.
  await withEnv({ STORE_MAX_AGE_HOURS: '2' }, async () => {
    const realSetInterval = globalThis.setInterval;
    const intervals = [];
    let server;
    try {
      globalThis.setInterval = (fn, ms, ...rest) => {
        intervals.push(ms);
        return realSetInterval(fn, ms, ...rest);
      };
      server = createReceiver({ port: null, storePath: tmpStorePath() });
    } finally {
      globalThis.setInterval = realSetInterval;
    }
    try {
      assert.deepEqual(intervals, [RETENTION_SWEEP_MS], 'the age sweep is armed on the documented cadence');
    } finally {
      server.close();
    }
  });
});

// ── INGEST_MAX_BODY_BYTES: the request-body cap ──────────────────────────────
// Not carried in the /summary payload, so this one is asserted BEHAVIOURALLY:
// an over-cap POST must be rejected 413. This is a SECOND, independent pin of
// the same two guards at a different call site — and the consequence here is a
// live unbounded read on an open-by-default listener, not just disk growth.

/** POST an over-cap body at /ingest and return the response. */
async function oversizedPost(env) {
  return withEnv(env, async () => {
    const server = createReceiver({ port: null, storePath: tmpStorePath() });
    try {
      const big = 'x'.repeat(DEFAULT_MAX_BODY_BYTES + 1024);
      return await request(server, {
        method: 'POST',
        url: '/ingest',
        headers: {
          'content-type': 'application/json',
          // The schema handshake is checked BEFORE the size pre-check, so this
          // header must be correct or the request 415s and never reaches the cap.
          'x-telemetry-schema': String(SCHEMA_VERSION),
          'content-length': String(Buffer.byteLength(big)),
        },
        body: big,
      });
    } finally {
      server.close();
    }
  });
}

test('INGEST_MAX_BODY_BYTES unset → an oversized POST is 413 under the bounded DEFAULT cap', async () => {
  const res = await oversizedPost({ INGEST_MAX_BODY_BYTES: undefined });
  assert.equal(res.statusCode, 413, 'the default body cap is enforced');
  assert.match(JSON.parse(res.body).error, /request body too large/);
});

test('INGEST_MAX_BODY_BYTES="" → STILL 413 — an empty env var must not unbind the body cap', async () => {
  // The body-cap twin of the STORE_MAX_EVENTS="" headline. `Number('') === 0`,
  // and 0 is the unbounded escape hatch for this cap specifically (the size
  // pre-check is `if (maxBodyBytes > 0)`), so without the empty-string guard an
  // empty assignment turns off the bound on an open-by-default network listener
  // and every oversized body is read into memory in full.
  const res = await oversizedPost({ INGEST_MAX_BODY_BYTES: '' });
  assert.equal(res.statusCode, 413, 'empty → default cap, NOT the unbounded 0');
});

test('INGEST_MAX_BODY_BYTES="abc" → STILL 413 — a malformed cap falls back to the bounded default', async () => {
  // Un-guarded, maxBodyBytes becomes NaN, `NaN > 0` is false, the pre-check is
  // skipped entirely, and the cap silently vanishes.
  const res = await oversizedPost({ INGEST_MAX_BODY_BYTES: 'abc' });
  assert.equal(res.statusCode, 413, 'malformed → default cap, never NaN');
});

test('INGEST_MAX_BODY_BYTES="64" (valid override) → 413 for a body over the OVERRIDDEN cap, proving the override is live', async () => {
  // The positive control for the body-cap rows: they are not passing merely
  // because everything oversized is always 413'd regardless of the env value.
  // 64 bytes is far below the default, and a body between the two is rejected
  // only if the override actually took effect.
  await withEnv({ INGEST_MAX_BODY_BYTES: '64' }, async () => {
    const server = createReceiver({ port: null, storePath: tmpStorePath() });
    try {
      const body = 'x'.repeat(256); // over 64, WAY under the 1 MiB default
      const res = await request(server, {
        method: 'POST',
        url: '/ingest',
        headers: {
          'content-type': 'application/json',
          'x-telemetry-schema': String(SCHEMA_VERSION),
          'content-length': String(Buffer.byteLength(body)),
        },
        body,
      });
      assert.equal(res.statusCode, 413, 'the 64-byte override is the cap actually enforced');
    } finally {
      server.close();
    }
  });
});

test('the env vars are RESTORED after each test — no ordering leak between the tests in this file', async () => {
  // withEnv restores in a `finally`, including restoring "was never set" by
  // DELETING rather than assigning '' (assigning '' would be a different
  // documented case). The 16 tests in this file share ONE process, so a leaked
  // STORE_MAX_EVENTS would silently reconfigure the tests below it. (It could
  // not reach another test file: Node's runner isolates one process per file.)
  const names = ['STORE_MAX_EVENTS', 'STORE_MAX_AGE_HOURS', 'INGEST_MAX_BODY_BYTES'];
  const before = names.map((n) => Object.prototype.hasOwnProperty.call(process.env, n));
  await withEnv({ STORE_MAX_EVENTS: '123', STORE_MAX_AGE_HOURS: '9', INGEST_MAX_BODY_BYTES: '64' }, async () => {
    for (const n of names) assert.equal(process.env[n] !== undefined, true, `${n} is set inside the block`);
  });
  const after = names.map((n) => Object.prototype.hasOwnProperty.call(process.env, n));
  assert.deepEqual(after, before, 'presence/absence of every var is exactly as it was before');
});
