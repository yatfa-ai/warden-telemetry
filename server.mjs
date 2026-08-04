// Minimal HTTP server wiring for the ingest keystone (WARDEN-547). Plain
// `node:http` — no framework (roadmap invariant: minimal & self-hostable).
//
// This file is the ONLY place that (a) loads the vendored `./schema.ts` via
// Node's native type-stripping (requires Node ≥ 22.6; target 24) and (b) opens
// the real persistence file. Both are INJECTED into `ingest()`, so the pure
// pipeline — and every test — depends on neither.
//
// Run:  node server.mjs
// Env:  PORT       (default 7421) — the port a warden client's endpointUrl points at
//       STORE      (default ./telemetry.ndjson) — the durable NDJSON store path
//       AUTH_TOKEN (default unset) — optional shared secret. When set, every route
//                  requires `Authorization: Bearer <AUTH_TOKEN>`; unset = OPEN (dev
//                  only). Gate every route so an unauthenticated reader can't reach
//                  /ingest (or any future read surface) — WARDEN-569.
//       STORE_MAX_EVENTS  (default 10000) — retention COUNT cap. The persisted file
//                  is compacted to the newest N events once N are exceeded. The
//                  DEFAULT IS BOUNDED (unbounded growth was the bug — WARDEN-579).
//                  `0` disables the count cap.
//       STORE_MAX_AGE_HOURS (default 0 = off) — retention AGE window. Events whose
//                  epoch-ms `timestamp` is older than now minus this many hours are
//                  dropped on compaction. `0` disables the age window. When set, a
//                  periodic sweep expires old events even on a quiet store.
//       INGEST_MAX_BODY_BYTES (default 1048576 = 1 MiB) — the body cap on POST
//                  /ingest, the one remaining unbounded INPUT after retention
//                  bounded the store (WARDEN-579). A single oversized POST is no
//                  longer buffered fully into memory — it is 413'd at the
//                  Content-Length pre-check (or mid-read by the cap-aware readBody)
//                  so it can't exhaust receiver RSS. Legit traffic is tiny (the
//                  client sends ONE redacted event per dispatch, ~1-2 KB), so 1 MiB
//                  is ~500x the real payload. The DEFAULT IS BOUNDED; `0` disables
//                  the cap (the unbounded escape hatch, matching STORE_MAX_EVENTS=0).
//
// The receiver owns its routes: POST /ingest (write), GET /summary (read —
// the maintainer aggregate surface, WARDEN-567), GET /capabilities (the
// config-time verification surface, WARDEN-595 — a client's Settings "Test
// connection" probe reads it to confirm reachability + schema match + auth
// before relying on the receiver), and GET /events (read — the maintainer
// full-fidelity drill-down surface, WARDEN-599). The GET /summary aggregate
// also carries a bounded `rejections` tally (WARDEN-591) — counts by status of
// the rejections that already happen at every rejection site, so a maintainer
// can tell "traffic is arriving and being hard-rejected" from "no traffic at
// all" — a bounded `persistErrors` tally (WARDEN-607) — count + most-recent
// sample of the persist failures that already happen when the store refuses a
// write, so a maintainer can tell "traffic is arriving, validating, but
// un-storable" from "no traffic at all" (and the ingest handler returns a clean
// retryable 503 on a persist failure instead of hanging the socket) — AND a
// bounded `timeline` distribution (WARDEN-603) — event counts per time bucket
// over a rolling recent window, so a maintainer can distinguish a recent volume
// spike (a regression / deploy) from a long-running baseline — AND a bounded
// `retention` tally (WARDEN-743) — configured bounds + retained count + a
// running total + the most-recent sample of what retention pruned, so a
// maintainer can tell their overview spans only the retained window (a capped/
// aged store) instead of mistaking it for the full history: the third "silent
// signal-loss" path (pruned events), made legible the way rejections + persist
// errors already are. The client POSTs
// the batch verbatim to its configured endpointUrl (e.g.
// http://host:7421/ingest) and never rewrites the host, so these route paths
// are the receiver's to define.

import { createServer } from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { SCHEMA_VERSION, validateEvent } from './schema.ts';
import { createNdjsonStore, fileSink, fileSource, fileRewrite, fileSeenKeysSource, fileSeenKeysSink } from './store.mjs';
import { ingest } from './ingest.mjs';
import { summarize, summarizeTimeline, DEFAULT_TIMELINE_MAX_BUCKETS, DEFAULT_TIMELINE_WINDOW_MS } from './summary.mjs';
import { selectEvents, filterEvents, resolveLimit, resolveOffset } from './events.mjs';

export const DEFAULT_PORT = 7421;
export const DEFAULT_STORE_PATH = new URL('./telemetry.ndjson', import.meta.url).pathname;
export const INGEST_PATH = '/ingest';
export const SUMMARY_PATH = '/summary';
// The config-time verification surface (WARDEN-595). A warden client probes
// GET /capabilities from its Settings "Test connection" button to confirm the
// receiver is reachable + schema-matched + authed BEFORE relying on it. Pure
// read: returns the receiver's SCHEMA_VERSION + whether auth is required; reads
// no body, persists nothing.
export const CAPABILITIES_PATH = '/capabilities';
// The maintainer full-fidelity drill-down surface (WARDEN-599). Where GET /summary
// returns AGGREGATES (counts only — diagnostic fields discarded), GET /events
// returns the recent persisted EVENTS THEMSELVES, BOUNDED to a newest-N window
// with optional type + since filters — so a maintainer can inspect the actual
// error/crash/stall payloads /summary only counts, in-product. Pure read over the
// existing readEvents() seam; the bound keeps a near-full store from yielding a
// multi-MB response. Inherits the AUTH_TOKEN gate below like every other route.
export const EVENTS_PATH = '/events';

// ── RETENTION CONFIG (WARDEN-579) ────────────────────────────────────────────
// The persisted store is bounded by default (unbounded growth was the bug). The
// count cap is the hard bound on record count (→ file size); the age window is
// an optional freshness complement. Both default to bounded/opt-in; an explicit
// `0` on a knob opts that policy out (both `0` = the unbounded escape hatch).
export const DEFAULT_MAX_EVENTS = 10000; // count cap — hard bound on record count
export const DEFAULT_MAX_AGE_HOURS = 0; // age OFF by default (count cap carries the bound)
// A compaction is debounced to >=1 min and never runs synchronously per event
// (WARDEN-88 Anti-Pattern 1/2: a per-event file rewrite would freeze the receiver
// the way the lifecycle poll once froze warden). The age-expiry sweep ticks on a
// bounded interval and only when an age window is configured.
export const RETENTION_DEBOUNCE_MS = 60_000;
export const RETENTION_SWEEP_MS = 5 * 60_000; // 5-min cadence for age-expiry
const HOUR_MS = 60 * 60 * 1000;

// ── INGEST BODY CAP (WARDEN-627) ──────────────────────────────────────────────
// Retention (WARDEN-579) bounded the unbounded STORE; this bounds the one remaining
// unbounded INPUT — the POST /ingest request body, which readBody once buffered
// fully into memory with no limit. A single oversized POST could exhaust receiver
// RSS and take down a persistent service the self-hosting maintainer runs (and the
// receiver is open by default in dev, so an unauthenticated attacker OR a buggy
// client can trigger it). The default IS BOUNDED (1 MiB); `0` is the unbounded
// escape hatch, mirroring the retention knobs' `0`-disables convention. Legit
// traffic is tiny — the client sends ONE redacted event per dispatch (~1-2 KB) — so
// 1 MiB is ~500x the real payload, ample headroom for any reasonable batch.
export const DEFAULT_MAX_BODY_BYTES = 1024 * 1024; // 1 MiB POST body cap — bounds the request input

// ── IDEMPOTENT INGEST / SEEN-KEY DEDUP (WARDEN-666) ───────────────────────────
// The pipeline is at-least-once: the warden client retries a batch (bounded ≤3)
// when its 2xx was LOST — a network reset, or a read timeout while the receiver
// does synchronous appendFile I/O under disk pressure. Without idempotency the
// receiver stores the retried batch AGAIN, so a single crash retried ≤3× becomes
// 2–4 identical NDJSON rows — plausible-looking duplicates that silently inflate
// the maintainer's /summary (counts + timeline spike) and /events (repeated
// payloads) read surfaces, and undermine appVersion-attribution slicing. The
// client now sends a per-batch `idempotency-key` header (a random UUID, reused
// across retries of the same bytes); this set remembers the keys the receiver has
// ALREADY accepted a batch for, so a retry is recognized and answered 202
// {accepted:0, deduped:true} WITHOUT re-persisting. Bounded by BOTH a TTL (each
// key expires `ttlMs` after it was recorded, observed lazily on access — no
// timers, so no setTimer/clearTimer dep) and a COUNT cap (FIFO eviction when
// exceeded), so neither a long-lived receiver nor a flood of distinct keys grows
// it without limit. Receiver-local: the set is persisted beside telemetry.ndjson
// via an OPTIONAL injected durability seam (load/persist, WARDEN-803) so it
// SURVIVES a receiver restart — a retried batch whose 2xx was lost before the
// restart still dedups, closing the restart-mid-retry-window residual edge (the
// observability tallies remain restart-local by design, WARDEN-768). ADDITIVE
// ONLY: dedup AVOIDS a duplicate event write, never expands collection, relaxes
// no check (handshake / validate / auth / retention / body cap all still run),
// and routes nothing to a third party; the persisted set holds only opaque client
// key strings + expiry ms — never an event payload, tier identifier, or credential.
// The TTL comfortably spans any client retry window (≤3 jittered attempts ≈
// seconds) plus receiver-side slack; the count cap mirrors the retention default.
export const DEFAULT_DEDUP_TTL_MS = 10 * 60 * 1000; // 10 min — ~300x the client retry window
export const DEFAULT_DEDUP_MAX_KEYS = 10000; // FIFO cap — mirrors STORE_MAX_EVENTS default
// The persist write is DEBOUNCED so a burst of accepted batches coalesces into one
// flush — the hot accept path pays no synchronous disk write per batch (mirrors the
// retention compaction debounce). Small (1s): the persisted set is fresh to within
// a second, so a restart any time after a burst still reloads those keys. Injectable
// for unit tests via setTimer/clearTimer (like createRetentionTrigger).
export const SEEN_KEYS_PERSIST_DEBOUNCE_MS = 1000;

// Parse a non-negative number env override for retention. Unset/empty → fallback;
// an explicit "0" disables that policy (the opt-out); a malformed/negative value
// falls back to the (bounded) default so a typo can never silently unbound the
// store — the default stays bounded under any misconfiguration.
function envRetentionInt(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return fallback;
  return n;
}

// The default shared-schema deps — the vendored schema.ts, loaded once at module
// init via Node's native type-stripping. Overridable per-handler for tests.
export const DEFAULT_SCHEMA = { SCHEMA_VERSION, validateEvent };

/**
 * Read a request body fully into a string. Telemetry batches are small, bounded,
 * and fire-and-forget, so buffering the whole body is fine (no streaming parse).
 * Exported so the handler is unit-testable without binding a socket.
 *
 * `maxBytes` (default 0 = unbounded, WARDEN-627): when > 0, the accumulated BYTE
 * length is checked on every chunk; cross the cap and the read ABORTS — listeners
 * are removed and the stream is RESUMED (drained-and-discarded) so its socket stays
 * healthy for the handler's 413 response (destroying the request would tear down the
 * SHARED req/res socket and the 413 would never reach the client) rather than pinned
 * on backpressured data we will never read — and the promise rejects with a TAGGED
 * error (`code: 'PAYLOAD_TOO_LARGE'`). The tag is load-
 * bearing: the handler maps a PAYLOAD_TOO_LARGE rejection to a non-retryable 413
 * (recorded at the rejection seam), whereas a plain read error stays the existing
 * 400 — without the tag the cap case would silently fall into the 400 path. Bytes
 * are counted (not string chars) because Content-Length is in bytes; measuring
 * chars would under-count multibyte payloads and let an attacker sneak past the
 * cap. `0` preserves today's behavior for any other caller.
 *
 * @param {import('node:http').IncomingMessage} req
 * @param {{ maxBytes?: number }} [opts]
 * @returns {Promise<string>}
 */
export function readBody(req, { maxBytes = 0 } = {}) {
  return new Promise((resolve, reject) => {
    let data = '';
    let size = 0;
    let settled = false;

    const cleanup = () => {
      req.off('data', onData);
      req.off('end', onEnd);
      req.off('error', onError);
    };

    // Free the socket WITHOUT tearing it down: our listeners are already removed
    // above (so we stop buffering into `data`), then we DRAIN-and-discard the
    // remainder via resume(). The socket stays healthy so the handler's 413 response
    // actually reaches the client. We deliberately do NOT destroy the request: req
    // and res share a socket, so req.destroy() would abort the connection and the 413
    // would NEVER be delivered — the client would see a connection reset instead of the
    // clean non-retryable 4xx it already drops on (breaking the ticket's trust model).
    // resume() discards the bytes (no memory growth) while keeping the connection
    // alive; a non-stream test double without resume falls back to destroy, and one
    // with neither is a harmless no-op — guarded so the abort never throws.
    const stopStream = () => {
      if (typeof req.resume === 'function') req.resume();
      else if (typeof req.destroy === 'function') req.destroy();
    };

    const onData = (chunk) => {
      if (settled) return;
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
      size += buf.byteLength;
      if (maxBytes > 0 && size > maxBytes) {
        settled = true;
        cleanup();
        stopStream();
        reject(
          Object.assign(new Error('request body exceeds size limit'), {
            code: 'PAYLOAD_TOO_LARGE',
            limit: maxBytes,
          })
        );
        return;
      }
      data += buf.toString('utf8');
    };
    const onEnd = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(data);
    };
    const onError = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    };

    req.on('data', onData);
    req.on('end', onEnd);
    req.on('error', onError);
  });
}

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify(body));
}

// Case-insensitive header lookup (Node already lowercases req.headers, but be
// robust to a proxy/caller that hands back the original casing — mirrors
// ingest.mjs's readHeader).
function readHeader(headers, name) {
  const lower = name.toLowerCase();
  for (const key of Object.keys(headers || {})) {
    if (key.toLowerCase() === lower) return headers[key];
  }
  return undefined;
}

// Read the bearer token from an `Authorization: Bearer <token>` header. Returns
// the token string, or null when the header is missing, lacks the "Bearer "
// scheme, or carries an empty/whitespace token. Case-insensitive on the scheme.
function readBearerToken(headers) {
  const raw = readHeader(headers, 'authorization');
  if (typeof raw !== 'string') return null;
  const m = raw.match(/^Bearer\s+(\S+)\s*$/i);
  return m ? m[1] : null;
}

// Constant-time string equality (timing-attack hardening for the shared secret).
// crypto.timingSafeEqual throws on unequal-length buffers, so when the lengths
// differ we run a constant-time compare of the provided buffer against ITSELF
// first — that keeps this branch's cost proportional to the attacker-controlled
// provided length (never the secret's length) and avoids the throw — then return
// false. The equal-length branch is a single timingSafeEqual. Net effect: a
// wrong/absent/malformed token never short-circuits, and the response timing
// reveals nothing about the secret's value OR its length.
function tokensMatch(provided, expected) {
  const a = Buffer.from(provided);
  const b = Buffer.from(expected);
  if (a.length !== b.length) {
    timingSafeEqual(a, a); // equal-length dummy compare → no throw, no length leak
    return false;
  }
  return timingSafeEqual(a, b);
}

/**
 * Build the maintenance trigger that keeps the persisted store bounded
 * (WARDEN-579). It invokes `store.prune(...)` OFF the request path, on a
 * DEBOUNCED (≥1 min), re-entrancy-guarded cadence — never a synchronous rewrite
 * per event (WARDEN-88 Anti-Pattern 1/2: a per-event compaction would freeze the
 * receiver the way the lifecycle poll once froze warden).
 *
 * Two arming paths, both coalesced by a single debounce timer:
 *   - `afterAppend(count)` — REACTIVE: once `maxEvents`-worth have been appended
 *     since the last prune, arm a debounced prune (handles volume-driven growth).
 *   - `sweep()`           — PERIODIC: arm a debounced prune on a timer (handles
 *     age-window expiry on a QUIET store, where no append would trigger it).
 *
 * `setTimer` / `clearTimer` / `now` are injected so the trigger is unit-testable
 * with a fake clock and a deterministic scheduler — no real timer in tests.
 *
 * `retention` (optional tally, WARDEN-743): an OPTIONAL retention-HEALTH tally
 * (built by `createRetentionTally`). On a SUCCESSFUL prune the trigger records
 * prune()'s already-computed {before, after, pruned, rewrote} (plus
 * `retainedCount: after`) via `retention.record(...)` so GET /summary can surface
 * what retention removed — the third "silent signal-loss" path, made legible.
 * The record call lives in `.then` (success), NOT `.catch`/`.finally`: a FAILED
 * prune removed nothing and must not record a spurious sample. No `retention`
 * tally wired = today's behavior exactly (the prune result is discarded, as
 * before) — the optional-dep discipline shared with the sibling tallies. Named
 * `retention` here (the trigger's opts have no such field today, so there is no
 * collision); the handler dep + /summary response use a distinct name.
 *
 * @param {{ prune(opts: object): Promise<void> }} store
 * @param {{ maxEvents?: number, maxAgeMs?: number, debounceMs?: number, now?: () => number, setTimer?: (fn: () => void, ms: number) => unknown, clearTimer?: (id: unknown) => void, retention?: { record(rec: { before?: number, after?: number, pruned?: number, rewrote?: boolean, retainedCount?: number }): void } }} [opts]
 * @returns {{ afterAppend(count?: number): void, sweep(): void, cancel(): void }}
 */
export function createRetentionTrigger(
  store,
  {
    maxEvents = 0,
    maxAgeMs = 0,
    debounceMs = RETENTION_DEBOUNCE_MS,
    now = Date.now,
    setTimer = (fn, ms) => setTimeout(fn, ms),
    clearTimer = (id) => clearTimeout(id),
    retention = null,
  } = {}
) {
  let timerId = null;
  let running = false;
  let appendedSincePrune = 0;

  function flush() {
    timerId = null;
    // Re-entrancy guard: if a prune is still mid-flight, let it finish. The next
    // append (or sweep tick) after it completes re-arms; under sustained ingest
    // there is always a next append, so nothing is dropped for long.
    if (running) return;
    running = true;
    appendedSincePrune = 0;
    Promise.resolve(store.prune({ maxEvents, maxAgeMs, now: now() }))
      .then((res) => {
        // WARDEN-743: a prune that COMPLETED — record its already-computed
        // result {before, after, pruned, rewrote} so GET /summary can surface
        // what retention removed (the third "silent signal-loss" path, now
        // legible). This MUST live in `.then` (success), NOT `.catch`/
        // `.finally`: a FAILED prune (the `.catch` below) removed nothing and
        // must not record a spurious sample. `retainedCount: after` carries the
        // post-prune store size. No tally wired (retention == null) = today's
        // behavior exactly (the result is discarded, as before).
        if (retention && res && typeof res === 'object') {
          retention.record({
            before: res.before,
            after: res.after,
            pruned: res.pruned,
            rewrote: res.rewrote,
            retainedCount: res.after,
          });
        }
      })
      .catch(() => {
        // A prune failure must NEVER kill the receiver (telemetry is best-effort).
        // The atomic rename in `fileRewrite` means a failed compaction leaves the
        // prior file intact; the next prune retries. Swallow, don't crash.
      })
      .finally(() => {
        running = false;
        // If appends landed during the prune and re-crossed the count bound, arm
        // another debounced prune so a burst-then-quiet doesn't leave the store
        // over the bound waiting for the next append.
        if (maxEvents > 0 && appendedSincePrune >= maxEvents) arm();
      });
  }

  function arm() {
    // Debounce: a burst of appends coalesces into ONE prune. Re-entrancy: don't
    // arm while a prune is running (the next append after it completes re-arms).
    if (timerId != null || running) return;
    timerId = setTimer(flush, debounceMs);
  }

  return {
    /** Record an append count; arm a debounced prune once the count bound is crossed. */
    afterAppend(countAppended = 0) {
      appendedSincePrune += countAppended;
      if (maxEvents > 0 && Number.isFinite(maxEvents) && appendedSincePrune >= maxEvents) {
        arm();
      }
    },
    /** Arm a debounced prune unconditionally (the periodic age-expiry sweep). */
    sweep() {
      arm();
    },
    /** Cancel any pending debounced prune (e.g. on server shutdown). */
    cancel() {
      if (timerId != null) {
        clearTimer(timerId);
        timerId = null;
      }
    },
  };
}

// ── BOUNDED ROLLING TIMELINE (WARDEN-834) ────────────────────────────────────
// The shared STATEFUL rolling-window timeline machinery extracted from the three
// tallies (createRejectionTally / createPersistErrorTally / createDedupTally),
// which had each inlined it byte-for-byte identically (modulo their scalar tally
// fields). This is the stateful twin of the PURE summarizeTimeline (summary.mjs):
// the tallies record INCREMENTALLY — one record() at a time, NO retained event
// list (unlike summarizeTimeline, a pure function over a retained event array with
// a single snapshot now()) — so they key counts by ABSOLUTE epoch-aligned bucket
// slot and re-relativize them against the CURRENT rolling window at snapshot(),
// dropping slots that have rolled off. Bounded at maxBuckets by construction:
// every surviving slot maps to a relative idx clamped to [0, maxBuckets-1].
//
// `bump(ts, count = 1)` takes the externally-read timestamp (it does NOT call
// now() itself), so a tally's single now() read serves both its scalar
// `lastSeen` and the slot — preserving the WARDEN-798 "one clock read per
// record, never two" invariant. The optional `count` weight defaults to 1 (one
// hit per call — the rejections / persistErrors / deduped tallies), and the
// retention tally (WARDEN-838) passes its `dropped` event count so a 5000-event
// compaction registers as 5000 in its bucket ("how much signal was lost WHEN"),
// not as 1. The weight is the ONLY retention-specific seam — every other tally
// calls `bump(ts)` and is unchanged.
/**
 * Build the shared bounded rolling-window timeline (WARDEN-834). Composed by the
 * three tallies so the bucket math exists in exactly ONE place.
 *
 * @param {{now?: () => number, maxBuckets?: number, windowMs?: number}} [opts]
 * @returns {{bump: (ts: number, count?: number) => void, snapshot: () => {buckets: Array<{bucketStart: number, bucketEnd: number, count: number}>, bucketMs: number}}}
 */
export function createBoundedRollingTimeline({
  now = Date.now,
  maxBuckets = DEFAULT_TIMELINE_MAX_BUCKETS,
  windowMs = DEFAULT_TIMELINE_WINDOW_MS,
} = {}) {
  // Degenerate-config guard mirrors summarizeTimeline (summary.mjs:347-349): a bad
  // windowMs/maxBuckets override collapses the timeline to { buckets: [], bucketMs: 0 }
  // — never a huge/NaN array. The composing tally's scalar bookkeeping is unaffected
  // and keeps working regardless.
  const validConfig = Number.isFinite(windowMs) && windowMs > 0 && Number.isFinite(maxBuckets) && maxBuckets >= 1;
  const bucketMs = validConfig ? windowMs / maxBuckets : 0;

  // Counts keyed by ABSOLUTE epoch-aligned bucket slot (floor(when / bucketMs)).
  const counts = new Map();

  return {
    /** Record one hit at absolute epoch-ms `ts`. Computes the absolute slot
     *  (floor(ts / bucketMs)) and increments its count. `count` (default 1) is the
     *  weight to add — the rejections / persistErrors / deduped tallies record one
     *  hit per call; the retention tally (WARDEN-838) passes its `dropped` event
     *  count so the bucket reflects how many events a compaction evicted, not how
     *  many prune OPERATIONS ran. A non-positive/non-finite weight falls back to 1
     *  (the historical behavior), so a misuse can never silently add 0 or NaN.
     *  No-op under a degenerate config (the snapshot collapses to
     *  { buckets: [], bucketMs: 0 } anyway). */
    bump(ts, count = 1) {
      if (!validConfig) return;
      const n = Number.isFinite(count) && count > 0 ? count : 1;
      const slot = Math.floor(ts / bucketMs);
      counts.set(slot, (counts.get(slot) ?? 0) + n);
    },
    /** A stable point-in-time copy of the timeline (a later bump does not mutate a
     *  previously-returned snapshot). Re-relativizes the absolute per-bucket counts
     *  against the current rolling window — buckets older than the window have rolled
     *  off and are dropped (and garbage-collected from the map so a long-lived
     *  receiver cannot leak slots without bound). Returns { buckets, bucketMs }, or
     *  { buckets: [], bucketMs: 0 } under a degenerate config. */
    snapshot() {
      if (!validConfig) {
        return { buckets: [], bucketMs: 0 };
      }
      const currentTime = now();
      const windowStart = currentTime - windowMs;
      // Re-relativize each absolute slot against the current window (mirrors
      // summarizeTimeline's idx math, summary.mjs:372-376) and merge into relative
      // buckets. Rolled-off slots (idx < 0) are deleted for good — a record can
      // never re-enter a slot whose time has passed, so dropping them on read bounds
      // the map at ≤ maxBuckets without losing signal.
      const relative = new Map();
      for (const [slot, count] of counts) {
        const bucketStart = slot * bucketMs; // a representative time inside the slot
        let idx = Math.floor((bucketStart - windowStart) / bucketMs);
        if (idx < 0) {
          counts.delete(slot); // rolled off the window — drop for good (memory bound)
          continue;
        }
        if (idx >= maxBuckets) idx = maxBuckets - 1; // fold the top boundary into the newest bucket
        relative.set(idx, (relative.get(idx) ?? 0) + count);
      }
      const buckets = [...relative.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([idx, count]) => {
          const bucketStart = windowStart + idx * bucketMs;
          return { bucketStart, bucketEnd: bucketStart + bucketMs, count };
        });
      return { buckets, bucketMs };
    },
  };
}

// ── REJECTIONS TALLY (WARDEN-591) ────────────────────────────────────────────
// A bounded, in-memory, receiver-local tally of the rejections that ALREADY
// happen at every rejection site in this handler (the auth-gate 401, the 404
// routing miss, the body-read 400, and the 400/415/422 returned from ingest).
// It exists so GET /summary can surface "traffic is arriving and being HARD-
// rejected" apart from "no traffic at all" — the two were indistinguishable
// before (an all-rejected receiver returned the SAME empty /summary as an idle
// one). This is the receiver-side twin of the client's "enabled but no endpoint
// configured" status, and the chief-risk symptom (schema drift → a flood of
// 415s) made visible. ADDITIVE ONLY: it records rejections that already happen,
// relaxes no check, mirrors no invariant, routes nothing, and persists nothing
// (a misconfiguration detector need not survive a restart).
//
// Bounded means: counts by status + a SINGLE most-recent sample (status/reason/
// ts), plus a `byDeclaredVersion` histogram of the DECLARED schema version on
// 415-rejected batches (the drift population, WARDEN-761). It does NOT keep one
// record per rejection or an unbounded set of reason strings, so a sustained
// drift storm can't grow it without limit. `byDeclaredVersion` is bounded by a
// top-N distinct-key cap + ONE overflow bucket (WARDEN-829): a client-declared
// schema version is the raw, attacker-controlled `x-telemetry-schema` header on
// an OPEN-by-default receiver (AUTH_TOKEN unset = OPEN, see createRequestHandler),
// NOT a fixed enum like the HTTP statuses in `byStatus` — so the distinct keys are
// CAPPED (≤ maxDeclaredVersions+1) rather than trusted to stay small. This closes
// the unbounded-memory + response-amplification DoS the prior "enum-bounded"
// comment falsely ruled out (the receiver is a network listener; one cheap probe
// per distinct header value grew both the live map and every GET /summary payload).

// The distinct-key cap on `byDeclaredVersion` (WARDEN-829). The declared schema
// version is the raw `x-telemetry-schema` header — free-text attacker input on an
// OPEN-by-default receiver, NOT a fixed enum — so unlike `byStatus` (HTTP statuses,
// a tiny fixed set) the distinct keys CANNOT be trusted to stay small. We track at
// most `DEFAULT_REJECTION_MAX_DECLARED_VERSIONS` distinct values as their own
// buckets; every further distinct value folds into ONE counted `__overflow__`
// bucket, so snapshot().byDeclaredVersion holds ≤ N+1 keys regardless of input
// cardinality. 32 sits comfortably ABOVE the realistic distinct-version set (a
// handful like 1/2/3/4 during a coordinated bump — the legit drift signal is
// preserved) and far below unbounded. Mirrors the COUNT-cap discipline of
// createSeenKeys's `maxKeys`, adapted to a histogram: top-N + overflow (NOT FIFO
// eviction) so an early legit version is never evicted by a later adversarial flood.
export const DEFAULT_REJECTION_MAX_DECLARED_VERSIONS = 32;

// The zeroed shape returned when no tally is wired OR no rejection has been
// recorded yet — identical to a fresh tally's snapshot(), so an idle receiver
// reads the same zeroed `rejections` whether or not the tally is wired (parity
// with today's empty-store /summary: no false alarm on a quiet receiver). The
// zeroed `timeline` (WARDEN-798) carries the SAME stable shape a wired tally's
// empty snapshot does — `buckets: []` + the default `bucketMs` (24h / 48) — so
// the field is shape-stable whether or not the tally is wired. `bucketMs`
// conveys granularity (it is the default, NOT 0: only a degenerate config
// override collapses it to 0, mirroring summarizeTimeline's empty-but-valid vs
// degenerate distinction).
const EMPTY_REJECTIONS = Object.freeze({
  total: 0,
  byStatus: {},
  byDeclaredVersion: {}, // 415 drift axis (WARDEN-761) — top-N + overflow bounded (WARDEN-829), zeroed when idle
  lastStatus: null,
  lastReason: null,
  lastSeen: null,
  timeline: Object.freeze({
    buckets: [],
    bucketMs: DEFAULT_TIMELINE_WINDOW_MS / DEFAULT_TIMELINE_MAX_BUCKETS,
  }),
});

/**
 * Build the rejection tally (WARDEN-591). Mirrors the injected-seam discipline of
 * `createRetentionTrigger`: an OPTIONAL handler dep (no tally wired = today's
 * behavior, exactly like an absent retention dep) with an injected `now` so the
 * tally is unit-testable with a fake clock (no real Date in tests).
 *
 * WARDEN-798 extends the snapshot with a bounded `timeline` — a per-bucket COUNT
 * of rejections over the SAME rolling window/granularity as the read-path
 * `timeline` (summarizeTimeline, WARDEN-603) and the persistErrors timeline
 * (WARDEN-777), so a maintainer reading `rejections.timeline` can tell an ONGOING
 * schema-drift storm (415s still landing in the newest bucket) from a RESOLVED one
 * (415s clustered in older buckets, newest bucket empty) — the spike-vs-baseline
 * question `total + lastSeen` provably cannot answer for the roadmap's flagship
 * risk (schema drift → a 415 flood).
 *
 * `maxBuckets` / `windowMs` default to the read-path `DEFAULT_TIMELINE_*` constants
 * (imported from summary.mjs) so the two timelines never drift in granularity.
 *
 * `maxDeclaredVersions` (WARDEN-829) caps the distinct-key cardinality of
 * `byDeclaredVersion` (top-N + overflow). Optional + defaulted like the timeline
 * knobs so a test can pass a small N to exercise the cap deterministically.
 *
 * @param {{ now?: () => number, maxBuckets?: number, windowMs?: number, maxDeclaredVersions?: number }} [opts]
 * @returns {{
 *   record(rec: { status: number, reason?: string, declaredVersion?: string }): void,
 *   snapshot(): {
 *     total: number,
 *     byStatus: Record<string, number>,
 *     byDeclaredVersion: Record<string, number>,
 *     lastStatus: number | null,
 *     lastReason: string | null,
 *     lastSeen: number | null,
 *     timeline: { buckets: { bucketStart: number, bucketEnd: number, count: number }[], bucketMs: number },
 *   }
 * }}
 */
export function createRejectionTally({
  now = Date.now,
  maxBuckets = DEFAULT_TIMELINE_MAX_BUCKETS,
  windowMs = DEFAULT_TIMELINE_WINDOW_MS,
  maxDeclaredVersions = DEFAULT_REJECTION_MAX_DECLARED_VERSIONS,
} = {}) {
  let total = 0;
  const byStatus = {};
  const byDeclaredVersion = {};
  // Distinct declared-version buckets currently tracked as their OWN keys in
  // `byDeclaredVersion` (excludes the `__overflow__` sentinel). Maintained in
  // lockstep with record() so the cap check is O(1) — never recomputed by scan.
  let distinctDeclaredVersions = 0;
  let lastStatus = null;
  let lastReason = null;
  let lastSeen = null;

  // Effective distinct-key cap (WARDEN-829): a finite positive int, else the
  // default — never unbounded under any misconfiguration (mirrors the
  // validConfig discipline in createBoundedRollingTimeline: a bad override
  // can't reopen the DoS).
  const maxDeclared = Number.isFinite(maxDeclaredVersions) && maxDeclaredVersions >= 1
    ? Math.floor(maxDeclaredVersions)
    : DEFAULT_REJECTION_MAX_DECLARED_VERSIONS;
  // Sentinel for the single overflow bucket. A client value that literally equals
  // `__overflow__` is indistinguishable from the aggregate (benign: same bound, the
  // count semantics are identical). Chosen to be an unlikely real schema version.
  const OVERFLOW_KEY = '__overflow__';

  // The bounded rolling-window timeline (WARDEN-834): the stateful twin of the
  // pure summarizeTimeline, now shared by all three tallies via a single helper.
  // See createBoundedRollingTimeline for the degenerate-config guard, the
  // absolute-slot + re-relativize math, the rolloff GC, and the top-boundary fold.
  const timeline = createBoundedRollingTimeline({ now, maxBuckets, windowMs });

  return {
    /** Record one rejection. Bounded: accumulates a per-status COUNT, tracks only
     *  the single most-recent {status, reason, ts}, and bumps the count in the
     *  absolute time-bucket the rejection landed in (the timeline's per-bucket
     *  distribution). When `declaredVersion` is present (only 415s carry one),
     *  buckets `String(declaredVersion)` into `byDeclaredVersion` — the drift
     *  population. */
    record({ status, reason, declaredVersion } = {}) {
      if (status == null) return;
      const key = String(status);
      total += 1;
      byStatus[key] = (byStatus[key] ?? 0) + 1;
      // byDeclaredVersion (WARDEN-761 / WARDEN-829): bucket the DECLARED schema
      //  version of a 415-rejected batch — the drift population (a wrong/old client
      //  version still sending during a coordinated bump). Top-N + overflow (NOT
      //  enum-bounded): a client-declared version is the raw, free-text
      //  `x-telemetry-schema` header on an OPEN-by-default receiver, so unlike
      //  `byStatus` (HTTP statuses, a fixed set) the distinct keys are CAPPED, not
      //  trusted to stay small. The first `maxDeclared` distinct values get their
      //  own bucket; every further distinct value folds into ONE counted
      //  `__overflow__` bucket — so the histogram holds ≤ maxDeclared+1 keys no
      //  matter how many distinct adversarial values arrive (WARDEN-829 closed the
      //  unbounded-memory + response-amplification DoS). The overflow COUNT is
      //  preserved (not dropped) so a maintainer still sees "drift across >N
      //  versions" without enumerating every adversarial value. Cardinality-cap
      //  precedent: createSeenKeys's `maxKeys` COUNT cap (top-N + overflow here, not
      //  FIFO eviction, so an early legit version is never evicted by a later flood).
      //  Bucket any PRESENT value (incl. scanner non-numerics like "abc" / "");
      //  absent/missing (undefined/null) → no bucket. Only the 415 seams pass a
      //  declaredVersion, so this naturally reflects drift alone. hasOwnProperty
      //  (not `in`/bracket-truthiness) keeps attacker keys like "toString" /
      //  "constructor" bucketing as ordinary own keys.
      if (declaredVersion !== undefined && declaredVersion !== null) {
        const dvKey = String(declaredVersion);
        if (Object.prototype.hasOwnProperty.call(byDeclaredVersion, dvKey)) {
          byDeclaredVersion[dvKey] += 1; // an already-tracked distinct version bumps its own bucket
        } else if (distinctDeclaredVersions < maxDeclared) {
          byDeclaredVersion[dvKey] = 1; // new distinct version under the cap → its own bucket
          distinctDeclaredVersions += 1;
        } else {
          // Cap reached → fold this and every further NEW distinct version into the
          // single overflow bucket: bounded cardinality, no count loss.
          byDeclaredVersion[OVERFLOW_KEY] = (byDeclaredVersion[OVERFLOW_KEY] ?? 0) + 1;
        }
      }
      lastStatus = status;
      lastReason = typeof reason === 'string' && reason.length > 0 ? reason : null;
      // A SINGLE now() read (WARDEN-798) serves both lastSeen and the timeline
      // slot — one clock read per rejection, never two. The helper's bump() takes
      // that externally-read timestamp rather than reading now() itself.
      const ts = now();
      lastSeen = ts;
      timeline.bump(ts);
    },
    /** A stable point-in-time copy of the aggregate (a later record does not mutate
     *  a previously-returned snapshot). The `timeline` is delegated to
     *  createBoundedRollingTimeline, which re-relativizes the absolute per-bucket
     *  counts against the current rolling window and drops rolled-off buckets. */
    snapshot() {
      return { total, byStatus: { ...byStatus }, byDeclaredVersion: { ...byDeclaredVersion }, lastStatus, lastReason, lastSeen, timeline: timeline.snapshot() };
    },
  };
}

// ── PERSIST-ERROR TALLY (WARDEN-607) ─────────────────────────────────────────
// A bounded, in-memory, receiver-local tally of the persist failures that ALREADY
// happen when `store.appendEvents()` throws (disk full / EACCES / EISDIR / a
// missing or rewritten store file / a sink rejection). Before WARDEN-607 such a
// failure was invisible twice over — the exact "silent signal-loss" WARDEN-591
// closed on the READ/reject path, still open on the WRITE/persist path:
//   - Client side (hung socket): the ingest request handler awaited ingest() with
//     NO try/catch, so a persist throw rejected the handler promise and Node never
//     called res.end() — the client's fetch HUNG until socket timeout, then
//     surfaced as a network error (recovered only slowly + noisily via the client's
//     accidental transient-retry path).
//   - Maintainer side (invisible): the `rejections` tally is documented + tested to
//     cover ONLY HTTP rejection sites (401/404/400/415/422) — a persist throw is
//     not a `!result.ok` branch, so a receiver that received + validated events but
//     could not write them returned the SAME empty /summary as an idle receiver.
// This tally closes both gaps on the WRITE/persist path. It is a SEPARATE signal
// from `rejections` by design: a persist failure is a distinct "validated but
// un-storable" class (the events passed the schema check; the store refused them),
// not an HTTP rejection, so it must not overload the rejection tally's documented
// HTTP-rejection-sites-only contract.
//
// Bounded means: a total count + a SINGLE most-recent sample {reason, ts}. It does
// NOT keep one record per failure, so a sustained store outage can't grow it. Like
// the rejections tally, it "persists nothing" — it is in-memory and receiver-local
// (a misconfiguration signal need not survive a restart), and ADDITIVE ONLY: it
// records failures that already happen, relaxes no check, mirrors no invariant,
// routes nothing.

// The zeroed shape returned when no tally is wired OR no failure has been recorded
// yet — identical to a fresh tally's snapshot(), so a healthy receiver reads the
// same zeroed `persistErrors` whether or not the tally is wired (parity with
// EMPTY_REJECTIONS: no false alarm on a quiet receiver). The zeroed `timeline`
// (WARDEN-777) carries the SAME stable shape a wired tally's empty snapshot does —
// `buckets: []` + the default `bucketMs` (24h / 48) — so the field is shape-stable
// whether or not the tally is wired. `bucketMs` conveys granularity (it is the
// default, NOT 0: only a degenerate config override collapses it to 0, mirroring
// summarizeTimeline's empty-but-valid vs degenerate distinction).
const EMPTY_PERSIST_ERRORS = Object.freeze({
  total: 0,
  lastReason: null,
  lastSeen: null,
  timeline: Object.freeze({
    buckets: [],
    bucketMs: DEFAULT_TIMELINE_WINDOW_MS / DEFAULT_TIMELINE_MAX_BUCKETS,
  }),
});

/**
 * Build the persist-error tally (WARDEN-607). Mirrors the injected-seam discipline
 * of `createRejectionTally`: an OPTIONAL handler dep (no tally wired = today's
 * behavior, exactly like an absent rejections dep) with an injected `now` so the
 * tally is unit-testable with a fake clock (no real Date in tests).
 *
 * WARDEN-777 extends the snapshot with a bounded `timeline` — a per-bucket COUNT of
 * persist failures over the SAME rolling window/granularity as the read-path
 * `timeline` (summarizeTimeline, WARDEN-603), so a maintainer reading
 * `persistErrors.timeline` can tell an ONGOING store outage (failures still landing
 * in the newest bucket) from a RESOLVED one (failures clustered in older buckets,
 * newest bucket empty) — the spike-vs-baseline question `total + lastSeen`
 * provably cannot answer for an episodic 503 that often recovers.
 *
 * `maxBuckets` / `windowMs` default to the read-path `DEFAULT_TIMELINE_*` constants
 * (imported from summary.mjs) so the two timelines never drift in granularity.
 *
 * @param {{ now?: () => number, maxBuckets?: number, windowMs?: number }} [opts]
 * @returns {{
 *   record(rec: { reason?: string }): void,
 *   snapshot(): {
 *     total: number,
 *     lastReason: string | null,
 *     lastSeen: number | null,
 *     timeline: { buckets: { bucketStart: number, bucketEnd: number, count: number }[], bucketMs: number },
 *   }
 * }}
 */
export function createPersistErrorTally({
  now = Date.now,
  maxBuckets = DEFAULT_TIMELINE_MAX_BUCKETS,
  windowMs = DEFAULT_TIMELINE_WINDOW_MS,
} = {}) {
  let total = 0;
  let lastReason = null;
  let lastSeen = null;

  // The bounded rolling-window timeline (WARDEN-834): the stateful twin of the
  // pure summarizeTimeline, shared by all three tallies via a single helper. See
  // createBoundedRollingTimeline for the degenerate-config guard, the absolute-slot
  // + re-relativize math, the rolloff GC, and the top-boundary fold.
  const timeline = createBoundedRollingTimeline({ now, maxBuckets, windowMs });

  return {
    /** Record one persist failure. Bounded: accumulates a total COUNT, tracks only
     *  the single most-recent {reason, ts}, and bumps the count in the absolute
     *  time-bucket the failure landed in (the timeline's per-bucket distribution). */
    record({ reason } = {}) {
      total += 1;
      lastReason = typeof reason === 'string' && reason.length > 0 ? reason : null;
      // A SINGLE now() read serves both lastSeen and the timeline slot — one clock
      // read per failure, never two. The helper's bump() takes that externally-read
      // timestamp rather than reading now() itself.
      lastSeen = now();
      timeline.bump(lastSeen);
    },
    /** A stable point-in-time copy of the aggregate (a later record does not mutate
     *  a previously-returned snapshot). The `timeline` is delegated to
     *  createBoundedRollingTimeline, which re-relativizes the absolute per-bucket
     *  counts against the current rolling window and drops rolled-off buckets. */
    snapshot() {
      return { total, lastReason, lastSeen, timeline: timeline.snapshot() };
    },
  };
}

// ── RETENTION-HEALTH TALLY (WARDEN-743) ──────────────────────────────────────
// The third and last "silent signal-loss" path on the receiver. An event a
// client sends can leave the pipeline in exactly three ways after ingest accepts
// it; the receiver already makes TWO visible on GET /summary:
//   1. Rejected at ingest (415/400/422) → `rejections` tally (WARDEN-591).
//   2. Failed to persist (store.appendEvents throws) → `persistErrors` tally
//      (WARDEN-607).
//   3. Pruned by retention (count cap / age window) → was INVISIBLE. ← this tally.
// `store.prune()` already computes {before, after, pruned, rewrote}, but
// createRetentionTrigger DISCARDED it (a .catch().finally() chain with no
// .then), so a busy self-hosted receiver silently evicted old signal on every
// persist once full — /summary.total flatlined at the cap, firstSeen/lastSeen
// crept forward — and the maintainer could not tell their overview spanned only
// the retained window. This tally observes prune()'s ALREADY-computed result (it
// changes no prune behavior) and surfaces it on GET /summary so a truncated
// signal is LEGIBLE instead of silent. It is a SEPARATE axis from the queued
// drill-down proposals: those are about drilling INTO the retained set; this is
// about knowing what retention has REMOVED (receiver operational health).
//
// Bounded means: the configured bounds + a retained count + a running total of
// pruned events + a SINGLE most-recent prune sample {before, after, pruned,
// rewrote, ts} + a bounded rolling-window `timeline` of PRUNED-event counts per
// bucket (WARDEN-838, the last event-flow tally to carry one). It does NOT keep
// one record per prune, so a sustained compaction storm can't grow it. Like the
// sibling tallies, it "persists nothing" — it is in-memory and receiver-local (a
// misconfiguration signal need not survive a restart), and ADDITIVE ONLY: it
// records what retention already does, relaxes no check, mirrors no invariant,
// routes nothing, touches no redaction. The recorded `last` and the timeline
// buckets carry only counts + timestamps — never raw event bytes or extended-tier
// identifiers (the trust model is preserved, same as the sibling tallies).

// The zeroed shape returned when no tally is wired OR no prune has run yet.
// `configured` is {maxEvents:0, maxAgeMs:0} here (the "unset" shape an absent dep
// yields); a WIRED tally carries the active bounds instead. A fresh wired tally
// whose prune has not fired yet also reads zeroed EXCEPT `configured` reflects
// the real bounds — so an idle receiver never false-alarms (parity with
// EMPTY_REJECTIONS / EMPTY_PERSIST_ERRORS / EMPTY_DEDUPED), but a maintainer still
// sees the cap. The zeroed `timeline` (WARDEN-838) carries the SAME stable shape a
// wired tally's empty snapshot does — `buckets: []` + the default `bucketMs`
// (24h / 48) — so the field is shape-stable whether or not the tally is wired
// (parity with the three sibling tallies' EMPTY_* timelines). `bucketMs` conveys
// granularity (it is the default, NOT 0: only a degenerate config override
// collapses it to 0, mirroring summarizeTimeline's empty-but-valid vs degenerate
// distinction).
const EMPTY_RETENTION = Object.freeze({
  configured: { maxEvents: 0, maxAgeMs: 0 },
  retainedCount: 0,
  totalPruned: 0,
  last: null,
  timeline: Object.freeze({
    buckets: [],
    bucketMs: DEFAULT_TIMELINE_WINDOW_MS / DEFAULT_TIMELINE_MAX_BUCKETS,
  }),
});

/**
 * Build the retention-health tally (WARDEN-743). Mirrors the injected-seam
 * discipline of `createRejectionTally` / `createPersistErrorTally`: an OPTIONAL
 * handler dep (no tally wired = today's behavior, exactly like an absent
 * rejections dep) with an injected `now` so the tally is unit-testable with a
 * fake clock (no real Date in tests). `maxEvents` / `maxAgeMs` capture the
 * ACTIVE retention bounds so a maintainer reading GET /summary can see what the
 * retained count is measured against (the trigger is created with the same
 * bounds, so the tally and the trigger agree on the configured window).
 *
 * WARDEN-838 extends the snapshot with a bounded `timeline` — a per-bucket COUNT
 * of PRUNED EVENTS over the SAME rolling window/granularity as the read-path
 * `timeline` (summarizeTimeline, WARDEN-603) and the three sibling tallies'
 * timelines (rejections WARDEN-798 / persistErrors WARDEN-777 / deduped
 * WARDEN-812), so a maintainer reading `retention.timeline` can tell ONGOING
 * eviction churn (the store is at cap and /summary's window is actively
 * shrinking — prunes still landing in the newest bucket → action: raise
 * STORE_MAX_EVENTS) from a RESOLVED one-time compaction an hour ago (prunes
 * clustered in older buckets, newest bucket empty → benign history, do nothing)
 * — the spike-vs-baseline question `totalPruned` + a single `last.ts` provably
 * cannot answer for an episodic eviction flood that often recovers. This is the
 * last event-flow tally to carry a timeline; it composes the SAME shared helper
 * (createBoundedRollingTimeline, WARDEN-834) the other three do.
 *
 * The bucket count is the prune's `dropped` (pruned) count — NOT +1 per prune —
 * so the timeline answers "how much signal was lost WHEN": a 5000-event
 * compaction registers as 5000 in its bucket. This is the one seam where
 * retention diverges from the +1-per-record siblings; it uses the helper's
 * `bump(ts, count)` weight.
 *
 * `maxBuckets` / `windowMs` default to the read-path `DEFAULT_TIMELINE_*`
 * constants (imported from summary.mjs) so the four timelines never drift in
 * granularity. Production constructs the tally with only `{ maxEvents, maxAgeMs }`
 * (the siblings are constructed no-arg), so the timeline window is always the
 * factory default.
 *
 * @param {{ now?: () => number, maxEvents?: number, maxAgeMs?: number, maxBuckets?: number, windowMs?: number }} [opts]
 * @returns {{
 *   record(rec: { before?: number, after?: number, pruned?: number, rewrote?: boolean, retainedCount?: number }): void,
 *   snapshot(): {
 *     configured: { maxEvents: number, maxAgeMs: number },
 *     retainedCount: number,
 *     totalPruned: number,
 *     last: { before: number, after: number, pruned: number, rewrote: boolean, ts: number } | null,
 *     timeline: { buckets: { bucketStart: number, bucketEnd: number, count: number }[], bucketMs: number },
 *   }
 * }}
 */
export function createRetentionTally({
  now = Date.now,
  maxEvents = 0,
  maxAgeMs = 0,
  maxBuckets = DEFAULT_TIMELINE_MAX_BUCKETS,
  windowMs = DEFAULT_TIMELINE_WINDOW_MS,
} = {}) {
  const configured = { maxEvents, maxAgeMs };
  let totalPruned = 0;
  let retainedCount = 0;
  let last = null;

  // The bounded rolling-window timeline (WARDEN-834): the stateful twin of the
  // pure summarizeTimeline, shared by all four tallies via a single helper. See
  // createBoundedRollingTimeline for the degenerate-config guard, the absolute-
  // slot + re-relativize math, the rolloff GC, and the top-boundary fold.
  const timeline = createBoundedRollingTimeline({ now, maxBuckets, windowMs });

  return {
    /** Record one COMPLETED prune. Called ONLY on a SUCCESSFUL prune (the
     *  trigger's `.then`, never `.catch`/`.finally`) — a failed prune removed
     *  nothing and must not record a spurious sample. Bounded: accumulates a
     *  running `totalPruned` and overwrites `last` with the single most-recent
     *  {before, after, pruned, rewrote, ts} — never one entry per call. A no-op
     *  prune (pruned:0) does NOT inflate `totalPruned` but DOES refresh `last`,
     *  so a maintainer sees when retention last RAN even on a quiet store.
     *  `retainedCount` carries the post-prune store size (the trigger passes
     *  `after`), so the snapshot reads "retained vs. cap" at a glance. */
    record({ before, after, pruned, rewrote, retainedCount: rc } = {}) {
      // A real prune result always carries finite before/after counts. A bare or
      // empty call (defensive misuse) records nothing — parity with
      // createRejectionTally's `if (status == null) return` guard (no spurious
      // sample, no false "retention ran" signal on an idle receiver).
      if (!Number.isFinite(before) && !Number.isFinite(after)) return;
      const dropped = Number.isFinite(pruned) ? pruned : 0;
      totalPruned += dropped;
      retainedCount = Number.isFinite(rc) ? rc : retainedCount;
      // A SINGLE now() read (WARDEN-798 / WARDEN-838) serves both last.ts and
      // the timeline slot — one clock read per prune, never two. The helper's
      // bump() takes that externally-read timestamp (+ the dropped count as its
      // weight) rather than reading now() itself.
      const ts = now();
      last = { before, after, pruned: dropped, rewrote: Boolean(rewrote), ts };
      // The timeline answers "how much signal was lost WHEN": a prune contributes
      // its `dropped` event count to the bucket it landed in (a 5000-event
      // compaction registers as 5000, not 1 — the weight that distinguishes this
      // tally from the +1-per-record siblings). A no-op prune (dropped:0) bumps
      // NOTHING — parity with `totalPruned += dropped` (which also adds 0) — so
      // an idle-but-healthy retention (a no-op sweep on a quiet store) does not
      // paint a spurious eviction bucket.
      if (dropped > 0) timeline.bump(ts, dropped);
    },
    /** A stable point-in-time copy of the aggregate (a later record does not
     *  mutate a previously-returned snapshot). The `timeline` is delegated to
     *  createBoundedRollingTimeline, which re-relativizes the absolute per-bucket
     *  counts against the current rolling window and drops rolled-off buckets. */
    snapshot() {
      return {
        configured: { ...configured },
        retainedCount,
        totalPruned,
        last: last ? { ...last } : null,
        timeline: timeline.snapshot(),
      };
    },
  };
}

// ── DEDUP TALLY (WARDEN-752) ─────────────────────────────────────────────────
// A bounded, in-memory, receiver-local tally of the transport-retries the
// receiver ABSORBED via idempotent ingest (WARDEN-666). When the warden client
// loses a 2xx (network reset / read timeout while the receiver did synchronous
// appendFile I/O under disk pressure), it retries the SAME bytes with the SAME
// idempotency-key; ingest() recognizes the key and returns 202 {accepted:0,
// deduped:true} WITHOUT re-persisting. That is the correctness mechanism — a
// single crash retried ≤3× lands as ONE event, not 2–4. But the receiver recorded
// NOTHING about the dedup, so a maintainer self-hosting the receiver was blind to
// it: the handler never inspected `result.body.deduped` and GET /summary had no
// `deduped` field. A sustained dedup spike is actionable signal ("clients are
// retrying because my receiver is slow / the network is flaky" vs "traffic is
// flowing cleanly"), and it is also the only symptom of a client-side
// idempotency-key bug (a key reused across DIFFERENT batches → unique events
// wrongly absorbed), which would otherwise surface as mysteriously low /summary
// counts with no diagnostic.
//
// Bounded means: a total count + a SINGLE most-recent `lastSeen` epoch-ms. A
// dedup carries no diagnostic string — it is "a batch we'd already accepted came
// back" — so the shape is the persistErrors shape MINUS `lastReason`. It does NOT
// keep one record per dedup, so a sustained retry storm can't grow it. Like the
// sibling tallies, it "persists nothing" — it is in-memory and receiver-local (a
// transport-health signal need not survive a restart), and ADDITIVE ONLY: it
// records a dedup that ALREADY happened, relaxes no check (handshake / validate /
// auth / retention / body cap / the dedup decision itself all still run), mirrors
// no invariant, routes nothing to a third party, and carries a COUNT and a
// timestamp only — never a raw client payload or extended-tier identifier (a
// dedup absorbs a batch WITHOUT reading or re-persisting its bytes, so there is
// no payload path to leak).

// The zeroed shape returned when no tally is wired OR no dedup has been recorded
// yet — identical to a fresh tally's snapshot(), so a healthy receiver reads the
// same zeroed `deduped` whether or not the tally is wired (parity with
// EMPTY_REJECTIONS / EMPTY_PERSIST_ERRORS: no false alarm on a quiet receiver).
// The zeroed `timeline` (WARDEN-812) carries the SAME stable shape a wired tally's
// empty snapshot does — `buckets: []` + the default `bucketMs` (24h / 48) — so the
// field is shape-stable whether or not the tally is wired (parity with
// EMPTY_PERSIST_ERRORS / EMPTY_REJECTIONS on the timeline shape). `bucketMs`
// conveys granularity (it is the default, NOT 0: only a degenerate config override
// collapses it to 0, mirroring summarizeTimeline's empty-but-valid vs degenerate
// distinction).
const EMPTY_DEDUPED = Object.freeze({
  total: 0,
  lastSeen: null,
  timeline: Object.freeze({
    buckets: [],
    bucketMs: DEFAULT_TIMELINE_WINDOW_MS / DEFAULT_TIMELINE_MAX_BUCKETS,
  }),
});

// The zeroed shape returned when no dedup set is wired (no seenKeys dep) — the
// capacity-health complement's parity with EMPTY_DEDUPED. `configured` is
// {maxKeys:0, ttlMs:0} here (the "unset" shape an absent dep yields, exactly like
// EMPTY_RETENTION's {maxEvents:0, maxAgeMs:0}); `size: 0`. So a caller that does
// not wire the set still gets a zeroed `seenKeys` field on /summary —
// backward-compatible additive shape, exactly like an absent deduped dep.
const EMPTY_SEEN_KEYS = Object.freeze({
  configured: { maxKeys: 0, ttlMs: 0 },
  size: 0,
});

/**
 * Build the dedup tally (WARDEN-752). Mirrors the injected-seam discipline of
 * `createPersistErrorTally`: an OPTIONAL handler dep (no tally wired = today's
 * behavior, exactly like an absent persistErrors dep) with an injected `now` so
 * the tally is unit-testable with a fake clock (no real Date in tests).
 *
 * WARDEN-812 extends the snapshot with a bounded `timeline` — a per-bucket COUNT of
 * dedup hits over the SAME rolling window/granularity as the read-path `timeline`
 * (summarizeTimeline, WARDEN-603), the persistErrors timeline (WARDEN-777), and the
 * rejections timeline (WARDEN-798), so a maintainer reading `deduped.timeline` can
 * tell an ONGOING retry storm (dedups still landing in the newest bucket — clients
 * hammering because the receiver is slow / the network is flaky RIGHT NOW) from a
 * RESOLVED blip (dedups clustered in older buckets, newest bucket empty — an
 * hour-old spike that stopped) — the spike-vs-baseline question `total` + `lastSeen`
 * provably cannot answer for an episodic retry flood that often recovers.
 *
 * `maxBuckets` / `windowMs` default to the read-path `DEFAULT_TIMELINE_*` constants
 * (imported from summary.mjs) so the four timelines never drift in granularity.
 *
 * @param {{ now?: () => number, maxBuckets?: number, windowMs?: number }} [opts]
 * @returns {{
 *   record(): void,
 *   snapshot(): {
 *     total: number,
 *     lastSeen: number | null,
 *     timeline: { buckets: { bucketStart: number, bucketEnd: number, count: number }[], bucketMs: number },
 *   }
 * }}
 */
export function createDedupTally({
  now = Date.now,
  maxBuckets = DEFAULT_TIMELINE_MAX_BUCKETS,
  windowMs = DEFAULT_TIMELINE_WINDOW_MS,
} = {}) {
  let total = 0;
  let lastSeen = null;

  // The bounded rolling-window timeline (WARDEN-834): the stateful twin of the
  // pure summarizeTimeline, shared by all three tallies via a single helper. See
  // createBoundedRollingTimeline for the degenerate-config guard, the absolute-slot
  // + re-relativize math, the rolloff GC, and the top-boundary fold.
  const timeline = createBoundedRollingTimeline({ now, maxBuckets, windowMs });

  return {
    /** Record one dedup hit. Bounded: accumulates a total COUNT, tracks only the
     *  single most-recent `lastSeen` — never one entry per call — and bumps the
     *  count in the absolute time-bucket the dedup landed in (the timeline's
     *  per-bucket distribution). A dedup carries no diagnostic string, so there is
     *  no reason sample (unlike the rejections / persistErrors tallies). */
    record() {
      total += 1;
      // A SINGLE now() read serves both lastSeen and the timeline slot — one clock
      // read per dedup, never two. The helper's bump() takes that externally-read
      // timestamp rather than reading now() itself.
      lastSeen = now();
      timeline.bump(lastSeen);
    },
    /** A stable point-in-time copy of the aggregate (a later record does not mutate
     *  a previously-returned snapshot). The `timeline` is delegated to
     *  createBoundedRollingTimeline, which re-relativizes the absolute per-bucket
     *  counts against the current rolling window and drops rolled-off buckets. */
    snapshot() {
      return { total, lastSeen, timeline: timeline.snapshot() };
    },
  };
}

// ── SEEN-KEY DEDUP SET (WARDEN-666) ───────────────────────────────────────────
// The receiver-local twin of the client's per-batch idempotency-key header. A
// bounded set of keys the receiver has ALREADY accepted a batch for, consulted in
// ingest()'s pure pipeline (it receives this set as an OPTIONAL injected dep
// alongside store/validateEvent). On a HIT (the key was recorded on a PRIOR
// successful persist and has not expired) ingest() returns 202
// {accepted:0, deduped:true} WITHOUT calling store.appendEvents — so a retried
// batch whose 2xx was lost does not double-count. Mirrors the injected-seam
// discipline of createRejectionTally / createPersistErrorTally: an OPTIONAL dep
// (no set wired = today's behavior — no dedup, exactly like an absent tally) with
// an injected `now` so the TTL is unit-testable with a fake clock.
//
// DURABILITY (WARDEN-803): the set optionally SURVIVES a receiver restart via two
// injected seams — `load()` (read the persisted {key, expiresAt} set on boot) and
// `persist(entries)` (write it back, debounced + off-path). A retried batch whose
// 2xx was lost BEFORE a restart would otherwise hit a wiped in-memory set and
// double-persist; reloading the set on boot closes that restart-mid-retry-window
// edge. UNWIRED (no load/persist) = today's behavior exactly: an in-memory,
// restart-local set (the optional-dep discipline shared with the store's injected
// sink/source/rewrite and the tallies). The persisted record is the opaque client
// key string + its expiry epoch-ms ONLY — never an event payload, tier identifier,
// or credential — identical trust posture to the on-disk NDJSON event store.

/**
 * Build the seen-key dedup set (WARDEN-666), with an OPTIONAL durability seam
 * (WARDEN-803) so the set survives a receiver restart.
 *
 * Bounded by BOTH a TTL (each key expires `ttlMs` after it was recorded; expiry
 * is observed LAZILY on access, so no timers are needed) and a COUNT cap (when a
 * new key would exceed `maxKeys`, the OLDEST insertion-ordered entry is evicted —
 * FIFO via Map insertion order). Neither a long-lived receiver nor a sustained
 * flood of distinct keys can grow it without limit.
 *
 * `load` / `persist` (WARDEN-803) are OPTIONAL: unwired = an in-memory,
 * restart-local set (today's behavior). When wired, `boot()` seeds the Map from
 * `load()` (dropping already-expired entries so a stale file can't resurrect dead
 * keys) and `record()` arms a DEBOUNCED, off-path, re-entrancy-guarded
 * `persist()` — the EXACT pattern `createRetentionTrigger` uses for retention
 * compaction. The hot accept path pays NO synchronous disk write per batch; a
 * burst of records coalesces into one debounced flush, and a `persist` rejection
 * is swallowed (telemetry is best-effort — the receiver never crashes on a dedup-
 * file write failure). The persisted record round-trips BOTH the key AND its
 * `expiresAt` (dropping expiry would collapse to a no-op — everything would
 * re-expire immediately and the post-restart HIT would never fire).
 *
 * @param {{ ttlMs?: number, maxKeys?: number, now?: () => number, load?: () => (Promise<{key: string, expiresAt: number}[]> | {key: string, expiresAt: number}[]), persist?: (entries: {key: string, expiresAt: number}[]) => (void | Promise<void>), persistDebounceMs?: number, setTimer?: (fn: () => void, ms: number) => unknown, clearTimer?: (id: unknown) => void }} [opts]
 * @returns {{
 *   has(key: string): boolean,
 *   record(key: string): void,
 *   snapshot(): { configured: { maxKeys: number, ttlMs: number }, size: number },
 *   boot(): Promise<void>,
 *   cancel(): void
 * }}
 */
export function createSeenKeys({
  ttlMs = DEFAULT_DEDUP_TTL_MS,
  maxKeys = DEFAULT_DEDUP_MAX_KEYS,
  now = Date.now,
  load = null,
  persist = null,
  persistDebounceMs = SEEN_KEYS_PERSIST_DEBOUNCE_MS,
  setTimer = (fn, ms) => setTimeout(fn, ms),
  clearTimer = (id) => clearTimeout(id),
} = {}) {
  // key -> expiresAt (a Map preserves insertion order, giving O(1)-amortized FIFO
  // eviction via keys().next()).
  const seen = new Map();
  // Debounce / re-entrancy state for the off-path persist write — mirrors
  // createRetentionTrigger's timerId / running / arm / flush discipline exactly.
  let timerId = null;
  let running = false;
  let dirty = false; // a record() landed since the last persist began

  // The live {key, expiresAt} set the Map currently holds, with already-expired
  // entries dropped (they are dead — persisting them would only bloat the file, and
  // load drops them anyway). Bounded by `maxKeys` (the FIFO cap applied in record()).
  function liveEntries() {
    const t = now();
    const out = [];
    for (const [key, expiresAt] of seen) {
      if (expiresAt > t) out.push({ key, expiresAt });
    }
    return out;
  }

  function flush() {
    timerId = null;
    // Re-entrancy guard: if a persist is still mid-flight, let it finish. A record()
    // that lands during it sets `dirty`, and the in-flight flush's .finally re-arms.
    if (running || !persist) return;
    running = true;
    dirty = false; // snapshotting the current set to persist
    const entries = liveEntries();
    Promise.resolve(persist(entries))
      .catch(() => {
        // A persist failure must NEVER crash the receiver (telemetry is best-effort,
        // mirroring createRetentionTrigger's swallow-don't-crash posture). The
        // in-memory set stays correct for this process; the next record() re-arms.
      })
      .finally(() => {
        running = false;
        // Records that landed during the write re-arm so the latest set reaches disk.
        if (dirty) arm();
      });
  }

  function arm() {
    if (!persist) return;
    if (timerId != null || running) return; // already armed / a flush is mid-flight
    timerId = setTimer(flush, persistDebounceMs);
  }

  return {
    /** True if a non-empty `key` was recorded and has not yet expired (a dedup
     *  HIT). Read-only with a lazy purge: an expired entry looked up here is
     *  dropped so it can be re-recorded fresh. A non-string/empty key (no header)
     *  is never a hit — an old client that sends no idempotency-key is unchanged. */
    has(key) {
      if (typeof key !== 'string' || key.length === 0) return false;
      const expiresAt = seen.get(key);
      if (expiresAt === undefined) return false;
      if (expiresAt <= now()) {
        seen.delete(key); // lazy purge of a single expired entry
        return false;
      }
      return true;
    },
    /** Record a key as seen (refreshing its expiry if already present). Call this
     *  ONLY after a successful persist — so a batch that was rejected (4xx) or
     *  failed to store is never cached and a retry is processed normally rather
     *  than wrongly dedup'd. FIFO cap evicts the OLDEST entry while over the bound.
     *  When a `persist` seam is wired, arms a debounced off-path flush (a burst
     *  coalesces into one write; the hot path pays no synchronous disk write). */
    record(key) {
      if (typeof key !== 'string' || key.length === 0) return;
      seen.set(key, now() + ttlMs);
      while (seen.size > maxKeys) {
        const oldest = seen.keys().next().value;
        seen.delete(oldest);
      }
      if (persist) {
        dirty = true;
        arm();
      }
    },
    /** Point-in-time capacity snapshot (observability; expired entries are purged
     *  only lazily, so `size` is an UPPER BOUND on live keys). `configured` carries
     *  the FIFO `maxKeys` cap + per-key `ttlMs` this set was built with, so GET
     *  /summary can show the live `size` against the bounds that back the dedup
     *  decision — the capacity-health complement to the `deduped` hit-count tally
     *  (WARDEN-790): `deduped` tells you "the dedup fired"; `configured` + `size`
     *  tell you "the set that backs it can still catch the next retry" (or is
     *  losing keys to FIFO eviction / TTL expiry). */
    snapshot() {
      return { configured: { maxKeys, ttlMs }, size: seen.size };
    },
    /** Seed the Map from the `load()` seam (WARDEN-803). Called ONCE at boot,
     *  BEFORE the server accepts traffic, so a retry landing in the first
     *  milliseconds after boot still dedups. Already-expired entries are DROPPED on
     *  load (a stale file can't resurrect a dead key); malformed entries are
     *  skipped. Best-effort and NEVER rejects — a missing/corrupt file starts the
     *  set empty (the receiver always comes up). No-op when no `load` is wired. */
    async boot() {
      if (typeof load !== 'function') return;
      let entries;
      try {
        entries = await load();
      } catch {
        // A missing/corrupt persisted file must never crash boot — start empty.
        return;
      }
      if (!Array.isArray(entries)) return;
      const t = now();
      for (const e of entries) {
        if (!e || typeof e.key !== 'string' || e.key.length === 0) continue;
        const { expiresAt } = e;
        if (typeof expiresAt !== 'number' || !Number.isFinite(expiresAt)) continue;
        if (expiresAt <= t) continue; // DROP already-expired on load
        seen.set(e.key, expiresAt);
      }
      // Apply the FIFO cap in case a stale file exceeds it.
      while (seen.size > maxKeys) {
        const oldest = seen.keys().next().value;
        seen.delete(oldest);
      }
      dirty = false; // freshly loaded — the Map mirrors the file
    },
    /** Clear any pending debounced persist (e.g. on server shutdown). Mirrors
     *  createRetentionTrigger.cancel(). */
    cancel() {
      if (timerId != null) {
        clearTimer(timerId);
        timerId = null;
      }
    },
  };
}

/**
 * Build the request handler. `store` and `schema` are injected so the handler is
 * testable with a capturing store and WITHOUT a live port (tests call the handler
 * directly with a fake req/res). The defaults wire the real file-backed store and
 * the vendored schema for production.
 *
 * `authToken` (optional shared secret, WARDEN-569): when set, the FIRST thing the
 * handler does is enforce `Authorization: Bearer <authToken>` — BEFORE any routing
 * — so the gate is uniform over EVERY route (POST /ingest today, any future read
 * surface) without rework. A request missing a valid token is rejected with 401
 * (a non-retryable 4xx; the client drops the batch rather than looping) before
 * ingest() runs, so nothing is persisted on a reject. When unset, behavior is
 * UNCHANGED (open) — the keystone stays runnable bare for local dev.
 *
 * `retention` (optional maintenance trigger, WARDEN-579): AFTER a successful
 * persist, the handler records the accepted count via `retention.afterAppend(n)`
 * — FIRE-AND-FORGET (not awaited). The trigger arms a debounced, off-path prune
 * if the count bound was crossed; the compaction itself never runs on the request
 * path, so the 202 is sent immediately and ingest latency is unaffected. No
 * `retention` dep = today's behavior (the handler is unchanged for callers that
 * don't wire retention).
 *
 * `rejections` (optional tally, WARDEN-591): at EVERY rejection site — the
 * auth-gate 401, the 404 routing miss, the body-read 400, AND the 400/415/422
 * returned from `ingest()` (the `!result.ok` branch) — the handler records
 * `{ status, reason }` via `rejections.record(...)`. GET /summary reads
 * `rejections.snapshot()` so a maintainer can tell "traffic is arriving and being
 * hard-rejected" from "no traffic at all" (the two were indistinguishable before).
 * This catches the auth-gate 401, which a naive "record on `!result.ok`" tally
 * would silently MISS (the gate returns at server.mjs BEFORE `ingest()` runs).
 * The reason is always the receiver's own short diagnostic string — never raw
 * client payloads or extended-tier identifiers (the trust model is preserved). No
 * `rejections` dep = a zeroed `rejections` field on /summary and no recording —
 * today's behavior, exactly like an absent retention dep.
 *
 * `persistErrors` (optional tally, WARDEN-607): a SEPARATE signal from
 * `rejections`. When `await ingest(...)` THROWS — a persist failure
 * (`store.appendEvents()` rejecting: disk full / EACCES / EISDIR / a missing or
 * rewritten store file / a sink rejection; the one path that escapes ingest's
 * rejection discipline as a throw rather than a `result`) — the handler catches
 * it, returns a clean retryable 503 (no hung socket), AND records the failure via
 * `persistErrors.record(...)`. GET /summary reads `persistErrors.snapshot()` so a
 * maintainer can tell "traffic is arriving, validating, but the store is refusing
 * writes" from "no traffic at all" (the two were indistinguishable before — a
 * persist throw bypassed the `!result.ok` recording entirely). Like `rejections`,
 * the recorded reason is the store/sink's OWN diagnostic (an OS errno such as
 * ENOSPC / EACCES, or a sink error) — never a raw client payload: by the time the
 * sink runs the store has already JSON.stringified each event, so a sink throw
 * carries system info, not event bytes or extended-tier identifiers. No
 * `persistErrors` dep = a zeroed `persistErrors` field on /summary and no
 * recording — today's behavior, exactly like an absent rejections dep.
 *
 * `retentionHealth` (optional tally, WARDEN-743): the retention-HEALTH tally
 * built by `createRetentionTally({ maxEvents, maxAgeMs })`. It is passed BOTH to
 * the handler (here, for /summary) AND into `createRetentionTrigger` (so the
 * trigger can `record()` on a successful prune). It is DISTINCTLY named from the
 * `retention` TRIGGER dep (which exposes `afterAppend/sweep/cancel` and has no
 * `.snapshot()`): the in-process variable is `retentionHealth` to avoid that
 * collision, while the /summary response KEY stays `retention` (what a
 * maintainer reads). GET /summary reads `retentionHealth.snapshot()` so a
 * maintainer sees (a) the configured retention bound (`maxEvents` + `maxAgeMs`),
 * (b) the current retained count against that bound, and (c) whether/when
 * retention last pruned and how many it dropped — so a truncated signal is
 * LEGIBLE instead of silent. Like `rejections`/`persistErrors`, it is UNSCOPED
 * by the /summary event filters (it tallies receiver operational health, not the
 * event subset). No `retentionHealth` dep = the zeroed `EMPTY_RETENTION` shape
 * on /summary — today's behavior, exactly like an absent persistErrors dep.
 *
 * `now` (optional clock, WARDEN-603): the GET /summary `timeline` distribution
 * is a rolling recent window measured back from `now`, so the handler — the
 * testable seam — takes an injectable `now` (default `Date.now`) to stay
 * fake-clock testable, mirroring `createRejectionTally({ now })`. Production is
 * unchanged when `now` is omitted (real clock). The timeline itself is ALWAYS
 * computed (it is a pure read over persisted `timestamp`s, like `summarize()`),
 * so every /summary response carries the field whether or not `now` is wired.
 *
 * `maxBodyBytes` (optional body cap, WARDEN-627, default 0 = unbounded): bounds
 * the POST /ingest request body — the one remaining unbounded INPUT after
 * retention bounded the store. The handler rejects an oversized body with a
 * non-retryable 413 (recorded at the rejection seam) BEFORE it can exhaust
 * receiver RSS, via TWO pre-/mid-buffer checks: a Content-Length pre-check that
 * 413's WITHOUT reading a byte when the declared length is over the cap, and a
 * cap-aware readBody that 413's mid-read when an unknown-length (chunked) body
 * crosses the cap. `0` (the default) preserves today's behavior — the cap is
 * wired by createReceiver from INGEST_MAX_BODY_BYTES (default 1 MiB), mirroring
 * how retention bounds flow in. A schema-handshake pre-check also runs before the
 * body is buffered (defense-in-depth alongside ingest()'s own check) so a wrong-
 * version request is 415'd without paying its body's memory cost.
 *
 * `seenKeys` (optional dedup set, WARDEN-666): a bounded, receiver-local set of
 * idempotency keys the receiver has ALREADY accepted a batch for. It is
 * passed THROUGH to `ingest()` as an optional dep (absent = today's behavior — no
 * dedup, exactly like an absent tally). Inside ingest(), a request whose
 * `idempotency-key` header is already in the set returns 202 {accepted:0,
 * deduped:true} WITHOUT calling store.appendEvents — so a retried batch whose 2xx
 * was lost (the client reuses one key across retries of the same bytes) does not
 * double-count and silently inflate /summary + /events. ADDITIVE ONLY: dedup avoids
 * a duplicate write, never expands collection, relaxes no check (handshake /
 * validate / auth / retention / body cap all still run), persists nothing, and
 * routes nothing to a third party. Built by `createSeenKeys()`. GET /summary ALSO
 * reads `seenKeys.snapshot()` (WARDEN-790) so a maintainer sees the set's live
 * FILL LEVEL (`size`) against its configured `maxKeys` cap + per-key `ttlMs` —
 * the capacity-health complement to the `deduped` hit-count tally: `deduped` says
 * the dedup fired, `seenKeys` says the set backing it can still catch the next
 * retry (or is losing keys to FIFO eviction / TTL expiry). No `seenKeys` dep = no
 * dedup AND the zeroed `EMPTY_SEEN_KEYS` shape on /summary (backward-compatible
 * with an old client that sends no idempotency-key header, and with a caller that
 * doesn't wire the set). When the set is built with an injected durability seam
 * (load/persist, WARDEN-803) it is reloaded from disk on boot and re-persisted
 * (debounced, off-path) on change, so a retried batch whose 2xx was lost before a
 * receiver restart still dedups post-restart; the persisted set holds opaque key
 * strings + expiry ms only (never an event payload or tier identifier). This
 * handler passes `seenKeys` through opaquely — boot/cancel are driven by
 * `createReceiver`'s lifecycle, not the per-request path.
 *
 * `deduped` (optional tally, WARDEN-752): a bounded tally of the transport-retries
 * the receiver ABSORBED via idempotent ingest. When `await ingest(...)` resolves
 * with `result.body.deduped === true` — a retried batch whose 2xx was lost, whose
 * idempotency-key ingest() recognized, so it returned 202 {accepted:0,
 * deduped:true} WITHOUT re-persisting (the WARDEN-666 correctness mechanism) — the
 * handler records it via `deduped.record()`. GET /summary reads
 * `deduped.snapshot()` so a maintainer can tell "clients are retrying because my
 * receiver is slow / the network is flaky" from "traffic is flowing cleanly", and
 * so a client-side idempotency-key bug (one key reused across DIFFERENT batches →
 * unique events wrongly absorbed) has a visible symptom instead of mysteriously
 * low /summary counts. `deduped:true` cleanly distinguishes such a retry from a
 * normal accept (`{accepted:n}` carries no `deduped` field) and from a rejection
 * (`!result.ok`, recorded in `rejections`). The tally carries a COUNT and a
 * timestamp only — never a raw client payload or extended-tier identifier (a dedup
 * absorbs a batch WITHOUT reading or re-persisting its bytes, so there is no
 * payload path to leak). No `deduped` dep = a zeroed `deduped` field on /summary
 * and no recording — today's behavior, exactly like an absent persistErrors dep.
 *
 * `now` (optional clock, default Date.now): passed THROUGH to `ingest()` as the
 * `receivedAt` stamp source (WARDEN-692). ingest() stamps each accepted event
 * with `now()` so the time-sensitive read surfaces (timeline / retention /
 * /events) can key off the RECEIVER's clock (with a `timestamp` fallback) and stay
 * robust to skewed client clocks. Injecting it here (mirroring the tally /
 * seenKeys / retention-trigger factories) keeps the stamp unit-testable with a
 * fake clock; absent it defaults to Date.now.
 *
 * @param {{ store: object, schema?: { SCHEMA_VERSION: number, validateEvent: (e: unknown) => boolean }, authToken?: string, retention?: { afterAppend(count?: number): void }, rejections?: { record(rec: { status: number, reason?: string }): void, snapshot(): object }, persistErrors?: { record(rec: { reason?: string }): void, snapshot(): object }, seenKeys?: { has(key: string): boolean, record(key: string): void, snapshot(): { configured: { maxKeys: number, ttlMs: number }, size: number } }, deduped?: { record(): void, snapshot(): object }, maxBodyBytes?: number, now?: () => number, retentionHealth?: { record(rec: { before?: number, after?: number, pruned?: number, rewrote?: boolean, retainedCount?: number }): void, snapshot(): object } }} deps
 * @returns {(req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => Promise<void>}
 */
export function createRequestHandler({ store, schema = DEFAULT_SCHEMA, authToken, retention, rejections, persistErrors, seenKeys, deduped, maxBodyBytes = 0, now = Date.now, retentionHealth } = {}) {
  if (!store) throw new TypeError('createRequestHandler: `store` is required');

  // Boot timestamp (WARDEN-768): captured ONCE here at handler construction
  // (the receiver's clock at boot) and frozen for the process — NOT re-read per
  // request. Emitted as a top-level epoch-ms on GET /summary so a maintainer
  // reading the restart-wiped tallies (rejections / persistErrors / retention /
  // deduped — NOT seenKeys, which is persisted beside telemetry.ndjson by
  // WARDEN-803) can tell a healthy quiet receiver (startedAt = hours ago, so the
  // zeroed tallies genuinely mean "no rejections in that whole window") apart
  // from a crash-looping one that zeroed every tally seconds ago (startedAt =
  // seconds ago). Those two states read byte-identical on /summary without this
  // field — the exact silent-empty-state hole the tallies were added to close,
  // applied to the restart boundary they themselves create. Captured HERE
  // (createRequestHandler) rather than createReceiver because the handler
  // already owns the receiver's clock via its `now` dep (default Date.now,
  // documented above as "the RECEIVER's clock … kept unit-testable with a fake
  // clock") — the boot timestamp IS that clock at construction, so this reuses
  // the existing injected-clock seam (the same `now: () => 86_400_000` pattern
  // used ~10× in test/server.test.mjs) with no createReceiver signature change.
  // Like the tallies it disambiguates, startedAt is receiver-local and
  // in-memory (captured once, never persisted) — it intentionally does NOT
  // survive a restart; after a restart it reflects the NEW boot, immediately
  // self-documenting that the tallies were just zeroed.
  const startedAt = now();

  // Centralized rejection recorder: a guarded no-op when no tally is wired (today's
  // behavior — no recording, exactly like an absent retention dep). Every rejection
  // site below reads `recordRejection(...)`; the tally dep is the single switch.
  // `declaredVersion` is passed ONLY at the 415 seams (the drift population,
  // WARDEN-761); other statuses omit it and the tally buckets nothing.
  const recordRejection = (status, reason, declaredVersion) => {
    if (rejections) rejections.record({ status, reason, declaredVersion });
  };
  // Centralized persist-error recorder: a guarded no-op when no tally is wired
  // (today's behavior — no recording, exactly like an absent rejections dep). The
  // single persist-failure site below reads `recordPersistError(...)`; the tally
  // dep is the single switch.
  const recordPersistError = (reason) => {
    if (persistErrors) persistErrors.record({ reason });
  };
  // Centralized dedup recorder: a guarded no-op when no tally is wired (today's
  // behavior — no recording, exactly like an absent persistErrors dep). The single
  // dedup-detection site below reads `recordDedup()`; the tally dep is the switch.
  const recordDedup = () => {
    if (deduped) deduped.record();
  };
  return async (req, res) => {
    // AUTH GATE — optional but, when authToken is set, the FIRST thing checked and
    // BEFORE routing. Placing it ahead of the route/404 dispatch keeps the gate
    // uniform over ALL routes: an unauthenticated request to ANY path (including a
    // non-existent one) is rejected here, so neither /ingest nor the read surface
    // (GET /summary) can be reached without the shared secret. Unset authToken = open.
    if (authToken) {
      const provided = readBearerToken(req.headers);
      if (!provided || !tokensMatch(provided, authToken)) {
        recordRejection(401, 'unauthorized');
        return sendJson(res, 401, { error: 'unauthorized' });
      }
    }

    // Parse the request-target. llhttp accepts origin-form targets like `//` and
    // `///` (common automated-scanner probes: `GET //etc/passwd`) and delivers them
    // as `req.url`, but `new URL('///', ...)` throws ERR_INVALID_URL. Because this
    // handler is `async`, an unguarded throw becomes a rejected promise the HTTP
    // server never awaits → unhandled rejection → process termination on the
    // default --unhandled-rejections=throw. Guard it: a malformed request-target is
    // a clean 400 (not a crash), feeding the rejections tally so the scanner noise
    // surfaces in GET /summary. This is the same hardening vein as the readBody cap
    // (WARDEN-627) and the persist catch (WARDEN-607) — the one remaining unguarded
    // throw at handler entry. Fixed string reason (no raw client payload in the
    // tally sample), consistent with the rejection-seam trust model.
    let pathname, searchParams;
    try {
      const url = new URL(req.url, 'http://localhost');
      ({ pathname, searchParams } = url);
    } catch {
      recordRejection(400, 'invalid request-target');
      return sendJson(res, 400, { error: 'invalid request-target' });
    }

    // GET /summary — the maintainer read surface (WARDEN-567). Returns AGGREGATES
    // of the already-validated, already-redacted events persisted by POST /ingest
    // (counts / per-type / top error names / schema-version histogram only —
    // never raw events, never extended-tier names). No request body is read.
    //
    // The aggregates are SCOPEABLE (WARDEN-727) via the SAME conjunctive filters
    // /events takes — ?type= / ?platform= / ?appVersion= / ?since= — applied
    // pre-summarize so a maintainer who spots a win32 spike or a v0.1.18 volume
    // bubble on /summary.platforms / appVersions can scope /summary to that
    // platform / release to read its topErrorNames / topSignatures / timeline,
    // then drill into /events for the individual payloads. The filter uses the
    // SHARED `filterEvents` core selectEvents also calls, so the two surfaces
    // agree on what each filter means. With NO filters the response is byte-for-
    // byte the legacy unscoped aggregate (backward compatible). `total` is ALWAYS
    // the FULL persisted count (how much the window is a window OF); `matched` is
    // the size of the scoped subset the aggregates were computed over (≤ total).
    //
    // Gated by the auth block above: like every other route, /summary requires the
    // shared secret when AUTH_TOKEN is set (so extended-tier-derived aggregates are
    // never broadcast to an unauthenticated reader on the LAN). Unset = open. (This
    // fulfills the earlier WARDEN-567 note to gate the read surface when auth landed.)
    if (req.method === 'GET' && pathname === SUMMARY_PATH) {
      try {
        // `unreadable` (WARDEN-825): a STATE count of the lines on disk that
        // failed to parse during THIS read — the read-path silent-signal-loss
        // the write-path tallies below do NOT cover. An event can clear every
        // write-path gate (validated → rejections; persisted → persistErrors;
        // not pruned → retention) and still VANISH from the maintainer's signal
        // with zero diagnostic if its line became unreadable on disk (a partial
        // append left by a process killed mid-write — `createReceiver` has no
        // SIGTERM/SIGINT handler, so a container stop during `fileSink`'s
        // non-atomic appendFile is the concrete cause). `parseNdjson` already
        // SKIPS the bad line (the defense exists); this is the observability for
        // it. The counter is installed via the OPTIONAL `onSkip` seam threaded
        // through readEvents → source → parseNdjson, invoked once per skipped
        // line during the single read this handler already performs.
        //
        // This is a STATE snapshot recomputed per read, NOT a cumulative tally
        // like rejections/persistErrors/retention/deduped: every /summary request
        // re-reads the file → re-skips the SAME line, so a cumulative counter
        // would inflate on every read. The count reflects the on-disk file AS IT
        // IS, so it SELF-HEALS to 0 the moment a retention compaction rewrites
        // the file (`prune` → `serializeNdjson` re-serializes only the PARSED
        // events, dropping the unparseable line). And because it reads the
        // persistent file, it SURVIVES a restart (unlike the in-memory tallies,
        // which `startedAt` exists to disambiguate) — the same corrupt line reads
        // the same count across reboots until a compaction rewrites it.
        let unreadable = 0;
        const events = await store.readEvents({
          onSkip: () => {
            unreadable += 1;
          },
        });
        // Scope the aggregates with the SAME conjunctive filter /events uses
        // (WARDEN-727): ?type= / ?platform= / ?appVersion= / ?since= select which
        // ALREADY-redacted, ALREADY-validated events get aggregated. filterEvents is
        // the SHARED core selectEvents also calls, so /summary and /events filter
        // identically forever. The filter lives HERE (pre-summarize) — NOT as a param
        // to summarize()/summarizeTimeline(), which stay PURE single-arg functions of
        // the (now-filtered) event array (the contract documented + tested in the
        // comment just below). Same `searchParams.get(…) ?? undefined` /
        // `searchParams.has(…) ? Number(…) : undefined` shape as the /events handler.
        const filtered = filterEvents(events, {
          type: searchParams.get('type') ?? undefined,
          platform: searchParams.get('platform') ?? undefined,
          appVersion: searchParams.get('appVersion') ?? undefined,
          since: searchParams.has('since') ? Number(searchParams.get('since')) : undefined,
        });
        // Compose the bounded `rejections` tally, the bounded `persistErrors`
        // tally, the bounded `deduped` tally, the bounded `retention` tally, AND
        // the bounded `timeline` distribution here — NOT inside summarize().
        // summarize(filtered) stays a PURE single-arg function of the
        // (already-filtered) event array (documented + tested that way); all five
        // are handler-composed, exactly the way the retention TRIGGER is
        // handler-injected rather than summarize-injected. `rejections`,
        // `persistErrors`, `deduped`, and `retention` are each the tally's
        // snapshot when wired, or their zeroed EMPTY_* shape otherwise (stable
        // shape for every caller). They are intentionally UNSCOPED — they tally
        // the REQUEST/OPERATIONAL seam (every rejection / persist site / dedup /
        // retention prune on THIS receiver), NOT the event subset; a
        // platform/release filter must not hide receiver-health signal.
        // `timeline` is ALWAYS computed — a pure read over the FILTERED events'
        // effective `receivedAt ?? timestamp`s measured back from the injected
        // `now` (default Date.now) — so the field is present for every caller,
        // wired or not, and now reflects the scoped subset. Counts only; never
        // raw events or extended-tier names.
        //
        // `retention` (WARDEN-743) is sourced from the `retentionHealth` dep (the
        // tally), NOT the `retention` trigger — the trigger has no `.snapshot()`.
        // The response KEY stays `retention` (what a maintainer reads); only the
        // in-process variable is renamed to dodge the collision.
        //
        // `total` overrides summarize()'s own `total` (which would be
        // `filtered.length`) to stay the FULL persisted count — `events.length`,
        // mirroring /events' `total: events.length` ("how much the window is a
        // window OF"). `matched` is the scoped count (`filtered.length`, ≤ total),
        // so a maintainer sees both the retained set and the slice the aggregates
        // were computed over. With no filters, matched === total.
        return sendJson(res, 200, {
          ...summarize(filtered),
          total: events.length,
          matched: filtered.length,
          // `startedAt` (WARDEN-768): top-level epoch-ms recording when THIS
          // receiver (re)booted, captured once at handler construction (frozen
          // for the process — see the capture site above). It is the key that
          // makes the restart-wiped tallies below interpretable: a maintainer
          // reading `rejections.total = 0` alongside `startedAt = <2 minutes
          // ago>` knows the tally only covers 2 minutes, not that the receiver
          // has been stably quiet. Flat top-level epoch-ms, exactly like
          // firstSeen / lastSeen / total / matched. Receiver-local: after a
          // restart it shows the NEW boot time (self-documenting the zeroed
          // tallies). Never persisted — operational metadata about the process,
          // counts/epoch-ms only, no JSONB allow-list concern.
          startedAt,
          rejections: rejections ? rejections.snapshot() : EMPTY_REJECTIONS,
          persistErrors: persistErrors ? persistErrors.snapshot() : EMPTY_PERSIST_ERRORS,
          retention: retentionHealth ? retentionHealth.snapshot() : EMPTY_RETENTION,
          deduped: deduped ? deduped.snapshot() : EMPTY_DEDUPED,
          // `seenKeys` (WARDEN-790): the capacity-health complement to `deduped`
          // above. `deduped` reports the dedup HIT COUNT (how many retries the set
          // absorbed); `seenKeys` reports the set's live FILL LEVEL against its
          // configured FIFO `maxKeys` cap + per-key `ttlMs` — so a maintainer can
          // tell the set that backs dedup is LOSING keys (a fleet / a client-side
          // idempotency-key bug emitting near-unique keys pinning it at the cap, or
          // a TTL too short for the retry window under disk pressure) from one with
          // room to spare. Without this, dedup degradation under FIFO eviction or
          // TTL expiry is SILENT: a retried batch whose key was evicted/expired
          // before its retry arrives is treated as FRESH and persisted again,
          // inflating /summary + /events while `deduped` still climbs on the keys
          // that DID hit. Sourced from the existing seenKeys.snapshot() (the same
          // set ingest() consults); no set wired = the zeroed EMPTY_SEEN_KEYS
          // shape (today's behavior, exactly like an absent deduped dep). `size` is
          // an upper bound — expired entries purge lazily on access, so a high read
          // means "the set WAS that full," never a false alarm of its own.
          seenKeys: seenKeys ? seenKeys.snapshot() : EMPTY_SEEN_KEYS,
          // `unreadable` (WARDEN-825): the STATE count of currently-unreadable
          // lines on the read path — see the read site above for the full
          // rationale. A bare integer ONLY (never the corrupt line's bytes: a
          // partial line could carry a payload fragment / residual identifier),
          // always present (additive, backward-compatible), recomputed per read
          // off the on-disk file. `total + unreadable` reconciles with the
          // on-disk non-blank line count, closing the silent undercount where a
          // skipped line drops out of `total` with no entry anywhere. Receiver-
          // local, in-memory computation, no new collection, no tier expansion,
          // no third party — identical posture to the sibling tallies.
          unreadable,
          timeline: summarizeTimeline(filtered, { now }),
        });
      } catch (e) {
        return sendJson(res, 500, { error: `could not read summary: ${e?.message ?? e}` });
      }
    }

    // GET /capabilities — the config-time verification surface (WARDEN-595). Lets
    // a warden client confirm the receiver is reachable + schema-matched + authed
    // BEFORE relying on it, via its Settings "Test connection" probe. Returns the
    // receiver's SCHEMA_VERSION (so the client can detect cross-repo drift
    // against its own vendored copy) and `authRequired` (whether AUTH_TOKEN is
    // set). No request body is read; nothing is persisted — the verdict is a LIVE,
    // on-demand probe, never a cached "connected" that could go stale (receiver
    // down, token rotated) and become a false trust signal.
    //
    // Gated by the auth block above like every other route — DO NOT bypass the
    // gate for this route. The gate is what makes the auth verdict meaningful: a
    // receiver with AUTH_TOKEN set 401s an unauthenticated probe BEFORE this body
    // is returned, and that 401 ITSELF communicates "auth required" (no
    // special-casing). A probe carrying a valid token gets the 200, so
    // `authRequired: true` is only ever observed once the caller is authenticated
    // (an open receiver returns `authRequired: false`). Unknown method on this
    // path still falls through to the 404 below.
    if (req.method === 'GET' && pathname === CAPABILITIES_PATH) {
      return sendJson(res, 200, {
        schemaVersion: schema.SCHEMA_VERSION,
        authRequired: Boolean(authToken),
      });
    }

    // GET /events — the maintainer full-fidelity drill-down surface (WARDEN-599).
    // Returns the recent persisted EVENTS THEMSELVES (not aggregates) — the
    // diagnostic payloads /summary deliberately discards: an ErrorEvent's message +
    // frames, a CrashEvent's reason, a StallEvent's lagMs. BOUNDED to a newest-N
    // window (default 100, hard cap 200) so a near-full store (up to the 10000
    // retention cap) can never yield a multi-MB response, with optional ?type=,
    // ?since=, ?platform=, ?appVersion= and ?signature= filters. No request body is read; nothing is persisted.
    //
    // ?signature= (WARDEN-746) is the failure-axis drill-down complement to
    // /summary.topSignatures (WARDEN-707): a maintainer who spots a high-count
    // distinct failure on /summary copies its `signature` here to read THAT
    // failure's actual payloads instead of eyeballing ?type=error mixed with every
    // other error in the window. The filter key is the SAME signatureOf() /summary
    // ranks by (reused, not copied — byte-identical round-trip). It is wired HERE
    // only, NOT on /summary (scoping the aggregates to a single signature is not
    // meaningful), so the filter is an /events-only drill-down even though it rides
    // the shared filterEvents core. An event whose signatureOf() is null never
    // matches, never crashes; no param = today's behavior exactly (backward compatible).
    //
    // `matched` + `?offset=` (WARDEN-755) close the silent-truncation gap on this
    // drill-down: `matched` is how many events match the filters (pre-bound, via
    // the SAME shared filterEvents core /summary uses — never a third path), and
    // `?offset=` pages OLDER matches past the newest-N window. A maintainer reading
    // /events?type=error&platform=win32 against an 847-match subset now sees
    // `matched: 847` alongside the newest 200, KNOWS 647 older matches exist, and
    // pages `?offset=200` (then 400, 600) until `offset + events.length >= matched`
    // — reaching every matching payload without the response ever exceeding the
    // 200-event cap. The /events-side twin of the truncation WARDEN-727 closed on
    // /summary (which got `matched`/`total`); /events has the SAME gap AND could
    // not be paged. `total` stays the FULL persisted count (mirrors /summary);
    // `matched` is the scoped subset size (≤ total). `limit` / `offset` echo the
    // RESOLVED bound actually applied (via resolveLimit / resolveOffset — the SAME
    // helpers selectEvents uses internally), so a `?limit=50000` surfaces as
    // `limit: 200` (the cap that bound the page), not the raw 50000.
    //
    // Reads ONLY already-persisted, already-schema-validated, already-client-
    // redacted events via the existing readEvents() seam — no new collection, no
    // re-collection, no third party, no server-side redaction, no tier expansion.
    // The identical trust posture as /summary, full-fidelity instead of aggregate.
    // The ≤200-per-page bound STAYS; `offset` only selects WHICH bounded slice.
    //
    // Strictly ADDITIVE: with no filters `matched === total`, and with no `?offset=`
    // the window is byte-identical to today (`offset` echoes 0). An un-filtered
    // /events response is unchanged except for the added `matched` / `limit` /
    // `offset` fields.
    //
    // Gated by the auth block above like every other route (the read surface
    // inherits the shared secret — auth is NOT re-implemented here). Unknown method
    // on this path still falls through to the 404 below.
    if (req.method === 'GET' && pathname === EVENTS_PATH) {
      try {
        const events = await store.readEvents();
        // selectEvents(events, query) is a PURE single-arg-of-the-array function
        // (sibling of summarize); the handler composes it with the parsed query,
        // exactly as it composes summarize. `total` is the FULL persisted count
        // (pre-bound) so a maintainer sees how much the window is a window OF; the
        // bounded `events` array is the newest-N matching the filters.
        //
        // `matched` is computed via the shared `filterEvents` core — the EXACT twin
        // of /summary's `matched` (same helper, same opts) so the two surfaces
        // cannot drift on what "type=error&platform=win32" matches. It is the
        // scoped subset size pre-bound, so a maintainer can tell a complete 200-
        // match window from a truncation of a larger set — and know how far to
        // page. `limit` / `offset` echo the RESOLVED bound + page offset actually
        // applied, via resolveLimit / resolveOffset — the SAME helpers selectEvents
        // calls internally — so the echoed values are provably the ones that shaped
        // `events` (a clamped / past-end query surfaces as the bound that ran, not
        // the raw query string).
        const filterOpts = {
          type: searchParams.get('type') ?? undefined,
          platform: searchParams.get('platform') ?? undefined,
          appVersion: searchParams.get('appVersion') ?? undefined,
          signature: searchParams.get('signature') ?? undefined,
          since: searchParams.has('since') ? Number(searchParams.get('since')) : undefined,
        };
        const limit = searchParams.has('limit') ? Number(searchParams.get('limit')) : undefined;
        const offset = searchParams.has('offset') ? Number(searchParams.get('offset')) : undefined;
        const matched = filterEvents(events, filterOpts).length;
        const selected = selectEvents(events, { ...filterOpts, limit, offset });
        return sendJson(res, 200, {
          events: selected,
          total: events.length,
          matched,
          limit: resolveLimit(limit),
          offset: resolveOffset(offset, matched),
        });
      } catch (e) {
        return sendJson(res, 500, { error: `could not read events: ${e?.message ?? e}` });
      }
    }

    // Route: only POST /ingest is ingest. Anything else is a 404 (so a maintainer
    // scanning logs can tell probe noise from a receiver bug).
    if (req.method !== 'POST' || pathname !== INGEST_PATH) {
      recordRejection(404, 'not found');
      return sendJson(res, 404, { error: 'not found' });
    }

    // ── PRE-READ BODY BOUNDS (WARDEN-627) ──────────────────────────────────────
    // Two checks run BEFORE the body is buffered, so a request we'll reject never
    // pays the memory cost of its body (the "don't buffer what you'll reject"
    // discipline — the write-path twin of retention bounding the store). Both
    // record at the existing rejection seam so the oversized/drift traffic surfaces
    // on GET /summary.rejections.byStatus the same way 415s already do.

    // (1) SCHEMA HANDSHAKE — a defense-in-depth EARLY copy; the canonical check
    // still lives in ingest() (its pure-function contract + suite assert there).
    // Hoisting a pre-read check here means a wrong-version request is 415'd
    // WITHOUT buffering its body — the drift case (WARDEN-591's chief-risk
    // symptom: a flood of 415s under a schema mismatch) collapses to ZERO memory
    // cost instead of paying for its whole body before ingest() rejects it.
    const declaredSchema = readHeader(req.headers, 'x-telemetry-schema');
    if (declaredSchema !== String(schema.SCHEMA_VERSION)) {
      const reason = `unsupported telemetry schema version: expected "${schema.SCHEMA_VERSION}", got ${JSON.stringify(declaredSchema)}`;
      recordRejection(415, reason, declaredSchema);
      return sendJson(res, 415, { error: reason });
    }

    // (2) CONTENT-LENGTH PRE-CHECK — when the header declares a length over the
    // cap, reject 413 WITHOUT reading a byte: the cheapest possible bound (no
    // buffering, no parsing). Skipped when the cap is `0` (the unbounded escape
    // hatch) or the header is absent (chunked/unknown-length) — the unknown-length
    // case is handled mid-read by the cap-aware readBody below.
    if (maxBodyBytes > 0) {
      const contentLength = readHeader(req.headers, 'content-length');
      if (contentLength != null) {
        const declared = Number(contentLength);
        if (Number.isFinite(declared) && declared > maxBodyBytes) {
          const reason = 'request body too large';
          recordRejection(413, reason);
          return sendJson(res, 413, { error: reason });
        }
      }
    }

    let body;
    try {
      body = await readBody(req, { maxBytes: maxBodyBytes });
    } catch (e) {
      // The cap-aware readBody rejects with a TAGGED PAYLOAD_TOO_LARGE error → a
      // non-retryable 413 (recorded at the rejection seam). This MUST be mapped to
      // 413 — NOT fall through to the 400 a plain read-error records below — so an
      // unknown-length (chunked) oversized body is rejected as 413 too, matching the
      // Content-Length path. A 413 is non-429 4xx, and the client ALREADY drops non-
      // retryable 4xx (telemetry-send.js isTransientStatus = 429|5xx only), so an
      // oversized batch is DROPPED, not retried forever.
      if (e && e.code === 'PAYLOAD_TOO_LARGE') {
        const reason = 'request body too large';
        recordRejection(413, reason);
        return sendJson(res, 413, { error: reason });
      }
      const reason = `could not read request body: ${e?.message ?? e}`;
      recordRejection(400, reason);
      return sendJson(res, 400, { error: reason });
    }

    // ingest's own rejection discipline guarantees a non-retryable 4xx (or 202);
    // those come back as a `result` mapped straight onto the response. The ONE path
    // that escapes that discipline as a THROW is a persist failure —
    // `store.appendEvents()` rejecting (disk full / EACCES / EISDIR / a missing or
    // rewritten store file / a sink rejection): ingest awaits the store, so a store
    // throw rejects the ingest promise. Without this catch that rejection propagates
    // → the handler promise rejects → Node never calls res.end() → the client's
    // fetch HANGS until socket timeout (the WARDEN-607 bug). Catch it and return a
    // clean RETRYABLE 503 (mirroring the /summary catch→clean-5xx handler above):
    // the events were already schema-valid; the store may recover — so the client's
    // existing isTransientStatus (5xx) bounded-retry path handles it WITHOUT a
    // protocol change, instead of the today's hang→socket-timeout→network-error
    // path. 4xx would be WRONG: ingest's rejection discipline reserves 4xx for the
    // non-retryable "drop the batch" verdicts, and the client fails fast on those.
    let result;
    try {
      result = await ingest({ headers: req.headers, body }, { ...schema, store, seenKeys, now });
    } catch (e) {
      // The recorded reason is the store/sink's OWN diagnostic — an OS errno
      // (ENOSPC / EACCES / EISDIR) or a sink error — NEVER a raw client payload:
      // by the time the sink runs the store has already JSON.stringified each event
      // (store.mjs), so a sink throw carries system info, not event bytes or
      // extended-tier identifiers (the trust model is preserved). Bounded by the
      // tally: a total count + this single most-recent sample only.
      recordPersistError(e?.message ?? String(e));
      return sendJson(res, 503, { error: 'could not persist telemetry batch' });
    }

    // DEDUP TALLY (WARDEN-752) — a transport-retry the receiver ABSORBED via
    // idempotent ingest (WARDEN-666): the client lost a 2xx and re-sent the SAME
    // bytes with the SAME idempotency-key, ingest() recognized the key, and
    // returned 202 {accepted:0, deduped:true} WITHOUT re-persisting. `deduped:true`
    // cleanly distinguishes such a retry from a normal accept ({accepted:n} carries
    // no `deduped` field, ingest.mjs step 6) and from a rejection (`!result.ok`,
    // recorded just below). This is the ONE detection site — the tally counts
    // transport-retries so a maintainer reading GET /summary can tell "clients are
    // retrying because my receiver is slow / the network is flaky" from "traffic is
    // flowing cleanly". A persist failure (caught above) never reaches here, and an
    // unwired `deduped` dep records nothing (guarded no-op). ADDITIVE ONLY: records
    // a dedup that already happened, relaxes no check (the dedup decision itself
    // still ran in ingest()), persists nothing.
    if (result.body?.deduped === true) recordDedup();

    // REJECTIONS (WARDEN-591) — record the ingest-result rejections (400/415/422)
    // for the /summary tally. These are the rejections that come BACK from ingest()
    // as a `result`; the auth-gate 401, the 404, and the body-read 400 were already
    // recorded at their own early-return sites above. The sample reason is ingest's
    // own diagnostic string (e.g. "unsupported telemetry schema version..."), never
    // a raw client payload. Accepted traffic (result.ok) records nothing here. A
    // persist failure (caught above) records into the SEPARATE persistErrors tally,
    // NOT here — a store throw never reaches this `!result.ok` branch. For a 415,
    // the declared version is threaded from ingest's structured `body.declaredVersion`
    // (WARDEN-761) so the drift population buckets by declared version here too —
    // the contract path used by direct ingest callers + tests (the production 415
    // is recorded earlier at the defense-in-depth pre-read; both record for
    // consistency, and they are mutually exclusive — the pre-read returns first).
    if (!result.ok) {
      recordRejection(result.status, result.body && result.body.error, result.body && result.body.declaredVersion);
    }

    // RETENTION (WARDEN-579) — fire-and-forget: AFTER a successful persist, tell
    // the maintenance trigger how many events landed so it can arm a DEBOUNCED,
    // off-path prune if the count bound was crossed. NOT awaited: the 202 is sent
    // at once and any compaction runs later — never a synchronous rewrite here.
    // Skipped on reject (nothing appended) and when no retention is wired.
    if (retention && result.ok && result.body && result.body.accepted > 0) {
      retention.afterAppend(result.body.accepted);
    }

    return sendJson(res, result.status, result.body);
  };
}

/**
 * Create (and default-start) a receiver. Every dependency is injectable; the
 * defaults wire the real file-backed store + vendored schema. `authToken` mirrors
 * the PORT/STORE env pattern: read from AUTH_TOKEN when not passed explicitly.
 * Retention bounds (WARDEN-579) mirror the same pattern: read from
 * STORE_MAX_EVENTS / STORE_MAX_AGE_HOURS when not passed, defaulting to bounded.
 * The ingest body cap (WARDEN-627) mirrors it again: read from
 * INGEST_MAX_BODY_BYTES when not passed, defaulting to a bounded 1 MiB.
 *
 * @param {{ port?: number, storePath?: string, seenKeysPath?: string, store?: object, schema?: object, authToken?: string, maxEvents?: number, maxAgeHours?: number, maxBodyBytes?: number }} [opts]
 * @returns {import('node:http').Server}
 */
export function createReceiver({
  port = process.env.PORT ? Number(process.env.PORT) : DEFAULT_PORT,
  storePath = process.env.STORE ?? DEFAULT_STORE_PATH,
  seenKeysPath = join(dirname(storePath), 'seen-keys.ndjson'),
  store = createNdjsonStore({
    sink: fileSink(storePath),
    source: fileSource(storePath),
    rewrite: fileRewrite(storePath),
  }),
  schema = DEFAULT_SCHEMA,
  authToken = process.env.AUTH_TOKEN,
  maxEvents = envRetentionInt('STORE_MAX_EVENTS', DEFAULT_MAX_EVENTS),
  maxAgeHours = envRetentionInt('STORE_MAX_AGE_HOURS', DEFAULT_MAX_AGE_HOURS),
  maxBodyBytes = envRetentionInt('INGEST_MAX_BODY_BYTES', DEFAULT_MAX_BODY_BYTES),
} = {}) {
  const maxAgeMs = maxAgeHours > 0 ? maxAgeHours * HOUR_MS : 0;
  const retentionHealth = createRetentionTally({ maxEvents, maxAgeMs });
  const retention = createRetentionTrigger(store, { maxEvents, maxAgeMs, retention: retentionHealth });
  const rejections = createRejectionTally();
  const persistErrors = createPersistErrorTally();
  // The seen-key dedup set, wired with its durability seams (WARDEN-803): the set
  // is reloaded from `seenKeysPath` on boot and re-persisted (debounced, off-path)
  // on change, so idempotent-ingest dedup survives a receiver restart. The file
  // lives beside telemetry.ndjson (same dir, same trust posture) by default.
  const seenKeys = createSeenKeys({
    load: fileSeenKeysSource(seenKeysPath),
    persist: fileSeenKeysSink(seenKeysPath),
  });
  const deduped = createDedupTally();
  const handler = createRequestHandler({ store, schema, authToken, retention, rejections, persistErrors, seenKeys, deduped, maxBodyBytes, retentionHealth });
  const server = createServer(handler);

  // Periodic age-expiry sweep — ONLY when an age window is set. A quiet store
  // (no appends) still needs old events to expire, so sweep on a timer; the
  // trigger debounces + re-entrancy-guards the actual prune. unref'd so it never
  // keeps the process alive solely to prune. Cleaned up on server close.
  let sweepInterval = null;
  if (maxAgeMs > 0) {
    sweepInterval = setInterval(() => retention.sweep(), RETENTION_SWEEP_MS);
    sweepInterval.unref?.();
  }
  server.on('close', () => {
    retention.cancel();
    if (sweepInterval) clearInterval(sweepInterval);
    seenKeys.cancel(); // drop any pending debounced seen-key persist
  });

  // Boot the seen-key set from disk BEFORE accepting traffic, so a retry landing
  // in the first milliseconds after boot still dedups (WARDEN-803). boot() is
  // best-effort and never rejects (a missing/corrupt file starts the set empty),
  // so `listen` fires either way — the receiver always comes up, just with a
  // possibly-empty dedup set if the persisted file was unreadable. createReceiver
  // returns the server synchronously; the deferred `listen` fires once boot
  // resolves (before which no connection can be accepted).
  seenKeys.boot().finally(() => {
    if (port != null) server.listen(port);
  });
  return server;
}

// Direct entrypoint: `node server.mjs`.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const server = createReceiver();
  const storePath = process.env.STORE ?? DEFAULT_STORE_PATH;
  server.on('listening', () => {
    const addr = server.address();
    const port = typeof addr === 'object' && addr ? addr.port : '(unknown)';
    const authed = process.env.AUTH_TOKEN ? 'auth: ON (Authorization: Bearer required)' : 'auth: OFF (open — dev only)';
    const maxEv = envRetentionInt('STORE_MAX_EVENTS', DEFAULT_MAX_EVENTS);
    const maxAh = envRetentionInt('STORE_MAX_AGE_HOURS', DEFAULT_MAX_AGE_HOURS);
    const retention =
      maxEv > 0 || maxAh > 0
        ? `retention: max ${maxEv} events${maxAh > 0 ? `, ${maxAh}h age` : ''}`
        : 'retention: OFF (unbounded — not recommended)';
    const maxBody = envRetentionInt('INGEST_MAX_BODY_BYTES', DEFAULT_MAX_BODY_BYTES);
    const bodyCap =
      maxBody > 0
        ? `body cap: ${maxBody} bytes`
        : 'body cap: OFF (unbounded — not recommended)';
    console.log(
      `warden-telemetry receiver listening on :${port} (POST ${INGEST_PATH}, GET ${SUMMARY_PATH}, GET ${CAPABILITIES_PATH}, GET ${EVENTS_PATH}; store: ${storePath}; ${authed}; ${retention}; ${bodyCap})`
    );
  });
}
