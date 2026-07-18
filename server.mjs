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
// spike (a regression / deploy) from a long-running baseline. The client POSTs
// the batch verbatim to its configured endpointUrl (e.g.
// http://host:7421/ingest) and never rewrites the host, so these route paths
// are the receiver's to define.

import { createServer } from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { SCHEMA_VERSION, validateEvent } from './schema.ts';
import { createNdjsonStore, fileSink, fileSource, fileRewrite } from './store.mjs';
import { ingest } from './ingest.mjs';
import { summarize, summarizeTimeline } from './summary.mjs';
import { selectEvents } from './events.mjs';

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
 * @param {{ prune(opts: object): Promise<void> }} store
 * @param {{ maxEvents?: number, maxAgeMs?: number, debounceMs?: number, now?: () => number, setTimer?: (fn: () => void, ms: number) => unknown, clearTimer?: (id: unknown) => void }} [opts]
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
// ts). It does NOT keep one record per rejection or an unbounded set of reason
// strings, so a sustained drift storm can't grow it without limit.

// The zeroed shape returned when no tally is wired OR no rejection has been
// recorded yet — identical to a fresh tally's snapshot(), so an idle receiver
// reads the same zeroed `rejections` whether or not the tally is wired (parity
// with today's empty-store /summary: no false alarm on a quiet receiver).
const EMPTY_REJECTIONS = Object.freeze({
  total: 0,
  byStatus: {},
  lastStatus: null,
  lastReason: null,
  lastSeen: null,
});

/**
 * Build the rejection tally (WARDEN-591). Mirrors the injected-seam discipline of
 * `createRetentionTrigger`: an OPTIONAL handler dep (no tally wired = today's
 * behavior, exactly like an absent retention dep) with an injected `now` so the
 * tally is unit-testable with a fake clock (no real Date in tests).
 *
 * @param {{ now?: () => number }} [opts]
 * @returns {{
 *   record(rec: { status: number, reason?: string }): void,
 *   snapshot(): { total: number, byStatus: Record<string, number>, lastStatus: number | null, lastReason: string | null, lastSeen: number | null }
 * }}
 */
export function createRejectionTally({ now = Date.now } = {}) {
  let total = 0;
  const byStatus = {};
  let lastStatus = null;
  let lastReason = null;
  let lastSeen = null;

  return {
    /** Record one rejection. Bounded: accumulates a per-status COUNT and tracks
     *  only the single most-recent {status, reason, ts} — never one entry per call. */
    record({ status, reason } = {}) {
      if (status == null) return;
      const key = String(status);
      total += 1;
      byStatus[key] = (byStatus[key] ?? 0) + 1;
      lastStatus = status;
      lastReason = typeof reason === 'string' && reason.length > 0 ? reason : null;
      lastSeen = now();
    },
    /** A stable point-in-time copy of the aggregate (a later record does not mutate
     *  a previously-returned snapshot). */
    snapshot() {
      return { total, byStatus: { ...byStatus }, lastStatus, lastReason, lastSeen };
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
// EMPTY_REJECTIONS: no false alarm on a quiet receiver).
const EMPTY_PERSIST_ERRORS = Object.freeze({
  total: 0,
  lastReason: null,
  lastSeen: null,
});

/**
 * Build the persist-error tally (WARDEN-607). Mirrors the injected-seam discipline
 * of `createRejectionTally`: an OPTIONAL handler dep (no tally wired = today's
 * behavior, exactly like an absent rejections dep) with an injected `now` so the
 * tally is unit-testable with a fake clock (no real Date in tests).
 *
 * @param {{ now?: () => number }} [opts]
 * @returns {{
 *   record(rec: { reason?: string }): void,
 *   snapshot(): { total: number, lastReason: string | null, lastSeen: number | null }
 * }}
 */
export function createPersistErrorTally({ now = Date.now } = {}) {
  let total = 0;
  let lastReason = null;
  let lastSeen = null;

  return {
    /** Record one persist failure. Bounded: accumulates a total COUNT and tracks
     *  only the single most-recent {reason, ts} — never one entry per call. */
    record({ reason } = {}) {
      total += 1;
      lastReason = typeof reason === 'string' && reason.length > 0 ? reason : null;
      lastSeen = now();
    },
    /** A stable point-in-time copy of the aggregate (a later record does not mutate
     *  a previously-returned snapshot). */
    snapshot() {
      return { total, lastReason, lastSeen };
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
 * @param {{ store: object, schema?: { SCHEMA_VERSION: number, validateEvent: (e: unknown) => boolean }, authToken?: string, retention?: { afterAppend(count?: number): void }, rejections?: { record(rec: { status: number, reason?: string }): void, snapshot(): object }, persistErrors?: { record(rec: { reason?: string }): void, snapshot(): object }, maxBodyBytes?: number, now?: () => number }} deps
 * @returns {(req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => Promise<void>}
 */
export function createRequestHandler({ store, schema = DEFAULT_SCHEMA, authToken, retention, rejections, persistErrors, maxBodyBytes = 0, now = Date.now } = {}) {
  if (!store) throw new TypeError('createRequestHandler: `store` is required');

  // Centralized rejection recorder: a guarded no-op when no tally is wired (today's
  // behavior — no recording, exactly like an absent retention dep). Every rejection
  // site below reads `recordRejection(...)`; the tally dep is the single switch.
  const recordRejection = (status, reason) => {
    if (rejections) rejections.record({ status, reason });
  };
  // Centralized persist-error recorder: a guarded no-op when no tally is wired
  // (today's behavior — no recording, exactly like an absent rejections dep). The
  // single persist-failure site below reads `recordPersistError(...)`; the tally
  // dep is the single switch.
  const recordPersistError = (reason) => {
    if (persistErrors) persistErrors.record({ reason });
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
    // Gated by the auth block above: like every other route, /summary requires the
    // shared secret when AUTH_TOKEN is set (so extended-tier-derived aggregates are
    // never broadcast to an unauthenticated reader on the LAN). Unset = open. (This
    // fulfills the earlier WARDEN-567 note to gate the read surface when auth landed.)
    if (req.method === 'GET' && pathname === SUMMARY_PATH) {
      try {
        const events = await store.readEvents();
        // Compose the bounded `rejections` tally, the bounded `persistErrors`
        // tally, AND the bounded `timeline` distribution here — NOT inside
        // summarize(). summarize(events) stays a PURE single-arg function of the
        // event array (documented + tested that way); all three are handler-
        // composed, exactly the way `retention` is handler-injected rather than
        // summarize-injected. `rejections` and `persistErrors` are each the tally's
        // snapshot when wired, or their zeroed EMPTY_* shape otherwise (stable shape
        // for every caller). `timeline` is ALWAYS computed — a pure read over the
        // events' `timestamp`s measured back from the injected `now` (default
        // Date.now) — so the field is present for every caller, wired or not.
        // Counts only; never raw events or extended-tier names.
        return sendJson(res, 200, {
          ...summarize(events),
          rejections: rejections ? rejections.snapshot() : EMPTY_REJECTIONS,
          persistErrors: persistErrors ? persistErrors.snapshot() : EMPTY_PERSIST_ERRORS,
          timeline: summarizeTimeline(events, { now }),
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
    // retention cap) can never yield a multi-MB response, with optional ?type= and
    // ?since= filters. No request body is read; nothing is persisted.
    //
    // Reads ONLY already-persisted, already-schema-validated, already-client-
    // redacted events via the existing readEvents() seam — no new collection, no
    // re-collection, no third party, no server-side redaction, no tier expansion.
    // The identical trust posture as /summary, full-fidelity instead of aggregate.
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
        const selected = selectEvents(events, {
          type: searchParams.get('type') ?? undefined,
          limit: searchParams.has('limit') ? Number(searchParams.get('limit')) : undefined,
          since: searchParams.has('since') ? Number(searchParams.get('since')) : undefined,
        });
        return sendJson(res, 200, { events: selected, total: events.length });
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
      recordRejection(415, reason);
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
      result = await ingest({ headers: req.headers, body }, { ...schema, store });
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

    // REJECTIONS (WARDEN-591) — record the ingest-result rejections (400/415/422)
    // for the /summary tally. These are the rejections that come BACK from ingest()
    // as a `result`; the auth-gate 401, the 404, and the body-read 400 were already
    // recorded at their own early-return sites above. The sample reason is ingest's
    // own diagnostic string (e.g. "unsupported telemetry schema version..."), never
    // a raw client payload. Accepted traffic (result.ok) records nothing here. A
    // persist failure (caught above) records into the SEPARATE persistErrors tally,
    // NOT here — a store throw never reaches this `!result.ok` branch.
    if (!result.ok) {
      recordRejection(result.status, result.body && result.body.error);
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
 * @param {{ port?: number, storePath?: string, store?: object, schema?: object, authToken?: string, maxEvents?: number, maxAgeHours?: number, maxBodyBytes?: number }} [opts]
 * @returns {import('node:http').Server}
 */
export function createReceiver({
  port = process.env.PORT ? Number(process.env.PORT) : DEFAULT_PORT,
  storePath = process.env.STORE ?? DEFAULT_STORE_PATH,
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
  const retention = createRetentionTrigger(store, { maxEvents, maxAgeMs });
  const rejections = createRejectionTally();
  const persistErrors = createPersistErrorTally();
  const handler = createRequestHandler({ store, schema, authToken, retention, rejections, persistErrors, maxBodyBytes });
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
  });

  if (port != null) server.listen(port);
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
