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
import { readFileSync } from 'node:fs';
import { timingSafeEqual } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { SCHEMA_VERSION, validateEvent } from './schema.ts';
import { createNdjsonStore, fileSink, fileSource, fileRewrite, fileSeenKeysSource, fileSeenKeysSink } from './store.mjs';
import { ingest, COMPATIBLE_SCHEMA_VERSIONS, acceptedSchemaVersionStrings, unsupportedSchemaVersionReason } from './ingest.mjs';
import { summarize, summarizeTimeline, summarizeStallsTimeline, lastAcceptedInstant, DEFAULT_TIMELINE_MAX_BUCKETS, DEFAULT_TIMELINE_WINDOW_MS } from './summary.mjs';
import { selectEvents, filterEvents, resolveLimit, resolveOffset } from './events.mjs';
import {
  createBoundedRollingTimeline,
  createRejectionTally,
  createPersistErrorTally,
  createRetentionTally,
  createDedupTally,
  createSeenKeys,
  DEFAULT_REJECTION_MAX_DECLARED_VERSIONS,
  REJECTION_OVERFLOW_KEY,
  DEFAULT_DEDUP_TTL_MS,
  DEFAULT_DEDUP_MAX_KEYS,
  SEEN_KEYS_PERSIST_DEBOUNCE_MS,
  EMPTY_REJECTIONS,
  EMPTY_PERSIST_ERRORS,
  EMPTY_RETENTION,
  EMPTY_DEDUPED,
  EMPTY_SEEN_KEYS,
} from './tallies.mjs';

// Re-exported so every previously-public name stays importable from server.mjs (WARDEN-1525).
export {
  createBoundedRollingTimeline,
  createRejectionTally,
  createPersistErrorTally,
  createRetentionTally,
  createDedupTally,
  createSeenKeys,
  DEFAULT_REJECTION_MAX_DECLARED_VERSIONS,
  REJECTION_OVERFLOW_KEY,
  DEFAULT_DEDUP_TTL_MS,
  DEFAULT_DEDUP_MAX_KEYS,
  SEEN_KEYS_PERSIST_DEBOUNCE_MS,
};

export const DEFAULT_PORT = 7421;
export const DEFAULT_STORE_PATH = new URL('./telemetry.ndjson', import.meta.url).pathname;

// Build identity of the running receiver (WARDEN-1585). Reads `version` from the
// package.json shipped beside server.mjs (the Dockerfile COPYs it into the image).
// Returns the version ONLY when it is a non-empty string; on ANY failure (missing
// file, unparseable JSON, absent / non-string / empty version) returns `null` —
// never throws and never a placeholder like 'unknown', so a reader can tell
// "unreadable" from a real release. `url` is injectable for tests.
export function readReceiverVersion(url = new URL('./package.json', import.meta.url)) {
  try {
    const version = JSON.parse(readFileSync(url, 'utf8'))?.version;
    return typeof version === 'string' && version.length > 0 ? version : null;
  } catch {
    return null;
  }
}
// Computed ONCE at module load: the process's build identity never changes.
export const DEFAULT_RECEIVER_VERSION = readReceiverVersion();
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

// Parse a non-negative number env override for retention. Unset/empty → fallback;
// an explicit "0" disables that policy (the opt-out); a malformed/negative value
// falls back to the (bounded) default so a typo can never silently unbound the
// store — the default stays bounded under any misconfiguration.
//
// The emptiness guard tests raw.trim(), not `raw === ''`, and that trim is
// load-bearing: `Number(' ') === 0`, and 0 is this function's documented
// OPT-OUT. So a whitespace-only value — a trailing space after `=` in a .env,
// or a CRLF-bearing line — would otherwise fall through both guards and read as
// "disable this cap", silently unbounding the store on an open-by-default
// listener. Whitespace-only belongs with ''/'abc'/'-5' in the bounded-default
// branch. This does NOT affect a valid override carrying incidental whitespace
// (`' 5 '` → 5): Number() trims already, so only the whitespace-ONLY family
// moves. An explicit '0' is untouched and still disables the policy.
function envRetentionInt(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw.trim() === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) return fallback;
  return n;
}

// The default shared-schema deps — the vendored schema.ts, loaded once at module
// init via Node's native type-stripping. Overridable per-handler for tests.
// `COMPATIBLE_SCHEMA_VERSIONS` (WARDEN-1445) is receiver-LOCAL — defined in
// ingest.mjs, never in the vendored schema.ts (pinned byte-identical by
// test/drift.test.mjs) — and rides this bundle so ONE `{...schema}` spread
// threads the window to BOTH handshake sites: the canonical check inside
// ingest() (via the ingest call's dep spread) and the pre-read defense-in-depth
// copy below. An override schema that omits it (the pre-existing test shape)
// collapses the window to that schema's own version — the strict pre-window
// handshake — which is exactly what those tests assert.
export const DEFAULT_SCHEMA = { SCHEMA_VERSION, validateEvent, COMPATIBLE_SCHEMA_VERSIONS };

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


// ── ACCEPTED-STREAM LIVENESS (WARDEN-1428) ───────────────────────────────────
// The twin of `startedAt` (WARDEN-768) for the ACCEPTED-event stream, closing the
// last hole the roadmap's own bar names: "Silence must mean 'nothing broke', not
// 'nobody was looking.'"
//
// Every tally on /summary answers "did an ARRIVING event get lost?" —
// `rejections`, `persistErrors`, `retention`, `deduped`, `unreadable` are each a
// loss counter, and all correctly read ~zero on a receiver nothing is talking to.
// NOT ONE of them answers "is anything ARRIVING?". `timeline` is the closest, and
// an empty `timeline` is shape-identical between a dark channel and a legitimately
// quiet one (app closed, consent off) — it cannot discriminate. So a receiver that
// has accepted NOTHING for days reads `total: 829`, every tally clean: "healthy".
//
// The failure this makes legible is SELF-ERASING BY DESIGN, which is why a
// rolling-window signal provably cannot carry it. A client/receiver schema skew
// 415s every batch; the client's drift circuit-breaker (WARDEN-614/631) then
// correctly STOPS SENDING within three requests — converting the loud rejection
// storm into total silence. The 24h `rejections.timeline` rolls, the 415s age out,
// and the only remaining evidence is an absence. This block therefore reads the
// tally's CUMULATIVE `lastSeen` / `byDeclaredVersion` snapshot, NEVER its rolling
// timeline: a cumulative last-instant survives exactly the window roll that erased
// the incident.
//
// TRUST POSTURE — identical to `startedAt` and every sibling tally: epoch-ms,
// ages, booleans and the already-bounded declared-version KEYS only. No event
// payloads, no identifiers, no raw header values beyond keys the WARDEN-829 cap
// already bounds. Receiver-local, in-memory, derived per read from state the
// handler ALREADY holds (the frozen boot instant, the events it already read, the
// rejection snapshot it already takes, its own schema version) — no new state, no
// new tally, no new store read, no clock call beyond the one already made.
//
// DELIBERATELY NO VERDICT WORD AND NO THRESHOLD: it reports instants, ages and
// booleans and never labels the channel "broken" / "stale" / "degraded". Naming an
// outage is the READER's judgement; the job here is to make the two states
// distinguishable, not to alarm.

// The "no measurement" shape of the REJECTION half of the liveness verdict, used
// when no rejection tally is wired. Mirrors the EMPTY_REJECTIONS /
// EMPTY_PERSIST_ERRORS discipline: an absent dep must yield the SAME stable shape a
// wired-but-idle tally yields, so every caller reads one shape whether or not the
// optional dep is present. Crucially it must NOT manufacture a false "no rejections
// ever" signal — so the instant is `null` (NO MEASUREMENT) rather than a fabricated
// timestamp or a `0`, exactly as `lastSeen` is `null` on an empty store. Note a
// wired-but-idle tally reads IDENTICALLY (its `lastSeen` is `null` and its
// `byDeclaredVersion` is `{}`), which is the parity the sibling EMPTY_* constants
// are built for.
const EMPTY_REJECTION_LIVENESS = Object.freeze({
  lastRejectionAt: null,
  ageSinceLastRejectionMs: null,
  mismatchedDeclaredVersions: Object.freeze([]),
});

/**
 * Compose the bounded accepted-stream liveness verdict for GET /summary
 * (WARDEN-1428). Handler-composed, NOT folded into `summarize()` — which is a
 * documented, tested PURE single-argument function of the event array with no
 * clock — exactly as `timeline` / `stallsTimeline` are composed for the same
 * reason.
 *
 * @param {{
 *   readAt: number,
 *   startedAt: number,
 *   lastAcceptedAt: number | null,
 *   rejectionSnapshot: { lastSeen?: number | null, byDeclaredVersion?: Record<string, number> } | null,
 *   schemaVersion: unknown,
 * }} input
 * @returns {{
 *   lastAcceptedAt: number | null,
 *   ageSinceLastAcceptedMs: number | null,
 *   acceptedSinceBoot: boolean,
 *   lastRejectionAt: number | null,
 *   ageSinceLastRejectionMs: number | null,
 *   mismatchedDeclaredVersions: string[],
 * }}
 * @private
 */
function _composeLiveness({ readAt, startedAt, lastAcceptedAt, rejectionSnapshot, schemaVersion }) {
  // An age with no anchor is `null`, NEVER `0` — `0` reads as "an event just
  // arrived", the precise false reassurance this block exists to prevent. Same
  // distinction the sibling fields already model (firstSeen/lastSeen are `null` on
  // an empty store, not `0`).
  //
  // Deliberately UNCLAMPED: a stored instant AHEAD of the read's clock yields a
  // NEGATIVE age, which is the honest arithmetic and a real skew signal (the
  // receiver stamps `receivedAt` itself, so this means its own clock moved
  // backwards — a restart onto a rewound clock, an NTP step). Clamping it to `0`
  // would render exactly the "an event just arrived" reassurance the `null` rule
  // one line up exists to forbid, on a receiver whose clock is untrustworthy.
  const ageSinceLastAcceptedMs = lastAcceptedAt === null ? null : readAt - lastAcceptedAt;

  // THE ONE-COMPARISON PROOF. `lastAcceptedAt < startedAt` means the newest event
  // this process holds arrived BEFORE it booted — i.e. it came off disk and this
  // process has accepted ZERO events in its entire uptime. That is exactly the
  // shape the live outage wears, and today it is unanswerable without performing
  // the comparison by hand, with nothing on the surface prompting a reader to try.
  // An empty store reads `false`: nothing has been accepted, which is the honest
  // answer (never `null` — "has this process accepted anything?" has a definite
  // answer even with no events at all).
  const acceptedSinceBoot = lastAcceptedAt !== null && lastAcceptedAt >= startedAt;

  const accepted = { lastAcceptedAt, ageSinceLastAcceptedMs, acceptedSinceBoot };
  if (!rejectionSnapshot) return { ...accepted, ...EMPTY_REJECTION_LIVENESS };

  // The rejection instant is read from the tally's CUMULATIVE `lastSeen`, NOT its
  // rolling `timeline` — see the block comment above: the timeline is precisely
  // what rolled past the live incident. With it, "rejected recently, accepted
  // nothing since boot" (drift) reads apart from "nothing arriving at all" (app
  // closed / consent off), which no other pair of fields on this surface separates.
  const rawLastRejection = rejectionSnapshot.lastSeen;
  const lastRejectionAt = Number.isFinite(rawLastRejection) ? rawLastRejection : null;
  const ageSinceLastRejectionMs = lastRejectionAt === null ? null : readAt - lastRejectionAt;

  // The declared versions in DISAGREEMENT with this receiver — read off the
  // EXISTING `byDeclaredVersion` histogram (populated only at the 415 seams, so it
  // is the drift population by construction), so the drift diagnosis rides beside
  // the liveness verdict instead of requiring a second inference.
  //
  // Two exclusions, both load-bearing:
  //  - `__overflow__` is the WARDEN-829 cardinality-cap SENTINEL, not a declared
  //    version any client sent. Emitting it would present an aggregate bucket as a
  //    real version a maintainer could go chase. Dropping it PRESERVES that bound:
  //    this list is a subset of an already-capped key set, so it inherits the ≤ N
  //    bound and adds no new unbounded surface.
  //  - the receiver's OWN version, which by definition is not a mismatch. String()
  //    compared because the histogram keys are strings while `schema.SCHEMA_VERSION`
  //    is a number — the same String() coercion the tally applies when bucketing.
  // A non-numeric scanner value ("abc", "") is a genuine disagreement and IS listed:
  // the tally deliberately buckets it verbatim, and silently dropping it here would
  // re-open the "real signal silently dropped" hole the tally closed.
  //
  // DELIBERATELY NOT excluded (WARDEN-1445): the accepted prior-window versions
  // (6, 7). The rejection tally is in-memory per process boot, so once this
  // receiver ships the window, a window version can NEVER be 415'd again — a '6'
  // bucket can only exist as genuine PRE-window history on a tally that has not
  // been restarted, and that history is exactly what a maintainer should still
  // see ("v6 clients WERE being rejected here"). Threading the window into this
  // composer to filter an unoccurring population would be dead code with a
  // second window copy; pinning this decision is the window-decision test in
  // test/server.test.mjs (the WARDEN-1445 liveness block).
  const ownVersion = String(schemaVersion);
  const histogram = rejectionSnapshot.byDeclaredVersion;
  const mismatchedDeclaredVersions =
    histogram && typeof histogram === 'object'
      ? Object.keys(histogram)
          .filter((v) => v !== REJECTION_OVERFLOW_KEY && v !== ownVersion)
          .sort()
      : [];

  return { ...accepted, lastRejectionAt, ageSinceLastRejectionMs, mismatchedDeclaredVersions };
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
 * The SAME dep is the boot clock for `startedAt` (WARDEN-768, read once at
 * construction) and, since WARDEN-1428, the read clock for the top-level `readAt`
 * plus every age inside `liveness` (read ONCE per request, so one body can never
 * carry two ages measured against two different instants).
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
 * @param {{ store: object, schema?: { SCHEMA_VERSION: number, validateEvent: (e: unknown) => boolean }, authToken?: string, retention?: { afterAppend(count?: number): void }, rejections?: { record(rec: { status: number, reason?: string }): void, snapshot(): object }, persistErrors?: { record(rec: { reason?: string }): void, snapshot(): object }, seenKeys?: { has(key: string): boolean, record(key: string): void, snapshot(): { configured: { maxKeys: number, ttlMs: number }, size: number } }, deduped?: { record(): void, snapshot(): object }, maxBodyBytes?: number, now?: () => number, retentionHealth?: { record(rec: { before?: number, after?: number, pruned?: number, rewrote?: boolean, retainedCount?: number }): void, snapshot(): object }, receiverVersion?: string | null }} deps
 * @returns {(req: import('node:http').IncomingMessage, res: import('node:http').ServerResponse) => Promise<void>}
 */
export function createRequestHandler({ store, schema = DEFAULT_SCHEMA, authToken, retention, rejections, persistErrors, seenKeys, deduped, maxBodyBytes = 0, now = Date.now, retentionHealth, receiverVersion = DEFAULT_RECEIVER_VERSION } = {}) {
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
    // never raw events; and never the extended-tier DECORATION fields
    // `chatName`/`sessionName`. The ONE identifier-bearing aggregate is
    // `workspaceShape`'s sibling `workspaceNames.names` — the bounded distinct
    // chat-name set from the `workspace-names` event type, which exists to carry
    // exactly those names behind its own consent category; see summary.mjs's
    // trust-model header for why that is a deliberate exception rather than an
    // erosion). No request body is read.
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
        // The READ's own clock (WARDEN-1428): ONE `now()` read per request, taken
        // here and reused for the top-level `readAt` AND every age inside
        // `liveness`. Read once rather than per-field so a body can never be
        // internally inconsistent (two ages computed against two different
        // instants). It uses the handler's ALREADY-INJECTED `now` dep — the same
        // seam `startedAt` and the two timeline composers use — so it adds no new
        // dependency and stays fake-clock testable; `summarizeTimeline` /
        // `summarizeStallsTimeline` keep taking the `now` FUNCTION (their
        // documented injected-clock contract, unchanged).
        const readAt = now();
        // The rejection snapshot, taken ONCE and shared by the `rejections`
        // response key and the `liveness` block below — a second `.snapshot()`
        // call could observe a rejection that landed between them, so one body
        // would report a `lastSeen` its own liveness verdict disagreed with.
        const rejectionSnapshot = rejections ? rejections.snapshot() : EMPTY_REJECTIONS;
        // Compose the bounded `rejections` tally, the bounded `persistErrors`
        // tally, the bounded `deduped` tally, the bounded `retention` tally, the
        // bounded `timeline` distribution, AND the bounded `liveness` verdict
        // (WARDEN-1428) here — NOT inside summarize().
        // summarize(filtered) stays a PURE single-arg function of the
        // (already-filtered) event array (documented + tested that way); all six
        // are handler-composed, exactly the way the retention TRIGGER is
        // handler-injected rather than summarize-injected. `rejections`,
        // `persistErrors`, `deduped`, and `retention` are each the tally's
        // snapshot when wired, or their zeroed EMPTY_* shape otherwise (stable
        // shape for every caller). They are intentionally UNSCOPED — they tally
        // the REQUEST/OPERATIONAL seam (every rejection / persist site / dedup /
        // retention prune on THIS receiver), NOT the event subset; a
        // platform/release filter must not hide receiver-health signal. `liveness`
        // is UNSCOPED for that SAME reason (it answers "is this CHANNEL receiving
        // anything?", a receiver question, not a release-slice one) — see its own
        // comment at the response site.
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
          // `receiverVersion` (WARDEN-1585): the build identity of the process that
          // answered — package.json `version`, read once at module load; `null`
          // when unreadable. Lets a reader tell "this /summary axis is not
          // deployed yet" (receiver predates it) from "axis exists, nothing to
          // report". One short string or null — bounded by construction.
          receiverVersion,
          // `readAt` (WARDEN-1428): the READ's OWN clock — epoch-ms stating when
          // THIS response was produced, from the single `now()` read above.
          // Before it, /summary served 22 top-level keys and NOT ONE of them was
          // the read's own clock: the surface carried two epoch-ms timestamps
          // (`startedAt`, `lastSeen`) and no *now* to subtract them from, so it
          // could not state its own staleness and EVERY consumer had to supply an
          // external clock to learn anything temporal. With it, every timestamp
          // already on this body becomes self-interpreting. Flat top-level
          // epoch-ms in the same shape as `startedAt` / `firstSeen` / `lastSeen`.
          // Unlike `startedAt` (frozen at boot) this is re-read PER REQUEST — it
          // is the read's clock, not the process's.
          readAt,
          // `liveness` (WARDEN-1428): the bounded accepted-stream liveness verdict
          // — the twin of `startedAt` for the ACCEPTED-event stream. See the
          // _composeLiveness block comment for the full rationale (every existing
          // tally answers "did an arriving event get LOST?"; none answers "is
          // anything ARRIVING?", and the drift failure erases its own louder
          // symptom into silence within three requests).
          //
          // UNSCOPED, on the same reasoning recorded beside the operational
          // tallies above: "is this channel receiving anything at all?" is a
          // question about the RECEIVER, not about a release slice, so a
          // ?platform= / ?appVersion= filter must not be able to hide it. Hence
          // `lastAcceptedInstant(events)` over the FULL array, never `filtered` —
          // the field would otherwise read "dark since boot" for any maintainer
          // who happened to scope to a platform with no recent traffic.
          //
          // ALWAYS present, wired or not: when no rejection tally is wired the
          // rejection half falls back to the zeroed EMPTY_REJECTION_LIVENESS
          // shape, exactly as `rejections` falls back to EMPTY_REJECTIONS — so
          // the response shape is stable for every caller.
          liveness: _composeLiveness({
            readAt,
            startedAt,
            lastAcceptedAt: lastAcceptedInstant(events),
            rejectionSnapshot: rejections ? rejectionSnapshot : null,
            schemaVersion: schema.SCHEMA_VERSION,
          }),
          rejections: rejectionSnapshot,
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
          // `stallsTimeline` (WARDEN-886): the TEMPORAL twin of the `stalls`
          // magnitude snapshot — a per-bucket `max` freeze `lagMs` (overall + split
          // by `source`) over the SAME rolling window / granularity as `timeline`.
          // handler-composed here (NOT inside summarize()) for the SAME reason
          // `timeline` is: it needs the injected `now` to anchor the rolling window,
          // and summarize()'s documented, tested contract is a PURE single-arg
          // function of the event array (no clock). It is a top-level sibling key of
          // `timeline` / `stalls`, always present (additive, backward-compatible),
          // exactly like the top-level `timeline`. A pure read over the FILTERED
          // events' stalls measured back from `now` (default Date.now), so a
          // ?platform= / ?type= scope narrows it for free. It answers the question
          // `stalls.max` provably cannot: a worst freeze in the NEWEST bucket is
          // happening now (ACTIVE regression) vs one in an older bucket that has
          // passed (RESOLVED blip). Counts + per-source maxes only; never raw events
          // or extended-tier names.
          stallsTimeline: summarizeStallsTimeline(filtered, { now }),
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
    // `acceptedSchemaVersions` (WARDEN-1445, ADDITIVE): the full ascending set of
    // declared versions the ingest handshake accepts — the current version plus
    // the prior versions proven additive subsets. `schemaVersion` itself is
    // UNCHANGED, so the client's existing Test-connection equality check (its
    // vendored copy vs this field) is untouched: a client ON the current version
    // sees no difference; an OLDER client can now read the window and learn its
    // own version is accepted instead of discovering it via a 415. Ascending
    // NUMBERS — the same shape as the receiver-local constant, derived from the
    // SAME accepted-set helper both handshake sites read (never a parallel list).
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
        acceptedSchemaVersions: acceptedSchemaVersionStrings(schema.COMPATIBLE_SCHEMA_VERSIONS, schema.SCHEMA_VERSION).map(Number),
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
    //
    // WARDEN-1445: BOTH sites share ONE accepted set — the current version plus
    // the prior versions proven additive subsets (COMPATIBLE_SCHEMA_VERSIONS,
    // threaded through the schema bundle) — so this copy and the canonical check
    // inside ingest() cannot disagree about who gets in. An exact-string Set
    // (never a Number() coercion) keeps the pre-window strictness: a numeric 8,
    // an "06", an "abc", or a missing header never matched, and still don't. A
    // declared version outside the window still 415s here EXACTLY as before,
    // still recorded at the rejection seam with its declaredVersion, and is
    // still tallied in rejections.byDeclaredVersion — the drift population is
    // unchanged; only the window's members stopped being part of it.
    const acceptedSchemaStrings = acceptedSchemaVersionStrings(schema.COMPATIBLE_SCHEMA_VERSIONS, schema.SCHEMA_VERSION);
    const declaredSchema = readHeader(req.headers, 'x-telemetry-schema');
    if (!new Set(acceptedSchemaStrings).has(declaredSchema)) {
      // ONE reason text, imported from ingest.mjs — the same template literal the
      // canonical check uses, so the two sites cannot drift on wording (a handler
      // test pins the two 415 error texts equal for the same declared value).
      const reason = unsupportedSchemaVersionReason(acceptedSchemaStrings, declaredSchema);
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
      result = await ingest({ headers: req.headers, body }, {
        ...schema,
        // WARDEN-1445: the window rides the schema bundle under its
        // receiver-local constant name (COMPATIBLE_SCHEMA_VERSIONS) but ingest's
        // dep is the descriptive `compatibleSchemaVersions` — mapped HERE, at the
        // one call site, so the canonical check inside ingest() and the pre-read
        // seam above read the SAME definition. An override schema without the
        // constant maps undefined → ingest's current-only default (the strict
        // pre-window handshake those overrides assert).
        compatibleSchemaVersions: schema.COMPATIBLE_SCHEMA_VERSIONS,
        store, seenKeys, now,
      });
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
