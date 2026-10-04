// Receiver tally / dedup layer (WARDEN-1525): the self-contained, pure in-memory
// tally machinery extracted VERBATIM from server.mjs — no HTTP coupling. server.mjs
// imports these and re-exports the previously-public names.
import { DEFAULT_TIMELINE_MAX_BUCKETS, DEFAULT_TIMELINE_WINDOW_MS } from './summary.mjs';

export const DEFAULT_DEDUP_TTL_MS = 10 * 60 * 1000; // 10 min — ~300x the client retry window
export const DEFAULT_DEDUP_MAX_KEYS = 10000; // FIFO cap — mirrors STORE_MAX_EVENTS default
// The persist write is DEBOUNCED so a burst of accepted batches coalesces into one
// flush — the hot accept path pays no synchronous disk write per batch (mirrors the
// retention compaction debounce). Small (1s): the persisted set is fresh to within
// a second, so a restart any time after a burst still reloads those keys. Injectable
// for unit tests via setTimer/clearTimer (like createRetentionTrigger).
export const SEEN_KEYS_PERSIST_DEBOUNCE_MS = 1000;

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

// Sentinel key for the SINGLE overflow bucket in `byDeclaredVersion` (WARDEN-829).
// A client value that literally equals `__overflow__` is indistinguishable from the
// aggregate (benign: same bound, identical count semantics). Chosen to be an
// unlikely real schema version. Module-scoped (WARDEN-1428) because the liveness
// composer must EXCLUDE it when listing mismatched declared versions — the sentinel
// is a cap artifact, not a version a client declared — and a second spelling of the
// key would silently let it leak through as a real version the day either copy moved.
export const REJECTION_OVERFLOW_KEY = '__overflow__';

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
export const EMPTY_REJECTIONS = Object.freeze({
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
  // Sentinel for the single overflow bucket — the module-scoped
  // REJECTION_OVERFLOW_KEY (hoisted by WARDEN-1428 so the /summary liveness
  // composer excludes exactly this key, from one definition).
  const OVERFLOW_KEY = REJECTION_OVERFLOW_KEY;

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
export const EMPTY_PERSIST_ERRORS = Object.freeze({
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
export const EMPTY_RETENTION = Object.freeze({
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
export const EMPTY_DEDUPED = Object.freeze({
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
export const EMPTY_SEEN_KEYS = Object.freeze({
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
