// The maintainer read surface's pure aggregators (WARDEN-567). Sibling of
// `ingest()`: PURE functions of an event array — no fs, no network, no deps.
// They take the events read back via the store and return the AGGREGATE object a
// self-hosting maintainer can act on (counts / histograms only):
//   - `summarize(events)`        → flat aggregates (total / byType / topErrorNames
//                                  / topSignatures / schemaVersions / appVersions
//                                  / platforms / crashReasons / stalls / firstSeen
//                                  / lastSeen).
//   - `summarizeTimeline(events)` → a bounded temporal distribution (event counts
//                                  per time bucket over a rolling recent window,
//                                  WARDEN-603) so a maintainer can distinguish a
//                                  recent volume spike from a long-running baseline.
//   - `summarizeStallsTimeline(events)` → the temporal twin of `stalls` (WARDEN-886):
//                                  a bounded per-bucket `max` lagMs (overall + split
//                                  by `source`) over the SAME rolling window as
//                                  `summarizeTimeline`, so a maintainer can tell an
//                                  ACTIVE freeze regression (the worst freeze in the
//                                  newest bucket — happening NOW) from a RESOLVED
//                                  blip (the same worst freeze hours ago — already
//                                  gone), which the magnitude snapshot `stalls.max`
//                                  provably cannot: it collapses the whole retained
//                                  window into one number with no time axis.
//
// ── TRUST MODEL (do not erode) ────────────────────────────────────────────────
// These return AGGREGATES of events that ALREADY landed — every one was schema-
// validated by `ingest` AND redacted client-side pre-collection before it ever
// reached disk. They introduce NO new data, re-collect nothing, and route to no
// third party (a local read on the self-hosted receiver). Return aggregates only:
// counts, per-type totals, non-identifying error `name`s, a non-identifying
// failure `signature` histogram (error name + top stack frame / crash reason /
// stall source), a schema-version histogram, an app-release `appVersion`
// histogram, an OS `platform` histogram, and a counts-only time distribution.
// NEVER echo raw events or extended-tier
// names (chatName /
// sessionName) — those are the only identifiers ever present, and a summary has
// no need of them.
//
// `appVersions` (WARDEN-665) buckets event counts by the client's non-identifying
// `appVersion` release label — a value identical for every user on a release (not
// an identifier, not content) — so a maintainer can attribute volume to a release.
// It is a COUNTS-only histogram keyed by the release label string; like the other
// aggregates it echoes no raw event and touches no identifier.
//
// `platforms` (WARDEN-684) is the OS sibling of `appVersions`: it buckets event
// counts by the client's non-identifying `platform` OS label (in practice
// `darwin` / `win32` / `linux` from process.platform — a value identical for
// millions of users on an OS, not an identifier, not content) so a maintainer can
// answer "is this crash/error spike Mac / Windows / Linux-specific?" instead of
// staring at un-attributable volume. Same COUNTS-only histogram shape, same trust
// posture, same skip-robust bucketing as `appVersions`.
//
// `byRuntime` (WARDEN-869) is the PROCESS sibling of `appVersions` / `platforms`:
// it buckets event counts by the client's non-identifying `runtime` process label
// (in practice `main` / `renderer` — the Electron/Node main process vs. a web-contents
// renderer, a value identical across millions of users on a process kind, not an
// identifier, not content) so a maintainer can answer "is the app being hard-killed
// by the OS (main) or is React/Electron throwing (renderer)?" — a native segfault /
// OOM-kill / SIGKILL detected on next launch (WARDEN-687) is tagged
// `runtime: 'main'`, and without this axis those severe native kills are
// indistinguishable from renderer crashes in the overview. `runtime` is MANDATORY
// on every receiver-accepted event (unlike the OPTIONAL `appVersion` / `platform`),
// so this is the only counts-only histogram keyed by a field that always reaches
// disk — same COUNTS-only shape, same trust posture, same skip-robust bucketing as
// `appVersions` / `platforms`.
//
// `crashReasons` (WARDEN-872) is the crash-CAUSE axis `byType.crash` (a bare count)
// and `topSignatures` (capped, exitCode-split, ranked) both obscure: it buckets
// crash counts by the client's `reason` string (in practice Electron's small
// enum — `oom` / `crashed` / `killed` … — plus the main-process
// `'unexpected-termination'` sentinel, WARDEN-687; NOTE the validator only
// type-checks `reason`, it is NOT a fixed enumeration — hence the WARDEN-1246
// bound on this histogram) so a maintainer can answer "of this crash spike, how much
// is OOM?" instead of staring at an un-attributable count. `topSignatures` already
// folds `reason` into its key (as `crash:${reason}:exit=${exitCode}`), but it is
// (1) capped at TOP_SIGNATURES_CAP across ALL types, (2) split by exitCode so the
// marginal "total OOM crashes" is NOT derivable, and (3) a ranked list, not a
// complete distribution — this histogram closes that blind spot. Same COUNTS-only
// histogram shape, same trust posture, same skip-robust bucketing as `platforms` /
// `appVersions`: it echoes no raw event and touches no identifier (`reason` is a
// redaction no-op, the same tier as `platform` / `appVersion`). The invariant is
// `sum(crashReasons.values()) ≤ byType.crash`, with equality iff every crash
// carries a present non-empty `reason` — a reasonless crash is counted by
// `byType.crash` but NOT bucketed here (skip-robust, never a junk bucket).
//
// `topSignatures` (WARDEN-707) is the failure axis `topErrorNames` cannot show:
// `topErrorNames` groups by `Error#name` ONLY, so `TypeError: 847` is unreadable
// (one regression × 847, or 847 distinct bugs?). `topSignatures` ranks DISTINCT
// failures via a per-type `signatureOf(event)` derivation built ONLY from
// schema-deemed-non-identifying structured fields — error `name` + the FIRST
// stack frame's `function`/`file`/`line`, crash `reason`+`exitCode`, stall
// `source`. It is a counts-only histogram of those bucket keys, identical in
// posture to `topErrorNames`/`appVersions`/`platforms`: it incorporates NO free
// text and NO extended-tier identifier. It MUST NOT read `message` (redacted free
// text — the field most likely to carry residual identifying fragments) nor
// `chatName`/`sessionName` (the only identifiers ever present); an error whose
// frames are empty / lack the location fields degrades to the bare `name` (exactly
// the `topErrorNames` bucket), so nothing regresses.
//
// `stalls` (WARDEN-854) is the MAGNITUDE axis the stall COUNT (`byType` /
// `topSignatures`) cannot show: 500 × 50ms micro-hitches and 500 × 5s hard freezes
// read byte-identically on every other surface. It captures the `lagMs`
// distribution (min / avg / max — the REAL user-perceived freeze duration the
// client already populates end-to-end) of `performance-stall` events, split by
// `source` so a maintainer can tell event-loop jank (`'event-loop'`) from renderer
// hangs (`'unresponsive'`); `max` is the headline (the worst freeze a user actually
// felt, not buried in the average). `count` is ALL `performance-stall` events (it
// MUST equal `byType['performance-stall']` so the magnitude surface and the count
// surface agree); `min`/`avg`/`max` are computed over the FINITE-`lagMs` subset.
// `lagMs` is a non-identifying magnitude (an epoch-ms-free integer ≥ 0) already
// enumerated in the consent / verifiability surface, so this introduces NO new
// collection, wire field, or schema bump — a pure read-side aggregate over
// already-accepted events, identical in posture to `appVersions`/`platforms`. A
// non-finite `lagMs` (NaN / Infinity — which `validateBaseEvent` does NOT reject,
// since `typeof NaN === 'number'`) is SKIPPED from `min`/`avg`/`max` but the event
// is STILL counted, so one bad record can never poison the whole aggregate to
// NaN/Infinity (the failure `summarize()` documents it defends against).
//
// `summarizeStallsTimeline` (WARDEN-886) is the TEMPORAL twin of that `stalls`
// snapshot — the last magnitude surface without a time axis. `stalls.max` collapses
// the whole retained window into ONE number; `stalls.lastSeen` keys off the last
// stall ARRIVAL (any stall, even a 50ms hitch), not the worst freeze's time. So a
// 5s freeze that landed 5 minutes ago (an ACTIVE regression — users feeling it now)
// and a 5s freeze that landed 5 hours ago (a RESOLVED blip — already gone) read
// byte-identical on `stalls`. `summarizeStallsTimeline` answers the maintainer's
// first question on a bad `stalls.max` — "is this still happening?" — with a
// per-bucket `max` lagMs (the worst freeze in each bucket) over the SAME rolling
// window / granularity as `summarizeTimeline` (the two SHARE the pure bucket-
// assignment helper, so they can never drift). Each bucket carries `count` (ALL
// stalls in the bucket, incl. non-finite `lagMs` — parity with `stalls.count`) and
// `max` (the worst FINITE `lagMs` in the bucket, `null` if none finite — THE
// headline), split by `source` (`bySource`, mirroring `stalls.bySource`). The
// WARDEN-854 `Number.isFinite(lagMs)` guard is load-bearing here too: a non-finite
// / absent `lagMs` is skipped from the bucket's `max` but the stall is still
// COUNTED. Identical trust posture to `stalls` + `summarizeTimeline` — a pure read
// over already-accepted, already-redacted events; NO new collection, wire field,
// schema bump, or identifier. A stall-free store reads a clean zeroed
// `{ buckets: [], bucketMs }` (no false alarm), always present and additive.

import { BASE_EVENT_TYPES } from './schema.ts';

// Cap the top-error-names list so a runaway variety of names stays readable.
const TOP_ERROR_NAMES_CAP = 10;

// Cap the top-signatures list at the same bound so a wide variety of distinct
// failures stays readable on the summary surface (mirrors TOP_ERROR_NAMES_CAP).
const TOP_SIGNATURES_CAP = 10;

// ── CLIENT-KEYED HISTOGRAM BOUNDS (WARDEN-1246) ───────────────────────────────
// The client-keyed histograms (`appVersions` / `platforms` / `byRuntime` /
// `crashReasons`) bucket FREE client-supplied strings — the validator only
// type-checks these fields, so a single accepted event can carry a multi-KB
// `reason` / `platform` / `appVersion` / `runtime`, and a hostile or buggy
// client can emit unlimited distinct values. Without a bound, one oversized or
// high-cardinality value is retained and then reproduced in full inside EVERY
// subsequent /summary response — a permanent response-amplification hole.
// Two bounds close it:
//   1. KEY LENGTH — a longer client value is TRUNCATED to CLIENT_KEY_MAX_LENGTH
//      chars before bucketing (two distinct long values sharing a prefix
//      collide into one bucket: acceptable, and honest — the response is bounded).
//   2. CARDINALITY — the first CLIENT_HISTOGRAM_CAP distinct keys get their own
//      bucket; every FURTHER distinct key folds into ONE counted `__overflow__`
//      bucket, reusing the exact top-N + overflow shape createRejectionTally
//      established on the ingest side (WARDEN-829) so the two surfaces stay
//      consistent. Overflow is REPRESENTED, never dropped.
export const CLIENT_KEY_MAX_LENGTH = 128;
export const CLIENT_HISTOGRAM_CAP = 10; // mirrors TOP_ERROR_NAMES_CAP / TOP_SIGNATURES_CAP

// Sentinel for the single overflow bucket (same literal as server.mjs's
// createRejectionTally, WARDEN-829 — one shared shape across both surfaces). A
// client value that literally equals `__overflow__` is indistinguishable from
// the aggregate (benign: same bound, same count semantics).
const OVERFLOW_KEY = '__overflow__';

/**
 * Bound a client-supplied histogram key: truncate to CLIENT_KEY_MAX_LENGTH so
 * one oversized value can never inflate every summary response. Pure.
 *
 * @param {string} key
 * @returns {string}
 * @private
 */
function _boundClientKey(key) {
  return key.length > CLIENT_KEY_MAX_LENGTH ? key.slice(0, CLIENT_KEY_MAX_LENGTH) : key;
}

/**
 * Create a bounded COUNTS-only histogram accumulator for a client-keyed axis
 * (WARDEN-1246): key-length truncation (via _boundClientKey) + top-N +
 * `__overflow__` cardinality cap, the same shape as createRejectionTally's
 * `byDeclaredVersion` (server.mjs, WARDEN-829). `snapshot()` returns a plain
 * `{ [key]: count }` object holding ≤ CLIENT_HISTOGRAM_CAP + 1 keys no matter
 * what any client sent. hasOwnProperty (not `in`) keeps attacker keys like
 * "toString" / "constructor" bucketing as ordinary own keys.
 *
 * @returns {{ record(value: string): void, snapshot(): Record<string, number> }}
 * @private
 */
function _createBoundedClientHistogram() {
  const counts = {};
  let distinct = 0;
  return {
    record(value) {
      const key = _boundClientKey(value);
      if (Object.prototype.hasOwnProperty.call(counts, key)) {
        counts[key] += 1; // an already-tracked distinct key bumps its own bucket
      } else if (distinct < CLIENT_HISTOGRAM_CAP) {
        counts[key] = 1; // new distinct key under the cap → its own bucket
        distinct += 1;
      } else {
        // Cap reached → fold this and every further NEW distinct key into the
        // single overflow bucket: bounded cardinality, no count loss.
        counts[OVERFLOW_KEY] = (counts[OVERFLOW_KEY] ?? 0) + 1;
      }
    },
    snapshot() {
      return { ...counts };
    },
  };
}

// ── TEMPORAL DISTRIBUTION config (WARDEN-603) ────────────────────────────────
// The rolling recent window a maintainer reads to spot a RECENT volume spike
// (a regression / deploy event) apart from long-running baseline. Events older
// than the window are excluded from the distribution but STILL counted in
// `summarize()`'s `total` / `byType` / `firstSeen` / `lastSeen` — the
// distribution shows recent SHAPE, the totals show the full retained set.
// 24h is the "did this JUST spike?" recency horizon.
export const DEFAULT_TIMELINE_WINDOW_MS = 24 * 60 * 60 * 1000; // 24h
// Cap the bucket count so a wide window can never yield a huge array: 48 buckets
// over 24h = 30-min granularity (readable on a summary surface). A 10k-event
// store spanning months collapses to at most this many slots, never one-per-event.
export const DEFAULT_TIMELINE_MAX_BUCKETS = 48;

/**
 * Derive a deterministic, NON-IDENTIFYING failure `signature` for an event, by
 * `type`, so `topSignatures` can rank DISTINCT failures (one regression × N vs N
 * distinct bugs) — the axis `topErrorNames` (Error#name only) cannot show.
 *
 * Pure and total: a non-object, or an event whose type yields no signature (an
 * unknown type, an error with no `name`, a crash with no `reason`, a stall with
 * no `source`), returns `null` — skipped by the aggregator, never fatal.
 *
 * TRUST MODEL (load-bearing for roadmap WARDEN-446 — do not erode): built ONLY
 * from schema-deemed-non-identifying structured fields. It MUST NOT incorporate
 * `message` (redacted free text — the field most likely to carry residual
 * identifying fragments, schema.ts) nor any extended-tier identifier
 * (`chatName` / `sessionName`). It reads at most: error `name` + the FIRST stack
 * frame's `function`/`file`/`line` (frames[0] — the top of the stack, closest to
 * where it threw; there is no "in-app" marker in `StackFrame`); crash `reason`
 * (in practice Electron's small enum — `oom`/`crashed`/`killed`… — not
 * identifying; the validator only type-checks it, not a fixed enumeration) +
 * optional `exitCode`; stall `source` (in practice `'event-loop'` /
 * `'unresponsive'`; the validator only type-checks it).
 *
 * An error with empty `frames`, or whose `frames[0]` lacks ALL of
 * `function`/`file`/`line`, degrades to the bare `name` — exactly today's
 * `topErrorNames` bucket — so this is a graceful superset and nothing regresses.
 *
 * @param {unknown} event
 * @returns {string | null} the signature, or `null` if the event yields none
 */
export function signatureOf(event) {
  if (!event || typeof event !== 'object') return null;
  const e = event;
  const type = e.type;

  if (type === 'error') {
    const name = e.name;
    // A nameless error yields no signature (it would also be skipped by
    // topErrorNames); skip rather than emit a junk key.
    if (typeof name !== 'string' || name.length === 0) return null;
    const seg = _frameSegment(Array.isArray(e.frames) ? e.frames[0] : undefined);
    return seg === null ? name : `${name}${seg}`;
  }

  if (type === 'crash') {
    const reason = e.reason;
    if (typeof reason !== 'string' || reason.length === 0) return null;
    const exitCode = e.exitCode;
    // Omit the `:exit=N` segment when exitCode is absent (an optional field).
    if (typeof exitCode === 'number' && Number.isFinite(exitCode)) {
      return `crash:${reason}:exit=${exitCode}`;
    }
    return `crash:${reason}`;
  }

  if (type === 'performance-stall') {
    const source = e.source;
    if (typeof source !== 'string' || source.length === 0) return null;
    return `stall:${source}`;
  }

  return null;
}

/**
 * Render the FIRST stack frame as a readable, non-identifying ` @ …` suffix
 * (e.g. ` @ App.tsx:142 (renderChat)`) for an error signature. Returns `null`
 * when the frame carries none of `function`/`file`/`line` so the caller degrades
 * to the bare error `name` (the `topErrorNames` bucket). Reads no other field.
 *
 * @param {unknown} frame
 * @returns {string | null}
 * @private
 */
function _frameSegment(frame) {
  if (!frame || typeof frame !== 'object') return null;
  const f = frame;
  const hasFile = typeof f.file === 'string' && f.file.length > 0;
  const hasFn = typeof f.function === 'string' && f.function.length > 0;
  const hasLine = typeof f.line === 'number' && Number.isFinite(f.line);
  // Identifying only if it carries at least one location/symbol field.
  if (!hasFile && !hasFn && !hasLine) return null;
  let loc = '';
  if (hasFile) {
    loc = hasLine ? `${f.file}:${f.line}` : f.file;
  } else if (hasLine) {
    loc = `:${f.line}`;
  }
  const fn = hasFn ? (loc ? ` (${f.function})` : `(${f.function})`) : '';
  return ` @ ${loc}${fn}`;
}

/**
 * Render a stall-severity accumulator (overall OR per-source) as the public
 * `{ count, min, avg, max }` snapshot (WARDEN-854). `count` is EVERY stall in the
 * accumulator (it underpins the `count === byType['performance-stall']` invariant);
 * `min`/`avg`/`max` reflect ONLY the finite-`lagMs` subset. With no finite record
 * seen, `min`/`max` are `null` (mirrors `firstSeen`/`lastSeen`; `lagMs ≥ 0` makes
 * `0` an ambiguous empty sentinel) and `avg` is `0` (the guarded `sum / count`,
 * so the empty case can never read as `NaN`).
 *
 * @param {{ count: number, sum: number, finiteCount: number, min: number | null, max: number | null }} acc
 * @returns {{ count: number, min: number | null, avg: number, max: number | null }}
 * @private
 */
function _stallSnapshot({ count, sum, finiteCount, min, max }) {
  return {
    count,
    min: finiteCount > 0 ? min : null,
    avg: finiteCount > 0 ? sum / finiteCount : 0,
    max: finiteCount > 0 ? max : null,
  };
}

/**
 * Summarize a batch of persisted telemetry events into aggregate signal.
 *
 * Pure and total: a non-array (or empty) input yields a fully-zeroed summary so
 * the empty-store case is a normal 200, never an error. Malformed entries
 * (null / primitives / non-objects) are SKIPPED, not fatal — in practice every
 * persisted event is JSON-validated first, but a partial read or shape drift is
 * defended against here so one bad record can never blank the whole summary.
 *
 * @param {object[]} [events]
 * @returns {{
 *   total: number,
 *   byType: Record<string, number>,
 *   topErrorNames: { name: string, count: number }[],
 *   topSignatures: { signature: string, type: BaseEventType, count: number }[],
 *   schemaVersions: Record<string, number>,
 *   appVersions: Record<string, number>,
 *   platforms: Record<string, number>,
 *   byRuntime: Record<string, number>,
 *   crashReasons: Record<string, number>,
 *   stalls: { count: number, min: number | null, avg: number, max: number | null,
 *             bySource: Record<string, { count: number, min: number | null, avg: number, max: number | null }> },
 *   firstSeen: number | null,
 *   lastSeen: number | null,
 * }}
 */
export function summarize(events) {
  const list = Array.isArray(events) ? events : [];

  // byType is pre-zeroed over BASE_EVENT_TYPES so the shape is stable — a
  // maintainer always sees every base type key, even when its count is 0.
  const byType = {};
  for (const t of BASE_EVENT_TYPES) byType[t] = 0;

  const errorNameCounts = {};
  const schemaVersions = {};
  // Client-keyed histograms (WARDEN-1246): appVersions / platforms / byRuntime /
  // crashReasons are all keyed by FREE client-supplied strings, so they go
  // through the bounded accumulator (key-length truncation + top-N + overflow)
  // — no single accepted event can permanently inflate every summary response.
  const appVersions = _createBoundedClientHistogram();
  const platforms = _createBoundedClientHistogram();
  // runtime process label histogram (WARDEN-869) — the PROCESS sibling of
  // `appVersions` / `platforms`. `runtime` is mandatory on valid events, but the
  // validator only type-checks it (NOT a fixed enumeration), so it is bounded
  // exactly like its siblings; the skip-robust guard still applies to a
  // malformed / partial-read entry.
  const byRuntime = _createBoundedClientHistogram();
  // Crash-CAUSE histogram (WARDEN-872): buckets crash counts by the client's
  // `reason` string, mirroring platforms/appVersions — and bounded by them
  // (WARDEN-1246), since `reason` is free client text (type-checked only), NOT
  // a fixed enum. Skip-robust — a reasonless crash is counted by byType.crash
  // but NOT bucketed here.
  const crashReasons = _createBoundedClientHistogram();
  // Stall-severity accumulators (WARDEN-854): the `lagMs` magnitude distribution of
  // performance-stall events, overall + per-source. `stallMin`/`stallMax` are null
  // until the first FINITE lagMs is seen (mirrors firstSeen/lastSeen's null-until-
  // seen shape; lagMs ≥ 0 per schema, so 0 would be an ambiguous empty sentinel).
  let stallCount = 0;
  let stallSum = 0;
  let stallFiniteCount = 0;
  let stallMin = null;
  let stallMax = null;
  const stallBySource = new Map();
  // Failure signatures (WARDEN-707). Keyed by `${type} ${signature}` so two
  // events of different types can NEVER collide into one bucket even if their
  // signature strings happened to match (defensive — in practice each type's
  // signature lives in its own namespace). Value carries the bare signature + the
  // event type for the ranked output.
  const signatureCounts = new Map();
  let total = 0;
  let firstSeen = null;
  let lastSeen = null;

  for (const event of list) {
    // Skip-robust: a non-object entry (null / primitive / a partial parse) must
    // not crash the summary — skip it and keep aggregating the good records.
    if (!event || typeof event !== 'object') continue;
    total += 1;

    const { type, name, schemaVersion, timestamp, appVersion, platform, runtime, reason, lagMs, source } = event;

    if (typeof type === 'string' && Object.prototype.hasOwnProperty.call(byType, type)) {
      byType[type] += 1;
    }
    // Error `name` (e.g. 'TypeError') is non-identifying by design (schema.ts).
    if (type === 'error' && typeof name === 'string' && name.length > 0) {
      errorNameCounts[name] = (errorNameCounts[name] ?? 0) + 1;
    }
    if (schemaVersion !== undefined && schemaVersion !== null) {
      const key = String(schemaVersion);
      schemaVersions[key] = (schemaVersions[key] ?? 0) + 1;
    }
    // appVersion release label (WARDEN-665). Skip-robust like schemaVersions: only
    // bucket a PRESENT, non-empty string — absent / null / non-string / empty is
    // ignored (a v2 source that cannot read the version emits no field), so a
    // malformed value never crashes or produces a junk bucket.
    if (typeof appVersion === 'string' && appVersion.length > 0) {
      appVersions.record(appVersion);
    }
    // platform OS label (WARDEN-684). Skip-robust exactly like appVersions: only
    // bucket a PRESENT, non-empty string — absent / null / non-string / empty is
    // ignored (a v3 source that cannot read process.platform emits no field), so a
    // malformed value never crashes or produces a junk bucket.
    if (typeof platform === 'string' && platform.length > 0) {
      platforms.record(platform);
    }
    // runtime process label (WARDEN-869). Skip-robust exactly like platforms: only
    // bucket a PRESENT non-empty string — absent / null / non-string / empty is
    // ignored, so a malformed value never crashes or produces a junk bucket.
    if (typeof runtime === 'string' && runtime.length > 0) {
      byRuntime.record(runtime);
    }
    // crash reason (WARDEN-872). Skip-robust exactly like platforms/appVersions:
    // only bucket a PRESENT, non-empty string — absent / null / non-string / empty
    // is ignored (a malformed value never crashes or produces a junk bucket). A
    // reasonless crash is still counted by `byType.crash`; it just yields no bucket
    // here, so `sum(crashReasons.values()) ≤ byType.crash` (equality iff every
    // crash carries a present non-empty `reason`).
    if (type === 'crash' && typeof reason === 'string' && reason.length > 0) {
      crashReasons.record(reason);
    }
    // Stall MAGNITUDE aggregate (WARDEN-854): the `lagMs` distribution the stall
    // COUNT (byType / topSignatures) discards. `count` is EVERY performance-stall
    // event (it MUST equal byType['performance-stall'] so the magnitude + count
    // surfaces agree); min/avg/max are computed over the FINITE-`lagMs` subset.
    // The Number.isFinite guard is load-bearing: validateBaseEvent only
    // typeof-checks lagMs (schema.ts), so NaN / Infinity can reach here — an
    // unguarded Math.min/max or running average would poison the whole aggregate
    // from one bad record. A non-finite / absent lagMs is skipped from the stats
    // but the event is STILL counted. Split by `source` (a PRESENT non-empty
    // string, matching signatureOf's stall rule) so event-loop jank is
    // distinguishable from renderer hangs; a sourceless stall is counted overall
    // but not bucketed in bySource.
    if (type === 'performance-stall') {
      stallCount += 1;
      const finiteLag = typeof lagMs === 'number' && Number.isFinite(lagMs);
      if (finiteLag) {
        stallFiniteCount += 1;
        stallSum += lagMs;
        if (stallMin === null || lagMs < stallMin) stallMin = lagMs;
        if (stallMax === null || lagMs > stallMax) stallMax = lagMs;
      }
      // `source` is client-supplied free text (type-checked only, NOT a fixed
      // enum), so its key is length-bounded and its cardinality capped
      // top-N + `__overflow__` exactly like the client-keyed histograms
      // (WARDEN-1246) — an overflow stall-source folds into a SHARED
      // `__overflow__` accumulator of the same shape (merged counts / stats,
      // never dropped).
      if (typeof source === 'string' && source.length > 0) {
        const srcKey = _boundClientKey(source);
        let acc;
        if (stallBySource.has(srcKey)) {
          acc = stallBySource.get(srcKey);
        } else if (stallBySource.size < CLIENT_HISTOGRAM_CAP) {
          acc = { count: 0, sum: 0, finiteCount: 0, min: null, max: null };
          stallBySource.set(srcKey, acc);
        } else {
          acc = stallBySource.get(OVERFLOW_KEY) ?? { count: 0, sum: 0, finiteCount: 0, min: null, max: null };
          stallBySource.set(OVERFLOW_KEY, acc);
        }
        acc.count += 1;
        if (finiteLag) {
          acc.finiteCount += 1;
          acc.sum += lagMs;
          if (acc.min === null || lagMs < acc.min) acc.min = lagMs;
          if (acc.max === null || lagMs > acc.max) acc.max = lagMs;
        }
      }
    }
    // Failure signature (WARDEN-707): rank DISTINCT failures across ALL base
    // types in one list. `signatureOf` is skip-robust (returns null for an
    // unknown type or a type-specific field gap) — null yields no bucket, never
    // throws. The composite key guarantees no cross-type merge.
    const signature = signatureOf(event);
    if (signature !== null) {
      const key = `${type} ${signature}`;
      const existing = signatureCounts.get(key);
      if (existing) existing.count += 1;
      else signatureCounts.set(key, { signature, type, count: 1 });
    }
    // Time bounds key off the RECEIVER's `receivedAt` (when IT saw the batch,
    // WARDEN-692) and fall back to the client's `timestamp` only when
    // `receivedAt` is absent — so a skewed client clock can no longer push
    // `lastSeen` into the future (or drag `firstSeen` into the past), exactly
    // the skew-robustness the timeline / retention / ?since surfaces already
    // have (summarizeTimeline / store applyRetention / selectEvents). Old
    // persisted events (pre-annotation, no receivedAt) still read via the
    // fallback, so nothing regresses and no migration is needed.
    const when = event.receivedAt ?? timestamp;
    if (typeof when === 'number' && Number.isFinite(when)) {
      if (firstSeen === null || when < firstSeen) firstSeen = when;
      if (lastSeen === null || when > lastSeen) lastSeen = when;
    }
  }

  // Sort by count desc, then name asc for a deterministic order on ties (so the
  // aggregate is stable across reads and trivially assertable in tests).
  const topErrorNames = Object.entries(errorNameCounts)
    .map(([errorName, count]) => ({ name: errorName, count }))
    .sort((a, b) => b.count - a.count || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
    .slice(0, TOP_ERROR_NAMES_CAP)
    // Key-length bound (WARDEN-1246): the list is capped in COUNT but its keys
    // are client-supplied free text, so truncate each at read time — ten
    // multi-KB names would still inflate the response.
    .map(({ name, count }) => ({ name: _boundClientKey(name), count }));

  // Rank DISTINCT failures by count desc, then signature asc for a deterministic
  // order on ties — mirrors topErrorNames so the aggregate is stable and trivially
  // assertable. Each entry carries its `type` so a maintainer can read a mixed
  // error/crash/stall ranking in one list.
  const topSignatures = [...signatureCounts.values()]
    .sort(
      (a, b) =>
        b.count - a.count ||
        (a.signature < b.signature ? -1 : a.signature > b.signature ? 1 : 0)
    )
    .slice(0, TOP_SIGNATURES_CAP)
    // Key-length bound (WARDEN-1246): a signature folds the crash `reason` (free
    // client text, type-checked only) into its key, so truncate at read time —
    // the list is capped in COUNT, not in key length, otherwise.
    .map(({ signature, type, count }) => ({ signature: _boundClientKey(signature), type, count }));

  // Stall-severity rollup (WARDEN-854): the overall magnitude snapshot plus the
  // per-source breakdown (insertion order — a maintainer reads the sources in the
  // order they first appeared; deepEqual is order-insensitive, so tests are stable).
  const stalls = {
    ..._stallSnapshot({ count: stallCount, sum: stallSum, finiteCount: stallFiniteCount, min: stallMin, max: stallMax }),
    bySource: Object.fromEntries(
      [...stallBySource.entries()].map(([source, acc]) => [source, _stallSnapshot(acc)])
    ),
  };

  return {
    total,
    byType,
    topErrorNames,
    topSignatures,
    schemaVersions,
    appVersions: appVersions.snapshot(),
    platforms: platforms.snapshot(),
    byRuntime: byRuntime.snapshot(),
    crashReasons: crashReasons.snapshot(),
    stalls,
    firstSeen,
    lastSeen,
  };
}

/**
 * Resolve the rolling-window grid and assign each event to a bucket index over it.
 * PURE — the shared bucket-assignment machinery consumed by BOTH `summarizeTimeline`
 * (a COUNT over ALL events) and `summarizeStallsTimeline` (per-bucket stall
 * SEVERITY), so the two can NEVER drift on window, granularity, or bucket
 * boundary: the effective-time resolution, the window math, and the index clamp +
 * top-boundary fold live HERE, computed once (WARDEN-886).
 *
 * Effective time PREFERS the receiver's `receivedAt` (when IT saw the batch,
 * WARDEN-692) and falls back to the client's `timestamp` — so a skewed client clock
 * can no longer push an event out of the "did this just spike?" window. An event
 * whose effective time is non-finite or outside `[windowStart, currentTime]` is
 * excluded from the distribution (still counted by `summarize()`'s totals, which
 * span the full retained set). A non-object entry is skipped, never fatal.
 *
 * Returns `null` on degenerate config (a non-positive / non-finite `windowMs` or
 * `maxBuckets`) so the caller collapses to its zeroed shape — a malformed knob can
 * never yield a huge / NaN array.
 *
 * @param {unknown} events
 * @param {{ now: () => number, maxBuckets: number, windowMs: number }} opts
 * @returns {{ windowStart: number, bucketMs: number, slots: Map<number, object[]> } | null}
 *   `slots` maps a bucket index → the in-window events that landed in it.
 * @private
 */
function _assignTimelineBuckets(events, { now, maxBuckets, windowMs }) {
  // Degenerate config → null. The defaults are always valid; this only fires on an
  // explicit bad override, and a malformed knob can never yield a huge/NaN array —
  // it collapses to empty (mirrors summarize()'s defensive totality).
  if (!Number.isFinite(windowMs) || windowMs <= 0 || !Number.isFinite(maxBuckets) || maxBuckets < 1) {
    return null;
  }

  const list = Array.isArray(events) ? events : [];
  const currentTime = now();
  const bucketMs = windowMs / maxBuckets;
  const windowStart = currentTime - windowMs;

  // Accumulate the events per bucket index over events whose FINITE effective time
  // falls in the rolling window [windowStart, currentTime]. Returning the EVENTS
  // (not a pre-counted number) lets each consumer derive its own per-bucket shape
  // (COUNT for summarizeTimeline, max + bySource for the stall timeline) off the
  // SAME assignment. The bucket count is structurally capped at `maxBuckets`:
  // every in-window event maps to one of at most `maxBuckets` grid slots.
  const slots = new Map();
  for (const event of list) {
    // Skip-robust: a non-object entry must not crash the distribution.
    if (!event || typeof event !== 'object') continue;
    const when = event.receivedAt ?? event.timestamp;
    if (typeof when !== 'number' || !Number.isFinite(when)) continue;
    if (when < windowStart || when > currentTime) continue;
    let idx = Math.floor((when - windowStart) / bucketMs);
    // The `when === currentTime` edge lands exactly on the top boundary; fold it
    // into the newest bucket rather than dropping it or overflowing.
    if (idx >= maxBuckets) idx = maxBuckets - 1;
    if (idx < 0) idx = 0;
    const slot = slots.get(idx);
    if (slot) slot.push(event);
    else slots.set(idx, [event]);
  }

  return { windowStart, bucketMs, slots };
}

/**
 * Summarize a batch of persisted telemetry events into a BOUNDED temporal
 * distribution — event counts per time bucket over a rolling recent window —
 * so a maintainer reading `/summary` can distinguish a recent volume spike
 * (a regression / deploy event) from a long-running baseline (WARDEN-603).
 *
 * Sibling of `summarize()`: a PURE function of an event array + an injected
 * `now` (no fs, no network, no deps). The injected `now` mirrors
 * `createRejectionTally({ now })` so this is unit-testable with a fake clock —
 * no real `Date` in tests. Like `summarize()`, it is computed on-the-fly from the
 * `receivedAt`/`timestamp` on ALREADY-persisted, ALREADY-redacted events; it
 * introduces no new collection, no schema change, and no new identifier.
 *
 * Pure and total: a non-array (or empty) input, or a store with no events in
 * the window, yields a zeroed shape (`buckets: []`) so a quiet receiver reads
 * cleanly — no false alarm, mirroring `byType`'s stable empty shape. Malformed
 * entries (null / primitives / non-objects) and non-finite / out-of-window
 * timestamps are SKIPPED, not fatal — in practice every persisted event is
 * JSON-validated first, but a partial read or shape drift is defended against
 * here so one bad record can never blank the whole distribution.
 *
 * TRUST MODEL: identical to `summarize()` — this reads ONLY `event.receivedAt`
 * / `event.timestamp` (both epoch-ms) and emits COUNTS. It never echoes raw
 * events or extended-tier names (`chatName` / `sessionName`); it touches no other
 * field, so there is no path by which an identifier could reach the distribution.
 *
 * @param {object[]} [events]
 * @param {{ now?: () => number, maxBuckets?: number, windowMs?: number }} [opts]
 * @returns {{
 *   buckets: { bucketStart: number, bucketEnd: number, count: number }[],
 *   bucketMs: number,
 * }}
 */
export function summarizeTimeline(
  events,
  {
    now = Date.now,
    maxBuckets = DEFAULT_TIMELINE_MAX_BUCKETS,
    windowMs = DEFAULT_TIMELINE_WINDOW_MS,
  } = {}
) {
  // Share the pure bucket-assignment math with `summarizeStallsTimeline` so the two
  // can never drift on window, granularity, or bucket boundary (WARDEN-886). A null
  // grid = degenerate config → zeroed shape.
  const grid = _assignTimelineBuckets(events, { now, maxBuckets, windowMs });
  if (!grid) return { buckets: [], bucketMs: 0 };
  const { windowStart, bucketMs, slots } = grid;

  // Emit the non-empty buckets chronologically (oldest → newest). Each is
  // self-locating in time (`bucketStart` / `bucketEnd` epoch-ms) + a count. The
  // bucket count is structurally capped at `maxBuckets`: every in-window event
  // maps to one of at most `maxBuckets` grid slots, so a 10k-event store yields
  // ≤ maxBuckets buckets, never one-per-event. `bucketMs` is always present so
  // the shape is stable (and the granularity legible) even when no bucket fired.
  const buckets = [...slots.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([idx, evs]) => {
      const bucketStart = windowStart + idx * bucketMs;
      return { bucketStart, bucketEnd: bucketStart + bucketMs, count: evs.length };
    });

  return { buckets, bucketMs };
}

/**
 * Summarize a batch of persisted telemetry events into a BOUNDED **stall-severity**
 * temporal distribution — the worst (`max`) freeze `lagMs` per time bucket over a
 * rolling recent window, overall and split by `source` (WARDEN-886). This is the
 * TEMPORAL twin of the `stalls` magnitude snapshot: `stalls.max` collapses the
 * whole retained window into one number, so a 5s freeze that landed minutes ago
 * (an ACTIVE regression) reads byte-identical to one that landed hours ago (a
 * RESOLVED blip). The per-bucket `max` answers the maintainer's first question on
 * a bad `stalls.max` — "is this still happening?" — by placing the worst freeze in
 * TIME: the worst freeze in the NEWEST bucket is happening now, the worst freeze
 * in an older bucket has passed.
 *
 * Sibling of `summarizeTimeline`: a PURE function of an event array + an injected
 * `now` (no fs, no network, no deps), and it SHARES `_assignTimelineBuckets` with
 * `summarizeTimeline` so the two use the SAME rolling window / granularity / bucket
 * boundaries and can never drift. It is computed on-the-fly from the
 * `receivedAt`/`timestamp` + `lagMs`/`source` on ALREADY-persisted, ALREADY-redacted
 * `performance-stall` events; it introduces no new collection, no schema change, and
 * no new identifier.
 *
 * Per-bucket shape (timeline-scoped mirror of `_stallSnapshot` / `stalls`):
 *   - `count`  — EVERY stall in the bucket (incl. non-finite `lagMs` — parity with
 *                `stalls.count`, so the finite-skip guard is visible and the surface
 *                pairs with the COUNT `timeline`).
 *   - `max`    — the worst FINITE `lagMs` in the bucket, `null` if none finite
 *                (THE headline — the worst freeze a user felt in that bucket).
 *   - `bySource` — `{ [source]: { count, max } }`, mirroring `stalls.bySource` so a
 *                maintainer can tell event-loop jank from renderer hangs per bucket.
 *
 * Pure and total: a non-array (or empty) input, a stall-free store, or a store with
 * no stalls in the window yields a zeroed shape (`buckets: []`) so a quiet receiver
 * reads cleanly — no false alarm, parity with `summarizeTimeline`'s empty shape.
 * Malformed entries (null / primitives / non-objects) and non-finite / absent /
 * out-of-window timestamps are SKIPPED, not fatal.
 *
 * The WARDEN-854 `Number.isFinite(lagMs)` guard is load-bearing here: `validateBaseEvent`
 * only `typeof`-checks `lagMs` (schema.ts), so NaN / Infinity can reach here — an
 * unguarded per-bucket `Math.max` would poison the bucket. A non-finite / absent
 * `lagMs` is SKIPPED from the bucket's `max` but the stall is STILL counted (mirror
 * of `summarize()`'s 333-356 stall guard). A sourceless stall is counted + feeds the
 * overall `max` but yields no `bySource` entry (mirror of `signatureOf`'s stall rule).
 *
 * TRUST MODEL: identical to `stalls` + `summarizeTimeline` — `lagMs` is a
 * non-identifying magnitude (an epoch-ms-free integer ≥ 0) and `source` is a fixed
 * enum, both already enumerated in the consent / verifiability surface. This reads
 * ONLY `receivedAt` / `timestamp` / `lagMs` / `source` and emits per-bucket counts +
 * maxes; it never echoes raw events or extended-tier names (`chatName` /
 * `sessionName`), so there is no path by which an identifier could reach a bucket.
 *
 * @param {object[]} [events]
 * @param {{ now?: () => number, maxBuckets?: number, windowMs?: number }} [opts]
 * @returns {{
 *   buckets: {
 *     bucketStart: number, bucketEnd: number, count: number, max: number | null,
 *     bySource: Record<string, { count: number, max: number | null }>,
 *   }[],
 *   bucketMs: number,
 * }}
 */
export function summarizeStallsTimeline(
  events,
  {
    now = Date.now,
    maxBuckets = DEFAULT_TIMELINE_MAX_BUCKETS,
    windowMs = DEFAULT_TIMELINE_WINDOW_MS,
  } = {}
) {
  // Pre-filter to `performance-stall` events — the severity timeline reads ONLY the
  // stall `lagMs` / `source` (parity with the `stalls` snapshot). Non-stall events
  // (errors / crashes sharing the filtered array) are dropped BEFORE bucketing, so a
  // bucket fires ONLY when a stall lands in it (no false "0 stalls here" bucket).
  const list = Array.isArray(events) ? events : [];
  const stalls = [];
  for (const event of list) {
    if (event && typeof event === 'object' && event.type === 'performance-stall') {
      stalls.push(event);
    }
  }

  // Share the pure bucket-assignment math with `summarizeTimeline` so the two can
  // never drift on window, granularity, or bucket boundary (WARDEN-886). A null grid
  // = degenerate config → zeroed shape.
  const grid = _assignTimelineBuckets(stalls, { now, maxBuckets, windowMs });
  if (!grid) return { buckets: [], bucketMs: 0 };
  const { windowStart, bucketMs, slots } = grid;

  // Emit the non-empty buckets chronologically (oldest → newest). Each is
  // self-locating in time (`bucketStart` / `bucketEnd`) + the stall-severity rollup
  // (count / max / bySource) over the stalls in it. Structurally capped at
  // `maxBuckets` grid slots; `bucketMs` is always present so the shape is stable.
  const buckets = [...slots.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([idx, evs]) => {
      const bucketStart = windowStart + idx * bucketMs;
      // Accumulate the stall-severity rollup over the bucket. `count` is EVERY stall
      // (it pairs with the COUNT `timeline` and makes the finite-skip guard visible);
      // `max` reflects ONLY the finite-`lagMs` subset. Split by `source` (a PRESENT
      // non-empty string, matching `signatureOf`'s stall rule) so event-loop jank is
      // distinguishable from renderer hangs; a sourceless stall is counted + feeds the
      // overall `max` but yields no `bySource` entry. The Number.isFinite guard is
      // load-bearing: an unguarded Math.max would poison the bucket from one NaN /
      // Infinity record (validateBaseEvent does NOT reject them — schema.ts).
      let count = 0;
      let max = null;
      const bySource = new Map();
      for (const e of evs) {
        count += 1;
        const lagMs = e.lagMs;
        const finiteLag = typeof lagMs === 'number' && Number.isFinite(lagMs);
        if (finiteLag && (max === null || lagMs > max)) max = lagMs;
        const source = e.source;
        if (typeof source === 'string' && source.length > 0) {
          // Key-length + cardinality bound (WARDEN-1246): `source` is free client
          // text; a new source past the cap folds into ONE shared `__overflow__`
          // accumulator of the same shape (merged, never dropped).
          const srcKey = _boundClientKey(source);
          let acc;
          if (bySource.has(srcKey)) {
            acc = bySource.get(srcKey);
          } else if (bySource.size < CLIENT_HISTOGRAM_CAP) {
            acc = { count: 0, max: null };
            bySource.set(srcKey, acc);
          } else {
            acc = bySource.get(OVERFLOW_KEY) ?? { count: 0, max: null };
            bySource.set(OVERFLOW_KEY, acc);
          }
          acc.count += 1;
          if (finiteLag && (acc.max === null || lagMs > acc.max)) acc.max = lagMs;
        }
      }
      return {
        bucketStart,
        bucketEnd: bucketStart + bucketMs,
        count,
        max,
        bySource: Object.fromEntries(
          [...bySource.entries()].map(([source, acc]) => [source, { count: acc.count, max: acc.max }])
        ),
      };
    });

  return { buckets, bucketMs };
}
