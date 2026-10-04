// The maintainer read surface's pure aggregators (WARDEN-567). Sibling of
// `ingest()`: PURE functions of an event array — no fs, no network, no deps.
// They take the events read back via the store and return the AGGREGATE object a
// self-hosting maintainer can act on (counts / histograms only):
//   - `summarize(events)`        → flat aggregates (total / byType / topErrorNames
//                                  / topSignatures / schemaVersions / appVersions
//                                  / platforms / crashReasons / stalls / operations
//                                  / operationLatency / workspaceShape
//                                  / workspaceNames / featureUsage / processMemory
//                                  / operationRejections
//                                  / firstSeen / lastSeen).
//   - `lastAcceptedInstant(events)` → the newest effective instant across the batch
//                                  (WARDEN-1428) — i.e. WHEN the newest ACCEPTED
//                                  event landed, or `null` on an empty store. Equal
//                                  by construction to `summarize(events).lastSeen`
//                                  (both read the same `_effectiveInstant` rule); it
//                                  exists separately so the `/summary` handler can
//                                  restate that instant inside its UNSCOPED liveness
//                                  verdict in one cheap pass, without a second
//                                  `summarize()` over the unfiltered array.
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
// NEVER echo raw events. NEVER echo the extended-tier decoration fields
// (`chatName` / `sessionName`) that other categories hang on incidents events —
// a summary has no need of them.
//
// ONE EXCEPTION, and it is a deliberate one, not an erosion (WARDEN-1473):
// `workspaceNames.names` reproduces the bounded DISTINCT chat-name set. It is an
// exception because the `workspace-names` event type EXISTS to carry those names
// (WARDEN-1416 — it is the `names` category's own carrying event, not a
// decoration on something else), so an aggregate over that type that refused to
// name anything could not answer the question the type was added for:
// `distinctCount` vs `maxChatCount` — whether a 25-chat catalog holds 25
// identically-named chats, roadmap WARDEN-1265's founding defect. The names
// arrive only behind the `names` consent category (the client's redactor drops
// them otherwise), only inside an event the user opted into, already redacted
// pre-collection; and the aggregate bounds them exactly like every other
// client-keyed axis (key truncation + cardinality cap + `__overflow__` fold), so
// one oversized or runaway catalog can never inflate every response. No OTHER
// aggregate here carries an identifier, and `workspace-shape` — despite its name
// — carries COUNTS ONLY, guaranteed by the closed key set the schema enforces.
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
// text — the field most likely to carry residual identifying fragments) nor any
// name field — neither the extended-tier decorations `chatName`/`sessionName` nor
// the `workspace-names` catalog's `chats[]` (WARDEN-1416), which only
// `workspaceNames` aggregates; an error whose frames are empty / lack the
// location fields degrades to the bare `name` (exactly
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
//   2. CARDINALITY — the first `cap` distinct keys (CLIENT_HISTOGRAM_CAP by
//      default) get their own bucket; every FURTHER distinct key folds into ONE
//      counted `__overflow__` bucket, reusing the exact top-N + overflow shape
//      createRejectionTally established on the ingest side (WARDEN-829) so the
//      two surfaces stay consistent. Overflow is REPRESENTED, never dropped. The
//      cap is a parameter because one axis legitimately needs a wider one — see
//      WORKSPACE_NAMES_SUMMARY_CAP (WARDEN-1473) and the same reasoning
//      OPERATIONS_SUMMARY_CAP records; every free-text axis here uses the default.
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
 * `{ [key]: count }` object holding ≤ `cap` + 1 keys no matter what any client
 * sent. hasOwnProperty (not `in`) keeps attacker keys like "toString" /
 * "constructor" bucketing as ordinary own keys.
 *
 * `cap` defaults to CLIENT_HISTOGRAM_CAP — the bound every free-text client
 * histogram here uses. It is a PARAMETER only because one axis legitimately
 * needs a wider one: chat names (WARDEN-1473) are a per-window CATALOG whose
 * realistic cardinality is an order of magnitude past 10, so folding it at 10
 * would answer "which chats exist?" with `__overflow__` and destroy the exact
 * capability that axis adds — the same reasoning OPERATIONS_SUMMARY_CAP records
 * for operation names. Every pre-existing call site passes nothing and is
 * bounded exactly as before.
 *
 * @param {number} [cap] max distinct keys before the `__overflow__` fold
 * @returns {{ record(value: string): void, snapshot(): Record<string, number> }}
 * @private
 */
function _createBoundedClientHistogram(cap = CLIENT_HISTOGRAM_CAP) {
  const counts = {};
  let distinct = 0;
  return {
    record(value) {
      const key = _boundClientKey(value);
      if (Object.prototype.hasOwnProperty.call(counts, key)) {
        counts[key] += 1; // an already-tracked distinct key bumps its own bucket
      } else if (distinct < cap) {
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

// ── PER-OPERATION AGGREGATE bound (WARDEN-1435) ───────────────────────────────
// `operations` (below) buckets per-operation aggregates by the event's
// `operation` NAME — and the live name space is an order of magnitude wider
// than the free-text client-keyed histograms above: the warden client's route
// census alone carries ~96 request-operation names (requestTelemetry.js
// REQUEST_MAX_OPERATIONS) beside the pane/fileExists/renderer-pane producers,
// so borrowing CLIENT_HISTOGRAM_CAP (10) unchanged would fold ~90% of the key
// space into `__overflow__` and "which route is slow?" would answer
// `__overflow__` — destroying the exact capability this axis exists to add.
// The bound is instead anchored to the WIRE's own structural cap: the schema
// caps ONE operational-metrics event at MAX_OPERATIONS_PER_EVENT = 129
// distinct operations (schema.ts:479, module-private — do not import; schema.ts
// is vendored byte-identical to the client and must not be edited). A
// per-summary cap at the same scale keeps the response bounded by the same
// constant the producer is already bounded by, while keeping every realistic
// operation name in its own readable bucket. Same shape as the histograms:
// the first OPERATIONS_SUMMARY_CAP distinct names get their own bucket; every
// FURTHER distinct name folds into ONE counted `__overflow__` accumulator —
// bounded cardinality, no count loss. (An operation literally named
// `__overflow__` is impossible on validated data — OPERATION_NAME_RE admits
// only `[a-z0-9-]` — the same benign collision note the histograms carry.)
//
// HISTOGRAM REFUSAL (deliberate): the two live producers ship DIFFERENT,
// incompatible bucket scales into this ONE event type — server-side
// aggregators use DEFAULT_BUCKET_BOUNDARIES_MS (8 boundaries / 9 buckets) while
// the renderer's pane-latency producer uses PANE_LATENCY_BOUNDARIES_MS
// (12 boundaries / 13 buckets). Summing or concatenating `buckets[]` across
// windows would merge two different x-axes into one meaningless array, so
// `operations` projects NO histogram axis at all: count / okCount / failCount /
// min / avg / max only. The numbers are scale-free and always comparable; the
// per-window histograms stay readable on /events. The refusal's rule (never sum
// across scales) is kept — the distribution is projected PER SCALE in the
// sibling `operationLatency` key (WARDEN-1500), never inside `operations`.
export const OPERATIONS_SUMMARY_CAP = 129; // anchored to schema.ts's MAX_OPERATIONS_PER_EVENT

// Distinct histogram scales (boundary arrays) tracked per operation name in
// `operationLatency` (WARDEN-1500). The two live producers ship two scales, so 4
// leaves headroom for a future producer; every further distinct scale for a name
// folds into that name's `excludedCount` (bounded memory, no silent loss).
export const OPERATION_LATENCY_SCALE_CAP = 4;

// ── WORKSPACE AGGREGATES (WARDEN-1473) ────────────────────────────────────────
// The two workspace event types (`workspace-shape`, WARDEN-1424; and
// `workspace-names`, WARDEN-1416) reduced to a bare `byType` integer until this
// axis existed — the "count for free, payload discarded" mechanism. That made
// roadmap WARDEN-1265's founding defect ILLEGIBLE on the read surface: "twenty-
// five chats and twenty-five identically-named chats are the same number".
// `workspaceShape` + `workspaceNames` (below) are the read-side completion of
// the vein `stalls` (WARDEN-854) and `operations` (WARDEN-1435) already cut.
//
// The COUNT axes a `workspace-shape` window carries. Fixed and closed — the
// schema enforces a CLOSED KEY SET on that event (schema.ts
// WORKSPACE_SHAPE_KEYS), so this list is the payload, not a sample of it, and a
// per-count snapshot is produced for every entry here whether or not any window
// ever carried one (the stable-zeroed-shape posture `byType` established).
// Each name is a literal from the schema's own field set; the two `peak*`
// entries are per-window maxima, so they compose under `max` for free — the
// validator guarantees `peak >= closing` INSIDE each event.
const WORKSPACE_SHAPE_COUNTS = Object.freeze([
  'workspaces', 'panesOpen', 'panesActive', 'chats', 'peakPanesOpen', 'peakChats',
]);

// The distinct-chat-name bound. Anchored to the PRODUCER's own per-window cap
// (the warden client's NAMES_MAX = 200, recorded in schema.ts's
// MAX_CHATS_PER_EVENT note) for exactly the reason OPERATIONS_SUMMARY_CAP is
// anchored to MAX_OPERATIONS_PER_EVENT rather than borrowing
// CLIENT_HISTOGRAM_CAP (10): a chat catalog's realistic cardinality is an order
// of magnitude past 10, so a cap of 10 would fold ~95% of the key space into
// `__overflow__` and "which names does the catalog hold?" would answer
// `__overflow__` — destroying the exact capability this axis exists to add. At
// this cap ONE full window's catalog keeps every name in its own readable
// bucket, and the response stays bounded by the same constant the producer is
// already bounded by (≤ 201 keys × CLIENT_KEY_MAX_LENGTH chars).
//
// It is NOT imported from schema.ts: MAX_CHATS_PER_EVENT is module-private
// there and schema.ts is vendored byte-identical to the client (it must never
// be edited to widen an export), the same posture the operations cap records.
export const WORKSPACE_NAMES_SUMMARY_CAP = 200;

// ── FEATURE USAGE aggregate (WARDEN-1488) ─────────────────────────────────────
// Schema v9 (WARDEN-1479) made the feature-adoption category carry a
// `feature-usage` event, and until this axis existed `summarize()` reduced it to
// the bare `byType['feature-usage']` integer — capability names and counts were
// discarded ("producer landed, read surface lagging", the same mechanism as
// WARDEN-1473 / WARDEN-1435). `featureUsage` is the read-side completion.
//
// TRUST / HONESTY (stated here so the diff carries it):
//  - COUNTS + CLOSED-SET KEBAB-CASE NAMES ONLY. The schema's structural
//    guarantees on this event (closed row keys `{name, count}`, the kebab-case
//    name regex, positive-integer counts, the renderer-runtime pin) already
//    prevent an identifier riding this channel, so no new exception to
//    "counts and histograms only" is needed — unlike `workspaceNames.names`.
//  - `feature-usage` is NOT a liveness signal. It is COUNT-driven: an idle
//    window sends NOTHING, so silence here means "no capability was used", never
//    "the client is down". `lastWindowAt` is therefore the producer-clock close of
//    the newest window that DID report usage — it must not be read as liveness;
//    the `channel` liveness verdict remains the liveness surface.
//
// The distinct-capability-name bound. Anchored BY COMMENT to schema.ts's
// MAX_FEATURES_PER_EVENT (64) plus one for the `__overflow__` bucket — so ONE
// full window's names each keep their own readable bucket (a cap of 10 would
// fold most of the vocabulary into `__overflow__`). It is NOT imported:
// MAX_FEATURES_PER_EVENT is module-private there and schema.ts is vendored
// byte-identical to the client, the same posture the operations cap records.
export const FEATURE_USAGE_SUMMARY_CAP = 64;

/**
 * Create the `feature-usage` accumulator (WARDEN-1488).
 *
 * Per capability NAME it folds `count` (the SUM of uses across windows) and
 * `windowsSeen` (the number of windows in which that capability appeared). Names
 * are truncated via `_boundClientKey`, capped at FEATURE_USAGE_SUMMARY_CAP
 * distinct names, and every FURTHER new name folds into one counted
 * `__overflow__` bucket — represented, never dropped. `distinctCount` counts the
 * snapshot's keys, so `__overflow__` counts as one (a floor, LOUD on the surface,
 * exactly as `workspaceNames.distinctCount`).
 *
 * Skip-robust (the `_foldWorkspaceShape` discipline): a malformed row (non-object,
 * non-string / empty name, non-finite or non-positive count) is SKIPPED and can
 * never throw or poison a sum to `NaN`; the EVENT still counts in `windowsSeen`
 * (parity with `byType['feature-usage']`). A name repeated inside one event
 * counts its uses but contributes ONE window to that name's `windowsSeen`.
 *
 * @returns {{ fold(event: object): void, snapshot(): object }}
 * @private
 */
function _createFeatureUsageAccumulator() {
  const features = new Map(); // bounded name → { count, windowsSeen }
  let windowsSeen = 0;
  let lastWindowAt = null;
  return {
    fold(event) {
      windowsSeen += 1; // the EVENT counts whatever its payload turns out to be
      const { features: rows, windowEndedAt } = event;
      if (typeof windowEndedAt === 'number' && Number.isFinite(windowEndedAt) && (lastWindowAt === null || windowEndedAt > lastWindowAt)) {
        lastWindowAt = windowEndedAt;
      }
      if (!Array.isArray(rows)) return;
      const touched = new Set(); // keys already credited a window by THIS event
      for (const row of rows) {
        if (!row || typeof row !== 'object') continue;
        const { name, count } = row;
        if (typeof name !== 'string' || name.length === 0) continue;
        if (typeof count !== 'number' || !Number.isFinite(count) || count <= 0) continue;
        let key = _boundClientKey(name);
        let acc = features.get(key);
        if (acc === undefined) {
          if (features.size < FEATURE_USAGE_SUMMARY_CAP) {
            acc = { count: 0, windowsSeen: 0 };
            features.set(key, acc);
          } else {
            // Cap reached → fold every further NEW name into the one overflow bucket.
            key = OVERFLOW_KEY;
            acc = features.get(key);
            if (acc === undefined) {
              acc = { count: 0, windowsSeen: 0 };
              features.set(key, acc);
            }
          }
        }
        acc.count += count;
        if (!touched.has(key)) {
          touched.add(key);
          acc.windowsSeen += 1;
        }
      }
    },
    snapshot() {
      const out = {};
      for (const [key, acc] of features) out[key] = { count: acc.count, windowsSeen: acc.windowsSeen };
      return {
        windowsSeen,
        lastWindowAt,
        features: out,
        distinctCount: features.size,
      };
    },
  };
}

// ── PROCESS MEMORY aggregate (WARDEN-1514) ────────────────────────────────────
// Schema v10 (WARDEN-1507/1508) made the operational-metrics category carry a
// `process-memory` event, and until this axis existed `summarize()` reduced every
// accepted memory window to the bare `byType['process-memory']` integer — the
// RSS / heap / process-age numbers were discarded ("producer landed, read surface
// lagging", the same mechanism as WARDEN-1488 / WARDEN-1473). `processMemory` is
// the read-side completion: the last pure receiver projection of the v9/v10 events.
//
// TRUST / HONESTY (stated here so the diff carries it):
//  - NUMBERS ONLY. No string from the event is echoed except the CLOSED runtime key
//    (main | renderer | server); a runtime outside that set is skipped by the fold
//    (the event still counts in the top-level `windowsSeen`). The schema already
//    guarantees the fields are non-identifying numbers.
//  - SILENCE ≠ LOW MEMORY. A runtime that never reported has `windowsSeen: 0` and
//    NULL min/avg/max/heap/peak/latest — never fabricated zeros.
//  - `rssAvgBytes` is the SAMPLE-WEIGHTED mean (sum(avg*samples)/sum(samples),
//    integer-rounded), not a mean of window means.
//  - `peak` is the window with the largest `rssMaxBytes` together with THAT
//    window's `processAgeMs` (a small age beside a high peak reads as "high right
//    after a restart"); `latest` is the window with the greatest producer-clock
//    `windowEndedAt` (not arrival order). Ties break deterministically so the
//    result never depends on event order. No restart-detection logic.
//  - `process-memory` is NOT a liveness signal: `lastWindowAt` is the producer-clock
//    close of the newest window seen and must not be read as liveness.
export const PROCESS_MEMORY_RUNTIMES = Object.freeze(['main', 'renderer', 'server']);

function _finiteNumber(v) {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

/**
 * Create the `process-memory` accumulator (WARDEN-1514). See the block comment
 * above for the contract. Skip-robust: a non-number / non-finite field is skipped
 * individually and can never throw or poison a sum to `NaN`.
 *
 * @returns {{ fold(event: object): void, snapshot(): object }}
 * @private
 */
function _createProcessMemoryAccumulator() {
  let windowsSeen = 0;
  let lastWindowAt = null;
  const runtimes = new Map();
  for (const r of PROCESS_MEMORY_RUNTIMES) {
    runtimes.set(r, {
      windowsSeen: 0,
      samples: 0,
      rssMin: null,
      rssMax: null,
      heapMax: null,
      weightedSum: 0,
      weightedSamples: 0,
      peak: null,
      latest: null,
    });
  }
  return {
    fold(event) {
      windowsSeen += 1; // the EVENT counts whatever its payload turns out to be
      const endedAt = _finiteNumber(event.windowEndedAt);
      if (endedAt !== null && (lastWindowAt === null || endedAt > lastWindowAt)) lastWindowAt = endedAt;
      const acc = typeof event.runtime === 'string' ? runtimes.get(event.runtime) : undefined;
      if (acc === undefined) return; // closed key set: unknown runtime is skipped
      acc.windowsSeen += 1;

      const samples = _finiteNumber(event.samples);
      const rssMin = _finiteNumber(event.rssMinBytes);
      const rssAvg = _finiteNumber(event.rssAvgBytes);
      const rssMax = _finiteNumber(event.rssMaxBytes);
      const heapMax = _finiteNumber(event.heapUsedMaxBytes);
      const ageMs = _finiteNumber(event.processAgeMs);

      if (samples !== null && samples > 0) acc.samples += samples;
      if (rssMin !== null && (acc.rssMin === null || rssMin < acc.rssMin)) acc.rssMin = rssMin;
      if (rssMax !== null && (acc.rssMax === null || rssMax > acc.rssMax)) acc.rssMax = rssMax;
      if (heapMax !== null && (acc.heapMax === null || heapMax > acc.heapMax)) acc.heapMax = heapMax;
      if (rssAvg !== null && samples !== null && samples > 0) {
        acc.weightedSum += rssAvg * samples;
        acc.weightedSamples += samples;
      }

      if (rssMax !== null) {
        // Largest rssMax wins; ties → later window close, then larger age (order-independent).
        const p = acc.peak;
        const better =
          p === null ||
          rssMax > p.rssMaxBytes ||
          (rssMax === p.rssMaxBytes &&
            ((endedAt ?? -Infinity) > (p.windowEndedAt ?? -Infinity) ||
              ((endedAt ?? -Infinity) === (p.windowEndedAt ?? -Infinity) && (ageMs ?? -Infinity) > (p.processAgeMs ?? -Infinity))));
        if (better) acc.peak = { rssMaxBytes: rssMax, processAgeMs: ageMs, windowEndedAt: endedAt };
      }
      if (endedAt !== null) {
        // Greatest windowEndedAt wins; ties → larger rssMax, then larger age, then larger avg.
        const l = acc.latest;
        const key = [rssMax ?? -Infinity, ageMs ?? -Infinity, rssAvg ?? -Infinity];
        let better = l === null || endedAt > l.windowEndedAt;
        if (!better && endedAt === l.windowEndedAt) {
          const lk = [l.rssMaxBytes ?? -Infinity, l.processAgeMs ?? -Infinity, l.rssAvgBytes ?? -Infinity];
          for (let i = 0; i < key.length; i++) {
            if (key[i] !== lk[i]) {
              better = key[i] > lk[i];
              break;
            }
          }
        }
        if (better) acc.latest = { rssAvgBytes: rssAvg, rssMaxBytes: rssMax, processAgeMs: ageMs, windowEndedAt: endedAt };
      }
    },
    snapshot() {
      const byRuntime = {};
      for (const [name, a] of runtimes) {
        byRuntime[name] = {
          windowsSeen: a.windowsSeen,
          samples: a.samples,
          rssMinBytes: a.rssMin,
          rssAvgBytes: a.weightedSamples > 0 ? Math.round(a.weightedSum / a.weightedSamples) : null,
          rssMaxBytes: a.rssMax,
          heapUsedMaxBytes: a.heapMax,
          peak: a.peak === null ? null : { ...a.peak },
          latest: a.latest === null ? null : { ...a.latest },
        };
      }
      return { windowsSeen, lastWindowAt, byRuntime };
    },
  };
}

// ── OPERATION REJECTIONS aggregate (WARDEN-1519) ──────────────────────────────
// The `operational-metrics` event carries a required `rejected` integer
// (schema.ts): observations the PRODUCER refused as invalid / out-of-range — in
// NEITHER the window's `count` NOR its histogram. `summarize()` used to discard it
// ("producer landed, read surface lagging"). `operationRejections` projects it per
// runtime.
//
// WHY IT MATTERS: the renderer pane-latency producer only folds an echo that
// arrives within 10 s (PENDING_INPUT_MAX_AGE_MS); an older pending input is counted
// as `rejected` and never reaches `count` or the histogram, so `operationLatency`
// is RIGHT-CENSORED at 10 s. `rejected` is the only place a >10 s wait is visible.
//
// WHAT `rejected` MEANS / DOES NOT MEAN (load-bearing — do not erode):
//  - It counts observations a producer refused as out-of-range or invalid. For the
//    renderer pane producer the dominant cause is an echo older than the 10 s
//    correlation window, which is EITHER a >10 s freeze OR a lost echo (pane died,
//    WS dropped). The counter CANNOT tell them apart and it is NOT an error count.
//  - THE SPLIT (WARDEN-1528, schema v11): a window MAY also carry `rejectedStale`
//    (echo older than the 10 s window — the right-censored unusable-tail count,
//    NOT a failure) and `rejectedInvalid` (malformed input — a caller-contract
//    violation, expected ~zero); `rejected` is their sum. `rejectedStaleTotal` /
//    `rejectedInvalidTotal` sum each field over the windows that CARRY it and are
//    `null` until one does (a v9/v10 window predates the split — its refusals are
//    UNCLASSIFIED, never zero). While a runtime mixes pre-split and split windows
//    the two split totals sum to LESS than `rejectedTotal`: the gap is the
//    unclassified pre-split refusals, not a leak. Each field folds independently
//    and skip-robustly (a malformed value is skipped, never poisons a sum).
//  - SILENCE ≠ ZERO. A runtime with `windowsSeen: 0` reports `rejectedTotal: null`
//    and `lastRejectedAt: null`; a runtime that reported windows with none refused
//    reports a MEASURED `rejectedTotal: 0`.
//  - Skip-robust: `rejected` must be a finite non-negative integer to fold; a
//    malformed value never throws and never poisons a sum, but the window still
//    counts in `windowsSeen`.
//  - `lastRejectedAt` is the greatest producer-clock `windowEndedAt` among windows
//    with rejected > 0 (never arrival time); not a liveness signal.
//  - Closed runtime key set (main | renderer | server); an unknown runtime is
//    skipped from `byRuntime` but still counted in the top-level `windowsSeen`.
export const OPERATION_REJECTIONS_RUNTIMES = Object.freeze(['main', 'renderer', 'server']);

/**
 * Create the operation-rejections accumulator (WARDEN-1519). See the block comment
 * above for the contract.
 *
 * @returns {{ fold(event: object): void, snapshot(): object }}
 * @private
 */
function _createOperationRejectionsAccumulator() {
  let windowsSeen = 0;
  const runtimes = new Map();
  for (const r of OPERATION_REJECTIONS_RUNTIMES) {
    runtimes.set(r, { windowsSeen: 0, windowsWithRejections: 0, rejectedTotal: 0, rejectedStaleTotal: null, rejectedInvalidTotal: null, lastRejectedAt: null });
  }
  return {
    fold(event) {
      windowsSeen += 1; // the EVENT counts whatever its payload turns out to be
      const acc = typeof event.runtime === 'string' ? runtimes.get(event.runtime) : undefined;
      if (acc === undefined) return; // closed key set: unknown runtime is skipped
      acc.windowsSeen += 1;
      // WARDEN-1528 — the optional split folds BEFORE the `rejected` guard and
      // independently of it: each field is its own skip-robust fact.
      const stale = event.rejectedStale;
      if (typeof stale === 'number' && Number.isInteger(stale) && stale >= 0) acc.rejectedStaleTotal = (acc.rejectedStaleTotal ?? 0) + stale;
      const invalid = event.rejectedInvalid;
      if (typeof invalid === 'number' && Number.isInteger(invalid) && invalid >= 0) acc.rejectedInvalidTotal = (acc.rejectedInvalidTotal ?? 0) + invalid;
      const rejected = event.rejected;
      if (typeof rejected !== 'number' || !Number.isInteger(rejected) || rejected < 0) return; // malformed: skipped
      acc.rejectedTotal += rejected;
      if (rejected > 0) {
        acc.windowsWithRejections += 1;
        const endedAt = _finiteNumber(event.windowEndedAt);
        if (endedAt !== null && (acc.lastRejectedAt === null || endedAt > acc.lastRejectedAt)) acc.lastRejectedAt = endedAt;
      }
    },
    snapshot() {
      const byRuntime = {};
      for (const [name, a] of runtimes) {
        byRuntime[name] = {
          windowsSeen: a.windowsSeen,
          windowsWithRejections: a.windowsWithRejections,
          rejectedTotal: a.windowsSeen > 0 ? a.rejectedTotal : null, // silence is not zero
          rejectedStaleTotal: a.rejectedStaleTotal, // null until a v11+ window carries the split
          rejectedInvalidTotal: a.rejectedInvalidTotal,
          lastRejectedAt: a.lastRejectedAt,
        };
      }
      return { windowsSeen, byRuntime };
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
 * identifying fragments, schema.ts) nor any name field — neither the
 * extended-tier decorations `chatName` / `sessionName` nor the
 * `workspace-names` catalog's `chats[]` (a workspace event yields NO signature
 * at all; only `workspaceNames` aggregates that list). It reads at most: error
 * `name` + the FIRST stack
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
 * Render ONE per-operation accumulator as the public
 * `{ count, okCount, failCount, min, avg, max }` snapshot (WARDEN-1435) — the
 * per-operation sibling of `_stallSnapshot`. `count`/`okCount`/`failCount` are
 * the folded totals (Σ over every admitted window entry for that operation
 * name); `avg` is the WEIGHTED mean `Σ(avg × count) / Σcount` over the entries
 * whose `avg` was finite and count-bearing — never a mean of window means (10
 * observations @ 100ms + 1 @ 1000ms must read ≈182, not 550); `min`/`max` are
 * the true extrema across windows. With no finite, count-bearing record seen,
 * `min`/`max` are `null` (the `_stallSnapshot` honesty posture — `0` is a REAL
 * measured duration here, e.g. cache hits, so it cannot double as the empty
 * sentinel) and `avg` is `0` (the guarded empty case can never read `NaN`).
 *
 * @param {{ count: number, okCount: number, failCount: number, weightedSum: number, weightCount: number, min: number | null, max: number | null }} acc
 * @returns {{ count: number, okCount: number, failCount: number, min: number | null, avg: number, max: number | null }}
 * @private
 */
function _operationSnapshot({ count, okCount, failCount, weightedSum, weightCount, min, max }) {
  return {
    count,
    okCount,
    failCount,
    min,
    avg: weightCount > 0 ? weightedSum / weightCount : 0,
    max,
  };
}

// An empty per-operation accumulator (the exact key set _operationSnapshot reads).
function _newOperationAccumulator() {
  return { count: 0, okCount: 0, failCount: 0, weightedSum: 0, weightCount: 0, min: null, max: null };
}

// An empty per-operation latency accumulator (WARDEN-1500): up to
// OPERATION_LATENCY_SCALE_CAP per-scale histograms plus the observations dropped
// because their scale did not fit under the cap.
function _newLatencyAccumulator() {
  return { scales: [], overflowExcluded: 0 };
}

/**
 * Is `boundaries` usable as a histogram scale: an array of finite numbers.
 * (Ascending order is the validator's job; this fold only needs element-wise
 * comparability, and never indexes outside the array.)
 * @private
 */
function _validBoundaries(boundaries) {
  return Array.isArray(boundaries) && boundaries.every((b) => typeof b === 'number' && Number.isFinite(b));
}

/**
 * Fold ONE window entry's `buckets[]` into the latency accumulator `lat`
 * (WARDEN-1500). SKIP-ROBUST on `_foldOperations`' terms and INDEPENDENT of its
 * count/min/avg/max fold: an entry whose `buckets` is not an array of
 * non-negative integers of length `boundaries.length + 1` (or whose window
 * `boundaries` is unusable) contributes NOTHING here. Buckets are only ever
 * index-summed into a scale whose `boundaries` are element-wise EQUAL — never
 * across scales (the HISTOGRAM REFUSAL rule). An all-zero histogram (an idle
 * placeholder window) registers nothing, so it can never occupy a scale slot.
 * @private
 */
function _foldLatency(lat, boundaries, buckets) {
  if (!_validBoundaries(boundaries)) return;
  if (!Array.isArray(buckets) || buckets.length !== boundaries.length + 1) return;
  let sum = 0;
  for (const b of buckets) {
    if (typeof b !== 'number' || !Number.isInteger(b) || b < 0) return;
    sum += b;
  }
  if (sum === 0) return;
  let scale = lat.scales.find(
    (sc) => sc.boundaries.length === boundaries.length && sc.boundaries.every((v, i) => v === boundaries[i])
  );
  if (scale === undefined) {
    if (lat.scales.length >= OPERATION_LATENCY_SCALE_CAP) {
      lat.overflowExcluded += sum;
      return;
    }
    scale = { boundaries: boundaries.slice(), buckets: new Array(boundaries.length + 1).fill(0), total: 0 };
    lat.scales.push(scale);
  }
  for (let i = 0; i < buckets.length; i += 1) scale.buckets[i] += buckets[i];
  scale.total += sum;
}

/**
 * Nearest-rank percentile as a BUCKET UPPER BOUND (WARDEN-1500): the upper
 * boundary of the first bucket whose cumulative count ≥ ceil(pct/100 × total).
 * Never interpolated. Overflow bucket (beyond the top boundary) or an empty
 * histogram → `null`. Integer arithmetic (pct × total / 100) so ceil can never be
 * tipped by float noise (0.07 × 100 = 7.000000000000001).
 * @private
 */
function _percentileUpperBound(boundaries, buckets, total, pct) {
  if (total <= 0) return null;
  const rank = Math.ceil((pct * total) / 100);
  let cumulative = 0;
  for (let i = 0; i < buckets.length; i += 1) {
    cumulative += buckets[i];
    if (cumulative >= rank) return i < boundaries.length ? boundaries[i] : null;
  }
  return null;
}

/**
 * Render ONE latency accumulator as the public per-operation distribution
 * (WARDEN-1500): the scale with the most observations (first-seen wins a tie);
 * every other scale's observations plus any cap overflow become `excludedCount`.
 * No histogram → `{ boundaries: [], buckets: [], histogramCount: 0,
 * excludedCount: 0, p50/p95/p99: null }`.
 * @private
 */
function _latencySnapshot(lat) {
  let best = null;
  let all = lat.overflowExcluded;
  for (const sc of lat.scales) {
    all += sc.total;
    if (best === null || sc.total > best.total) best = sc;
  }
  if (best === null) {
    return { boundaries: [], buckets: [], histogramCount: 0, excludedCount: all, p50: null, p95: null, p99: null };
  }
  return {
    boundaries: best.boundaries.slice(),
    buckets: best.buckets.slice(),
    histogramCount: best.total,
    excludedCount: all - best.total,
    p50: _percentileUpperBound(best.boundaries, best.buckets, best.total, 50),
    p95: _percentileUpperBound(best.boundaries, best.buckets, best.total, 95),
    p99: _percentileUpperBound(best.boundaries, best.buckets, best.total, 99),
  };
}

/**
 * Fold ONE operational-metrics event's `operations[]` into the per-name
 * accumulator map `accs` (WARDEN-1435). Skip-robust per `summarize()`'s stated
 * discipline: a malformed or partial entry is SKIPPED — it can never throw and
 * never poison an aggregate to `NaN` — while its EVENT still counts in
 * `byType` (counting is independent of this fold).
 *
 * Admission + guards, and WHY each exists:
 * - An entry needs a non-empty string `operation` (attributable) and a finite
 *   non-negative `count` (countable) — anything else is skipped whole.
 * - `okCount` / `failCount` fold only when finite and non-negative. The
 *   producer maintains `okCount + failCount == count` (telemetry-metrics.cjs /
 *   paneLatency.ts) but the validator range-checks the three integers
 *   INDEPENDENTLY — it never checks the sum — so this fold PRESERVES the
 *   identity for inputs that satisfy it and never ASSUMES it of an arbitrary
 *   stored row. (A renderer operation reading 100% ok is correct, not a bug —
 *   that producer only ever increments okCount.)
 * - `min` / `avg` / `max` fold only when the entry's `count` > 0 AND the value
 *   is finite. The count>0 guard is load-bearing: a producer that resets its
 *   accumulators but KEEPS the keys (paneLatency.ts emptyAccumulator → project)
 *   emits zero-count placeholder entries (`min: 0 / avg: 0 / max: 0`) in every
 *   idle window — folding their extrema would report a false `min: 0` for an
 *   operation that was simply idle, and their `avg × count` would be
 *   `0 × 0` at best, `NaN × 0` at worst. The finite guard is the
 *   `_stallSnapshot` honesty posture: a non-finite value is skipped from the
 *   extrema / weighted sum but the observation (its `count`) is still counted.
 * - NO histogram axis is folded into `accs`, deliberately: the two live
 *   producers ship incompatible boundary scales into this one event type (see
 *   the OPERATIONS_SUMMARY_CAP block above), so `buckets[]` is never summed,
 *   concatenated, or index-merged here — the numbers in `accs` are scale-free.
 *   The distribution is folded PER SCALE into the separate `latency` map
 *   (WARDEN-1500, `_foldLatency`), under the same name key, independently: an
 *   entry with unusable buckets still folds count/min/avg/max above.
 *
 * `operation` names are ≤64 chars on every validator-accepted event
 * (OPERATION_NAME_RE), so `_boundClientKey` can never truncate one — it is
 * defence-in-depth over a NON-validated store row (a partial read or hand-written
 * line), never a wire need. New distinct names fill their own buckets up to
 * OPERATIONS_SUMMARY_CAP; past the cap every further NEW name folds into the
 * one shared `__overflow__` accumulator (bounded cardinality, no count loss),
 * mirroring `stallBySource`'s overflow exactly.
 *
 * @param {unknown} operations the event's `operations` array (any shape)
 * @param {Map<string, object>} accs name → accumulator (mutated in place)
 * @param {Map<string, object>} [latency] name → latency accumulator (mutated in place)
 * @param {unknown} [boundaries] the event's histogram `boundaries`
 * @private
 */
function _foldOperations(operations, accs, latency, boundaries) {
  if (!Array.isArray(operations)) return;
  for (const op of operations) {
    if (!op || typeof op !== 'object') continue;
    const { operation, count, okCount, failCount, min, avg, max, buckets } = op;
    if (typeof operation !== 'string' || operation.length === 0) continue;
    if (typeof count !== 'number' || !Number.isFinite(count) || count < 0) continue;
    let key = _boundClientKey(operation);
    let acc = accs.get(key);
    if (acc === undefined) {
      if (accs.size < OPERATIONS_SUMMARY_CAP) {
        acc = _newOperationAccumulator();
        accs.set(key, acc);
      } else {
        key = OVERFLOW_KEY;
        // Cap reached → fold every further NEW distinct name into the single
        // overflow accumulator: bounded cardinality, no count loss.
        acc = accs.get(OVERFLOW_KEY);
        if (acc === undefined) {
          acc = _newOperationAccumulator();
          accs.set(OVERFLOW_KEY, acc);
        }
      }
    }
    acc.count += count;
    if (latency instanceof Map) {
      let lat = latency.get(key);
      if (lat === undefined) {
        lat = _newLatencyAccumulator();
        latency.set(key, lat);
      }
      _foldLatency(lat, boundaries, buckets);
    }
    if (typeof okCount === 'number' && Number.isFinite(okCount) && okCount >= 0) acc.okCount += okCount;
    if (typeof failCount === 'number' && Number.isFinite(failCount) && failCount >= 0) acc.failCount += failCount;
    if (count > 0) {
      if (typeof min === 'number' && Number.isFinite(min) && (acc.min === null || min < acc.min)) acc.min = min;
      if (typeof max === 'number' && Number.isFinite(max) && (acc.max === null || max > acc.max)) acc.max = max;
      if (typeof avg === 'number' && Number.isFinite(avg)) {
        acc.weightedSum += avg * count;
        acc.weightCount += count;
      }
    }
  }
}

/**
 * Render ONE workspace-shape count accumulator as the public
 * `{ windowsSeen, min, avg, max }` snapshot (WARDEN-1473) — the per-count
 * sibling of `_stallSnapshot` / `_operationSnapshot`.
 *
 * `windowsSeen` is the number of windows that contributed a FINITE value to
 * THIS count (never the event total — that is `workspaceShape.windowsSeen`,
 * which keeps byType parity), so a producer that ships one malformed field in
 * one window degrades that ONE count's sample size and nothing else.
 *
 * `avg` is a PLAIN mean, deliberately NOT `_operationSnapshot`'s weighted one:
 * a shape event IS one window's snapshot and carries no observation count to
 * weight by, so every window gets equal weight (5 windows of 2 panes and 1
 * window of 20 panes read ≈5, which is the honest "typical window" figure).
 *
 * With no finite value folded, `min`/`max` are `null` (the `_stallSnapshot`
 * honesty posture — `0` is a REAL measured count here, e.g. a workspace with no
 * panes open, so it cannot double as the empty sentinel) and `avg` is `0` (the
 * guarded empty case can never read `NaN`).
 *
 * @param {{ windowsSeen: number, sum: number, min: number | null, max: number | null }} acc
 * @returns {{ windowsSeen: number, min: number | null, avg: number, max: number | null }}
 * @private
 */
function _workspaceCountSnapshot({ windowsSeen, sum, min, max }) {
  return {
    windowsSeen,
    min,
    avg: windowsSeen > 0 ? sum / windowsSeen : 0,
    max,
  };
}

// An empty per-count accumulator (the exact key set _workspaceCountSnapshot reads).
function _newWorkspaceCountAccumulator() {
  return { windowsSeen: 0, sum: 0, min: null, max: null };
}

/**
 * Fold ONE `workspace-shape` event's counts into the per-count accumulator map
 * `accs` (WARDEN-1473). Skip-robust per `summarize()`'s stated discipline: a
 * malformed or partial field is SKIPPED — it can never throw and never poison
 * an aggregate to `NaN` — while its EVENT still counts in `byType` AND in
 * `workspaceShape.windowsSeen` (counting is independent of this fold, exactly
 * as `stalls.count` counts a stall whose `lagMs` was unusable).
 *
 * Admission: a count folds only when it is a finite non-negative number. The
 * validator already enforces non-negative integers on every one of them
 * (schema.ts isWorkspaceShapeShape), so this guard is defence-in-depth over a
 * NON-validated store row (a partial read or a hand-written line) — the same
 * posture `_foldOperations` records for `_boundClientKey` on operation names.
 *
 * The two `peak*` fields need NO special handling: they are per-window maxima
 * whose own accumulators compose under `max` for free, and the validator
 * guarantees `peak >= closing` INSIDE each event — so a window that opened nine
 * panes and closed on one surfaces `peakPanesOpen.max === 9` while
 * `panesOpen.max` reads the closing `1`, which is precisely the open-then-close
 * burst the peaks exist to keep visible.
 *
 * @param {object} event a `workspace-shape` event (already known to be an object)
 * @param {Map<string, object>} accs count name → accumulator (mutated in place)
 * @private
 */
function _foldWorkspaceShape(event, accs) {
  for (const key of WORKSPACE_SHAPE_COUNTS) {
    const value = event[key];
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) continue;
    let acc = accs.get(key);
    if (acc === undefined) {
      // The key space is the schema's own CLOSED set (WORKSPACE_SHAPE_COUNTS),
      // so it is bounded by construction — no cardinality cap is needed or
      // meaningful here, unlike the free-text / free-name axes.
      acc = _newWorkspaceCountAccumulator();
      accs.set(key, acc);
    }
    acc.windowsSeen += 1;
    acc.sum += value;
    if (acc.min === null || value < acc.min) acc.min = value;
    if (acc.max === null || value > acc.max) acc.max = value;
  }
}

/**
 * Create the `workspace-names` accumulator (WARDEN-1473) — the fold that makes
 * roadmap WARDEN-1265's founding defect legible in ONE query.
 *
 * WHAT IT ANSWERS, and what nothing else can: a `workspace-names` window carries
 * the catalog's de-duplicated NAMES alongside `chatCount`, the TRUE catalog size
 * before any cap. Comparing the two is the whole capability — a catalog holding
 * 25 chats under 5 distinct names reads `distinctCount: 5` against
 * `maxChatCount: 25`, so `distinctCount < maxChatCount` IS the identically-named-
 * chats verdict, readable without paging `/events` and hand-folding windows.
 *
 * The distinct set is BOUNDED exactly like every other client-keyed axis
 * (WARDEN-1246): each name is truncated at CLIENT_KEY_MAX_LENGTH and the
 * cardinality is capped at WORKSPACE_NAMES_SUMMARY_CAP distinct names, past
 * which every FURTHER new name folds into ONE counted `__overflow__` bucket —
 * represented, never dropped. `distinctCount` counts the SNAPSHOT's keys, so the
 * `__overflow__` bucket counts as one: the cap is LOUD on the surface rather
 * than silently shrinking the number (a reader seeing `__overflow__` in `names`
 * knows `distinctCount` is a floor, and `truncatedEver` is the separate
 * producer-side cap bit).
 *
 * Honesty posture: `maxChatCount` / `lastChatCount` are `null` until a window
 * reported a finite one (`0` is a REAL catalog size — the `_stallSnapshot` rule),
 * and `lastSnapshotAt` is `null` on an empty store. `lastChatCount` tracks the
 * window with the greatest `windowEndedAt` (the producer's own window clock, not
 * arrival order) so a batch persisted out of order still reports the LATEST
 * catalog size — degrading to arrival order ONLY when no usable `windowEndedAt`
 * has been seen at all, so a window with an unreadable producer clock still
 * contributes a last-known size rather than leaving `lastChatCount` null beside
 * a `maxChatCount` that plainly reports a number. `lastSnapshotAt` takes NO such
 * fallback: freshness with no readable clock stays honestly absent.
 *
 * @returns {{ fold(event: object): void, snapshot(): object }}
 * @private
 */
function _createWorkspaceNamesAccumulator() {
  const names = _createBoundedClientHistogram(WORKSPACE_NAMES_SUMMARY_CAP);
  let windowsSeen = 0;
  let maxChatCount = null;
  let lastChatCount = null;
  let lastChatCountAt = null;
  let truncatedEver = false;
  let lastSnapshotAt = null;
  return {
    fold(event) {
      // The EVENT counts as a window seen whatever its payload turns out to be —
      // parity with `byType['workspace-names']`, exactly as `stalls.count`
      // counts a stall whose `lagMs` was unusable. Every field below is then
      // folded only when individually usable (skip-robust, never NaN).
      windowsSeen += 1;
      const { chats, chatCount, truncated, windowEndedAt } = event;
      if (Array.isArray(chats)) {
        for (const name of chats) {
          // A non-string / empty name yields no bucket (the skip-robust rule the
          // client-keyed histograms use); the window is still counted above.
          if (typeof name !== 'string' || name.length === 0) continue;
          names.record(name);
        }
      }
      const finiteWindowEnd = typeof windowEndedAt === 'number' && Number.isFinite(windowEndedAt);
      if (finiteWindowEnd && (lastSnapshotAt === null || windowEndedAt > lastSnapshotAt)) {
        lastSnapshotAt = windowEndedAt;
      }
      if (typeof chatCount === 'number' && Number.isFinite(chatCount) && chatCount >= 0) {
        if (maxChatCount === null || chatCount > maxChatCount) maxChatCount = chatCount;
        // "Most recent" keys off the producer's window clock when it is usable,
        // and degrades to arrival order when it is not — so a window with a
        // malformed `windowEndedAt` still contributes a last-known catalog size
        // rather than silently leaving `lastChatCount` null.
        if (finiteWindowEnd) {
          if (lastChatCountAt === null || windowEndedAt >= lastChatCountAt) {
            lastChatCount = chatCount;
            lastChatCountAt = windowEndedAt;
          }
        } else if (lastChatCountAt === null) {
          lastChatCount = chatCount;
        }
      }
      // The producer's own cap bit: ANY window reporting a partial list flips it,
      // so a reader knows at least one `chats` list was already capped upstream
      // (and therefore that the distinct set is a floor for that window too).
      if (truncated === true) truncatedEver = true;
    },
    snapshot() {
      const nameCounts = names.snapshot();
      return {
        windowsSeen,
        names: nameCounts,
        distinctCount: Object.keys(nameCounts).length,
        maxChatCount,
        lastChatCount,
        truncatedEver,
        lastSnapshotAt,
      };
    },
  };
}

/**
 * Resolve an event's EFFECTIVE observation instant — the single shared rule every
 * time-axis in this module keys off (WARDEN-1428 extracted it; the expression
 * predates it and was previously written out three times).
 *
 * PREFERS the receiver's `receivedAt` (when IT saw the batch, WARDEN-692) and falls
 * back to the client's `timestamp` only when `receivedAt` is absent — so a skewed
 * client clock cannot push an event's apparent time around. Returns `null` (never
 * `0`, never `NaN`) for a non-object entry or a non-finite instant, so every caller
 * gets the same skip-robust "no measurement" sentinel.
 *
 * It is shared rather than copied because `summarize()`'s `lastSeen`,
 * `lastAcceptedInstant()` (which RESTATES that same instant for the liveness
 * verdict) and `_assignTimelineBuckets()`'s window math must never disagree about
 * WHEN an event happened — a divergence there would make `/summary` contradict
 * itself.
 *
 * @param {unknown} event
 * @returns {number | null} the finite effective epoch-ms, or `null`
 * @private
 */
function _effectiveInstant(event) {
  if (!event || typeof event !== 'object') return null;
  const when = event.receivedAt ?? event.timestamp;
  // Number.isFinite is false for every non-number, so this subsumes the typeof guard.
  return Number.isFinite(when) ? when : null;
}

/**
 * The most recent effective instant across a batch of persisted events — i.e. WHEN
 * the newest ACCEPTED event landed (WARDEN-1428).
 *
 * Sibling of `summarize()` / `summarizeTimeline()`: PURE, single-arg, no clock, no
 * fs, no deps. It returns exactly the value `summarize(events).lastSeen` returns
 * for the same array — by construction, since both read `_effectiveInstant` — so it
 * can never drift from the `lastSeen` a caller reads beside it. It exists as its own
 * function because the `/summary` handler needs that instant over the UNSCOPED event
 * array (the channel-liveness question is operational, so a `?platform=` filter must
 * not be able to hide it) while `summarize()` runs over the SCOPED subset; computing
 * it separately is one cheap pass instead of a second full `summarize()`.
 *
 * `null` on an empty store / an array with no finite instant — a genuine ABSENCE of
 * measurement, never `0`, which would read as "an event just arrived at the epoch".
 *
 * @param {unknown} [events]
 * @returns {number | null} the newest effective epoch-ms, or `null`
 */
export function lastAcceptedInstant(events) {
  const list = Array.isArray(events) ? events : [];
  let latest = null;
  for (const event of list) {
    const when = _effectiveInstant(event);
    if (when === null) continue;
    if (latest === null || when > latest) latest = when;
  }
  return latest;
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
 *   operations: Record<string, { count: number, okCount: number, failCount: number,
 *                                min: number | null, avg: number, max: number | null }>,
 *   operationLatency: Record<string, { boundaries: number[], buckets: number[],
 *                                      histogramCount: number, excludedCount: number,
 *                                      p50: number | null, p95: number | null, p99: number | null }>,
 *   workspaceShape: { windowsSeen: number, lastSnapshotAt: number | null,
 *                     counts: Record<string, { windowsSeen: number, min: number | null,
 *                                              avg: number, max: number | null }> },
 *   workspaceNames: { windowsSeen: number, names: Record<string, number>,
 *                     distinctCount: number, maxChatCount: number | null,
 *                     lastChatCount: number | null, truncatedEver: boolean,
 *                     lastSnapshotAt: number | null },
 *   featureUsage: { windowsSeen: number, lastWindowAt: number | null,
 *                   features: Record<string, { count: number, windowsSeen: number }>,
 *                   distinctCount: number },
 *   processMemory: { windowsSeen: number, lastWindowAt: number | null,
 *                    byRuntime: Record<'main' | 'renderer' | 'server', {
 *                      windowsSeen: number, samples: number,
 *                      rssMinBytes: number | null, rssAvgBytes: number | null,
 *                      rssMaxBytes: number | null, heapUsedMaxBytes: number | null,
 *                      peak: { rssMaxBytes: number, processAgeMs: number | null, windowEndedAt: number | null } | null,
 *                      latest: { rssAvgBytes: number | null, rssMaxBytes: number | null, processAgeMs: number | null, windowEndedAt: number } | null }> },
 *   operationRejections: { windowsSeen: number,
 *                          byRuntime: Record<'main' | 'renderer' | 'server', {
 *                            windowsSeen: number, windowsWithRejections: number,
 *                            rejectedTotal: number | null,
 *                            rejectedStaleTotal: number | null, rejectedInvalidTotal: number | null,
 *                            lastRejectedAt: number | null }> },
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
  // Per-operation aggregates (WARDEN-1435): the per-operation tallies the
  // `operational-metrics` COUNT (byType) discards — every window entry's
  // count / ok / fail / min / weighted-avg / max folded by operation NAME into
  // bounded buckets (see _foldOperations + OPERATIONS_SUMMARY_CAP). Populated
  // in the event loop below, snapshotted into the `operations` return key.
  const operationsByName = new Map();
  // Per-operation latency distribution (WARDEN-1500): the PER-SCALE histogram
  // sibling of operationsByName, keyed by the SAME (bounded / overflow-folded)
  // name key so `operationLatency` and `operations` always share a key set.
  const latencyByName = new Map();
  // Workspace aggregates (WARDEN-1473): the two workspace event types' payloads
  // that the COUNT (byType) discards. `workspaceShapeCounts` folds every
  // `workspace-shape` window's counts per count NAME (see _foldWorkspaceShape);
  // `workspaceNames` folds every `workspace-names` window's catalog into the
  // bounded distinct-name set + the TRUE catalog sizes, which is what makes
  // `distinctCount < maxChatCount` — the identically-named-chats verdict —
  // readable in one query. Both snapshotted into their return keys below.
  const workspaceShapeCounts = new Map();
  let workspaceShapeWindows = 0;
  let workspaceShapeLastAt = null;
  const workspaceNamesAcc = _createWorkspaceNamesAccumulator();
  // Feature-usage aggregate (WARDEN-1488): per-capability count + windowsSeen.
  const featureUsageAcc = _createFeatureUsageAccumulator();
  const processMemoryAcc = _createProcessMemoryAccumulator();
  const operationRejectionsAcc = _createOperationRejectionsAccumulator();
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

    // `timestamp` is deliberately NOT destructured here: the ONLY consumer of it in
    // this loop was the time-bounds fallback, which now reads it through the shared
    // `_effectiveInstant(event)` helper (WARDEN-1428).
    const { type, name, schemaVersion, appVersion, platform, runtime, reason, lagMs, source, operations, boundaries } = event;

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
    // Per-operation aggregate (WARDEN-1435): fold this window's `operations[]`
    // into the bounded per-name accumulators. Skip-robust — a malformed or
    // partial entry is skipped inside _foldOperations and never poisons an
    // aggregate, while the EVENT is already counted in byType above (the two
    // are independent, exactly like a crash's reason and its byType count).
    if (type === 'operational-metrics') {
      _foldOperations(operations, operationsByName, latencyByName, boundaries);
      operationRejectionsAcc.fold(event); // WARDEN-1519: skip-robust; the window still counts in windowsSeen
    }
    // Workspace aggregates (WARDEN-1473): fold this window's payload into the
    // shape / names accumulators. Skip-robust on exactly the `_foldOperations`
    // terms — a malformed or partial field is skipped inside the fold and never
    // poisons an aggregate, while the EVENT is already counted in byType above
    // (the two are independent, exactly like a crash's reason and its byType
    // count). `windowsSeen` here counts EVERY event of the type, so it keeps
    // parity with `byType` no matter how unusable one window's payload was.
    if (type === 'workspace-shape') {
      workspaceShapeWindows += 1;
      _foldWorkspaceShape(event, workspaceShapeCounts);
      // Freshness: the greatest window-close instant seen, read from the
      // PRODUCER's own window clock (not the receiver's arrival time), so a
      // batch persisted out of order still reports the LATEST snapshot.
      const endedAt = event.windowEndedAt;
      if (typeof endedAt === 'number' && Number.isFinite(endedAt) && (workspaceShapeLastAt === null || endedAt > workspaceShapeLastAt)) {
        workspaceShapeLastAt = endedAt;
      }
    }
    if (type === 'workspace-names') {
      workspaceNamesAcc.fold(event);
    }
    if (type === 'feature-usage') {
      featureUsageAcc.fold(event); // skip-robust; the event still counts in windowsSeen
    }
    if (type === 'process-memory') {
      processMemoryAcc.fold(event); // skip-robust; the event still counts in windowsSeen
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
    // fallback, so nothing regresses and no migration is needed. The rule itself
    // lives in the shared `_effectiveInstant` helper (WARDEN-1428) so `lastSeen`
    // here and `lastAcceptedInstant()` — which RESTATES this instant inside the
    // `/summary` liveness verdict — can never disagree about WHEN an event happened.
    const when = _effectiveInstant(event);
    if (when !== null) {
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

  // Per-operation snapshot (WARDEN-1435): insertion order = the order names first
  // appeared (mirrors stalls.bySource; deepEqual is order-insensitive, so tests
  // are stable). Bounded at OPERATIONS_SUMMARY_CAP + 1 keys no matter what any
  // client sent.
  const operations = Object.fromEntries(
    [...operationsByName.entries()].map(([name, acc]) => [name, _operationSnapshot(acc)])
  );

  // Per-operation latency snapshot (WARDEN-1500): one distribution per name in
  // `operations` (same keys, same order). Percentiles are bucket UPPER BOUNDS.
  const operationLatency = Object.fromEntries(
    [...operationsByName.keys()].map((name) => [name, _latencySnapshot(latencyByName.get(name) ?? _newLatencyAccumulator())])
  );

  // Workspace-shape rollup (WARDEN-1473): `windowsSeen` is EVERY shape event (it
  // MUST equal byType['workspace-shape'] so the count and payload surfaces
  // agree), `lastSnapshotAt` is the newest window-close instant, and `counts`
  // carries one min/avg/max snapshot per schema count. The `counts` key set is
  // STABLE over WORKSPACE_SHAPE_COUNTS — every count is present even when no
  // window ever carried a usable value for it (the `byType` zeroed-shape
  // posture), in which case its own `windowsSeen` is 0 and min/max are `null`
  // rather than fabricated zeros.
  const workspaceShape = {
    windowsSeen: workspaceShapeWindows,
    lastSnapshotAt: workspaceShapeLastAt,
    counts: Object.fromEntries(
      WORKSPACE_SHAPE_COUNTS.map((key) => [
        key,
        _workspaceCountSnapshot(workspaceShapeCounts.get(key) ?? _newWorkspaceCountAccumulator()),
      ])
    ),
  };

  // Workspace-names rollup (WARDEN-1473): `windowsSeen` is EVERY names event (it
  // MUST equal byType['workspace-names']); `names` is the bounded distinct set in
  // first-seen insertion order (mirrors stalls.bySource / operations; deepEqual
  // is order-insensitive, so tests are stable).
  const workspaceNames = workspaceNamesAcc.snapshot();

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
    operations,
    operationLatency,
    workspaceShape,
    workspaceNames,
    featureUsage: featureUsageAcc.snapshot(),
    processMemory: processMemoryAcc.snapshot(),
    operationRejections: operationRejectionsAcc.snapshot(),
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
    // Skip-robust: a non-object entry must not crash the distribution. The
    // effective-instant rule (receivedAt preferred, timestamp fallback, non-finite
    // → null) is the SHARED `_effectiveInstant` helper (WARDEN-1428), so the window
    // math here can never disagree with `summarize()`'s firstSeen/lastSeen.
    const when = _effectiveInstant(event);
    if (when === null) continue;
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
 * TRUST MODEL: the same posture as `summarize()`, and STRICTER on one point —
 * this reads ONLY `event.receivedAt` / `event.timestamp` (both epoch-ms) and
 * emits COUNTS. It never echoes raw events and never touches ANY name field
 * (neither the extended-tier decorations `chatName` / `sessionName` nor the
 * `workspace-names` catalog `summarize()` deliberately aggregates), so there is
 * no path by which an identifier could reach the distribution.
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
 * maxes; it never echoes raw events and never touches ANY name field (neither the
 * extended-tier decorations `chatName` / `sessionName` nor the `workspace-names`
 * catalog `summarize()` deliberately aggregates), so there is no path by which an
 * identifier could reach a bucket.
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
