// The maintainer read surface's pure aggregators (WARDEN-567). Sibling of
// `ingest()`: PURE functions of an event array — no fs, no network, no deps.
// They take the events read back via the store and return the AGGREGATE object a
// self-hosting maintainer can act on (counts / histograms only):
//   - `summarize(events)`        → flat aggregates (total / byType / topErrorNames
//                                  / schemaVersions / firstSeen / lastSeen).
//   - `summarizeTimeline(events)` → a bounded temporal distribution (event counts
//                                  per time bucket over a rolling recent window,
//                                  WARDEN-603) so a maintainer can distinguish a
//                                  recent volume spike from a long-running baseline.
//
// ── TRUST MODEL (do not erode) ────────────────────────────────────────────────
// These return AGGREGATES of events that ALREADY landed — every one was schema-
// validated by `ingest` AND redacted client-side pre-collection before it ever
// reached disk. They introduce NO new data, re-collect nothing, and route to no
// third party (a local read on the self-hosted receiver). Return aggregates only:
// counts, per-type totals, non-identifying error `name`s, a schema-version
// histogram, and a counts-only time distribution. NEVER echo raw events or
// extended-tier names (chatName / sessionName) — those are the only identifiers
// ever present, and a summary has no need of them.

import { BASE_EVENT_TYPES } from './schema.ts';

// Cap the top-error-names list so a runaway variety of names stays readable.
const TOP_ERROR_NAMES_CAP = 10;

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
 *   schemaVersions: Record<string, number>,
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
  let total = 0;
  let firstSeen = null;
  let lastSeen = null;

  for (const event of list) {
    // Skip-robust: a non-object entry (null / primitive / a partial parse) must
    // not crash the summary — skip it and keep aggregating the good records.
    if (!event || typeof event !== 'object') continue;
    total += 1;

    const { type, name, schemaVersion, timestamp } = event;

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
    if (typeof timestamp === 'number' && Number.isFinite(timestamp)) {
      if (firstSeen === null || timestamp < firstSeen) firstSeen = timestamp;
      if (lastSeen === null || timestamp > lastSeen) lastSeen = timestamp;
    }
  }

  // Sort by count desc, then name asc for a deterministic order on ties (so the
  // aggregate is stable across reads and trivially assertable in tests).
  const topErrorNames = Object.entries(errorNameCounts)
    .map(([errorName, count]) => ({ name: errorName, count }))
    .sort((a, b) => b.count - a.count || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
    .slice(0, TOP_ERROR_NAMES_CAP);

  return {
    total,
    byType,
    topErrorNames,
    schemaVersions,
    firstSeen,
    lastSeen,
  };
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
 * no real `Date` in tests. Like `summarize()`, it is computed on-the-fly from
 * `timestamp`s on ALREADY-persisted, ALREADY-redacted events; it introduces no
 * new collection, no schema change, and no new identifier.
 *
 * Pure and total: a non-array (or empty) input, or a store with no events in
 * the window, yields a zeroed shape (`buckets: []`) so a quiet receiver reads
 * cleanly — no false alarm, mirroring `byType`'s stable empty shape. Malformed
 * entries (null / primitives / non-objects) and non-finite / out-of-window
 * timestamps are SKIPPED, not fatal — in practice every persisted event is
 * JSON-validated first, but a partial read or shape drift is defended against
 * here so one bad record can never blank the whole distribution.
 *
 * TRUST MODEL: identical to `summarize()` — this reads ONLY `event.timestamp`
 * and emits COUNTS. It never echoes raw events or extended-tier names
 * (`chatName` / `sessionName`); it touches no other field, so there is no path
 * by which an identifier could reach the distribution.
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
  const list = Array.isArray(events) ? events : [];

  // Degenerate config → zeroed shape. The defaults are always valid; this only
  // fires on an explicit bad override, and a malformed knob can never yield a
  // huge/NaN array — it collapses to empty (mirrors summarize()'s defensive totality).
  if (!Number.isFinite(windowMs) || windowMs <= 0 || !Number.isFinite(maxBuckets) || maxBuckets < 1) {
    return { buckets: [], bucketMs: 0 };
  }

  const currentTime = now();
  const bucketMs = windowMs / maxBuckets;
  const windowStart = currentTime - windowMs;

  // Accumulate a COUNT per bucket index over events whose FINITE timestamp falls
  // in the rolling window [windowStart, currentTime]. An event older than the
  // window (or timestamped in the future via client/server clock skew) is
  // excluded from the distribution — it is still counted by `summarize()`'s
  // `total` / `byType` / `firstSeen` / `lastSeen`, which span the full retained set.
  const counts = new Map();
  for (const event of list) {
    // Skip-robust: a non-object entry must not crash the distribution.
    if (!event || typeof event !== 'object') continue;
    const { timestamp } = event;
    if (typeof timestamp !== 'number' || !Number.isFinite(timestamp)) continue;
    if (timestamp < windowStart || timestamp > currentTime) continue;
    let idx = Math.floor((timestamp - windowStart) / bucketMs);
    // The `timestamp === currentTime` edge lands exactly on the top boundary;
    // fold it into the newest bucket rather than dropping it or overflowing.
    if (idx >= maxBuckets) idx = maxBuckets - 1;
    if (idx < 0) idx = 0;
    counts.set(idx, (counts.get(idx) ?? 0) + 1);
  }

  // Emit the non-empty buckets chronologically (oldest → newest). Each is
  // self-locating in time (`bucketStart` / `bucketEnd` epoch-ms) + a count. The
  // bucket count is structurally capped at `maxBuckets`: every in-window event
  // maps to one of at most `maxBuckets` grid slots, so a 10k-event store yields
  // ≤ maxBuckets buckets, never one-per-event. `bucketMs` is always present so
  // the shape is stable (and the granularity legible) even when no bucket fired.
  const buckets = [...counts.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([idx, count]) => {
      const bucketStart = windowStart + idx * bucketMs;
      return { bucketStart, bucketEnd: bucketStart + bucketMs, count };
    });

  return { buckets, bucketMs };
}
