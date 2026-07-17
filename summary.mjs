// The maintainer read surface's pure aggregator (WARDEN-567). Sibling of
// `ingest()`: a PURE function of an event array — no fs, no network, no deps.
// It takes the events read back via the store and returns the AGGREGATE object a
// self-hosting maintainer can act on (counts / histograms only).
//
// ── TRUST MODEL (do not erode) ────────────────────────────────────────────────
// This returns AGGREGATES of events that ALREADY landed — every one was schema-
// validated by `ingest` AND redacted client-side pre-collection before it ever
// reached disk. It introduces NO new data, re-collects nothing, and routes to no
// third party (a local read on the self-hosted receiver). Return aggregates only:
// counts, per-type totals, non-identifying error `name`s, and a schema-version
// histogram. NEVER echo raw events or extended-tier names (chatName / sessionName)
// — those are the only identifiers ever present, and a summary has no need of them.

import { BASE_EVENT_TYPES } from './schema.ts';

// Cap the top-error-names list so a runaway variety of names stays readable.
const TOP_ERROR_NAMES_CAP = 10;

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
