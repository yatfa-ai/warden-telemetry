// The maintainer read surface's full-fidelity selector (WARDEN-599). Sibling of
// `summarize()`: a PURE function of an event array — no fs, no network, no deps.
// Where `summarize()` REDUCES the events to aggregates (counts / histograms —
// deliberately discarding every diagnostic field), `selectEvents()` returns the
// EVENTS THEMSELVES at full fidelity, BOUNDED to a recent-N window with optional
// type + time filters. It is the drill-down a maintainer uses to inspect the
// actual error/crash/stall payloads (message / frames / reason / lagMs) that
// /summary only counts — in-product, without SSH-ing to the host to hand-parse
// telemetry.ndjson with jq.
//
// ── TRUST MODEL (do not erode) ────────────────────────────────────────────────
// This returns events that ALREADY landed — every one was schema-validated by
// `ingest` AND redacted client-side / pre-collection before it ever reached disk.
// It introduces NO new collection, re-collects nothing, routes to no third party,
// and performs NO server-side redaction (the roadmap forbids relying on it);
// redaction stays client-side / pre-collection, unchanged. It expands NO tier —
// it returns exactly what consent + redaction already allowed to land, full-
// fidelity instead of aggregate. The IDENTICAL trust posture as /summary. Auth is
// INHERITED (the route is gated by the same pre-routing AUTH_TOKEN check as every
// other route), not re-implemented here.
//
// ── BOUND ────────────────────────────────────────────────────────────────────
// The store is append-ordered newest-last and bounded to a 10000-event retention
// cap (WARDEN-579). A read of the whole persisted set could be large, so the
// window is HARD-CAPPED (≤ EVENTS_LIMIT_MAX): a near-full store can never yield a
// multi-MB response. The default is the recent N (EVENTS_LIMIT_DEFAULT).

// The default + hard cap on the recent-N window (WARDEN-599). The cap is the
// single bound that keeps a full-fidelity read O(N) and bounded regardless of how
// full the persisted store is (up to the 10000 retention cap) — the only real
// risk of this route (an unbounded response) is mitigated here.
export const EVENTS_LIMIT_DEFAULT = 100;
export const EVENTS_LIMIT_MAX = 200;

/**
 * Select a bounded, full-fidelity window of recent persisted events.
 *
 * Pure and total: a non-array (or empty) input yields `[]`. Non-object entries
 * (null / primitives / a partial parse) are SKIPPED, not fatal — mirroring
 * `summarize()`'s skip-robustness so a partial read or shape drift can never
 * crash the read and one bad record never taints the window.
 *
 * Filters (all optional, all conjunctive — an event is kept iff it survives all
 * that apply):
 *   - `type`  — keep only events whose `type` matches (e.g. 'error' | 'crash' |
 *     'performance-stall'). A non-string / empty `type` applies no type filter.
 *   - `since` — keep only events whose effective epoch-ms time — `receivedAt` if
 *     present (WARDEN-692), else the client's `timestamp` — is `>= since` (an
 *     ABSOLUTE cutoff). A non-finite `since` applies no time filter; an event
 *     without a finite effective time does not satisfy the window (dropped by the
 *     filter, never a crash).
 *
 * `limit` bounds the window to the NEWEST N events. The store is append-ordered
 * newest-last, so the newest N are the LAST N in arrival order. A missing /
 * non-finite / sub-1 `limit` falls back to the default; a `limit` above
 * the hard cap is clamped to the cap — so the response is always bounded
 * regardless of how full the store is. Sub-1 fractions are grouped with the
 * fallback (not floored to 0) precisely because `slice(-0)` returns the WHOLE
 * array: floor(0.5) is 0, and slicing the last 0 is the entire set, which would
 * defeat the cap.
 *
 * `since` is an ABSOLUTE epoch-ms cutoff, so unlike `applyRetention`'s age window
 * this filter needs no reference clock; the helper stays pure and fs-free — the
 * sibling of `summarize()`, which is a pure single-arg function of the event
 * array. The handler composes it, exactly as it composes `summarize()`.
 *
 * @param {object[]} [events]
 * @param {{ limit?: number, type?: string, since?: number }} [opts]
 * @returns {object[]}
 */
export function selectEvents(events, { limit, type, since } = {}) {
  // Skip-robust up front (mirrors summarize()): drop non-object entries so a
  // partial read or shape drift never crashes the read.
  let list = (Array.isArray(events) ? events : []).filter((e) => e && typeof e === 'object');

  // Type filter — exact match on `type`. A non-string / empty type applies no
  // filter (the common case: no ?type= query param).
  if (typeof type === 'string' && type.length > 0) {
    list = list.filter((e) => e.type === type);
  }

  // Since filter — keep events whose effective time (receivedAt if present,
  // WARDEN-692, else the client's `timestamp`) is finite and at/after the absolute
  // cutoff. Keying off the receiver's receipt time makes "show me events since the
  // deploy" robust to skewed client clocks.
  if (typeof since === 'number' && Number.isFinite(since)) {
    list = list.filter((e) => {
      const when = e.receivedAt ?? e.timestamp;
      return typeof when === 'number' && Number.isFinite(when) && when >= since;
    });
  }

  // Resolve the bound: a missing / non-finite / sub-1 limit → default;
  // above the hard cap → clamped to the cap. A typo or absurd value can never
  // unbound the response. The guard is `>= 1` (not `> 0`): a sub-1 fraction
  // like 0.5 floors to 0, and `slice(-0)` === `slice(0)` returns the WHOLE
  // array — which would bypass the cap on a large store. `>= 1` routes such a
  // value to the default instead, keeping the response bounded for EVERY input.
  let n = EVENTS_LIMIT_DEFAULT;
  if (typeof limit === 'number' && Number.isFinite(limit) && limit >= 1) {
    n = Math.min(Math.floor(limit), EVENTS_LIMIT_MAX);
  }

  // Newest N: the store is append-ordered newest-last, so the newest N are the
  // LAST N. slice(-n) returns exactly the newest N, preserving arrival order
  // within the window (oldest-of-the-window first, newest last).
  return list.slice(-n);
}
