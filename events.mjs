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
// The CONJUNCTIVE FILTER CORE (skip-robust + type/platform/appVersion/signature/
// since) is extracted as `filterEvents()` (WARDEN-727) so the two read surfaces —
// `selectEvents` (the /events drill-down) AND the /summary handler (the scoped-
// OVERVIEW complement) — select the SAME events for the SAME query forever.
// `selectEvents` calls `filterEvents` and then bounds the matches to the newest-N
// window (the part /summary does NOT want — it aggregates the whole scoped set).
// `filterEvents` is the canonical owner of the filter semantics; `selectEvents`
// owns the bound. (`signature` rides the shared core but is wired on /events only
// — WARDEN-746; /summary does not pass it, so the core's semantics stay shared
// while the drill-down filter stays an /events concern.)
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

// `signatureOf` (summary.mjs) is the single source of truth for the failure-
// signature key /summary ranks `topSignatures` by (WARDEN-707). Reusing it here
// keeps the GET /events?signature= drill-down (WARDEN-746) byte-identical to what
// /summary COUNTS — no drift between the ranked key and the filtered key (drift
// would silently break the drill-down). One-directional edge events → summary;
// summary already imports ./schema.ts only, so this introduces NO import cycle.
import { signatureOf } from './summary.mjs';

// The default + hard cap on the recent-N window (WARDEN-599). The cap is the
// single bound that keeps a full-fidelity read O(N) and bounded regardless of how
// full the persisted store is (up to the 10000 retention cap) — the only real
// risk of this route (an unbounded response) is mitigated here.
export const EVENTS_LIMIT_DEFAULT = 100;
export const EVENTS_LIMIT_MAX = 200;

/**
 * Resolve the page bound N for a /events window (≤ EVENTS_LIMIT_MAX). Shared by
 * `selectEvents` (the bound it applies) and the /events handler (the `limit` it
 * echoes) so the echoed bound is provably the one that shaped the response — the
 * two can never drift on what a "page of N" means, the way `filterEvents` keeps
 * them from drifting on what a filter matches. PURE: a function of the raw
 * limit value + the exported constants — no fs, no network, no deps.
 *
 * A missing / non-finite / sub-1 `limit` → EVENTS_LIMIT_DEFAULT; otherwise
 * floor + clamp to the hard cap. The guard is `>= 1` (not `> 0`): a sub-1
 * fraction like 0.5 floors to 0, and `slice(-0)` === `slice(0)` returns the
 * WHOLE array — which would bypass the cap on a large store. `>= 1` routes such
 * a value to the default instead, keeping the response bounded for EVERY input.
 *
 * @param {number} [limit]
 * @returns {number}
 */
export function resolveLimit(limit) {
  if (typeof limit === 'number' && Number.isFinite(limit) && limit >= 1) {
    return Math.min(Math.floor(limit), EVENTS_LIMIT_MAX);
  }
  return EVENTS_LIMIT_DEFAULT;
}

/**
 * Resolve the /events page offset — how many of the NEWEST matches to skip
 * before taking the next page (WARDEN-755). Shared by `selectEvents` (the skip
 * it applies) and the /events handler (the `offset` it echoes) so the echoed
 * offset is provably the one that shaped the response. PURE: a function of the
 * raw offset + the matched-set size — no fs, no network, no deps, no reference
 * clock.
 *
 * A missing / non-finite / sub-0 `offset` → 0 (today's newest-N, byte-identical
 * — backward compatible); otherwise floor + clamp to `matched` (the filtered
 * set length). Clamping to `matched` keeps a pathological `offset=1e9` from
 * reaching `slice()` with a huge index AND makes the past-end edge uniform:
 * `selectEvents` returns `[]` when the resolved skip >= the matched length (no
 * older page to show).
 *
 * @param {number} [offset]
 * @param {number} matched — the filtered set length (skip is clamped to this)
 * @returns {number}
 */
export function resolveOffset(offset, matched) {
  if (typeof offset === 'number' && Number.isFinite(offset) && offset > 0) {
    return Math.min(Math.floor(offset), matched);
  }
  return 0;
}

/**
 * The conjunctive filter core shared by the read surfaces (WARDEN-727).
 *
 * Selects the subset of `events` matching ALL of the supplied filters — the exact
 * core `selectEvents` applies before its newest-N bound, factored out so the
 * `/summary` handler can scope its aggregates with the IDENTICAL semantics (a
 * maintainer who scopes `/summary?platform=win32` and `/events?platform=win32`
 * sees the two surfaces agree on what "win32" means, forever). PURE: a function
 * of the event array + the filter opts — no fs, no network, no deps, no reference
 * clock. Returns a NEW array (the matching subset in input order); it never
 * mutates the input.
 *
 * Pure and total: a non-array (or empty) input yields `[]`. Non-object entries
 * (null / primitives / a partial parse) are SKIPPED, not fatal — mirroring
 * `summarize()`'s skip-robustness so a partial read or shape drift can never
 * crash the read and one bad record never taints the subset.
 *
 * Filters (all optional, all conjunctive — an event is kept iff it survives all
 * that apply):
 *   - `type`       — keep only events whose `type` matches (e.g. 'error' | 'crash' |
 *     'performance-stall'). A non-string / empty `type` applies no type filter.
 *   - `platform`   — keep only events whose `platform` OS label matches (one of
 *     'darwin' | 'win32' | 'linux', WARDEN-684). Exact match; an event whose source
 *     omitted the field (`platform === undefined`) is excluded — a maintainer asking
 *     "show me win32" does not want un-attributed events (mirrors `type` semantics
 *     exactly). A non-string / empty `platform` applies no filter.
 *   - `appVersion` — keep only events whose `appVersion` release label matches
 *     (e.g. '0.1.19', WARDEN-665). Exact match; same omit-excluded + guard
 *     semantics as `platform`.
 *   - `signature`  — keep only events whose DERIVED failure signature (signatureOf,
 *     summary.mjs — byte-identical to the keys /summary ranks `topSignatures` by,
 *     WARDEN-707) exactly matches. The failure-axis drill-down (WARDEN-746): step
 *     from a /summary.topSignatures ranking straight into THAT distinct failure's
 *     payloads. A non-string / empty `signature` applies no filter; an event whose
 *     signatureOf() is null (an unknown type, or a type-specific field gap — a
 *     nameless error, a reasonless crash, a sourceless stall) never matches — never
 *     crashes (mirrors the skip-robustness of every other filter). NOTE: wired on
 *     /events only; /summary does not pass it (scoping the aggregates to one
 *     signature is not meaningful), so this filter is an /events-only drill-down
 *     that merely RIDES the shared core.
 *   - `since`      — keep only events whose effective epoch-ms time — `receivedAt` if
 *     present (WARDEN-692), else the client's `timestamp` — is `>= since` (an
 *     ABSOLUTE cutoff). A non-finite `since` applies no time filter; an event
 *     without a finite effective time does not satisfy the window (dropped by the
 *     filter, never a crash).
 *
 * `since` is an ABSOLUTE epoch-ms cutoff, so unlike `applyRetention`'s age window
 * this filter needs no reference clock; the helper stays pure and fs-free. This is
 * the sibling of `summarize()`'s "pure single-arg function of the event array"
 * contract: the filter lives OUTSIDE the aggregator, composed by the handler.
 *
 * @param {object[]} [events]
 * @param {{ type?: string, since?: number, platform?: string, appVersion?: string, signature?: string }} [opts]
 * @returns {object[]}
 */
export function filterEvents(events, { type, since, platform, appVersion, signature } = {}) {
  // Skip-robust up front (mirrors summarize()): drop non-object entries so a
  // partial read or shape drift never crashes the read.
  let list = (Array.isArray(events) ? events : []).filter((e) => e && typeof e === 'object');

  // Type filter — exact match on `type`. A non-string / empty type applies no
  // filter (the common case: no ?type= query param).
  if (typeof type === 'string' && type.length > 0) {
    list = list.filter((e) => e.type === type);
  }

  // Platform filter — exact match on the OS label (WARDEN-684). A non-string /
  // empty platform applies no filter (the common case: no ?platform= param). An
  // event whose source omitted the field (`platform === undefined`) correctly
  // fails the match and is excluded — a maintainer asking "show me win32" does not
  // want un-attributed events (mirrors ?type= semantics exactly).
  if (typeof platform === 'string' && platform.length > 0) {
    list = list.filter((e) => e.platform === platform);
  }

  // appVersion filter — exact match on the release label (WARDEN-665). Same guard
  // and omit-excluded semantics as `platform`.
  if (typeof appVersion === 'string' && appVersion.length > 0) {
    list = list.filter((e) => e.appVersion === appVersion);
  }

  // Signature filter — keep only events whose DERIVED failure signature
  // (signatureOf, summary.mjs — byte-identical to the keys /summary ranks
  // topSignatures by, WARDEN-707) exactly matches. The failure-axis drill-down
  // (WARDEN-746): a maintainer who spots a high-count distinct failure on
  // /summary copies its `signature` here to read THAT failure's actual payloads
  // (message + frames / reason / lagMs) instead of eyeballing ?type=error mixed
  // with every other error in the window. A non-string / empty signature applies
  // no filter (the common case: no ?signature= param). An event whose
  // signatureOf() is null (an unknown type, or a type-specific field gap — a
  // nameless error, a reasonless crash, a sourceless stall) simply never matches,
  // never crashes (mirrors the skip-robustness of every other filter). The filter
  // key is the SAME function /summary ranks by, so the round-trip
  // /summary.topSignatures[].signature → /events?signature= is exact (no drift
  // between what is counted and what is filtered).
  if (typeof signature === 'string' && signature.length > 0) {
    list = list.filter((e) => signatureOf(e) === signature);
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

  return list;
}

/**
 * Select a bounded, full-fidelity window of recent persisted events.
 *
 * Filtering delegates to the shared `filterEvents` core (WARDEN-727) — see its
 * doc for the conjunctive / omit-excluded / `receivedAt ?? timestamp` semantics,
 * which are identical to `/summary`'s scoped aggregates. `selectEvents` then owns
 * the part `/summary` does NOT want: the newest-N bound that keeps a near-full
 * store from yielding a multi-MB full-fidelity response.
 *
 * Pure and total: a non-array (or empty) input yields `[]`. Non-object entries
 * (null / primitives / a partial parse) are SKIPPED by `filterEvents`, not fatal —
 * mirroring `summarize()`'s skip-robustness so a partial read or shape drift can
 * never crash the read and one bad record never taints the window.
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
 * `offset` (WARDEN-755) pages OLDER matches past the newest-N window. It is how
 * many of the NEWEST matches to SKIP before taking the next page of N — the
 * drill-down twin of `/summary`'s `matched` count, closing the silent-truncation
 * gap where a maintainer reading `/events?type=error` against a 847-match subset
 * could not tell whether the newest-200 was the whole set or a truncation, nor
 * reach the older 647. A missing / non-finite / sub-0 `offset` → `0` (today's
 * newest-N, BYTE-IDENTICAL — backward compatible). A pathological `offset` is
 * clamped to `filtered.length` so a giant value never reaches `slice()`; once an
 * offset skips to or past the end of the matched set (`offset >= filtered.length`)
 * it returns `[]` (no older page). The bound N STAYS ≤ EVENTS_LIMIT_MAX on every
 * page — `offset` only selects WHICH bounded slice; the response never grows.
 *
 * @param {object[]} [events]
 * @param {{ limit?: number, offset?: number, type?: string, since?: number, platform?: string, appVersion?: string, signature?: string }} [opts]
 * @returns {object[]}
 */
export function selectEvents(events, { limit, offset, type, since, platform, appVersion, signature } = {}) {
  // The conjunctive filter core (skip-robust + type/platform/appVersion/signature/
  // since) is the SHARED `filterEvents` helper (WARDEN-727) — the same one the
  // /summary handler calls — so /events and /summary filter identically forever.
  // selectEvents then bounds the matches to the newest-N window (the part /summary
  // does NOT want: it aggregates the whole scoped set, not a bounded tail). The
  // `signature` filter rides this shared core but is passed by /events only; /summary
  // does not forward it (WARDEN-746), so its semantics stay shared while the
  // drill-down stays an /events concern.
  const filtered = filterEvents(events, { type, since, platform, appVersion, signature });

  // The bound N and the page offset `skip` are resolved via the SHARED
  // `resolveLimit` / `resolveOffset` helpers (WARDEN-755) — the SAME helpers the
  // /events handler calls to echo `limit` / `offset` — so the response's echoed
  // bound is provably the one that shaped `events`. The two surfaces can never
  // drift on what "a page of N, offset by skip" means, the way filterEvents
  // already keeps them from drifting on what a filter matches.
  const n = resolveLimit(limit);
  const skip = resolveOffset(offset, filtered.length);

  // No older page to show: the offset has skipped to or past the end of the
  // matched set. Bounded and explicit (never a negative-range slice), so a
  // maintainer paging `?offset=200&offset=400…` on a finite matching set stops
  // cleanly at `[]` once every match has been traversed.
  if (skip >= filtered.length) return [];

  // Newest N (offset 0): the store is append-ordered newest-last, so the newest
  // N are the LAST N. slice(-n) returns exactly the newest N, preserving arrival
  // order within the window (oldest-of-the-window first, newest last) —
  // BYTE-IDENTICAL to the pre-paging behavior (backward compatible).
  //
  // Older page (offset > 0): skip the newest `skip` matches and take the next N
  // before them. slice(-(skip + n), -skip) drops the trailing `skip` (the
  // already-paged newest matches) and bounds the remainder to N — e.g. skip=200,
  // n=100 on a 500-match set takes matches [-300:-200], the page right before the
  // newest-200. The bound N stays ≤ EVENTS_LIMIT_MAX on EVERY page; `offset`
  // only selects which bounded slice — the response never exceeds the cap.
  // (offset 0 is special-cased because slice(-n, -0) === slice(-n, 0) === [],
  // NOT the newest N — -0 collapses to 0 and an explicit 0 end empties the slice.)
  return skip === 0 ? filtered.slice(-n) : filtered.slice(-(skip + n), -skip);
}
