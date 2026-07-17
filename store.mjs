// Durable persistence — an NDJSON store for accepted telemetry events, with a
// maintainer-configurable RETENTION bound so the persisted file does not grow
// unbounded over the lifetime of a self-hosted receiver (WARDEN-579 / roadmap
// WARDEN-446).
//
// Minimal v1 (WARDEN-547): one NDJSON record per accepted event, stored VERBATIM
// (exactly what `ingest` schema-validated is what lands). The maintainer read
// surface — fileSource + readEvents (WARDEN-567) — is in place. Retention
// (WARDEN-579) trims the persisted set to a count cap and/or an age window by
// REWRITING the file through an injected `rewrite` seam (the persisted-seam
// discipline). Multi-version schema support remains a later slice.
//
// THE SEAMS ARE INJECTED. A store is created with:
//   - `sink(line)`     — append one NDJSON line (write).
//   - `source()`       — read back the parsed events (read).
//   - `rewrite(text)`  — replace the ENTIRE file content (retention compaction).
// Production passes `fileSink(path)` / `fileSource(path)` / `fileRewrite(path)`
// (real fs); tests pass capturing fns so the suite touches ZERO real filesystem.
// This mirrors the warden client's injected-seam discipline (telemetry-send.js
// injects fetchImpl/sleepImpl and its tests open no real socket). There is no
// implicit real-fs default — a sink MUST be supplied, which keeps tests hermetic
// by construction (you cannot accidentally write to disk from a test). `source`
// and `rewrite` are optional only because read-only / write-only stores are
// legitimate wirings; calling readEvents()/prune() without the matching seam
// throws a loud TypeError (fail loud, never a silent no-op that hides the bug).
//
// RETENTION (WARDEN-579) — TRUST MODEL (do not erode): pruning ONLY EVER REMOVES
// events. It never expands what a tier collects, routes data elsewhere, or
// touches the pre-collection redaction contract. A prune rewrites the retained
// set through `rewrite`; if nothing exceeds the bound, the rewrite is SKIPPED
// (no churn). `applyRetention` is the PURE, fs-free policy core (unit-testable
// with plain arrays — no seams, no disk), the retention sibling of `parseNdjson`.
//
// CONCURRENCY: mutations (appendEvents / prune) are SERIALIZED per-store so an
// in-flight compaction can't lose a concurrent append (an in-place rename while
// an appendFile is mid-write would drop the just-appended line). The request
// path performs only the fast append; the compaction itself runs OFF the request
// path (the server triggers it via a debounced, re-entrancy-guarded hook — see
// createRetentionTrigger in server.mjs), never synchronously per event.
//
// Zero runtime dependencies (node:fs/promises only).

import { appendFile, readFile, writeFile, rename } from 'node:fs/promises';
import { resolve } from 'node:path';

/**
 * A real-filesystem sink: appends one line (plus `\n`) to `path`, creating the
 * file on first write. Used only by the production server wiring; never reached
 * from tests (they inject their own sink).
 *
 * @param {string} path — NDJSON file path (resolved relative to cwd).
 * @returns {(line: string) => Promise<void>}
 */
export function fileSink(path) {
  const dest = resolve(path);
  return async (line) => {
    await appendFile(dest, `${line}\n`, 'utf8');
  };
}

/**
 * Parse NDJSON text into an array of objects — the PURE, fs-free core of the
 * read seam (so the line-split / skip-malformed discipline is unit-testable
 * without touching real disk). Blank lines are dropped; a line that fails to
 * JSON.parse (e.g. a partial append left by a crashed process mid-write) is
 * SKIPPED rather than aborting the whole read — a future partial-line write must
 * not crash the maintainer summary.
 *
 * @param {string} text
 * @returns {object[]}
 */
export function parseNdjson(text) {
  const events = [];
  for (const line of String(text).split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      events.push(JSON.parse(trimmed));
    } catch {
      // A partial-line write must not poison the whole read — skip the bad line
      // and keep the good records on either side of it.
    }
  }
  return events;
}

/**
 * A real-filesystem source: reads `path` as NDJSON and returns the parsed event
 * objects (the read-side mirror of `fileSink`). A MISSING file returns `[]` — a
 * fresh receiver with no traffic yet has no `telemetry.ndjson`, and that is not
 * an error. Each non-blank line is JSON.parse'd via `parseNdjson` (so a partial
 * line is skipped, not fatal). Used only by the production server wiring; never
 * reached from tests (they inject their own source).
 *
 * @param {string} path — NDJSON file path (resolved relative to cwd).
 * @returns {() => Promise<object[]>}
 */
export function fileSource(path) {
  const dest = resolve(path);
  return async () => {
    let text;
    try {
      text = await readFile(dest, 'utf8');
    } catch (e) {
      // A missing store file is the normal state for a receiver with no traffic
      // yet — surface it as an empty event set, not a throw.
      if (e && e.code === 'ENOENT') return [];
      throw e;
    }
    return parseNdjson(text);
  };
}

/**
 * A real-filesystem REWRITE seam: ATOMICALLY replace the entire NDJSON file
 * content with `text` (the full retained set, re-serialized by `serializeNdjson`).
 * Writes a sibling temp file then renames it over the original — `rename` is
 * atomic on POSIX (and same-volume Windows), so a crash mid-compaction leaves
 * either the old-complete file or the new-complete file, never a truncated/partial
 * one (a partial read is already tolerated by `parseNdjson`; atomic rename makes
 * that path unreachable here). The temp name is fixed (`.tmp` suffix) because
 * rewrites are serialized by the store, so no two rewrites contend. Used only by
 * the production server wiring; never reached from tests (they inject their own
 * rewrite fn).
 *
 * @param {string} path — NDJSON file path (resolved relative to cwd).
 * @returns {(text: string) => Promise<void>}
 */
export function fileRewrite(path) {
  const dest = resolve(path);
  const tmp = `${dest}.tmp`;
  return async (text) => {
    await writeFile(tmp, text, 'utf8');
    await rename(tmp, dest);
  };
}

/**
 * The PURE, fs-free retention policy core (so the count-cap / age-window
 * discipline is unit-testable with plain arrays — no seams, no disk). A retention
 * bound REMOVES events only (never reorders, never invents):
 *   - Age window (`maxAgeMs` > 0): drop any event whose finite epoch-ms
 *     `timestamp` is older than `now - maxAgeMs`. An event WITHOUT a finite
 *     timestamp is KEPT (age can't be judged — prefer retaining over silently
 *     dropping a record the schema otherwise accepted).
 *   - Count cap (`maxEvents` > 0): keep the LAST `maxEvents` events (newest by
 *     arrival/append order — the store is append-ordered), dropping the older
 *     excess off the front.
 * Both apply when set (an event is retained iff it survives BOTH). `maxEvents: 0`
 * / `maxAgeMs: 0` disable that policy; both 0 = retain everything (the explicit
 * unbounded escape hatch — the DEFAULT is bounded, set by the server wiring).
 *
 * @param {object[]} [events]
 * @param {{ maxEvents?: number, maxAgeMs?: number, now?: number }} [opts]
 * @returns {object[]}
 */
export function applyRetention(events, { maxEvents = 0, maxAgeMs = 0, now = 0 } = {}) {
  let kept = Array.isArray(events) ? events.slice() : [];

  if (maxAgeMs > 0 && Number.isFinite(maxAgeMs)) {
    const cutoff = now - maxAgeMs;
    kept = kept.filter((e) => {
      // No finite timestamp → age is unknowable → retain (never silently drop).
      if (!e || typeof e.timestamp !== 'number' || !Number.isFinite(e.timestamp)) return true;
      return e.timestamp >= cutoff;
    });
  }

  if (maxEvents > 0 && Number.isFinite(maxEvents) && kept.length > maxEvents) {
    // Keep the newest-by-arrival N: drop the older excess off the front.
    kept = kept.slice(kept.length - maxEvents);
  }

  return kept;
}

// Re-serialize retained events as NDJSON text — one JSON record per line, each
// terminated by `\n` (byte-identical in shape to what `fileSink` appends line by
// line, so a compacted file reads back exactly like a freshly-appended one).
// Round-tripping through JSON.stringify is stable for these records: they are
// plain JSON objects with string keys, and V8 preserves insertion order through
// parse → stringify, so the bytes are unchanged by a compaction that drops lines.
function serializeNdjson(events) {
  if (!Array.isArray(events) || events.length === 0) return '';
  let text = '';
  for (const event of events) text += `${JSON.stringify(event)}\n`;
  return text;
}

/**
 * Create an NDJSON store backed by the given `sink` (write), and optionally a
 * `source` (read) and `rewrite` (retention compaction).
 *
 * `source` / `rewrite` are optional for backward-compat with write-only and
 * read-only callers, BUT `readEvents()`/`prune()` always exist: on a store
 * created WITHOUT the matching seam, calling them throws a loud `TypeError` (a
 * read/prune on a store missing that seam is a wiring bug — fail loud, never a
 * silent [] / silent no-op, mirroring `createRequestHandler`'s "requires a store"
 * discipline).
 *
 * All mutations (`appendEvents`, `prune`) are SERIALIZED per-store: an in-flight
 * compaction and a concurrent append never interleave (an in-place `rename` while
 * `appendFile` is writing would otherwise drop the appended line).
 *
 * @param {{ sink: (line: string) => (void | Promise<void>), source?: () => (object[] | Promise<object[]>), rewrite?: (text: string) => (void | Promise<void>) }} opts
 * @returns {{ appendEvents(events: unknown[]) => Promise<void>, readEvents() => Promise<object[]>, prune(opts?: object) => Promise<{ before: number, after: number, pruned: number, rewrote: boolean }> }}
 */
export function createNdjsonStore({ sink, source, rewrite } = {}) {
  if (typeof sink !== 'function') {
    throw new TypeError(
      'createNdjsonStore: an injected `sink(line)` function is required ' +
        '(pass fileSink(path) in production, a capturing fn in tests).'
    );
  }

  // Per-store serialization of all MUTATIONS (append + prune). A compaction
  // rewrites the whole file via `rename`; an append appends a line. Running them
  // strictly in turn means an append that lands during a compaction is applied to
  // the file the compaction reads (or queued after it) — it is never lost to an
  // in-place rename. `runSerialized` propagates each op's real outcome to its
  // caller while keeping the chain alive for the next op regardless of failure.
  let chain = Promise.resolve();
  function runSerialized(op) {
    const result = chain.then(op, op);
    chain = result.then(
      () => {},
      () => {}
    );
    return result;
  }

  return {
    /**
     * Persist every event in an accepted batch as its own NDJSON line, in order.
     * The events are already schema-validated by `ingest` before this is called,
     * so each line is the JSON-serialized event verbatim.
     *
     * @param {unknown[]} events
     */
    appendEvents(events) {
      // Serialized: a concurrent prune can't rename the file out from under us.
      return runSerialized(async () => {
        for (const event of events) {
          // JSON.stringify is the exact bytes a maintainer reads back; the sink
          // appends the newline on write so captured test lines are pure records.
          await sink(JSON.stringify(event));
        }
      });
    },

    /**
     * Read every persisted event back via the injected `source`. Throws a loud
     * `TypeError` if the store was created without a `source` (write-only store)
     * — see the JSDoc on `createNdjsonStore`.
     *
     * @returns {Promise<object[]>}
     */
    readEvents() {
      if (typeof source !== 'function') {
        throw new TypeError(
          'createNdjsonStore: readEvents() requires an injected `source()` ' +
            'function (pass fileSource(path) in production, a capturing fn in tests).'
        );
      }
      return Promise.resolve(source());
    },

    /**
     * Apply the retention bound to the PERSISTED store: read the current events,
     * drop the ones outside the count cap / age window (REMOVAL only), and — ONLY
     * if anything was actually dropped — REWRITE the file with the retained set
     * via the injected `rewrite` seam. A no-op (nothing to prune) performs NO
     * rewrite (no churn, no disk write). Throws a loud `TypeError` if `source` or
     * `rewrite` is missing (a prune on a store missing a seam is a wiring bug).
     *
     * Serialized against `appendEvents`, so a concurrent append is never lost.
     *
     * Async so a missing-seam misconfiguration surfaces as a REJECTED promise
     * (not a synchronous throw) — the server's maintenance trigger drives prune
     * through `Promise.resolve(...).catch(...)`, which only catches rejections.
     *
     * @param {{ maxEvents?: number, maxAgeMs?: number, now?: number }} [opts]
     * @returns {Promise<{ before: number, after: number, pruned: number, rewrote: boolean }>}
     */
    async prune({ maxEvents = 0, maxAgeMs = 0, now = Date.now() } = {}) {
      if (typeof source !== 'function') {
        throw new TypeError(
          'createNdjsonStore: prune() requires an injected `source()` function ' +
            '(pass fileSource(path) in production, a capturing fn in tests).'
        );
      }
      if (typeof rewrite !== 'function') {
        throw new TypeError(
          'createNdjsonStore: prune() requires an injected `rewrite(text)` function ' +
            '(pass fileRewrite(path) in production, a capturing fn in tests).'
        );
      }
      return runSerialized(async () => {
        const events = await source();
        const retained = applyRetention(events, { maxEvents, maxAgeMs, now });
        // Nothing removed → no rewrite (a no-op prune must not churn the file).
        if (retained.length === events.length) {
          return { before: events.length, after: retained.length, pruned: 0, rewrote: false };
        }
        await rewrite(serializeNdjson(retained));
        return {
          before: events.length,
          after: retained.length,
          pruned: events.length - retained.length,
          rewrote: true,
        };
      });
    },
  };
}
