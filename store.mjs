// Durable persistence — an append-only NDJSON store for accepted telemetry events.
//
// Minimal v1 (per WARDEN-547 / roadmap WARDEN-446): a maintainer can read the
// signal back from disk. One NDJSON record per accepted event, stored VERBATIM
// (exactly what `ingest` schema-validated is what lands). The maintainer
// read/query surface — fileSource + readEvents (WARDEN-567) — is now in place;
// retention / rotation / compaction are the remaining later slices.
//
// THE SEAM IS INJECTED. A store is created with a `sink(line)` function (write)
// and, for the read surface, a `source()` function (read): the production wiring
// passes `fileSink(path)` / `fileSource(path)` (real fs); tests pass capturing
// fns (e.g. an array push / an array read) so the suite touches ZERO real
// filesystem. This mirrors the warden client's injected-seam discipline
// (telemetry-send.js injects fetchImpl/sleepImpl and its tests open no real
// socket). There is no implicit real-fs default — a sink MUST be supplied, which
// keeps tests hermetic by construction (you cannot accidentally write to disk
// from a test).
//
// Zero runtime dependencies (node:fs/promises only).

import { appendFile, readFile } from 'node:fs/promises';
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
 * Create an append-only NDJSON store backed by the given `sink` (write) and,
 * optionally, a `source` (read).
 *
 * `source` is optional for backward-compat with write-only callers (the ingest
 * tests inject only a sink), BUT `readEvents()` always exists: on a store
 * created WITHOUT a `source`, calling `readEvents()` throws a loud `TypeError`
 * (a read on a write-only store is a wiring bug — fail loud, never a silent [],
 * mirroring `createRequestHandler`'s "requires a store" discipline).
 *
 * @param {{ sink: (line: string) => (void | Promise<void>), source?: () => (object[] | Promise<object[]>) }} opts
 * @returns {{ appendEvents(events: unknown[]) => Promise<void>, readEvents() => Promise<object[]> }}
 */
export function createNdjsonStore({ sink, source } = {}) {
  if (typeof sink !== 'function') {
    throw new TypeError(
      'createNdjsonStore: an injected `sink(line)` function is required ' +
        '(pass fileSink(path) in production, a capturing fn in tests).'
    );
  }
  return {
    /**
     * Persist every event in an accepted batch as its own NDJSON line, in order.
     * The events are already schema-validated by `ingest` before this is called,
     * so each line is the JSON-serialized event verbatim.
     *
     * @param {unknown[]} events
     */
    async appendEvents(events) {
      for (const event of events) {
        // JSON.stringify is the exact bytes a maintainer reads back; the sink
        // appends the newline on write so captured test lines are pure records.
        await sink(JSON.stringify(event));
      }
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
  };
}
