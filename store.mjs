// Durable persistence — an append-only NDJSON store for accepted telemetry events.
//
// Minimal v1 (per WARDEN-547 / roadmap WARDEN-446): a maintainer can read the
// signal back from disk. One NDJSON record per accepted event, stored VERBATIM
// (exactly what `ingest` schema-validated is what lands). Retention / rotation /
// compaction and a query API are explicitly later slices — this is the keystone.
//
// THE SEAM IS INJECTED. A store is created with a `sink(line)` function: the
// production wiring passes `fileSink(path)` (real fs); tests pass a capturing
// sink (e.g. an array push) so the suite touches ZERO real filesystem. This
// mirrors the warden client's injected-seam discipline (telemetry-send.js injects
// fetchImpl/sleepImpl and its tests open no real socket). There is no implicit
// real-fs default — a sink MUST be supplied, which keeps tests hermetic by
// construction (you cannot accidentally write to disk from a test).
//
// Zero runtime dependencies (node:fs/promises only).

import { appendFile } from 'node:fs/promises';
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
 * Create an append-only NDJSON store backed by the given `sink`.
 *
 * @param {{ sink: (line: string) => (void | Promise<void>) }} opts
 * @returns {{ appendEvents(events: unknown[]) => Promise<void> }}
 */
export function createNdjsonStore({ sink } = {}) {
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
  };
}
