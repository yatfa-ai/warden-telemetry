// The ingest pipeline — a PURE function of `{ headers, body }` plus injected
// dependencies `{ SCHEMA_VERSION, validateEvent, store }`. This is the slice's
// observation point (WARDEN-547): the handler trace lives here —
//
//   1. SCHEMA HANDSHAKE  — read x-telemetry-schema; reject unknown version
//                          WITHOUT parsing the body.
//   2. PARSE             — the { schemaVersion, events } JSON body.
//   3. VALIDATE          — run the shared validateEvent on EVERY event; hard-
//                          reject the whole batch if ANY is out-of-schema.
//   4. PERSIST           — append the accepted batch via the injected store.
//   5. RESPOND           — 2xx on success.
//
// It is factored for injection (the client's discipline, telemetry-send.js) so
// the suite asserts on inputs/outputs with ZERO real network and a store that
// touches ZERO real filesystem. `validateEvent` is the client's shared schema
// validator, vendored verbatim in ./schema.ts — NEVER a hand-rolled parallel
// copy (the roadmap's drift guard: two validators that disagree IS the drift).
//
// ── REJECTION DISCIPLINE (load-bearing) ──────────────────────────────────────
// EVERY rejection is a NON-RETRYABLE 4xx — never 429, never 5xx. The warden
// client retries 429 / 5xx / network errors (bounded ≤3) but DROPs the batch
// fail-fast on any OTHER 4xx (telemetry-send.js isTransientStatus + the 4xx-drop
// path; tested for 400/401/403/404/422). A schema/shape rejection MUST make the
// client DROP — retrying the identical bad batch forever would be worse — so this
// function returns only 4xx (or 202) statuses. See the `never retryable` test.

// Case-insensitive header lookup. Node lowercases `req.headers`, but be robust to
// a proxy or caller that hands back the original casing.
function readHeader(headers, name) {
  const lower = name.toLowerCase();
  for (const key of Object.keys(headers || {})) {
    if (key.toLowerCase() === lower) return headers[key];
  }
  return undefined;
}

// Build a non-retryable 4xx rejection result.
function reject(status, error) {
  return { ok: false, status, body: { error } };
}

/**
 * Ingest one telemetry batch.
 *
 * @param {{ headers: Record<string, string>, body: string }} request
 * @param {{ SCHEMA_VERSION: number, validateEvent: (e: unknown) => boolean, store: { appendEvents: (e: unknown[]) => Promise<void> } }} deps
 * @returns {Promise<{ ok: boolean, status: number, body: object }>}
 *   - success: `{ ok: true, status: 202, body: { accepted: <n> } }`
 *   - rejection: `{ ok: false, status: 4xx, body: { error: string } }` (nothing persisted)
 */
export async function ingest({ headers, body }, { SCHEMA_VERSION, validateEvent, store }) {
  if (typeof SCHEMA_VERSION !== 'number') {
    throw new TypeError('ingest: `SCHEMA_VERSION` dependency is required (number)');
  }
  if (typeof validateEvent !== 'function') {
    throw new TypeError('ingest: `validateEvent` dependency is required (function)');
  }
  if (!store || typeof store.appendEvents !== 'function') {
    throw new TypeError('ingest: `store` with an `appendEvents(events)` method is required');
  }

  // 1. SCHEMA HANDSHAKE — the header exists precisely so the receiver can
  //    reject/coordinate on drift WITHOUT parsing the body (telemetry-send.js).
  //    An unknown/missing version → reject before we ever look at the body.
  const declared = readHeader(headers, 'x-telemetry-schema');
  if (declared !== String(SCHEMA_VERSION)) {
    return reject(
      415,
      `unsupported telemetry schema version: expected "${SCHEMA_VERSION}", got ${JSON.stringify(declared)}`
    );
  }

  // 2. PARSE the { schemaVersion, events } body.
  let parsed;
  try {
    parsed = JSON.parse(body);
  } catch {
    return reject(400, 'request body is not valid JSON');
  }
  if (!parsed || typeof parsed !== 'object') {
    return reject(400, 'request body must be a JSON object');
  }
  const { events } = parsed;
  if (!Array.isArray(events)) {
    return reject(400, 'request body must be { schemaVersion, events: [...] }');
  }

  // 3. VALIDATE every event against the shared schema. If ANY event is
  //    out-of-schema → hard-reject the WHOLE batch and persist nothing (invalid
  //    data never lands — mirrors the client's own redact→validate pipeline).
  //    Note: validateBaseEvent also checks each event's own `schemaVersion`
  //    field, so a body whose events carry a different version is caught here.
  for (const event of events) {
    if (!validateEvent(event)) {
      return reject(422, 'one or more events failed schema validation; batch rejected');
    }
  }

  // 4. PERSIST — only reached once EVERY event validated (atomicity: a bad event
  //    anywhere in the batch means nothing is stored).
  if (events.length > 0) {
    await store.appendEvents(events);
  }

  // 5. 2xx — delivered. 202 Accepted: the receiver owns the events now.
  return { ok: true, status: 202, body: { accepted: events.length } };
}
