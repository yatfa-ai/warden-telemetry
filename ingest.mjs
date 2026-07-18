// The ingest pipeline — a PURE function of `{ headers, body }` plus injected
// dependencies `{ SCHEMA_VERSION, validateEvent, store }`. This is the slice's
// observation point (WARDEN-547): the handler trace lives here —
//
//   1. SCHEMA HANDSHAKE  — read x-telemetry-schema; reject unknown version
//                          WITHOUT parsing the body.
//   2. PARSE             — the { schemaVersion, events } JSON body.
//   3. VALIDATE          — run the shared validateEvent on EVERY event; hard-
//                          reject the whole batch if ANY is out-of-schema.
//   4. IDEMPOTENCY DEDUP — (opt-in via seenKeys, WARDEN-666) a request whose
//                          idempotency-key was already accepted returns 202
//                          {accepted:0, deduped:true} WITHOUT re-persisting, so a
//                          retried batch whose 2xx was lost doesn't double-count.
//   5. STAMP RECEIVEDAT  — annotate each accepted event with the receiver's own
//                          `receivedAt` epoch-ms (when IT saw the batch), so the
//                          time-sensitive read surfaces can key off the receiver's
//                          clock instead of the client's (clock-skew robustness,
//                          WARDEN-692). Runs ONLY on the persist path — a dedup HIT
//                          (step 4) persists nothing, so it stamps nothing.
//   6. PERSIST           — append the accepted batch via the injected store; then
//                          record the key (only on success) so future retries dedup.
//   7. RESPOND           — 2xx on success.
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
 * @param {{ SCHEMA_VERSION: number, validateEvent: (e: unknown) => boolean, store: { appendEvents: (e: unknown[]) => Promise<void> }, seenKeys?: { has(key: string): boolean, record(key: string): void }, now?: () => number }} deps
 * @returns {Promise<{ ok: boolean, status: number, body: object }>}
 *   - success: `{ ok: true, status: 202, body: { accepted: <n> } }`
 *   - dedup:   `{ ok: true, status: 202, body: { accepted: 0, deduped: true } }` (nothing persisted — a retried batch whose 2xx was lost; WARDEN-666)
 *   - rejection: `{ ok: false, status: 4xx, body: { error: string } }` (nothing persisted)
 */
export async function ingest({ headers, body }, { SCHEMA_VERSION, validateEvent, store, seenKeys, now = Date.now } = {}) {
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

  // IDEMPOTENCY KEY (WARDEN-666) — read ONCE here, after the handshake (a wrong
  // version is still 415'd first; dedup never relaxes the handshake). It is used
  // for both the dedup HIT check before persist and the record after. undefined
  // when no seenKeys set is wired (no dedup — today's behavior) OR when an old
  // client sends no idempotency-key header (also unchanged) — either way the two
  // sites below are no-ops.
  const idempotencyKey = seenKeys ? readHeader(headers, 'idempotency-key') : undefined;

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

  // 4a. IDEMPOTENCY DEDUP (WARDEN-666) — guard the PERSIST. This runs AFTER
  //     handshake + parse + validate, so NO check is relaxed: a wrong version is
  //     415'd, a malformed body is 400'd, an out-of-schema event is 422'd — all
  //     BEFORE this point — and only a batch that would otherwise be ACCEPTED can
  //     be a HIT. A HIT means the receiver already accepted this exact batch: the
  //     warden client generates ONE idempotency-key per batch and reuses it across
  //     retries of the same bytes, so a retry whose 2xx was lost (network reset /
  //     read timeout under disk pressure) carries a key the receiver already
  //     recorded on the prior successful persist. Return 202 {accepted:0,
  //     deduped:true} WITHOUT calling store.appendEvents, so a single crash retried
  //     ≤3× lands as ONE event, not 2–4 — keeping /summary (counts + timeline) and
  //     /events honest. OPT-IN via seenKeys: absent (or no header) → no dedup.
  if (idempotencyKey && seenKeys.has(idempotencyKey)) {
    return { ok: true, status: 202, body: { accepted: 0, deduped: true } };
  }

  // 4b. STAMP RECEIVEDAT (WARDEN-692) — annotate each accepted event with the
  //     epoch-ms the RECEIVER saw the batch, AFTER the dedup-HIT early-return
  //     above (a deduped retry persists nothing → it stamps nothing) and BEFORE
  //     appendEvents, so the annotation lands in the persisted record. This is
  //     receiver-owned operational metadata — when the batch arrived — added after
  //     the client's redacted payload is in memory and before it touches disk, so
  //     it changes NOTHING about what the client sends or what consent covers. The
  //     three time-sensitive read surfaces (summarizeTimeline / applyRetention /
  //     selectEvents) prefer `receivedAt` with a `timestamp` fallback, so a skewed
  //     client clock can no longer make a real volume spike vanish from the
  //     maintainer's signal. The batch arrived at one instant, so a single `now()`
  //     stamps every event identically (no N clock reads). validateBaseEvent checks
  //     required fields only and is tolerant of extra fields, and validation
  //     already ran at step 3, so this never re-trips validation.
  const receivedAt = now();
  for (const event of events) event.receivedAt = receivedAt;

  // 4c. PERSIST — only reached once EVERY event validated (atomicity: a bad event
  //     anywhere in the batch means nothing is stored).
  if (events.length > 0) {
    await store.appendEvents(events);
  }

  // 4d. RECORD the idempotency key ONLY on this successful-persist path — so a
  //     batch that was rejected above (4xx) or that failed to store
  //     (appendEvents throws, caught by the handler) is NEVER cached, and a retry
  //     is processed normally rather than wrongly dedup'd. (WARDEN-666)
  if (idempotencyKey) seenKeys.record(idempotencyKey);

  // 5. 2xx — delivered. 202 Accepted: the receiver owns the events now.
  return { ok: true, status: 202, body: { accepted: events.length } };
}
