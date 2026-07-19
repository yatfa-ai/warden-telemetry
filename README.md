# Warden Telemetry

### The self-hostable telemetry receiver for Yatfa Warden.

`warden-telemetry` is a minimal, open-source, **self-hostable ingest service** that receives
optional telemetry events from [Yatfa Warden](https://github.com/yatfa-ai/warden) — the desktop
dashboard for AI agents.

It is the **receiver half** of Warden's telemetry system. The **client** that collects, redacts, and
sends events lives in the [warden](https://github.com/yatfa-ai/warden) repo. This repo hosts only the
server that accepts those events.

> **Status: ingest + read surface implemented (R1 + R2).** The minimal Node ingest service — accept a client's
> POSTed `{ schemaVersion, events }` batch, enforce the `x-telemetry-schema` handshake, validate every
> event against the shared schema, hard-reject anything outside it, and durably persist accepted events —
> is in place (`server.mjs`), alongside a maintainer **`GET /summary`** read surface that aggregates the
> persisted events into act-on-able signal (totals, per-type counts, top error names, schema-version
> distribution), a **`GET /capabilities`** config-time verification surface a client probes to confirm
> reachability + schema match + auth before relying on the receiver, and a **`GET /events`** drill-down
> that returns the recent events themselves at full fidelity (the diagnostic payloads `/summary` only
> counts), bounded to a newest-N window with type/since filters. **Every route — `/ingest`, `/summary`,
> `/capabilities`, and `/events` — is gated behind an optional shared-secret bearer token (`AUTH_TOKEN`).**
> The persisted store is bounded by a maintainer-configurable **retention** policy (a count cap and/or an
> age window) so events don't accumulate without limit over the receiver's lifetime. Multi-version schema
> support is a later slice. See [Running](#running-the-receiver) below and the design reference at the bottom.

## Why a separate repo

Telemetry is conceptually a hosted service, distinct from the local desktop app, so it lives in its own
repo. The coupling between the client and this receiver is exactly one thing: a **versioned event
schema** (see below).

## Trust model (by design)

Telemetry in Warden is **optional and OFF by default** — framed as voluntary help-with-development, never
surveillance. Nothing leaves the user's machine until they explicitly turn it on in Warden's Settings.
There is **no first-run prompt.**

- **No third-party SaaS.** No Sentry, PostHog, or similar. This receiver is the only destination.
- **Configurable endpoint.** The Warden client points at whatever instance a maintainer self-hosts.

### Two consent tiers — both off by default, each revocable anytime

| Tier | What it collects |
|------|------------------|
| **Base** | Anonymous error/crash reports + performance metrics (event-loop stalls / freezes). |
| **Extended** *(gated behind base)* | Additionally: chat names and Claude session names — for maintainer debugging of real-world reports. |

### The redaction contract

Credentials and content **never** reach the network — by construction, not by hope:

- **Hard exclusions — never collected or sent, at any tier:** API keys, auth tokens, SSH keys, chat
  content/output, prompts, file paths, hostnames.
- **The only identifiers permitted**, and only at the extended tier: chat names and Claude session names.
  Content is never sent — names only.
- **Redaction is client-side and pre-collection.** Credentials and content never enter the telemetry
  pipeline at all.
- This receiver additionally **hard-rejects** anything outside the agreed schema.

## The versioned event schema

The client and this receiver agree on a **schema version**. A schema bump is a **coordinated change across
both repos** — schema drift between client and receiver is the main risk this design guards against, so the
schema is pinned and version-bumped deliberately.

The shared schema lives in **[`schema.ts`](./schema.ts)**, vendored **verbatim** from the client
(`warden/web/src/lib/telemetry/schema.ts`). It is zero-dependency and runtime-import-free, and loads
standalone here via Node's native TypeScript type-stripping (Node ≥ 22.6; target 24) — no transpile step,
no Vite, no validation library (no zod). **Never hand-roll a parallel validator:** a second validator that
can disagree with the client's is exactly the drift this design guards against.

A drift guard (`test/drift.test.mjs`) asserts the vendored constants match the pinned client contract and
that `validateEvent` still accepts the canonical client fixtures — so a schema change can't land here
silently; a bump must consciously update both the vendored file and the pinned assertions.

## Running the receiver

Requires **Node ≥ 22.6** (target 24, to match the Warden backend) for native type-stripping of the
vendored `schema.ts`.

```bash
node server.mjs            # listens on :7421, POST /ingest + GET /summary + GET /capabilities + GET /events, persists ./telemetry.ndjson
PORT=8080 STORE=/var/lib/warden/events.ndjson node server.mjs
AUTH_TOKEN=cpy0kr3v... node server.mjs   # gate every route behind a shared secret
STORE_MAX_EVENTS=50000 STORE_MAX_AGE_HOURS=168 node server.mjs   # retain newest 50k events / 7 days
npm test                   # node --test (zero real network, zero real filesystem)
```

Point a Warden client at it by setting its telemetry `endpointUrl` to `http://<host>:7421/ingest` and
opting in (base tier). A `schemaVersion: 1` batch of valid events returns `202 Accepted` and is appended to
the NDJSON store; an unknown `x-telemetry-schema` version or any out-of-schema event is hard-rejected with
a **non-retryable 4xx** (the client drops the batch rather than retrying it forever).

### Reading the signal back — `GET /summary`

A maintainer reads **aggregates** (not raw NDJSON) of the persisted events:

```bash
curl http://localhost:7421/summary
# {
#   "total": 42,
#   "byType": { "error": 30, "crash": 4, "performance-stall": 8 },
#   "topErrorNames": [{ "name": "TypeError", "count": 17 }, ...],   # capped at 10, sorted desc
#   "schemaVersions": { "1": 42 },
#   "firstSeen": 1719820800000,
#   "lastSeen": 1720000000000,
#   "rejections": {
#     "total": 13,                          # count of hard-rejected requests
#     "byStatus": { "415": 11, "401": 2 },  # histogram keyed by HTTP status
#     "lastStatus": 415,
#     "lastReason": "unsupported telemetry schema version...",  # receiver diagnostic, not a payload
#     "lastSeen": 1720000099000
#   },
#   "persistErrors": {
#     "total": 2,                            # count of accepted batches that failed to persist
#     "lastReason": "ENOSPC: no space left on device, write",   # store/sink diagnostic, not a payload
#     "lastSeen": 1720000099000
#   },
#   "deduped": {
#     "total": 7,                            # count of transport-retries the receiver absorbed
#     "lastSeen": 1720000099000
#   },
#   "timeline": {
#     "buckets": [
#       { "bucketStart": 1719913600000, "bucketEnd": 1719915400000, "count": 2 },   # earlier baseline
#       { "bucketStart": 1719998200000, "bucketEnd": 1720000000000, "count": 15 }   # recent spike — suspect a deploy
#     ],
#     "bucketMs": 1800000                                                          # 30-min buckets (24h window / 48 cap)
#   }
# }
```

The response carries **counts and histograms only** — it never echoes raw events or extended-tier
identifiers (chat/session names). `total` is the record count; `byType` is always the full
`{ error, crash, performance-stall }` set (zeroed when empty); `topErrorNames` comes from the non-identifying
error `name` field; `schemaVersions` is a histogram keyed by version; `firstSeen`/`lastSeen` bound the
observed time window (`null` on an empty store). A fresh receiver with no traffic returns `total: 0` with
zeroed counters.

`rejections` is a bounded, in-memory tally of the requests the receiver **hard-rejected** — a `401` at the
auth gate, a `404` routing miss, a `400` malformed body, or a `400`/`415`/`422` from schema validation. It
reports a `total` count, a `byStatus` histogram, and the single most-recent sample (`lastStatus` /
`lastReason` / `lastSeen`). It exists so you can tell **traffic is arriving and being rejected** (e.g. a
flood of `415`s — typically the first observable symptom of a client/receiver schema-version skew) apart
from **no traffic at all**: an idle receiver returns a zeroed `rejections` (`total: 0`, empty `byStatus`),
identical to the empty-store shape, so a quiet receiver never looks like a rejecting one. `lastReason` is
the receiver's own short diagnostic string — never raw event payloads or extended-tier identifiers. The
tally is receiver-local and **does not survive a restart** (a misconfiguration detector need not), and it is
purely additive: it records rejections that already happen, relaxes no check, and routes nothing anywhere.
When `AUTH_TOKEN` is set, add the bearer header (e.g. `-H "Authorization: Bearer <token>"`) to this and
every other request — see below.

`persistErrors` is the write-path twin of `rejections`: a bounded, in-memory tally of the accepted batches
that **validated but could not be persisted** — a persist failure (`store.appendEvents()` throwing: disk
full, `EACCES`, `EISDIR`, a missing/rewritten store file, a sink rejection). It reports a `total` count and
the single most-recent sample (`lastReason` / `lastSeen`). It exists so you can tell **traffic is arriving,
validating, but the store is refusing writes** apart from **no traffic at all**: an idle receiver returns a
zeroed `persistErrors`, identical to the healthy shape. It is a **separate signal** from `rejections` by
design — a persist failure is a distinct "validated but un-storable" class, not an HTTP rejection, so it does
not overload `rejections`' rejection-sites-only contract. `lastReason` is the store/sink's own diagnostic
(an OS `errno` such as `ENOSPC` / `EACCES`, or a sink error) — never a raw event payload or extended-tier
identifier (by the time the sink runs, each event is already serialized to a line, so a sink throw carries
system info, not event bytes). Like `rejections`, it is receiver-local, does not survive a restart, and is
purely additive. A persist failure is also returned to the client as a clean **retryable `503`** (not a hung
socket): the events were already schema-valid, the store may recover, and the client's existing 5xx
bounded-retry path handles it without a protocol change.

`deduped` is the transport-retry tally: a bounded, in-memory count of the batches the receiver **absorbed as duplicates** via idempotent ingest (WARDEN-666). When the warden client loses a 2xx (a network reset or a read timeout while the receiver did synchronous `appendFile` I/O under disk pressure), it retries the SAME bytes with the SAME `idempotency-key`; the receiver recognizes the key and answers `202 {accepted:0, deduped:true}` WITHOUT re-persisting — so a single crash retried ≤3× lands as ONE event, not 2–4. That correctness mechanism ran silently before this tally: nothing on the receiver recorded it. `deduped` reports a `total` count and the single most-recent `lastSeen`. It exists so you can tell **clients are retrying because my receiver is slow / the network is flaky** (a sustained dedup spike — the receiver's synchronous persist is the retry cause documented at the ingest path) apart from **traffic is flowing cleanly**: an idle receiver returns a zeroed `deduped`, identical to the healthy shape. It is also the only visible symptom of a client-side idempotency-key bug (one key reused across DIFFERENT batches → unique events wrongly absorbed), which would otherwise surface as mysteriously low `/summary` counts with no diagnostic. It carries a COUNT and a timestamp only — never a raw event payload or extended-tier identifier (a dedup absorbs a batch WITHOUT reading or re-persisting its bytes, so there is no payload path to leak; there is no `lastReason`, because a dedup is simply "a batch we'd already accepted came back"). Like `rejections` and `persistErrors`, it is receiver-local, does not survive a restart, and is purely additive — it records a dedup that already happened, relaxes no check (the dedup decision itself still ran), and routes nothing anywhere.

`timeline` is a bounded temporal distribution — event **counts per time bucket** over a rolling recent
window (the last 24h, split into at most 48 buckets of 30 min each) — so you can distinguish a **recent
volume spike** (a regression or a deploy event — e.g. 15 errors landing in the newest bucket) from a
**long-running baseline** (a flat trickle across the window). It reads only the `timestamp` on
already-persisted events and emits counts, never raw events or extended-tier identifiers. Events older
than the window are excluded from the distribution but are still counted in `total` / `byType` /
`firstSeen` / `lastSeen` (those span the full retained set); a quiet store returns `buckets: []` with the
`bucketMs` granularity still conveyed. It is purely additive — computed on read, it collects nothing new.

#### Scoping the aggregates — `?type=` / `?platform=` / `?appVersion=` / `?since=`

Every aggregate above can be **scoped** to a platform / release / type / time window with the SAME
conjunctive filters `GET /events` takes — the scoped-OVERVIEW complement to `/events`' scoped drill-down.
Spot a `win32` spike or a `v0.1.18` volume bubble on `platforms` / `appVersions`, then scope `/summary`
to answer the follow-through without hand-parsing `/events`:

```bash
curl 'http://localhost:7421/summary?platform=win32'
# {
#   "total": 42,                 # ALWAYS the full persisted count (the whole retained set)
#   "matched": 17,               # the scoped subset the aggregates below were computed over (≤ total)
#   "byType": { "error": 15, "crash": 2, "performance-stall": 0 },
#   "platforms": { "win32": 17 },           # collapses to the scoped platform
#   "topErrorNames": [{ "name": "TypeError", "count": 11 }, ...],   # win32-only
#   "timeline": { ... },                     # win32 arrivals only
#   ...
# }

# Intersect filters to attribute a regression end-to-end:
curl 'http://localhost:7421/summary?appVersion=0.1.18&platform=darwin&type=crash'
```

- **`?type=`** — filter to one base type (`error` | `crash` | `performance-stall`). Exact match.
- **`?platform=`** — filter to one OS label (`darwin` | `win32` | `linux`). Exact match; an event whose
  source omitted the field is excluded (a maintainer asking "show me win32" does not want un-attributed
  events).
- **`?appVersion=`** — filter to one release label (e.g. `0.1.19`). Exact match.
- **`?since=`** — keep only events whose effective epoch-ms time (`receivedAt` if present, else the
  client's `timestamp`) is `>= since` (an absolute cutoff, inclusive). Keying off the receiver's receipt
  time makes "since the deploy" robust to skewed client clocks.

Filters are conjunctive (an event must match all that apply). `total` is ALWAYS the **full persisted
count** (independent of any filter); `matched` is the size of the scoped subset, so you can see both the
retained set and the slice the aggregates describe. `rejections`, `persistErrors`, and `deduped` are
intentionally **unscoped** — they describe receiver health (every rejection / persist / dedup site), not
the event subset, so a platform filter never hides them. With no filters the response is the legacy unscoped aggregate
(`matched === total`). The trust posture is unchanged: filters only SELECT which already-redacted,
already-schema-validated events get aggregated — no new collection, no server-side redaction, no tier
expansion; auth is inherited from the route's existing `AUTH_TOKEN` gate.

### Verifying the receiver is reachable — `GET /capabilities`

The receiver advertises a small self-description so a Warden client can confirm it has pointed at a
**real, schema-matched, authed** receiver *before* it relies on one — this is the user-side complement to the
maintainer's rejection signal (`/summary` → `rejections`) and the roadmap's first line of defense against
cross-repo schema drift:

```bash
curl http://localhost:7421/capabilities
# { "schemaVersion": 1, "authRequired": false }
```

- **`schemaVersion`** — the receiver's own `SCHEMA_VERSION` (sourced from the vendored `schema.ts`, never a
  parallel literal). The client compares it against its own vendored copy: a mismatch means client and
  receiver are on different schema versions — events would be hard-rejected at `/ingest` (the `415`).
- **`authRequired`** — `true` when `AUTH_TOKEN` is set, `false` when the receiver is open.

Like every other route, `/capabilities` is **gated behind `AUTH_TOKEN`** — checked before routing, so the gate
is uniform. This is what makes the "auth" verdict meaningful rather than a guess: a gated receiver `401`s an
unauthenticated probe *before* the body is returned, and that `401` **is** the "auth required" signal (no
special-casing). Only a probe carrying a valid token gets the `200` and observes `authRequired: true`; an open
receiver returns `authRequired: false`. The client drives this from its Settings **"Test connection"** button
(through the Warden backend, so the cross-origin fetch is not CORS-blocked) and renders a live verdict —
connected / schema-drift / auth-required / no-receiver — that is **never persisted**: a cached "connected"
would go stale (receiver down, token rotated) and become a false trust signal, so it stays an on-demand probe.

### Drilling into individual events — `GET /events`

`/summary` tells you *how many*; `GET /events` lets you inspect the *actual* recent events at **full
fidelity** — the diagnostic payloads `/summary` deliberately discards: an error's `message` + `frames`, a
crash's `reason`, a stall's `lagMs`. It is the drill-down that turns `/summary`'s counts into inspectable,
act-on-able events, in-product — without SSH-ing to the receiver host to hand-parse `telemetry.ndjson` with
`jq`.

```bash
curl 'http://localhost:7421/events?type=error&limit=3'
# {
#   "events": [
#     {
#       "schemaVersion": 1, "type": "error", "runtime": "main",
#       "timestamp": 1720000000000, "name": "TypeError",
#       "message": "Cannot read properties of undefined",   # /summary drops this
#       "frames": [{ "function": "foo", "file": "app.js", "line": 42 }]
#     },
#     ...
#   ],
#   "total": 42                                          # full persisted count (pre-window)
# }
```

The response is **bounded and filterable**:

- **`?limit=N`** (default `100`, hard-capped at `200`) — returns the **newest N** persisted events by arrival
  order. The hard cap means a near-full store (up to the 10000-event retention cap) can never yield a
  multi-megabyte response; a request for `limit=50000` returns at most `200`. A missing/non-numeric/
  non-positive `limit` falls back to the default.
- **`?type=`** — filter to one base type (`error` | `crash` | `performance-stall`). Exact match; a value that
  matches nothing returns an empty `events` array.
- **`?platform=`** — filter to one OS label (`darwin` | `win32` | `linux`). Exact match; an event whose source
  omitted the field is excluded (a maintainer asking "show me win32" does not want un-attributed events).
- **`?appVersion=`** — filter to one release label (e.g. `0.1.19`). Exact match.
- **`?signature=`** — filter to one **distinct failure** by the *exact* key `/summary` ranks `topSignatures` by
  (error `name` + first frame, crash `reason`, stall `source`). This is the drill-down complement to
  `topSignatures`: spot a high-count failure on `/summary`, copy its `signature` here, and read *that*
  failure's actual payloads instead of eyeballing `?type=error` mixed with every other error in the window.
  The filter key is the same `signatureOf()` `/summary` uses, so the round-trip is byte-identical (no drift
  between what is counted and what is filtered). An event that yields no signature (a nameless error, a
  reasonless crash, an unknown type) never matches — never errors. `/summary` itself does not take this
  filter (scoping the aggregates to one signature is not meaningful); it is an `/events`-only drill-down.
- **`?since=`** — keep only events whose epoch-ms `timestamp` is `>= since` (an absolute cutoff, inclusive).
  Useful for "what landed since I last looked."

Filters are conjunctive (an event must match all that apply), and the newest-N window is taken from the
*filtered* set. `total` is always the **full persisted count** (before the window/filter), so you can see how
much the window is a window *of*. A fresh receiver with no traffic returns `{ "events": [], "total": 0 }`
with `200` (parity with `/summary`'s empty-store shape).

The `?signature=` drill-down turns a `topSignatures` ranking straight into inspectable payloads — copy the
`signature` (and its `type`) off `/summary` into `/events`:

```bash
# /summary ranked it…
curl 'http://localhost:7421/summary?type=error'
#   "topSignatures": [
#     { "signature": "TypeError @ app.js:42 (foo)", "type": "error", "count": 847 }, ...
#   ]

# …/events?signature= drills into THAT failure's message + frames (URL-encode the value):
curl 'http://localhost:7421/events?type=error&signature=TypeError%20%40%20app.js%3A42%20(foo)'
```

The trust posture is identical to `/summary`: the route reads **only already-persisted, already-schema-
validated, already-client-redacted** events via the existing store read seam. It introduces no new
collection, re-collects nothing, routes to no third party, performs no server-side redaction, and expands no
tier — it returns exactly what consent + redaction already allowed to land, full-fidelity instead of
aggregate. When `AUTH_TOKEN` is set, `GET /events` inherits that gate (add the bearer header) the same as
every other route — it is not re-implemented per route.

### Keeping the store bounded — retention

Accepted events are appended to one NDJSON file. Without a bound that file grows for as long as the
receiver runs, which eventually degrades the very signal it exists to deliver (disk pressure, a `/summary`
read that grows slow, ingest persistence that degrades). Retention compacts the file to a maintainer-
configurable bound. **The default is bounded** — you do not have to opt in:

- **`STORE_MAX_EVENTS`** (default `10000`) — the **count cap**. Once the store exceeds this many events, it is
  compacted down to the newest N (oldest excess dropped off the front). This is the hard bound on record
  count, and therefore on file size. Set `0` to disable the count cap.
- **`STORE_MAX_AGE_HOURS`** (default `0` = off) — the **age window**. Events whose `timestamp` is older than
  now minus this many hours are dropped on the next compaction. When set, a maintainer-curated periodic sweep
  also expires old events on a quiet store (one with no incoming traffic). Set `0` to disable the age window.

Both may be set; an event is retained only if it survives both policies. To opt out of retention entirely
(run unbounded — **not recommended**), set both to `0`.

Compaction preserves the trust model exactly: retention **only ever removes** events. It never expands what
a tier collects, never routes data elsewhere, and never touches the pre-collection redaction contract — it
trims a file of events that already landed, schema-validated and redacted. `/summary` aggregates are derived
purely from whatever the store currently holds, so they stay self-consistent after a compaction (`firstSeen` /
`lastSeen` bound the retained window, `byType` / `topErrorNames` / `schemaVersions` reflect only the retained
set). A compaction rewrites the file atomically (temp file + rename), runs **off the request path** on a
debounced (≥1 min), re-entrancy-guarded trigger, and performs **no** rewrite at all when nothing exceeds the
bound — ingest latency is unaffected.

### Authenticating with a shared secret (`AUTH_TOKEN`)

By default the receiver is **OPEN** — any client that can reach the port can POST `/ingest` (or read
`/summary`). That is fine for a quick local dev run, but on a shared/LAN host you should gate it with a
shared secret:

```bash
AUTH_TOKEN=$(openssl rand -hex 32) node server.mjs   # generate a strong secret, then run gated
```

When `AUTH_TOKEN` is set, **every** route requires an `Authorization: Bearer <AUTH_TOKEN>` header, checked
before routing — so none of `/ingest`, `/summary`, `/capabilities`, or `/events` can be reached without the
secret. A request missing a valid token is rejected with **401** (a non-retryable 4xx; the client drops the
batch rather than looping). When `AUTH_TOKEN` is **unset**, behavior is unchanged (open) — the keystone stays
runnable bare for local dev. The secret is compared in constant time. Set the matching token on the Warden
client side via the Settings "Receiver auth token" field (sent as the same `Authorization: Bearer` header).

## Design reference

This repository implements the design described in the Warden knowledge article **"Telemetry design:
optional, off-by-default, multi-repo"** (`WARDEN-443`). The principles there are load-bearing — do not erode
them casually:

1. Off-by-default + Settings-only consent is the trust foundation.
2. The two tiers exist so identifying data (names) requires a second, conscious opt-in.
3. Redaction is pre-collection and client-side — the pipeline must make it impossible for
   credentials/content to reach the network.
4. The cross-repo schema is versioned and authoritative.
