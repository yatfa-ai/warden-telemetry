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
> distribution). **Every route — both `/ingest` and `/summary` — is gated behind an optional shared-secret
> bearer token (`AUTH_TOKEN`).** The persisted store is bounded by a maintainer-configurable **retention**
> policy (a count cap and/or an age window) so events don't accumulate without limit over the receiver's
> lifetime. Multi-version schema support is a later slice. See [Running](#running-the-receiver)
> below and the design reference at the bottom.

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
node server.mjs            # listens on :7421, POST /ingest + GET /summary, persists ./telemetry.ndjson
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
#   "lastSeen": 1720000000000
# }
```

The response carries **counts and histograms only** — it never echoes raw events or extended-tier
identifiers (chat/session names). `total` is the record count; `byType` is always the full
`{ error, crash, performance-stall }` set (zeroed when empty); `topErrorNames` comes from the non-identifying
error `name` field; `schemaVersions` is a histogram keyed by version; `firstSeen`/`lastSeen` bound the
observed time window (`null` on an empty store). A fresh receiver with no traffic returns `total: 0` with
zeroed counters. When `AUTH_TOKEN` is set, add the bearer header (e.g. `-H "Authorization: Bearer <token>"`)
to this and every other request — see below.

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
before routing — so neither `/ingest` nor `/summary` can be reached without the secret. A request missing a
valid token is rejected with **401** (a non-retryable 4xx; the client drops the batch rather than looping).
When `AUTH_TOKEN` is **unset**, behavior is unchanged (open) — the keystone stays runnable bare for local
dev. The secret is compared in constant time. Set the matching token on the Warden client side via the
Settings "Receiver auth token" field (sent as the same `Authorization: Bearer` header).

## Design reference

This repository implements the design described in the Warden knowledge article **"Telemetry design:
optional, off-by-default, multi-repo"** (`WARDEN-443`). The principles there are load-bearing — do not erode
them casually:

1. Off-by-default + Settings-only consent is the trust foundation.
2. The two tiers exist so identifying data (names) requires a second, conscious opt-in.
3. Redaction is pre-collection and client-side — the pipeline must make it impossible for
   credentials/content to reach the network.
4. The cross-repo schema is versioned and authoritative.
