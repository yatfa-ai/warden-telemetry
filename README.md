# Warden Telemetry

### The self-hostable telemetry receiver for Yatfa Warden.

`warden-telemetry` is a minimal, open-source, **self-hostable ingest service** that receives
optional telemetry events from [Yatfa Warden](https://github.com/yatfa-ai/warden) — the desktop
dashboard for AI agents.

It is the **receiver half** of Warden's telemetry system. The **client** that collects, redacts, and
sends events lives in the [warden](https://github.com/yatfa-ai/warden) repo. This repo hosts only the
server that accepts those events.

> **Status: ingest keystone implemented (R1).** The minimal Node ingest service — accept a client's
> POSTed `{ schemaVersion, events }` batch, enforce the `x-telemetry-schema` handshake, validate every
> event against the shared schema, hard-reject anything outside it, and durably persist accepted events —
> is in place (`server.mjs`). A maintainer query/view API, retention, auth, and multi-version support are
> later slices. See [Running](#running-the-receiver) below and the design reference at the bottom.

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
node server.mjs            # listens on :7421, POST /ingest, persists ./telemetry.ndjson
PORT=8080 STORE=/var/lib/warden/events.ndjson node server.mjs
npm test                   # node --test (zero real network, zero real filesystem)
```

Point a Warden client at it by setting its telemetry `endpointUrl` to `http://<host>:7421/ingest` and
opting in (base tier). A `schemaVersion: 1` batch of valid events returns `202 Accepted` and is appended to
the NDJSON store; an unknown `x-telemetry-schema` version or any out-of-schema event is hard-rejected with
a **non-retryable 4xx** (the client drops the batch rather than retrying it forever).

## Design reference

This repository implements the design described in the Warden knowledge article **"Telemetry design:
optional, off-by-default, multi-repo"** (`WARDEN-443`). The principles there are load-bearing — do not erode
them casually:

1. Off-by-default + Settings-only consent is the trust foundation.
2. The two tiers exist so identifying data (names) requires a second, conscious opt-in.
3. Redaction is pre-collection and client-side — the pipeline must make it impossible for
   credentials/content to reach the network.
4. The cross-repo schema is versioned and authoritative.
