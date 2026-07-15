# Warden Telemetry

### The self-hostable telemetry receiver for Yatfa Warden.

`warden-telemetry` is a minimal, open-source, **self-hostable ingest service** that receives
optional telemetry events from [Yatfa Warden](https://github.com/yatfa-ai/warden) — the desktop
dashboard for AI agents.

It is the **receiver half** of Warden's telemetry system. The **client** that collects, redacts, and
sends events lives in the [warden](https://github.com/yatfa-ai/warden) repo. This repo hosts only the
server that accepts those events.

> **Status: early / not yet implemented.** This repository currently contains only its design and this
> README. See the design reference at the bottom.

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

> The shared schema definition will be published here once implemented.

## Design reference

This repository implements the design described in the Warden knowledge article **"Telemetry design:
optional, off-by-default, multi-repo"** (`WARDEN-443`). The principles there are load-bearing — do not erode
them casually:

1. Off-by-default + Settings-only consent is the trust foundation.
2. The two tiers exist so identifying data (names) requires a second, conscious opt-in.
3. Redaction is pre-collection and client-side — the pipeline must make it impossible for
   credentials/content to reach the network.
4. The cross-repo schema is versioned and authoritative.
