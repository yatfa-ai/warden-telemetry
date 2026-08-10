# Warden Telemetry — minimal self-hostable ingest receiver.
# Zero runtime dependencies (see package.json: no `dependencies`), so there is
# no `npm install` step — the source is copied straight in.
#
# schema.ts is imported by server.mjs and stripped of types by Node's built-in
# type stripping (unflagged since Node 23.6; node:24 has it on by default), so
# no build/transpile step is needed either.
#
# Build: container image; deploy manifests are maintained outside this repo.
#   registry.default.svc.cluster.local:5000/warden-telemetry:<tag>
# Run env (supplied by the deployment):
#   PORT=7421  STORE=/data/telemetry.ndjson  AUTH_TOKEN=<shared-secret>
FROM node:24-alpine

ENV NODE_ENV=production

WORKDIR /app

# No node_modules to copy — the app is zero-dep. Copy only what server.mjs loads.
COPY package.json ./
COPY server.mjs ingest.mjs store.mjs summary.mjs events.mjs schema.ts ./

# Telemetry events are persisted to a PVC mounted at /data (see deployment.yaml).
# The path itself comes from the STORE env var; the dir just needs to be writable
# by the node user (UID 1000) — deployment.yaml sets fsGroup: 1000 for that.
RUN mkdir -p /data

EXPOSE 7421

# Run as the non-root `node` user (UID/GID 1000 in node:*-alpine).
USER node

CMD ["node", "server.mjs"]
