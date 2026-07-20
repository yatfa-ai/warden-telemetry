# Kaniko build setup (warden-telemetry)

Builds the warden-telemetry image inside the cluster (no Docker daemon, no
external registry, no GitHub runner). Kaniko runs as a Job, clones the (private)
Git repo in-pod using a GitHub token, builds the single-stage Dockerfile, and
pushes to the in-cluster registry.

Reuses the cluster-wide build infra already set up for yatfa / yatfa-router:
`ServiceAccount/github-actions` (+ its `Role` permitting `create` on Jobs and
`patch` on Deployments in `default`), and `Secret/yatfa-registry-pull` (the
imagePullSecret used both as Kaniko's push creds and the Deployments' pull
creds). No new RBAC or registry objects are needed.

## Usage

### From laptop (manual — initial deploy)

```bash
export GH_TOKEN="$(gh auth token)"   # Kaniko clones the private repo with this
./deployment/kaniko/build.sh          # builds HEAD, pushes, pins Deployment, rolls out
./deployment/kaniko/build.sh main     # build a specific ref
./deployment/kaniko/build.sh main v0.2.0   # ref + custom tag
```

### From GitHub Actions (automatic)

See `deployment/release/`. A push to `prod` (or the manual "Release" workflow)
enqueues an in-cluster release Job that renders + applies this Kaniko template,
waits for the build, pins the Deployment, and cleans up — GHA bills only for the
enqueue.

## How Kaniko works here

- `--context=git://github.com/yatfa-ai/warden-telemetry.git#<ref>` — clone at
  build time, no source tarball.
- `GIT_USERNAME=x-access-token` + `GIT_PASSWORD=<token>` — auth for the private
  clone (laptop: `gh auth token`; in-cluster: the GitHub App installation token).
- `--destination=registry.default.svc.cluster.local:5000/warden-telemetry:<tag>`
  + `--insecure` — push via the in-cluster ClusterIP (HTTP) to dodge Cloudflare's
  100MB body cap on the public hostname. The image lands at the same path;
  pulls go via HTTPS to `registry.tinkerai.win`.
- No `--target` — the Dockerfile is single-stage.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Kaniko `404` / `could not read Username` during clone | Private repo, bad/expired token | `export GH_TOKEN="$(gh auth token)"` (laptop) or install the GitHub App on the repo (release flow) |
| Kaniko `413 Request Entity Too Large` | Pushing via the public hostname | Shouldn't happen — push goes via `registry.default.svc.cluster.local:5000` |
| Kaniko `EOF` mid-build | OOM | Bump `limits.memory` in `kaniko-job.yaml.template` |
| `error writing layer: no space left on device` | PVC full | `kubectl create job --from=cronjob/registry-gc manual-gc-$(date +%s)` |
