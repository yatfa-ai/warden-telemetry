# In-cluster release orchestrator (warden-telemetry)

GitHub Actions (`build-image.yml`) enqueues a single k8s Job in the cluster and
exits in ~10 seconds. That Job (`release-job.yaml.template`) does the **whole
warden-telemetry deploy**:

1. clone warden-telemetry at the target ref (`prod`)
2. render + apply the kaniko build (`deployment/kaniko/kaniko-job.yaml.template`)
3. `kubectl wait` for the kaniko build (30 min cap)
4. pin `deployment/warden-telemetry` to the exact `sha-<short>` image tag
5. delete the kaniko Job

Build + deploy happen entirely in-cluster. GHA bills only for the enqueue. Same
architecture as yatfa / yatfa-router.

## Why clone as the GitHub App (not `GITHUB_TOKEN`, not a PAT)

`release.yml` fast-forwards `prod` with `GITHUB_TOKEN`, then dispatches
`build-image.yml`, which enqueues this Job. As soon as `build-image.yml` exits,
the `GITHUB_TOKEN` dies — but the kaniko clone runs minutes later inside the
cluster. So we need credentials that survive the dispatching workflow.

Those credentials are the GitHub App `tinker-ai-app`, **not** a PAT. The release
Job signs a JWT with the App's PEM and exchanges it for a 1h installation token
at runtime. No PAT to create, rotate, or store — and the App credentials it
needs are already shared with yatfa-web / yatfa-router:

| Source | Used for |
|---|---|
| `Secret/yatfa-github-app` (`privkey.pem`) | PEM, mounted at `/etc/github-app/privkey.pem` |
| `ConfigMap/yatfa-config` (`GH_APP_ID`) | App id, read via `configMapKeyRef` |

The App **must be installed on `yatfa-ai/warden-telemetry`** with `contents: read`.
If it isn't, the mint step fails fast with `!! GitHub App not installed on
yatfa-ai/warden-telemetry` — no silent stale-deploy.

## One-time setup

1. **Install `tinker-ai-app` on `yatfa-ai/warden-telemetry`** with `contents: read`
   (org-wide on `yatfa-ai` is fine — it's likely already installed org-wide).
2. **Create the `prod` branch** pointing at `main` (the release flow builds `prod`):
   ```bash
   git push origin main:prod
   ```
3. **Add repo Action secrets** (copy values from the yatfa repo — they're the same
   cluster creds): `K8S_API_URL`, `K8S_CA`, `K8S_TOKEN` (the long-lived
   `github-actions` SA token). See `deployment/kaniko/README.md` in the yatfa
   repo for how to extract them.
4. **No new cluster objects.** Reuses `Secret/yatfa-github-app`,
   `ConfigMap/yatfa-config`, `ServiceAccount/github-actions` (+ its `Role`, which
   already permits `create` on Jobs and `patch` on Deployments in `default`),
   `Secret/yatfa-secret` (Telegram notifier token), and `Secret/yatfa-registry-pull`.

The runtime manifests (Deployment/Service/Ingress/ConfigMap/Secret/PVC) are **not**
applied by the release flow — apply those once by hand (see
`deployment/manifests/README.md`). The release flow only patches the image.

## Triggering a release

| Path | What happens |
|---|---|
| **Manual (preferred)** — run "Release (Advance prod & Deploy)" | `release.yml` fast-forwards `prod` to `main`, then dispatches `build-image.yml`, which enqueues the Job. |
| **Direct push to prod** (human token) | Push event triggers `build-image.yml`, which enqueues the Job. (Pushes made by `GITHUB_TOKEN` are suppressed by GitHub and do NOT trigger this — use the manual path.) |

## Watching a release

GHA is not where the action happens. Watch in-cluster:

```bash
# Latest warden-telemetry release Job
kubectl get jobs -l app=warden-telemetry,component=release

# Stream its logs
RELEASE=$(kubectl get jobs -l app=warden-telemetry,component=release -o name | tail -1)
kubectl logs -f "$RELEASE"
```

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `!! GitHub App not installed on yatfa-ai/warden-telemetry` / `could not read Username` during clone | App not installed on the repo, PEM stale, or `GH_APP_ID` wrong | Install `tinker-ai-app` on `yatfa-ai/warden-telemetry` (`contents: read`) |
| Job stuck `Pending` | RBAC missing | Re-apply `yatfa/deployment/kaniko/rbac.yaml` (the `github-actions` SA is shared) |
| Kaniko `413 Request Entity Too Large` | Pushing via the public hostname (Cloudflare cap) | Shouldn't happen — kaniko pushes via `registry.default.svc.cluster.local:5000` |
| Same SHA enqueued twice | Push event + workflow_dispatch race | `build-image.yml` dedupes by job name — second enqueue is a no-op |

## Re-running a release

```bash
kubectl delete job warden-telemetry-release-<short>   # clear the prior attempt
gh workflow run build-image.yml --ref prod            # from the warden-telemetry repo
```
