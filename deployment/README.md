# deployment/ — warden-telemetry in Kubernetes

Self-contained deploy artifacts for the telemetry receiver, following the same
convention as yatfa / yatfa-router: build (`kaniko/`), release (`release/`), and
runtime manifests (`manifests/`) all live in the app's own repo.

The cluster is the shared yatfa k3s cluster (`kubectl config current-context` =
`new-cluster`, namespace `default`). warden-telemetry reuses the cluster-wide
build/registry/auth infra already set up for yatfa — no new RBAC, registry, or
GitHub App is needed.

## Layout

```
deployment/
  manifests/   # runtime k8s objects (apply once by hand)
    deployment.yaml service.yaml ingress.yaml configmap.yaml
    pvc.yaml secret.yaml.template render-secret.sh README.md
  kaniko/      # in-cluster image build
    build.sh kaniko-job.yaml.template README.md
  release/     # in-cluster release orchestrator + GHA trigger
    release-job.yaml.template README.md
.github/workflows/
  build-image.yml   # push:prod → enqueue in-cluster release Job
  release.yml       # manual → fast-forward prod → dispatch build-image.yml
Dockerfile          # single-stage node:24-alpine, zero-dep
```

## First-time deploy (do once)

```bash
# 1. Runtime manifests (ConfigMap, PVC, Deployment, Service, Ingress)
kubectl apply -f deployment/manifests/

# 2. Secret (AUTH_TOKEN) — generated + preserved by render-secret.sh
./deployment/manifests/render-secret.sh
kubectl apply -f deployment/manifests/secret.yaml

# 3. Build the image in-cluster and pin the Deployment to it
export GH_TOKEN="$(gh auth token)"      # Kaniko clones the private repo with this
./deployment/kaniko/build.sh

# 4. One-time DNS (in Cloudflare) for the public hostname:
#    telemetry.yatfa.ai → 159.195.194.69  (proxied / orange cloud)
```

After step 4 resolves, the receiver is live at `https://telemetry.yatfa.ai`.

## Subsequent deploys

Either push to `prod` (triggers `build-image.yml`), or run the "Release (Advance
prod & Deploy)" workflow — see `release/README.md` for the one-time GHA setup
(Action secrets + GitHub App install + `prod` branch).

## Wiring warden clients

Each warden desktop client (Settings → Telemetry) is configured with:

- `endpointUrl`: `https://telemetry.yatfa.ai/ingest`
- `token`: the `AUTH_TOKEN` value (in `deployment/manifests/secret.yaml`, or read
  via `kubectl -n default get secret warden-telemetry-secret -o jsonpath='{.data.AUTH_TOKEN}' | base64 -d`)

Every route (`/ingest`, `/summary`, `/capabilities`, `/events`) requires
`Authorization: Bearer <token>`.
