# Runtime manifests for warden-telemetry

The receiver's live Kubernetes objects (Deployment / Service / Ingress /
ConfigMap / Secret / PVC) live HERE, in the warden-telemetry repo — so the repo
is self-contained: code (`*.mjs` / `schema.ts`) + build (`deployment/kaniko/`)
+ release (`deployment/release/`) + **these runtime manifests**. Mirrors the
yatfa-router convention.

## What's here

| File | Object |
|---|---|
| `deployment.yaml` | `Deployment/warden-telemetry` (1 replica, node:24-alpine non-root, /data PVC) |
| `service.yaml` | `Service/warden-telemetry` (ClusterIP 80→7421) |
| `ingress.yaml` | `Ingress/warden-telemetry-ingress` (host `warden-telemetry.yatfa.com`) |
| `configmap.yaml` | `ConfigMap/warden-telemetry-config` (PORT/STORE/retention) |
| `secret.yaml.template` | `Secret/warden-telemetry-secret` template (render with `render-secret.sh`) |
| `render-secret.sh` | Renders `secret.yaml` from the template (preserves existing AUTH_TOKEN) |
| `pvc.yaml` | `PVC/warden-telemetry-data` (durable NDJSON store) |

## Apply (out-of-band / initial deploy)

The release flow (`deployment/release/`) only **patches the image**; it does NOT
apply these manifests. Apply them by hand once:

```bash
kubectl apply -f deployment/manifests/             # ConfigMap, PVC, Deployment, Service, Ingress
# Secret is rendered (gitignored), then:
./deployment/manifests/render-secret.sh            # → deployment/manifests/secret.yaml
kubectl apply -f deployment/manifests/secret.yaml
```

Then build the image in-cluster and pin the Deployment to it:

```bash
./deployment/kaniko/build.sh                        # build HEAD + pin deployment to sha-<short>
```

## Standalone / new-cluster values to edit

These manifests are the **live yatfa config** (working example). For a separate
deployment, edit:

- `ingress.yaml` → host `warden-telemetry.yatfa.com` (your domain; add the matching DNS
  record + edge TLS at your CDN)
- `deployment.yaml` → `imagePullSecrets` (`yatfa-registry-pull` → your registry
  pull secret), image registry host
- `kaniko/kaniko-job.yaml.template` → `--destination` registry host

(Full per-tenant parameterization is a later slice, not this one.)
