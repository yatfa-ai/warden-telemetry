#!/usr/bin/env bash
# Launch a Kaniko build of warden-telemetry in the cluster, wait for it, verify
# the image landed in the registry, then pin Deployment/warden-telemetry to the
# new image and wait for rollout.
#
# Usage:
#   ./deployment/kaniko/build.sh                 # current HEAD (default ref), tag sha-<short>
#   ./deployment/kaniko/build.sh <git-ref>       # build a specific ref
#   ./deployment/kaniko/build.sh <git-ref> <tag> # ref + custom tag
#
# Prereqs:
#   - kubectl context = new-cluster (the in-cluster registry + SA already exist)
#   - GH_TOKEN exported (or `gh auth token` works) — needed to clone the PRIVATE
#     repo from inside the Kaniko pod.
set -euo pipefail

KANIKO_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(cd "$KANIKO_DIR/.." && pwd)"
REPO_DIR="$(cd "$DEPLOY_DIR/.." && pwd)"

# Resolve repo URL from git remote and normalize to kaniko's git-context format
# (git://github.com/owner/repo.git).
REPO_URL="$(cd "$REPO_DIR" && git remote get-url origin 2>/dev/null || true)"
if [[ -z "$REPO_URL" ]]; then
  echo "!! could not determine origin remote URL — set REPO_URL env var" >&2
  exit 1
fi
case "$REPO_URL" in
  git@github.com:*)    REPO_URL="github.com/${REPO_URL#git@github.com:}" ;;
  https://github.com/*) REPO_URL="github.com/${REPO_URL#https://github.com/}" ;;
  git://github.com/*)  REPO_URL="github.com/${REPO_URL#git://github.com/}" ;;
esac
[[ "$REPO_URL" == *.git ]] || REPO_URL="${REPO_URL}.git"

# Token to clone the private repo in-pod. Prefer GH_TOKEN, else the gh CLI.
if [[ -z "${GH_TOKEN:-}" ]]; then
  if command -v gh >/dev/null 2>&1; then
    GH_TOKEN="$(gh auth token)"
  else
    echo "!! set GH_TOKEN (or run `gh auth login`) so Kaniko can clone the private repo" >&2
    exit 1
  fi
fi
GIT_PASSWORD="$GH_TOKEN"

GIT_REF="${1:-$(cd "$REPO_DIR" && git rev-parse --abbrev-ref HEAD)}"
SHORT_SHA="$(cd "$REPO_DIR" && git rev-parse --short HEAD)"
TAG="${2:-sha-${SHORT_SHA}}"
REGISTRY="registry.tinkerai.win"   # pull hostname (push goes to the in-cluster ClusterIP)

c_blu() { printf '\033[34m%s\033[0m\n' "$*"; }
c_grn() { printf '\033[32m%s\033[0m\n' "$*"; }
c_red() { printf '\033[31m%s\033[0m\n' "$*"; }
die()   { c_red "!! $*"; exit 1; }

c_blu "== Kaniko build =="
echo "  repo    : $REPO_URL"
echo "  ref     : $GIT_REF"
echo "  tag     : $TAG"
echo "  registry: $REGISTRY"

job="warden-telemetry-build-${TAG}"
job="${job//[^a-z0-9-]/-}"   # sanitize
job="${job:0:63}"             # k8s name limit
job="${job%-}"                # no trailing dash

manifest="$(sed \
  -e "s|__JOB_NAME__|${job}|g" \
  -e "s|__GIT_REF__|${GIT_REF}|g" \
  -e "s|__IMAGE_TAG__|${TAG}|g" \
  -e "s|__REPO_URL__|${REPO_URL}|g" \
  -e "s|__GIT_PASSWORD__|${GIT_PASSWORD}|g" \
  "$KANIKO_DIR/kaniko-job.yaml.template")"

c_blu "== launching Kaniko Job: $job =="
echo "$manifest" | kubectl apply -f - >/dev/null

# Stream logs until done
c_blu "== streaming build logs =="
kubectl wait --for=condition=complete --timeout=1800s "job/${job}" -n default &
wait_pid=$!
kubectl logs -f "job/${job}" -n default || true
wait "$wait_pid"

if kubectl get "job/${job}" -o jsonpath='{.status.succeeded}' | grep -q 1; then
  c_grn "== build OK: $REGISTRY/warden-telemetry:${TAG} =="
else
  die "build failed — see logs above"
fi

# Verify the image is present in the registry before deploying. The registry
# requires htpasswd auth even on localhost:5000, so authenticate with the creds
# from the shared yatfa-registry-pull pull secret (same creds kaniko pushed
# with). We check tags/list (not the manifests endpoint) because kaniko pushes
# OCI manifests and a manifest GET without the matching Accept media-type 404s
# regardless of whether the tag exists.
AUTH="$(kubectl -n default get secret yatfa-registry-pull \
  -o jsonpath='{.data.\.dockerconfigjson}' 2>/dev/null | base64 -d 2>/dev/null \
  | grep -oE '"auth"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 \
  | sed -E 's/.*:.*"([^"]*)"$/\1/' || true)"
if [ -n "$AUTH" ]; then
  TAGS="$(kubectl exec deploy/registry -- sh -c \
    "wget -qO- --header='Authorization: Basic ${AUTH}' http://localhost:5000/v2/warden-telemetry/tags/list" 2>/dev/null || true)"
  echo "$TAGS" | grep -q "\"${TAG}\"" \
    || die "registry missing tag ${TAG} (tags/list: ${TAGS:-<empty>})"
else
  echo "  (skip registry verify: could not read pull-secret creds — kaniko reported Pushed; relying on the rollout pull below)"
fi

# Pin the Deployment to the explicit sha tag (not :latest) and roll out.
c_blu "== pinning Deployment/warden-telemetry → ${REGISTRY}/warden-telemetry:${TAG} =="
kubectl set image "deployment/warden-telemetry" \
  "warden-telemetry=${REGISTRY}/warden-telemetry:${TAG}" -n default
kubectl rollout status "deployment/warden-telemetry" -n default --timeout=180s

c_grn "== deploy complete =="
kubectl get pods -n default -l app=warden-telemetry -o wide
