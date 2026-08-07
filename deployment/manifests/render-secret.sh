#!/usr/bin/env bash
# Renders deployment/manifests/secret.yaml (gitignored) from secret.yaml.template.
#
# AUTH_TOKEN is PRESERVED from the live warden-telemetry-secret when already
# present (generated only on first run / when missing). Never regenerate it
# blindly: it is the shared secret every warden client's endpointUrl is paired
# with, so rotating it breaks ingestion from all clients until they're
# reconfigured.
#
# Output file is gitignored (deployment/.gitignore matches secret.yaml).
#
# Usage: ./deployment/manifests/render-secret.sh
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPL="$DIR/secret.yaml.template"
OUT="$DIR/secret.yaml"

# --- Preserve existing AUTH_TOKEN (never rotate blindly) ---
existing() { kubectl -n default get secret warden-telemetry-secret -o jsonpath="{.data.$1}" 2>/dev/null | base64 -d 2>/dev/null || true; }
AUTH_TOKEN="$(existing AUTH_TOKEN)"
if [[ -z "$AUTH_TOKEN" ]]; then
  AUTH_TOKEN="$(openssl rand -hex 32)"
fi

sed -e "s|__AUTH_TOKEN__|${AUTH_TOKEN}|g" "$TPL" > "$OUT"
chmod 600 "$OUT"

echo "rendered $OUT"
echo "  AUTH_TOKEN: ${#AUTH_TOKEN} chars (preserved if already set)"
echo
echo "Configure each warden client (Settings → Telemetry) with:"
echo "  endpointUrl: https://warden-telemetry.yatfa.com/ingest"
echo "  token:       <the AUTH_TOKEN value in $OUT>"
