#!/usr/bin/env bash
# Run from the homelab workloads repo, after copying apps/ufc-data-refresh there.
set -Eeuo pipefail
umask 077
if [[ $# != 2 || ! "$1" =~ ^(dev|prod)$ ]]; then
    printf 'Usage: %s dev|prod /absolute/path/to/private-runtime.env\n' "$0" >&2
    exit 2
fi
environment="$1"
secret_input="$2"
target="apps/ufc-data-refresh/envs/$environment/runtime-secret.enc.yaml"
[[ -f .sops.yaml && -d "$(dirname "$target")" && -f "$secret_input" ]]
temporary="$(mktemp)"
trap 'rm -f -- "$temporary"' EXIT
python3 - "$environment" "$secret_input" <<'PY' | sops --encrypt --filename-override "$target" --input-type json --output-type yaml /dev/stdin > "$temporary"
import json
import sys
from pathlib import Path

environment, filename = sys.argv[1:]
entries = {}
for line in Path(filename).read_text().splitlines():
    if not line.strip() or line.lstrip().startswith("#"):
        continue
    key, separator, value = line.partition("=")
    if not separator or key.strip() != "DATABASE_URL" or not value:
        raise SystemExit("Invalid runtime secret input; expected DATABASE_URL only")
    if "DATABASE_URL" in entries:
        raise SystemExit("Duplicate runtime secret field")
    entries["DATABASE_URL"] = value
if not entries or "REPLACE_" in entries["DATABASE_URL"]:
    raise SystemExit("Populate the private runtime secret before encryption")
print(json.dumps({"apiVersion": "v1", "kind": "Secret", "metadata": {
    "name": "ufc-data-refresh-runtime", "namespace": f"ufc-data-refresh-{environment}"
}, "type": "Opaque", "stringData": entries}))
PY
mv -- "$temporary" "$target"
printf 'Encrypted runtime secret written to %s\n' "$target"
