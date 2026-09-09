#!/usr/bin/env bash
# Run source-bound ARC VCC verification/apply on the EU worker only.
set -uo pipefail

MODE=${1:?usage: run_arc_vcc_obs_var_eu.sh verify|apply}
case "$MODE" in verify|apply) ;; *) echo "invalid mode: $MODE" >&2; exit 64 ;; esac

TASK=t_23e86449
RUN="/tmp/arc-vcc-${TASK}"
MOUNT="$RUN/mount"
SOURCES="$RUN/sources"
RECEIPT="$RUN/${MODE}.json"
LOG="$RUN/${MODE}.log"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG_URI="gs://scperturb/pert-gym/staging/arc_vcc/2025/${TASK}/${MODE}-${STAMP}.log"
RECEIPT_URI="gs://scperturb/pert-gym/staging/arc_vcc/2025/${TASK}/${MODE}-${STAMP}.json"
mkdir -p "$RUN" "$MOUNT" "$SOURCES"
exec > >(tee -a "$LOG") 2>&1

cleanup() {
  rc=$?
  set +e
  if mountpoint -q "$MOUNT"; then fusermount -u "$MOUNT"; fi
  printf 'TERMINAL_STATUS=%s\n' "$rc"
  gcloud storage cp --billing-project=jkobject-1549353370965 "$LOG" "$LOG_URI"
  gcloud storage objects describe "$LOG_URI" --billing-project=jkobject-1549353370965 --format='json(generation,size,md5Hash)'
  if [[ -f "$RECEIPT" ]]; then
    gcloud storage cp --billing-project=jkobject-1549353370965 "$RECEIPT" "$RECEIPT_URI"
    gcloud storage objects describe "$RECEIPT_URI" --billing-project=jkobject-1549353370965 --format='json(generation,size,md5Hash)'
  fi
  printf 'LOG_URI=%s\nRECEIPT_URI=%s\n' "$LOG_URI" "$RECEIPT_URI"
  exit "$rc"
}
trap cleanup EXIT

if pgrep -af 'curate_arc_vcc_obs_var|run_arc_vcc_obs_var_eu' | grep -v "$$"; then
  echo 'CONFLICTING_ARC_WRITER'
  exit 70
fi
command -v gcsfuse
heartbeat="$RUN/product_execution.json"
python3 - "$heartbeat" "$$" preflight <<'PY'
import json, socket, sys
from datetime import datetime, timezone
with open(sys.argv[1], 'w') as handle:
    json.dump({'product_execution': {'host': socket.gethostname(), 'pid': int(sys.argv[2]), 'phase': sys.argv[3], 'payload_heartbeat_at': datetime.now(timezone.utc).isoformat(), 'metric': 'source_members', 'current': 0, 'denominator': 6}}, handle, sort_keys=True)
PY

gcsfuse --implicit-dirs --only-dir virtual-cell-challenge/2025 --file-cache-max-size-mb 5120 arc-institute-virtual-cell-atlas "$MOUNT"
ln -sfn "$MOUNT/test/adata_Test.h5ad" "$SOURCES/adata_Test.h5ad"
ln -sfn "$MOUNT/train/adata_Training.h5ad" "$SOURCES/adata_Training.h5ad"
ln -sfn "$MOUNT/validation/adata_Validation.h5ad" "$SOURCES/adata_Validation.h5ad"

args=(python tools/curate_arc_vcc_obs_var.py --source-root "$SOURCES" --receipt "$RECEIPT")
if [[ "$MODE" == apply ]]; then args+=(--apply); fi
XDG_CACHE_HOME="$RUN/cache" LAMIN_SETTINGS_DIR="$RUN/lamin" uv run "${args[@]}"
sha256sum "$RECEIPT"
cat "$heartbeat"
