#!/usr/bin/env bash
# Run source-bound ARC VCC verification/apply on the EU worker only.
set -euo pipefail

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

heartbeat="$RUN/product_execution.json"
write_heartbeat() {
  phase=$1
  current=$2
  python3 - "$heartbeat" "$$" "$phase" "$current" <<'PY'
import json, os, socket, sys, tempfile
from datetime import datetime, timezone

path, pid, phase, current = sys.argv[1:]
payload = {
    'product_execution': {
        'host': socket.gethostname(),
        'pid': int(pid),
        'phase': phase,
        'payload_heartbeat_at': datetime.now(timezone.utc).isoformat(),
        'metric': 'source_members',
        'current': int(current),
        'denominator': 6,
    }
}
fd, temporary = tempfile.mkstemp(prefix='.heartbeat-', dir=os.path.dirname(path))
with os.fdopen(fd, 'w') as handle:
    json.dump(payload, handle, sort_keys=True)
    handle.flush()
    os.fsync(handle.fileno())
os.replace(temporary, path)
PY
}

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
write_heartbeat preflight 0

for object in \
  gs://arc-institute-virtual-cell-atlas/virtual-cell-challenge/2025/test/adata_Test.h5ad \
  gs://arc-institute-virtual-cell-atlas/virtual-cell-challenge/2025/train/adata_Training.h5ad \
  gs://arc-institute-virtual-cell-atlas/virtual-cell-challenge/2025/validation/adata_Validation.h5ad; do
  gcloud storage objects describe "$object" --format='json(generation,size,crc32c,md5Hash,updateTime)'
done

gcsfuse --implicit-dirs --only-dir virtual-cell-challenge/2025 --file-cache-max-size-mb 5120 arc-institute-virtual-cell-atlas "$MOUNT"
ln -sfn "$MOUNT/test/adata_Test.h5ad" "$SOURCES/adata_Test.h5ad"
ln -sfn "$MOUNT/train/adata_Training.h5ad" "$SOURCES/adata_Training.h5ad"
ln -sfn "$MOUNT/validation/adata_Validation.h5ad" "$SOURCES/adata_Validation.h5ad"

args=(python tools/curate_arc_vcc_obs_var.py --source-root "$SOURCES" --receipt "$RECEIPT")
if [[ "$MODE" == apply ]]; then args+=(--apply); fi
write_heartbeat "${MODE}_source_join" 0
XDG_CACHE_HOME="$RUN/cache" LAMIN_SETTINGS_DIR="$RUN/lamin" uv run "${args[@]}"
sha256sum "$RECEIPT"
write_heartbeat terminal 6
cat "$heartbeat"
