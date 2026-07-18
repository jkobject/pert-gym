#!/bin/bash

set -euo pipefail

VM_HOST="${PERT_GYM_WORKER_HOST:-pert-gym-worker-eu}"
VM_ZONE="${PERT_GYM_WORKER_ZONE:-europe-west1-b}"
BILLING_PROJECT="${PERT_GYM_BILLING_PROJECT:-jkobject-1549353370965}"
VM_CACHE_DIR="${PERT_GYM_VM_CACHE_DIR:-/home/jkobject/cache}"
GCS_URI="${1:-}"

if [[ -z "$GCS_URI" ]]; then
  cat >&2 <<'EOF'
Usage: ./tools/sync_to_vm_cache.sh gs://scperturb/path/to/object-or-prefix

Runs the copy on the EU GCP VM so data moves bucket -> VM, not bucket -> Mac -> VM.
Default target: pert-gym-worker-eu in europe-west1-b, same region as gs://scperturb.
Requester Pays: passes --billing-project=${PERT_GYM_BILLING_PROJECT:-jkobject-1549353370965}.

This helper is for big jobs that must NOT run on the Mac mini. If the EU worker is absent,
recreate/provision it first; do not fall back to Mac-local GCS/Lamin reads.
Override PERT_GYM_WORKER_HOST/PERT_GYM_WORKER_ZONE only when intentionally using another EU worker.
EOF
  exit 2
fi

case "$GCS_URI" in
  gs://scperturb/*) ;;
  *) echo "Expected a gs://scperturb/... URI, got: $GCS_URI" >&2; exit 2 ;;
esac

if ! gcloud compute instances describe "$VM_HOST" --zone "$VM_ZONE" >/dev/null 2>&1; then
  echo "Required EU worker $VM_HOST ($VM_ZONE) is missing. Recreate/provision it; do not run this big GCS job on the Mac mini." >&2
  exit 3
fi

STATUS=$(gcloud compute instances describe "$VM_HOST" --zone "$VM_ZONE" --format='value(status)')
if [[ "$STATUS" != "RUNNING" ]]; then
  echo "Starting $VM_HOST ($VM_ZONE); current status=$STATUS" >&2
  gcloud compute instances start "$VM_HOST" --zone "$VM_ZONE" --quiet >/dev/null
fi

echo "Preparing VM cache on $VM_HOST ($VM_ZONE): $VM_CACHE_DIR"
echo "Requester Pays billing project: $BILLING_PROJECT"

gcloud compute ssh "$VM_HOST" --zone "$VM_ZONE" --command \
  "mkdir -p '$VM_CACHE_DIR' && gcloud config set billing/quota_project '$BILLING_PROJECT' >/dev/null && gcloud storage cp --billing-project='$BILLING_PROJECT' --recursive '$GCS_URI' '$VM_CACHE_DIR/'"

echo "Remote cache copy requested on $VM_HOST:$VM_CACHE_DIR"
echo "Run remote jobs with: gcloud compute ssh $VM_HOST --zone $VM_ZONE --command 'cd ~/work/pert-gym && <command>'"
