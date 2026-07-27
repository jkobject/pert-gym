# Canonical GCS migration evidence

This directory is the compact, Git-tracked audit packet for the one-time migration from `gs://scperturb/pert-gym/...` into the canonical dataset-oriented hierarchy.

## Contents

- `raw_plan.jsonl`, `raw_execution_receipt.jsonl`, `raw_readback.json`: frozen raw-object plan, resumable execution receipts, and final identity/source-absence readback.
- `cleaned_direct_plan.jsonl`, `cleaned_direct_execution_receipt.jsonl`, `cleaned_readback.json`: accepted cleaned triplets and their final readback.
- `cleaned_direct_datasets.json`, `cleaned_readme_receipt.json`: per-dataset provenance inputs and README upload receipts.
- `lamin_key_remap_receipt.json`: LaminDB key remap receipt for moved cleaned artifacts.
- `junk_delete_allowlist.json`, `junk_delete_readback.json`: explicit disposable-prefix policy and bounded readback.
- `xatlas_client_delete_partial.json`: compact record of the initial client-side deletion tranche before switching to server-side batch operations.
- `xatlas_batch_dryrun.json`, `xatlas_batch_delete.json`: GCS Storage Batch Operations dry-run and terminal deletion counters.
- `final_layout_readback.json`: bounded final canonical-layout audit.
- `final_legacy_prefixes.json`: delimiter-based residual legacy prefix tree; it is not a payload inventory.

Large command logs and payload inventories are intentionally excluded. Detailed per-object planning and receipts remain compact JSON/JSONL; no payload bytes were downloaded for migration.

## Safety contract

Moves use GCS rewrite with exact source-generation and destination-creation preconditions, metadata checksum/size readback, then exact-generation source deletion. Unknown objects fail closed. `other/` is allowlist-only. The hidden `.lamindb/` control prefix and unresolved `logical/`/`vars/` surfaces are not treated as disposable data.
