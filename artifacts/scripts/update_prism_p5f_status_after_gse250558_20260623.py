#!/usr/bin/env python3
"""Update PRISM P5F status/inventory artifacts after verified GSE250558 ingestion."""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATUS_JSON = ROOT / "artifacts/schema_audit/prism_p5f_status_20260622.json"
STATUS_MD = ROOT / "artifacts/schema_audit/prism_p5f_status_20260622.md"
PREV_INV_JSON = ROOT / "artifacts/schema_audit/prism_p5f_cont7_inventory_20260623.json"
OUT_INV_JSON = ROOT / "artifacts/schema_audit/prism_p5f_cont8_inventory_20260623.json"
OUT_INV_MD = ROOT / "artifacts/schema_audit/prism_p5f_cont8_inventory_20260623.md"

DATASET = "GSE250558"
GCS_URI = "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE250558.h5ad"
ENTRY = {
    "prefix": "prism_collection/GSE250558",
    "n_obs": 59837,
    "n_vars": 36713,
    "chunk_size": 1000,
    "chunks_verified": 60,
    "controls": 12828,
    "verification_json": "artifacts/schema_audit/prism_GSE250558_chunked_verification.json",
    "gcs_uri": GCS_URI,
    "source_bytes": 1339211295,
    "backing": "CSRDataset int32",
    "chunk_size_rationale": "Source is 1.25 GiB with backed CSRDataset int32 layout, 59,837 rows, duplicate obs names, and 36,713 genes; kept conservative 1,000-row chunks after a smoke chunk to bound per-chunk materialization and preserve the P5F recovery pattern.",
}


def row_bytes(row: dict) -> int:
    return int(
        row.get("canonical_bytes")
        or row.get("canonical_size_bytes")
        or row.get("source_bytes")
        or row.get("canonical", {}).get("size_bytes")
        or 0
    )


def row_uri(row: dict) -> str:
    return str(row.get("canonical_uri") or row.get("canonical", {}).get("uri") or "")


def gib(n: int) -> float:
    return n / 1024**3


now = datetime.now(timezone.utc).isoformat()
status = json.loads(STATUS_JSON.read_text())
status["updated_at_utc"] = now
completed = status.setdefault("completed_datasets", {})
completed[DATASET] = {k: ENTRY[k] for k in ["prefix", "n_obs", "n_vars", "chunk_size", "chunks_verified", "verification_json"]}
remaining = status.setdefault("remaining", {})
# Keep these as estimates because the live remaining inventory is tracked in cont8 JSON/MD.
remaining["completed_now"] = len(completed)
remaining["single_staged_smoke_first_candidates_remaining_estimate"] = 18
remaining["total_accessible_candidates_remaining_estimate"] = 18
remaining["next_candidate"] = "GSE247599"
notes = [n for n in status.setdefault("notes", []) if DATASET not in n]
notes.append(
    "GSE250558 was smoke-first ingested from the canonical staged object and verified as 60 same-prefix chunks at chunk size 1000 (59,837 obs / 36,713 vars); source backed layout was CSRDataset int32."
)
status["notes"] = notes
STATUS_JSON.write_text(json.dumps(status, indent=2) + "\n")

prev = json.loads(PREV_INV_JSON.read_text())
inv = copy.deepcopy(prev)
inv["updated_from"] = "prism_p5f_status_20260622.json + prism_p5f_cont7_inventory_20260623.json + live GCS byte/backing check for GSE250558"
completed_list = sorted(set(inv.get("completed", [])) | {DATASET})
inv["completed"] = completed_list
remaining_candidates = [r for r in inv.get("remaining_candidates_by_size", []) if r.get("dataset") != DATASET]
inv["remaining_candidates_by_size"] = remaining_candidates
for row in inv.get("all_rows", []):
    if row.get("dataset") == DATASET:
        row["status"] = "completed_verified"
        row["verification_json"] = ENTRY["verification_json"]
        row["chunk_size"] = ENTRY["chunk_size"]
        row["chunks_verified"] = ENTRY["chunks_verified"]
        row["n_obs"] = ENTRY["n_obs"]
        row["n_vars"] = ENTRY["n_vars"]
        row["controls"] = ENTRY["controls"]
        row["note"] = ENTRY["chunk_size_rationale"]
counts = inv.setdefault("counts", {})
counts["completed"] = len(completed_list)
counts["candidate"] = len(remaining_candidates)
counts["staged_payload_issue"] = 1
counts["missing_source"] = 5
counts["user_excluded"] = 1
inv["newly_completed_this_run"] = {
    "dataset": DATASET,
    "prefix": ENTRY["prefix"],
    "source_bytes": ENTRY["source_bytes"],
    "n_obs": ENTRY["n_obs"],
    "n_vars": ENTRY["n_vars"],
    "chunk_size": ENTRY["chunk_size"],
    "chunks_verified": ENTRY["chunks_verified"],
    "controls": ENTRY["controls"],
    "verification_json": ENTRY["verification_json"],
    "backing": ENTRY["backing"],
    "chunk_size_rationale": ENTRY["chunk_size_rationale"],
}
OUT_INV_JSON.write_text(json.dumps(inv, indent=2) + "\n")

rows = []
for row in remaining_candidates[:10]:
    bytes_ = row_bytes(row)
    rows.append(f"| {row['dataset']} | {gib(bytes_):.2f} | {bytes_} | `{row_uri(row)}` |")

OUT_INV_MD.write_text(
    "# PRISM P5F cont8 remaining inventory — 2026-06-23\n\n"
    "- source: current status artifacts + live GCS/backing check for `GSE250558`\n"
    f"- completed datasets preserved: `{len(completed_list)}`\n"
    f"- newly completed in this run: `{DATASET}` (`{ENTRY['chunks_verified']}` chunks, `{ENTRY['n_obs']:,} × {ENTRY['n_vars']:,}`)\n"
    f"- chunk-size rationale: {ENTRY['chunk_size_rationale']}\n"
    f"- remaining smoke-first candidates: `{len(remaining_candidates)}`\n"
    "- staged payload issues: `1`\n"
    "- missing/source rows: `5`\n"
    "- user-excluded rows: `1`\n\n"
    "## Next candidates by staged object size\n\n"
    "| dataset | GiB | bytes | uri |\n"
    "|---|---:|---:|---|\n"
    + "\n".join(rows)
    + "\n\n## Preserved blockers / exclusions\n\n"
    "- `GSE274751`: staged payload issue; current h5ad remains truncated/corrupt; do not retry until re-staged.\n"
    "- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.\n"
    "- User-excluded: `GSE90063_human-004`.\n"
    "- Browser duplicate-named `(1)` copies for `GSE247274` and `GSE267982` remain redundant; canonical objects only were/should be ingested.\n"
)

completed_names = ", ".join(sorted(completed))
STATUS_MD.write_text(
    "# PRISM P5F recovery status — updated 2026-06-23 after GSE250558\n\n"
    "- staging prefix: `gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/`\n"
    "- Lamin target: `laminlabs/pertdata` branch `jkobject`\n"
    f"- completed verified P5F datasets: `{len(completed)}`\n"
    "- remaining smoke-first staged candidates: `18`\n"
    "- staged payload issues: `1`\n"
    "- missing/source rows: `5`\n"
    "- user-excluded rows: `1`\n\n"
    "## Newly completed in this update\n\n"
    "- `GSE250558`: verified `60/60` same-prefix chunks at chunk size `1000`, shape `59,837 × 36,713`, controls `12,828`.\n"
    "- verification artifacts: `artifacts/schema_audit/prism_GSE250558_chunked_verification.json` and `.md`.\n"
    "- run note: staged object byte-verified at `1,339,211,295` bytes; backed layout `CSRDataset int32`; smoke chunk succeeded then full resume completed without full-loading the matrix.\n\n"
    "## Completed verified P5F datasets\n\n"
    f"{completed_names}\n\n"
    "## Next remaining candidates by staged object size\n\n"
    "| dataset | GiB | bytes | uri |\n"
    "|---|---:|---:|---|\n"
    + "\n".join(rows)
    + "\n\n## Preserved blockers / exclusions\n\n"
    "- `GSE274751`: staged payload issue; current h5ad remains truncated/corrupt (GCS size 531,628,032 bytes; HDF5 stored EOF 2,308,911,126 bytes). Do not retry until re-staged/recovered.\n"
    "- Missing/source-blocked: `GSE247598`, `GSE261157`, `GSE272093`, `GSE272457`, `GSE282731`.\n"
    "- User-excluded: `GSE90063_human-004`.\n"
    "- Browser duplicate-named `(1)` copies for `GSE247274` and `GSE267982` remain redundant; canonical objects only were/should be ingested.\n"
)
print(json.dumps({
    "updated": [str(p.relative_to(ROOT)) for p in [STATUS_JSON, STATUS_MD, OUT_INV_JSON, OUT_INV_MD]],
    "completed": len(completed_list),
    "remaining_candidates": len(remaining_candidates),
    "next_candidate": remaining_candidates[0]["dataset"] if remaining_candidates else None,
}, indent=2))
