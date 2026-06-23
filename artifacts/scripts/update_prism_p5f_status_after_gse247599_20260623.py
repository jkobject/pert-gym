#!/usr/bin/env python3
"""Update PRISM P5F status/progress artifacts after verified GSE247599 ingestion."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATASET = "GSE247599"
SOURCE_URI = "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247599.h5ad"
SOURCE_BYTES = 1_433_706_825
VERIFICATION_JSON = "artifacts/schema_audit/prism_GSE247599_chunked_verification.json"
CHUNK_SIZE = 1000
PREFIX = f"prism_collection/{DATASET}"


def load(rel: str):
    return json.loads((ROOT / rel).read_text())


def save(rel: str, data) -> None:
    (ROOT / rel).write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")


def upsert_by_dataset(items: list[dict], entry: dict) -> None:
    items[:] = [item for item in items if item.get("dataset") != entry.get("dataset")]
    items.append(entry)


def main() -> int:
    now = datetime.now(timezone.utc).isoformat()
    verification = load(VERIFICATION_JSON)
    assert verification["ok"] is True
    assert verification["dataset"] == DATASET
    assert verification["chunks_verified"] == 25
    assert verification["rows_verified"] == 24435
    assert verification["expected_vars"] == 36602

    status_rel = "artifacts/schema_audit/prism_p5f_status_20260622.json"
    status = load(status_rel)
    status["updated_at_utc"] = now
    status["completed_datasets"][DATASET] = {
        "prefix": PREFIX,
        "n_obs": verification["rows_verified"],
        "n_vars": verification["expected_vars"],
        "chunk_size": CHUNK_SIZE,
        "chunks_verified": verification["chunks_verified"],
        "verification_json": VERIFICATION_JSON,
        "source_uri": SOURCE_URI,
        "source_bytes": SOURCE_BYTES,
        "controls": verification["controls"],
    }
    status["remaining"].update(
        {
            "completed_now": len(status["completed_datasets"]),
            "single_staged_smoke_first_candidates_remaining_estimate": 17,
            "total_accessible_candidates_remaining_estimate": 17,
            "still_missing_source": 5,
            "user_excluded": 1,
            "staged_payload_issue": 1,
            "next_candidate": "GSE283614",
        }
    )
    notes = status.setdefault("notes", [])
    note = (
        "2026-06-23 t_5b591c9d: GSE247599 ingested from staged Google Drive h5ad "
        "as 25 backed CSR chunks (chunk_size=1000), verified obs->X->var links, X payloads, "
        "24,435 rows, 36,602 vars, and required obs fields."
    )
    if note not in notes:
        notes.append(note)
    save(status_rel, status)

    prev_inventory_rel = "artifacts/schema_audit/prism_p5f_cont8_inventory_20260623.json"
    prev = load(prev_inventory_rel)
    remaining = [row for row in prev["remaining_candidates_by_size"] if row.get("dataset") != DATASET]
    all_rows = []
    for row in prev["all_rows"]:
        if row.get("dataset") == DATASET:
            updated = dict(row)
            updated.update(
                {
                    "status": "completed",
                    "gate_decision": "ingested_verified",
                    "prefix": PREFIX,
                    "chunk_size": CHUNK_SIZE,
                    "chunks_verified": verification["chunks_verified"],
                    "rows_verified": verification["rows_verified"],
                    "n_vars": verification["expected_vars"],
                    "controls": verification["controls"],
                    "verification_json": VERIFICATION_JSON,
                }
            )
            all_rows.append(updated)
        else:
            all_rows.append(row)
    prev_completed = prev.get("completed", {})
    if isinstance(prev_completed, list):
        completed = sorted(set(prev_completed) | {DATASET})
    else:
        completed = dict(prev_completed)
        completed[DATASET] = status["completed_datasets"][DATASET]
    cont9 = {
        "updated_from": prev_inventory_rel,
        "updated_at_utc": now,
        "completed": completed,
        "counts": {
            "gcs_h5ad_objects": prev["counts"]["gcs_h5ad_objects"],
            "completed": 13,
            "candidate": 17,
            "staged_payload_issue": 1,
            "missing_source": 5,
            "user_excluded": 1,
        },
        "remaining_candidates_by_size": remaining,
        "all_rows": all_rows,
        "newly_completed_this_run": [DATASET],
        "next_candidate": "GSE283614",
        "preserved_blockers": {
            "staged_h5ad_truncated": ["GSE274751"],
            "missing_source": ["GSE247598", "GSE261157", "GSE272093", "GSE272457", "GSE282731"],
            "user_excluded": ["GSE90063_human-004"],
        },
    }
    save("artifacts/schema_audit/prism_p5f_cont9_inventory_20260623.json", cont9)

    progress_rel = "artifacts/phase3_ingestion_progress.json"
    progress = load(progress_rel)
    progress["last_updated"] = now
    progress["downloaded_not_ingested"] = [
        row for row in progress.get("downloaded_not_ingested", []) if row.get("dataset") != DATASET
    ]
    chunks = [
        {"prefix": c["prefix"], "start": (c["chunk"] * CHUNK_SIZE), "end": min((c["chunk"] + 1) * CHUNK_SIZE, verification["expected_rows"]), "status": "verified"}
        for c in verification["chunks"]
    ]
    upsert_by_dataset(
        progress.setdefault("ingested", []),
        {
            "dataset": DATASET,
            "prefix": PREFIX,
            "path": "data/gcs_cache/scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE247599.h5ad",
            "gcs_uri": SOURCE_URI,
            "gcs_bytes": SOURCE_BYTES,
            "n_obs": verification["rows_verified"],
            "n_vars": verification["expected_vars"],
            "chunk_size": CHUNK_SIZE,
            "status": "chunked_verified",
            "chunks": chunks,
            "verification_json": VERIFICATION_JSON,
            "controls": verification["controls"],
            "note": "Memory-bounded backed h5ad ingestion; verified obs->X->var links, X payloads, counts, and required obs fields.",
        },
    )
    save(progress_rel, progress)

    print(json.dumps({"updated": [status_rel, "artifacts/schema_audit/prism_p5f_cont9_inventory_20260623.json", progress_rel], "next_candidate": "GSE283614"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
