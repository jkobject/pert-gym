#!/usr/bin/env python3
"""Update PRISM P5F status/progress artifacts after verified GSE280767 ingestion."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATASET = "GSE280767"
SOURCE_URI = "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE280767.h5ad"
SOURCE_BYTES = 2_010_340_238
VERIFICATION_JSON = "artifacts/schema_audit/prism_GSE280767_chunked_verification.json"
SOURCE_PROBE_JSON = "artifacts/schema_audit/prism_GSE280767_source_probe_20260623.json"
CHUNK_SIZE = 1000
PREFIX = f"prism_collection/{DATASET}"
NEXT_CANDIDATE = "GSE269596"


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
    source_probe = load(SOURCE_PROBE_JSON)
    assert verification["ok"] is True
    assert verification["dataset"] == DATASET
    assert verification["chunks_verified"] == 247
    assert verification["rows_verified"] == 246027
    assert verification["expected_vars"] == 36601
    assert source_probe["local_bytes"] == SOURCE_BYTES

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
        "source_probe_json": SOURCE_PROBE_JSON,
        "source_uri": SOURCE_URI,
        "source_bytes": SOURCE_BYTES,
        "controls": verification["controls"],
    }
    status["remaining"].update(
        {
            "completed_now": len(status["completed_datasets"]),
            "single_staged_smoke_first_candidates_remaining_estimate": 15,
            "total_accessible_candidates_remaining_estimate": 15,
            "still_missing_source": 5,
            "user_excluded": 1,
            "staged_payload_issue": 1,
            "next_candidate": NEXT_CANDIDATE,
        }
    )
    note = (
        "2026-06-23 t_ac8dc1a3: GSE280767 ingested from staged Google Drive h5ad "
        "as 247 backed CSR chunks (chunk_size=1000), verified obs->X->var links, X payloads, "
        "246,027 rows, 36,601 vars, and required obs fields."
    )
    notes = status.setdefault("notes", [])
    if note not in notes:
        notes.append(note)
    save(status_rel, status)

    progress_rel = "artifacts/phase3_ingestion_progress.json"
    progress = load(progress_rel)
    progress["last_updated"] = now
    progress["downloaded_not_ingested"] = [
        row for row in progress.get("downloaded_not_ingested", []) if row.get("dataset") != DATASET
    ]
    chunks = [
        {
            "prefix": c["prefix"],
            "start": c["chunk"] * CHUNK_SIZE,
            "end": min((c["chunk"] + 1) * CHUNK_SIZE, verification["expected_rows"]),
            "status": "verified",
        }
        for c in verification["chunks"]
    ]
    upsert_by_dataset(
        progress.setdefault("ingested", []),
        {
            "dataset": DATASET,
            "prefix": PREFIX,
            "path": "data/gcs_cache/scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE280767.h5ad",
            "gcs_uri": SOURCE_URI,
            "gcs_bytes": SOURCE_BYTES,
            "n_obs": verification["rows_verified"],
            "n_vars": verification["expected_vars"],
            "chunk_size": CHUNK_SIZE,
            "status": "chunked_verified",
            "chunks": chunks,
            "verification_json": VERIFICATION_JSON,
            "source_probe_json": SOURCE_PROBE_JSON,
            "controls": verification["controls"],
            "note": "Memory-bounded backed h5ad ingestion; verified obs->X->var links, X payloads, counts, and required obs fields.",
        },
    )
    save(progress_rel, progress)

    inventory_rel = "artifacts/schema_audit/prism_p5f_cont11_inventory_20260623.json"
    inventory = load(inventory_rel)
    inventory["post_ingestion_update_at_utc"] = now
    for row in inventory.get("all_rows", []):
        if row.get("dataset") == DATASET:
            row.update(
                {
                    "status": "completed",
                    "decision": "completed",
                    "prefix": PREFIX,
                    "chunk_size": CHUNK_SIZE,
                    "chunks_verified": verification["chunks_verified"],
                    "rows_verified": verification["rows_verified"],
                    "n_vars": verification["expected_vars"],
                    "controls": verification["controls"],
                    "verification_json": VERIFICATION_JSON,
                    "source_probe_json": SOURCE_PROBE_JSON,
                }
            )
    inventory["remaining_candidates_by_size"] = [
        row for row in inventory.get("remaining_candidates_by_size", []) if row.get("dataset") != DATASET
    ]
    decision_counts = Counter(row["decision"] for row in inventory.get("all_rows", []))
    inventory["counts"].update(
        {
            "completed": int(decision_counts.get("completed", 0)),
            "candidate": len(inventory["remaining_candidates_by_size"]),
            "staged_payload_issue": int(decision_counts.get("staged_h5ad_truncated", 0)),
            "missing_source": int(decision_counts.get("still_missing_source", 0)),
            "user_excluded": int(decision_counts.get("skip_user_excluded", 0)),
        }
    )
    inventory["decision_counts"] = dict(decision_counts)
    inventory["newly_completed_this_run"] = [DATASET]
    inventory["next_candidate"] = NEXT_CANDIDATE
    save(inventory_rel, inventory)

    print(json.dumps({"updated": [status_rel, progress_rel, inventory_rel], "next_candidate": NEXT_CANDIDATE}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
