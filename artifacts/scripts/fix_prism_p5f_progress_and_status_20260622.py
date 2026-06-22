#!/usr/bin/env python3
"""Write P5F status and normalize progress entries for verified P5F PRISM ingestions."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROGRESS = ROOT / "artifacts/phase3_ingestion_progress.json"
OUT_JSON = ROOT / "artifacts/schema_audit/prism_p5f_status_20260622.json"
OUT_MD = ROOT / "artifacts/schema_audit/prism_p5f_status_20260622.md"
GATE_JSON = ROOT / "artifacts/schema_audit/prism_p5f_google_drive_recovery_gate_20260622.json"

COMPLETED = {
    "GSE255832": {
        "gcs_uri": "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE255832.h5ad",
        "verification": ROOT / "artifacts/schema_audit/prism_GSE255832_chunked_verification.json",
    },
    "GSE263524": {
        "gcs_uri": "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/GSE263524.h5ad",
        "verification": ROOT / "artifacts/schema_audit/prism_GSE263524_chunked_verification.json",
    },
}


def build_progress_entry(dataset: str, info: dict) -> dict:
    verification = json.loads(Path(info["verification"]).read_text())
    results = verification["results"]
    return {
        "dataset": dataset,
        "prefix": f"prism_collection/{dataset}",
        "path": None,
        "gcs_uri": info["gcs_uri"],
        "n_obs": sum(int(r["obs_rows"]) for r in results),
        "n_vars": int(results[0]["var_rows"]) if results else None,
        "chunk_size": int(verification["chunk_size"]),
        "status": "chunked_verified" if verification["ok"] else "chunked_verification_failed",
        "chunks": [
            {
                "prefix": r["prefix"],
                "start": int(r["start"]),
                "end": int(r["end"]),
                "status": "verified" if r["ok"] else "failed",
            }
            for r in results
        ],
        "verification_json": str(Path(info["verification"]).relative_to(ROOT)),
        "note": "P5F Google Drive recovery; memory-bounded backed h5ad ingestion; obs->X->var/payload/counts verified.",
    }


def main() -> int:
    progress = json.loads(PROGRESS.read_text())
    verified_entries = {ds: build_progress_entry(ds, info) for ds, info in COMPLETED.items()}
    for section in ("ingested", "downloaded_not_ingested"):
        progress.setdefault(section, [])
        progress[section] = [e for e in progress[section] if e.get("dataset") not in verified_entries]
    progress["ingested"].extend(verified_entries.values())
    PROGRESS.write_text(json.dumps(progress, indent=2, sort_keys=False) + "\n")

    gate = json.loads(GATE_JSON.read_text())
    summary = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "staging_prefix": gate["summary"]["staging_prefix"],
        "gate_summary": gate["summary"],
        "completed_datasets": {},
        "remaining": {
            "candidate_ingest_smoke_first_before_completed": gate["summary"]["decision_counts"].get("candidate_ingest_smoke_first", 0),
            "completed_now": len(verified_entries),
            "candidate_ingest_smoke_first_remaining_estimate": gate["summary"]["decision_counts"].get("candidate_ingest_smoke_first", 0) - len(verified_entries),
            "duplicate_named_staged_object_blocked": gate["summary"]["decision_counts"].get("block_duplicate_named_staged_object", 0),
            "still_missing_source": gate["summary"]["decision_counts"].get("still_missing_source", 0),
            "user_excluded": gate["summary"]["decision_counts"].get("skip_user_excluded", 0),
        },
        "notes": [
            "GSE90063_human-004 remains excluded by user decision.",
            "GSE247274 and GSE267982 remain blocked as duplicate-named staged object pairs until compared/cleaned.",
            "GSE247598, GSE261157, GSE272093, GSE272457, and GSE282731 remain missing from the staged recovery prefix.",
            "The first GSE255832 smoke chunk was written before ln.track() was patched into the chunker; subsequent P5F writes use the tracked transform cqKr10EUOIPg0000.",
            "GSE263524 was interrupted by the 600s foreground cap at chunk_0037; resume used --overwrite from chunk_0037 and final verification passed 43/43.",
        ],
    }
    for dataset, entry in verified_entries.items():
        summary["completed_datasets"][dataset] = {
            "prefix": entry["prefix"],
            "n_obs": entry["n_obs"],
            "n_vars": entry["n_vars"],
            "chunk_size": entry["chunk_size"],
            "chunks_verified": len(entry["chunks"]),
            "verification_json": entry["verification_json"],
        }
    OUT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=False) + "\n")

    lines = [
        "# PRISM P5F Google Drive recovery status — 2026-06-22",
        "",
        f"- staging prefix: `{summary['staging_prefix']}`",
        f"- duplicate gate: `{Path(GATE_JSON).relative_to(ROOT)}`",
        f"- completed now: `{len(verified_entries)}` datasets",
        f"- remaining smoke-first candidates estimate: `{summary['remaining']['candidate_ingest_smoke_first_remaining_estimate']}`",
        f"- duplicate-named staged pairs blocked: `{summary['remaining']['duplicate_named_staged_object_blocked']}`",
        f"- still missing source rows: `{summary['remaining']['still_missing_source']}`",
        f"- user-excluded rows: `{summary['remaining']['user_excluded']}`",
        "",
        "## Completed and verified",
        "",
        "| dataset | obs | vars | chunks | prefix | verification |",
        "|---|---:|---:|---:|---|---|",
    ]
    for ds, row in summary["completed_datasets"].items():
        lines.append(
            f"| {ds} | {row['n_obs']} | {row['n_vars']} | {row['chunks_verified']} | `{row['prefix']}` | `{row['verification_json']}` |"
        )
    lines.extend(["", "## Notes", ""])
    lines.extend(f"- {note}" for note in summary["notes"])
    OUT_MD.write_text("\n".join(lines) + "\n")
    print(json.dumps({"status_json": str(OUT_JSON), "status_md": str(OUT_MD), "completed": list(verified_entries)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
