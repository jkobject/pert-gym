#!/usr/bin/env python3
"""Recompute the PRISM P5F residual inventory for cont12.

Read-only with respect to Lamin: parses the current GCS staging listing and visible
Lamin triplet keys on branch jkobject, then derives remaining candidates from the
current status artifact rather than trusting a stale handoff count.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
GCS_LS = ROOT / "artifacts/schema_audit/prism_p5f_cont12_gcs_h5ad_ls_20260623.txt"
STATUS_JSON = ROOT / "artifacts/schema_audit/prism_p5f_status_20260622.json"
OUT_JSON = ROOT / "artifacts/schema_audit/prism_p5f_cont12_inventory_20260623.json"
OUT_MD = ROOT / "artifacts/schema_audit/prism_p5f_cont12_inventory_20260623.md"
STAGING_PREFIX = "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/"
KNOWN_TRUNCATED = {"GSE274751"}
MISSING_SOURCE = {"GSE247598", "GSE261157", "GSE272093", "GSE272457", "GSE282731"}
USER_EXCLUDED = {"GSE90063_human-004"}


def canonical_dataset_from_stem(stem: str) -> str:
    return re.sub(r" \(\d+\)$", "", stem)


def accession_from_name(name: str) -> str:
    m = re.search(r"GSE\d+", name or "")
    return m.group(0) if m else ""


def load_gcs_h5ads() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in GCS_LS.read_text().splitlines():
        m = re.match(r"\s*(\d+)\s+\S+\s+(gs://.+\.h5ad)$", line)
        if not m:
            continue
        uri = m.group(2)
        stem = Path(uri).stem
        rows.append(
            {
                "dataset": canonical_dataset_from_stem(stem),
                "object_name": Path(uri).name,
                "uri": uri,
                "bytes": int(m.group(1)),
                "duplicate_browser_copy": bool(re.search(r" \(\d+\)$", stem)),
            }
        )
    return rows


def visible_triplet_prefixes() -> tuple[str, str, pd.DataFrame]:
    from tools.lamin_context import connect_pertdata, ensure_project_cache

    ensure_project_cache()
    ln = connect_pertdata()
    instance = ln.setup.settings.instance.slug
    branch = ln.setup.settings.branch.name
    rows: list[dict[str, Any]] = []
    for artifact in ln.Artifact.filter().all():
        key = getattr(artifact, "key", "") or ""
        m = re.match(r"(.+)/(obs\.parquet|X\.h5ad|var\.parquet)$", key)
        if not m:
            continue
        prefix, leaf = m.groups()
        rows.append({"prefix": prefix, "leaf": leaf, "key": key})
    df = pd.DataFrame(rows)
    if df.empty:
        return instance, branch, pd.DataFrame(columns=["prefix", "complete_triplet", "source_accession", "family"])
    piv = df.pivot_table(index="prefix", columns="leaf", values="key", aggfunc="first").reset_index()
    piv["complete_triplet"] = piv[["obs.parquet", "X.h5ad", "var.parquet"]].notna().all(axis=1)
    piv["source_accession"] = piv["prefix"].str.extract(r"(GSE\d+)", expand=False).fillna("")
    piv["family"] = piv["prefix"].str.split("/").str[0]
    return instance, branch, piv


def main() -> int:
    status = json.loads(STATUS_JSON.read_text())
    completed = set(status.get("completed_datasets", {}))
    gcs_rows = load_gcs_h5ads()
    by_dataset: dict[str, list[dict[str, Any]]] = {}
    for row in gcs_rows:
        by_dataset.setdefault(row["dataset"], []).append(row)

    instance, branch, visible = visible_triplet_prefixes()
    assert instance == "laminlabs/pertdata", instance
    assert branch == "jkobject", branch

    all_datasets = sorted(set(by_dataset) | completed | KNOWN_TRUNCATED | MISSING_SOURCE | USER_EXCLUDED)
    all_rows: list[dict[str, Any]] = []
    for dataset in all_datasets:
        staged = sorted(by_dataset.get(dataset, []), key=lambda r: r["uri"])
        accession = accession_from_name(dataset)
        exact_prefix = f"prism_collection/{dataset}"
        exact_or_chunks: list[str] = []
        same_accession_prefixes: list[str] = []
        if not visible.empty:
            exact_or_chunks = sorted(visible[visible["prefix"].str.startswith(exact_prefix)]["prefix"].tolist())
            if accession:
                same_accession_prefixes = sorted(visible[visible["source_accession"] == accession]["prefix"].tolist())
        exact_complete = False
        if exact_or_chunks and not visible.empty:
            exact_complete = bool(visible[visible["prefix"].isin(exact_or_chunks)]["complete_triplet"].all())
        duplicate_named = any(r["duplicate_browser_copy"] for r in staged)
        if dataset in completed or exact_complete:
            decision = "completed"
        elif dataset in USER_EXCLUDED:
            decision = "skip_user_excluded"
        elif dataset in MISSING_SOURCE and not staged:
            decision = "still_missing_source"
        elif dataset in KNOWN_TRUNCATED:
            decision = "staged_h5ad_truncated"
        elif duplicate_named:
            decision = "block_duplicate_named_staged_object"
        elif len(staged) == 1:
            decision = "candidate_ingest_smoke_first"
        elif len(staged) == 0:
            decision = "still_missing_source"
        else:
            decision = "block_unclassified"
        all_rows.append(
            {
                "dataset": dataset,
                "status": "candidate" if decision == "candidate_ingest_smoke_first" else decision,
                "decision": decision,
                "accession": accession,
                "gcs_objects": staged,
                "canonical_uri": staged[0]["uri"] if len(staged) == 1 else None,
                "canonical_bytes": staged[0]["bytes"] if len(staged) == 1 else None,
                "existing_exact_or_chunk_prefixes": exact_or_chunks,
                "existing_same_accession_prefixes": same_accession_prefixes,
            }
        )

    candidates = sorted(
        [r for r in all_rows if r["decision"] == "candidate_ingest_smoke_first"],
        key=lambda r: (int(r["canonical_bytes"] or 0), r["dataset"]),
    )
    result = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "staging_prefix": STAGING_PREFIX,
        "source_gcs_listing": str(GCS_LS.relative_to(ROOT)),
        "lamin_instance": instance,
        "lamin_branch": branch,
        "counts": {
            "gcs_h5ad_objects": len(gcs_rows),
            "gcs_h5ad_bytes": sum(int(r["bytes"]) for r in gcs_rows),
            "completed": sum(1 for r in all_rows if r["decision"] == "completed"),
            "candidate": len(candidates),
            "staged_payload_issue": sum(1 for r in all_rows if r["decision"] == "staged_h5ad_truncated"),
            "missing_source": sum(1 for r in all_rows if r["decision"] == "still_missing_source"),
            "user_excluded": sum(1 for r in all_rows if r["decision"] == "skip_user_excluded"),
        },
        "decision_counts": dict(Counter(r["decision"] for r in all_rows)),
        "remaining_candidates_by_size": candidates,
        "all_rows": all_rows,
        "next_candidate": candidates[0]["dataset"] if candidates else None,
        "preserved_blockers": {
            "staged_h5ad_truncated": sorted(KNOWN_TRUNCATED),
            "missing_source": sorted(MISSING_SOURCE),
            "user_excluded": sorted(USER_EXCLUDED),
        },
    }
    OUT_JSON.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n")
    md = ["# PRISM P5F cont12 inventory — 2026-06-23\n\n"]
    md.append("Recomputed from the current GCS staging listing and visible Lamin triplet keys on branch `jkobject`; no Lamin writes and no X matrix loads.\n\n")
    md.append(f"- GCS h5ad objects: {result['counts']['gcs_h5ad_objects']}\n")
    md.append(f"- GCS h5ad bytes: {result['counts']['gcs_h5ad_bytes']}\n")
    md.append(f"- Completed datasets: {result['counts']['completed']}\n")
    md.append(f"- Accessible smoke-first candidates: {result['counts']['candidate']}\n")
    md.append(f"- Staged payload issues: {result['counts']['staged_payload_issue']}\n")
    md.append(f"- Missing/source-blocked: {result['counts']['missing_source']}\n")
    md.append(f"- User-excluded: {result['counts']['user_excluded']}\n")
    md.append(f"- Next candidate: `{result['next_candidate']}`\n\n")
    md.append("## Remaining candidates by current GCS size\n\n")
    md.append("| dataset | bytes | uri |\n| --- | ---: | --- |\n")
    for row in candidates:
        md.append(f"| {row['dataset']} | {row['canonical_bytes']} | `{row['canonical_uri']}` |\n")
    OUT_MD.write_text("".join(md))
    print(json.dumps({"counts": result["counts"], "next_candidate": result["next_candidate"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
