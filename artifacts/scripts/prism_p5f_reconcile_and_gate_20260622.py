#!/usr/bin/env python3
"""P5F PRISM residual reconciliation and duplicate gate.

Read-only: reconciles the late-2026-06-22 Google Drive recovery GCS prefix
against historical P5D/P5E residual rows and visible Lamin triplet prefixes.
Does not write to Lamin and does not materialize X matrices.
"""
from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OLD_TSV = ROOT / "artifacts/schema_audit/prism_residual_cleanup_20260622.tsv"
GCS_LS = ROOT / "artifacts/schema_audit/tmp_p5f/gcs_h5ad_ls.txt"
OUT_TSV = ROOT / "artifacts/schema_audit/prism_p5f_google_drive_recovery_gate_20260622.tsv"
OUT_JSON = ROOT / "artifacts/schema_audit/prism_p5f_google_drive_recovery_gate_20260622.json"
OUT_MD = ROOT / "artifacts/schema_audit/prism_p5f_google_drive_recovery_gate_20260622.md"
STAGING_PREFIX = "gs://scperturb/pert-gym/staging/data/main/prism_google_drive_datasets_20260622/"

KNOWN_PUBLIC_ACCESSION_ALIASES = {
    "GSE90063": [
        "scperturb/dixit16_K562_TFs_7_days",
        "scperturb/dixit16_K562_TFs_13_days",
    ],
}


def accession_from_name(name: str) -> str:
    m = re.search(r"GSE\d+", name or "")
    return m.group(0) if m else ""


def canonical_dataset_from_gcs_stem(stem: str) -> str:
    # Browser/manual duplicate downloads were staged as "GSE... (1).h5ad".
    return re.sub(r" \(\d+\)$", "", stem)


def load_gcs_h5ads() -> list[dict[str, Any]]:
    rows = []
    for line in GCS_LS.read_text().splitlines():
        m = re.match(r"\s*(\d+)\s+\S+\s+(gs://.+\.h5ad)$", line)
        if not m:
            continue
        uri = m.group(2)
        stem = Path(uri).stem
        rows.append({
            "dataset_stem": stem,
            "dataset": canonical_dataset_from_gcs_stem(stem),
            "uri": uri,
            "size_bytes": int(m.group(1)),
            "is_duplicate_named_object": bool(re.search(r" \(\d+\)$", stem)),
        })
    return rows


def visible_lamin_triplets() -> tuple[str, str, pd.DataFrame]:
    from tools.lamin_context import connect_pertdata, ensure_project_cache

    ensure_project_cache()
    ln = connect_pertdata()
    instance = ln.setup.settings.instance.slug
    branch = ln.setup.settings.branch.name
    rows = []
    for a in ln.Artifact.filter().all():
        key = getattr(a, "key", "") or ""
        m = re.match(r"(.+)/(obs\.parquet|X\.h5ad|var\.parquet)$", key)
        if not m:
            continue
        prefix, leaf = m.groups()
        rows.append({
            "prefix": prefix,
            "leaf": leaf,
            "key": key,
            "n_observations": getattr(a, "n_observations", None),
            "size": getattr(a, "size", None),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return instance, branch, pd.DataFrame(columns=["prefix", "complete_triplet", "source_accession", "family"])
    piv = df.pivot_table(index="prefix", columns="leaf", values="key", aggfunc="first").reset_index()
    piv["complete_triplet"] = piv[["obs.parquet", "X.h5ad", "var.parquet"]].notna().all(axis=1)
    piv["source_accession"] = piv["prefix"].str.extract(r"(GSE\d+)", expand=False).fillna("")
    piv["family"] = piv["prefix"].str.split("/").str[0]
    return instance, branch, piv


def main() -> int:
    old_rows = list(csv.DictReader(OLD_TSV.open(), delimiter="\t"))
    old_by_dataset = {r["dataset"]: r for r in old_rows}
    old_blocked = [r for r in old_rows if r["decision"] == "blocked_source_or_metadata"]
    old_blocked_set = {r["dataset"] for r in old_blocked}
    gcs_rows = load_gcs_h5ads()
    by_dataset: dict[str, list[dict[str, Any]]] = {}
    for row in gcs_rows:
        by_dataset.setdefault(row["dataset"], []).append(row)

    instance, branch, visible = visible_lamin_triplets()
    assert instance == "laminlabs/pertdata", instance
    assert branch == "jkobject", branch

    out_rows = []
    all_datasets = sorted(old_blocked_set | set(by_dataset) | {"GSE90063_human-004"})
    for dataset in all_datasets:
        accession = accession_from_name(dataset)
        staged = by_dataset.get(dataset, [])
        exact_prefix = f"prism_collection/{dataset}"
        exact_or_chunks = []
        same_accession_prefixes = []
        family_overlap = []
        if not visible.empty:
            exact_or_chunks = sorted(visible[visible["prefix"].str.startswith(exact_prefix)]["prefix"].tolist())
            same = visible[visible["source_accession"] == accession].copy() if accession else pd.DataFrame()
            alias_prefixes = KNOWN_PUBLIC_ACCESSION_ALIASES.get(accession, [])
            if alias_prefixes:
                alias = visible[visible["prefix"].isin(alias_prefixes)].copy()
                if not alias.empty:
                    same = pd.concat([same, alias], ignore_index=True).drop_duplicates("prefix")
            if not same.empty:
                same_accession_prefixes = sorted(same["prefix"].tolist())
                family_overlap = sorted(set(same["family"].tolist()))
        exact_complete = False
        if exact_or_chunks and not visible.empty:
            exact_complete = bool(visible[visible["prefix"].isin(exact_or_chunks)]["complete_triplet"].all())

        prior_decision = old_by_dataset.get(dataset, {}).get("decision", "not_in_p5d_residual")
        duplicate_named = any(r["is_duplicate_named_object"] for r in staged)
        staged_count = len(staged)
        staged_bytes = sum(int(r["size_bytes"]) for r in staged)

        if dataset == "GSE90063_human-004":
            decision = "skip_user_excluded"
            rationale = "Preserve explicit user exclusion due duplicate/subset ambiguity."
        elif exact_complete:
            decision = "skip_already_present_exact"
            rationale = "Exact PRISM prefix/chunks already visible as complete triplets."
        elif same_accession_prefixes and set(family_overlap) - {"prism_collection"}:
            decision = "block_same_accession_outside_prism"
            rationale = "Same accession visible outside PRISM/public family; needs manual subset review."
        elif same_accession_prefixes and "prism_collection" in family_overlap and not exact_or_chunks:
            decision = "block_same_accession_prism_variant"
            rationale = "Same accession visible in PRISM but not exact dataset suffix; inspect before ingesting variant."
        elif duplicate_named:
            decision = "block_duplicate_named_staged_object"
            rationale = "Both canonical and browser duplicate-named staged h5ads exist for this dataset; compare/delete duplicate before ingestion."
        elif staged_count == 1 and prior_decision == "blocked_source_or_metadata":
            decision = "candidate_ingest_smoke_first"
            rationale = "Historical Drive-blocked row is now staged once; no visible exact/same-accession duplicate found by key gate."
        elif staged_count == 0 and prior_decision == "blocked_source_or_metadata":
            decision = "still_missing_source"
            rationale = "Historical Drive-blocked row still has no staged h5ad under the recovery prefix."
        elif staged_count >= 1:
            decision = "block_unexpected_staged_dataset"
            rationale = "Staged h5ad does not map cleanly to historical blocked residual set."
        else:
            decision = "block_unclassified"
            rationale = "No safe action category matched."

        out_rows.append({
            "dataset": dataset,
            "source_accession": accession,
            "prior_p5d_decision": prior_decision,
            "staged_h5ad_count": staged_count,
            "staged_bytes": staged_bytes,
            "staged_uris": ";".join(r["uri"] for r in sorted(staged, key=lambda x: x["uri"])),
            "duplicate_named_staged_object": str(duplicate_named).lower(),
            "existing_exact_or_chunk_prefixes": ";".join(exact_or_chunks),
            "existing_same_accession_prefixes": ";".join(same_accession_prefixes),
            "existing_family_overlap": ";".join(family_overlap),
            "decision": decision,
            "rationale": rationale,
        })

    df = pd.DataFrame(out_rows)
    df.to_csv(OUT_TSV, sep="\t", index=False)
    summary = {
        "staging_prefix": STAGING_PREFIX,
        "h5ad_objects": len(gcs_rows),
        "h5ad_bytes": sum(r["size_bytes"] for r in gcs_rows),
        "historical_blocked_rows": len(old_blocked),
        "historical_blocked_staged_datasets": len([d for d in old_blocked_set if d in by_dataset]),
        "historical_blocked_missing_datasets": sorted([d for d in old_blocked_set if d not in by_dataset]),
        "decision_counts": dict(Counter(df["decision"])),
        "lamin_instance": instance,
        "lamin_branch": branch,
        "outputs": {"tsv": str(OUT_TSV), "json": str(OUT_JSON), "md": str(OUT_MD)},
    }
    OUT_JSON.write_text(json.dumps({"summary": summary, "rows": out_rows}, indent=2) + "\n")

    md = ["# PRISM P5F Google Drive recovery gate — 2026-06-22\n\n"]
    md.append("Read-only reconciliation of the staged Google Drive recovery prefix against P5D/P5E residuals and visible Lamin triplet keys. No Lamin writes and no X matrix loads were performed.\n\n")
    for k, v in summary.items():
        if k != "outputs":
            md.append(f"- {k}: `{v}`\n")
    md.append("\n## Decisions\n\n")
    cols = ["dataset", "staged_h5ad_count", "staged_bytes", "existing_family_overlap", "decision", "rationale"]
    md.append("| " + " | ".join(cols) + " |\n")
    md.append("| " + " | ".join(["---"] * len(cols)) + " |\n")
    for _, r in df[cols].iterrows():
        md.append("| " + " | ".join(str(r[c]).replace("|", "\\|") for c in cols) + " |\n")
    OUT_MD.write_text("".join(md))
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
