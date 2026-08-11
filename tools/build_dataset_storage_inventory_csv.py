#!/usr/bin/env python3
"""Build the review CSV joining canonical GCS storage and Lamin catalog records.

The GCS rows come from the canonical migration receipts. Branch-scoped Lamin
catalog evidence comes from the committed jkobject-vs-main inventory snapshot.
Catalog presence is not publication or completion. Scientific completion is
fail-closed: only datasets named in the independently accepted OBS and VAR
status ledgers are marked done.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE = ROOT / "data/pert_gym_dataset_storage_inventory.csv"
DEFAULT_OUTPUT = DEFAULT_BASE
LAMIN_SNAPSHOT = (
    ROOT / "artifacts/notebook_decisions/jkobject_vs_main_dataset_inventory.json"
)
CLEANED_RECEIPT = (
    ROOT / "artifacts/gcs_canonical_migration/cleaned_direct_datasets.json"
)
LAMIN_REMAP_RECEIPT = (
    ROOT / "artifacts/gcs_canonical_migration/lamin_key_remap_receipt.json"
)
LAMIN_SNAPSHOT_LABEL = "jkobject-vs-main-v1@2026-07-11T23:11:48Z"
SCIENTIFIC_LEDGER_LABEL = "TODO.md accepted OBS=10/70 VAR=8/70 @ 2026-07-27"

# These aliases are the intersection of the independently accepted OBS and VAR
# ledgers described in TODO.md. Keep fail-closed: adding a file or Collection is
# not sufficient to enter either set.
OBS_ACCEPTED = {
    "drug-seq",
    "ginkgo-datapoints/vcpi",
    "lincs-level2-phase2-all",
    "scperturb/datlinger17",
    "scperturb/chang22",
    "depmap_ccle/26q1",
    "depmap_ccle26q1",
    "scperturb/adamson16",
    "schiebingerlander2019",
    "gse132080",
    "gse197452",
}
VAR_ACCEPTED = {
    "drug-seq",
    "scperturb/chang22",
    "depmap_ccle/26q1",
    "depmap_ccle26q1",
    "scperturb/adamson16",
    "schiebingerlander2019",
    "gse132080",
    "scperturb/datlinger17",
    "gse197452",
}

LAYER_COLUMNS = [
    "dataset_name",
    "storage_membership",
    "in_raw",
    "raw_objects",
    "raw_bytes",
    "raw_size",
    "in_cleaned",
    "cleaned_objects",
    "cleaned_bytes",
    "cleaned_size",
    "in_lamindb",
    "lamin_artifacts",
    "lamin_bytes",
    "lamin_size",
]
REVIEW_COLUMNS = [
    "lamin_dataset_id",
    "scientific_dataset_id",
    "lamin_classification",
    "lamin_collection_membership",
    "lamin_artifact_uids",
    "lamin_inventory_evidence",
    "lamin_branch_scope",
    "in_canonical_lamindb",
    "lamin_catalog_status",
    "x_present",
    "obs_present",
    "var_present",
    "chunking_status",
    "structurally_done",
    "obs_done",
    "var_done",
    "completely_done",
    "completion_status",
    "scientific_review_evidence",
]


def _truth(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "/", value.lower()).strip("/")


def _tokens(row: dict[str, Any]) -> set[str]:
    values = [
        row.get("dataset_id", ""),
        row.get("accession", ""),
        row.get("source", ""),
        *row.get("logical_prefixes", []),
    ]
    tokens = {_norm(str(value)) for value in values if value and value != "unknown"}
    for value in list(tokens):
        tokens.update(part for part in value.split("/") if part)
    return tokens


def _accepted(dataset_id: str, accepted: set[str]) -> bool:
    token = _norm(dataset_id)
    return any(token == _norm(item) or _norm(item) in token for item in accepted)


def _scientific_dataset_id(dataset_id: str) -> str:
    return re.sub(r"_GSM\d+_10X\d+$", "", dataset_id)


def _best_lamin_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    allowed = {"added_dataset", "branch_revision"}
    for row in snapshot["rows"]:
        if row.get("classification") not in allowed:
            continue
        if str(row.get("dataset_id", "")).startswith("excluded/"):
            continue
        suffixes = row.get("jkobject_counts_by_suffix", {})
        if not ({".h5ad", ".parquet"} & set(suffixes)):
            continue
        grouped[row["dataset_id"]].append(row)

    result = []
    for dataset_id, candidates in grouped.items():
        candidates.sort(
            key=lambda row: (
                row.get("classification") == "branch_revision",
                bool(row.get("collection_membership")),
                int(row.get("jkobject_artifact_record_count", 0)),
            ),
            reverse=True,
        )
        result.append(candidates[0])
    return sorted(result, key=lambda row: row["dataset_id"].lower())


def _match_cleaned_name(
    lamin_row: dict[str, Any], cleaned_names: set[str]
) -> str | None:
    row_tokens = _tokens(lamin_row)
    exact = [name for name in cleaned_names if _norm(name) in row_tokens]
    if len(exact) == 1:
        return exact[0]
    accession = str(lamin_row.get("accession", ""))
    if accession in cleaned_names:
        return accession
    return None


def _empty_layer_row(dataset_name: str) -> dict[str, Any]:
    return {
        "dataset_name": dataset_name,
        "storage_membership": "Lamin catalog working/historical only",
        "in_raw": False,
        "raw_objects": 0,
        "raw_bytes": 0,
        "raw_size": "0 B",
        "in_cleaned": False,
        "cleaned_objects": 0,
        "cleaned_bytes": 0,
        "cleaned_size": "0 B",
        "in_lamindb": False,
        "lamin_artifacts": 0,
        "lamin_bytes": 0,
        "lamin_size": "unknown",
    }


def _build_rows_from_evidence(
    base_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    snapshot = json.loads(LAMIN_SNAPSHOT.read_text())
    cleaned_receipt = json.loads(CLEANED_RECEIPT.read_text())["datasets"]
    remap_receipt = json.loads(LAMIN_REMAP_RECEIPT.read_text())
    canonical_lamin_names = {
        change["new_key"].split("/")[2]
        for change in remap_receipt["changes"]
        if change.get("new_key", "").startswith("data/cleaned/")
    }
    rows_by_name = {
        row["dataset_name"]: dict(row)
        for row in base_rows
        if not str(row.get("dataset_name", "")).startswith("excluded/")
    }
    cleaned_names = set(cleaned_receipt)

    for row in rows_by_name.values():
        row.update({column: "" for column in REVIEW_COLUMNS})
        name = row["dataset_name"]
        is_canonical_lamin = name in canonical_lamin_names
        row["in_lamindb"] = is_canonical_lamin
        row["lamin_artifacts"] = 3 if is_canonical_lamin else 0
        row["storage_membership"] = " + ".join(
            label
            for present, label in [
                (_truth(row.get("in_raw")), "raw"),
                (_truth(row.get("in_cleaned")), "cleaned"),
                (is_canonical_lamin, "canonical LaminDB"),
            ]
            if present
        )
        sources = cleaned_receipt.get(name, {}).get("sources", {})
        row["x_present"] = "X" in sources
        row["obs_present"] = "obs" in sources
        row["var_present"] = "var" in sources
        row["chunking_status"] = (
            "single_triplet_not_chunked"
            if all(key in sources for key in ("X", "obs", "var"))
            else "not_applicable_or_unknown"
        )
        row["structurally_done"] = all(key in sources for key in ("X", "obs", "var"))
        row["obs_done"] = False
        row["var_done"] = False
        row["completely_done"] = False
        row["completion_status"] = (
            "structurally_done_scientific_review_pending"
            if row["structurally_done"]
            else "not_structurally_done"
        )
        row["scientific_review_evidence"] = SCIENTIFIC_LEDGER_LABEL
        row["in_canonical_lamindb"] = name in canonical_lamin_names
        row["lamin_branch_scope"] = (
            "jkobject_canonical_cleaned"
            if name in canonical_lamin_names
            else "not_established_by_branch_delta_snapshot"
        )
        row["lamin_catalog_status"] = (
            "canonical_cleaned_artifact"
            if name in canonical_lamin_names
            else "no_canonical_lamin_evidence"
        )

    for lamin_row in _best_lamin_rows(snapshot):
        matched = _match_cleaned_name(lamin_row, cleaned_names)
        dataset_name = matched or lamin_row["dataset_id"]
        target = rows_by_name.setdefault(dataset_name, _empty_layer_row(dataset_name))
        is_canonical_lamin = matched in canonical_lamin_names
        target["in_lamindb"] = is_canonical_lamin
        lamin_layer = (
            "canonical LaminDB"
            if is_canonical_lamin
            else "Lamin catalog working/historical"
        )
        layers = [
            label
            for present, label in [
                (_truth(target.get("in_raw")), "raw"),
                (_truth(target.get("in_cleaned")), "cleaned"),
                (True, lamin_layer),
            ]
            if present
        ]
        target["storage_membership"] = " + ".join(layers)
        suffixes = lamin_row.get("jkobject_counts_by_suffix", {})
        artifact_count = int(lamin_row.get("jkobject_artifact_record_count", 0))
        target["lamin_artifacts"] = max(
            int(target.get("lamin_artifacts") or 0), artifact_count
        )
        target["lamin_dataset_id"] = lamin_row["dataset_id"]
        target["scientific_dataset_id"] = _scientific_dataset_id(
            lamin_row["dataset_id"]
        )
        target["lamin_classification"] = lamin_row["classification"]
        target["lamin_collection_membership"] = " | ".join(
            lamin_row.get("collection_membership", [])
        )
        target["lamin_artifact_uids"] = " | ".join(
            lamin_row.get("branch_only_evidence", {}).get("artifact_uids", [])
        )
        target["lamin_inventory_evidence"] = LAMIN_SNAPSHOT_LABEL
        target["lamin_branch_scope"] = {
            "added_dataset": "jkobject_only",
            "branch_revision": "main_and_jkobject_with_jkobject_revision",
        }[lamin_row["classification"]]
        if is_canonical_lamin:
            target["in_canonical_lamindb"] = True
            target["lamin_catalog_status"] = "canonical_cleaned_artifact"
        else:
            target["in_canonical_lamindb"] = False
            target["lamin_catalog_status"] = (
                "working_or_historical_not_in_canonical_cleaned_layout"
            )

        # For Lamin catalog rows outside the canonical cleaned layout, suffix counts provide candidate structure,
        # not proof of obs→X→var links. Keep structural completion fail-closed.
        if not _truth(target.get("in_cleaned")):
            target["x_present"] = int(suffixes.get(".h5ad", 0)) > 0
            target["obs_present"] = int(suffixes.get(".parquet", 0)) >= 1
            target["var_present"] = int(suffixes.get(".parquet", 0)) >= 2
            target["chunking_status"] = "unknown_requires_link_readback"
            target["structurally_done"] = False

        target["obs_done"] = _accepted(lamin_row["dataset_id"], OBS_ACCEPTED)
        target["var_done"] = _accepted(lamin_row["dataset_id"], VAR_ACCEPTED)
        if target["obs_done"] and target["var_done"]:
            # The accepted scientific ledgers bind the OBS and VAR revisions to
            # the inherited X plus exact row/feature parity. Branch snapshots
            # frequently contain only the revised parquet artifacts, so suffix
            # counts alone would incorrectly call these incomplete.
            target["structurally_done"] = True
            target["chunking_status"] = "accepted_revision_link_readback"
        target["completely_done"] = bool(
            _truth(target.get("structurally_done"))
            and target["obs_done"]
            and target["var_done"]
        )
        if target["completely_done"]:
            target["completion_status"] = "complete_obs_var_structure_accepted"
        elif target["obs_done"] or target["var_done"]:
            target["completion_status"] = "partially_scientifically_accepted"
        elif _truth(target.get("structurally_done")):
            target["completion_status"] = "structurally_done_scientific_review_pending"
        else:
            target["completion_status"] = "catalog_record_requires_structural_readback"
        target["scientific_review_evidence"] = SCIENTIFIC_LEDGER_LABEL

    return [rows_by_name[name] for name in sorted(rows_by_name, key=str.lower)]


def build_rows(
    base_rows: list[dict[str, Any]], *, refresh_from_evidence: bool = False
) -> list[dict[str, Any]]:
    """Canonicalize the frozen snapshot or explicitly refresh local evidence."""
    if refresh_from_evidence:
        return _build_rows_from_evidence(base_rows)
    expected = LAYER_COLUMNS + REVIEW_COLUMNS
    if not base_rows or list(base_rows[0]) != expected:
        raise RuntimeError("frozen storage inventory has unexpected columns")
    if any(list(row) != expected for row in base_rows):
        raise RuntimeError("frozen storage inventory rows have inconsistent columns")
    names = [str(row["dataset_name"]) for row in base_rows]
    if len(names) != 404 or len(set(names)) != 404:
        raise RuntimeError("frozen storage inventory must contain 404 unique rows")
    rows = [dict(row) for row in base_rows]
    for row in rows:
        # Historical/working catalog evidence remains available in the explicit
        # Lamin metadata columns. Reserve this broad-facing boolean for accepted
        # canonical cleaned publication only.
        row["in_lamindb"] = _truth(row["in_canonical_lamindb"])
    return sorted(rows, key=lambda row: str(row["dataset_name"]).lower())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, default=DEFAULT_BASE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--refresh-from-evidence", action="store_true")
    args = parser.parse_args()
    with args.base.open(newline="", encoding="utf-8") as handle:
        base_rows = list(csv.DictReader(handle))
    if not base_rows:
        raise RuntimeError(f"empty base inventory: {args.base}")
    rows = build_rows(
        base_rows,
        refresh_from_evidence=args.refresh_from_evidence,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fields = LAYER_COLUMNS + REVIEW_COLUMNS
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    complete = sum(_truth(row["completely_done"]) for row in rows)
    canonical_lamin_without_raw_or_cleaned = sum(
        _truth(row["in_lamindb"])
        and not _truth(row["in_raw"])
        and not _truth(row["in_cleaned"])
        for row in rows
    )
    historical_catalog_without_raw_or_cleaned = sum(
        row["lamin_catalog_status"]
        == "working_or_historical_not_in_canonical_cleaned_layout"
        and not _truth(row["in_raw"])
        and not _truth(row["in_cleaned"])
        for row in rows
    )
    print(
        json.dumps(
            {
                "rows": len(rows),
                "completely_done": complete,
                "canonical_lamin_without_raw_or_cleaned": canonical_lamin_without_raw_or_cleaned,
                "historical_catalog_without_raw_or_cleaned": historical_catalog_without_raw_or_cleaned,
                "output": str(args.output),
                "lamin_evidence": LAMIN_SNAPSHOT_LABEL,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
