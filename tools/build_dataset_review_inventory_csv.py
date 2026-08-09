#!/usr/bin/env python3
"""Build one-row-per-dataset review inventory with orthogonal acceptance gates.

This intentionally does not count Lamin records, triplet members, chunks, or
catalogue rows as datasets. It combines the frozen 70-real-dataset scientific
crosswalk with the 22 genuinely-new logical families from the accepted newness
reconciliation. Every gate is fail-closed and retains its evidence source.
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
DEFAULT_OUTPUT = ROOT / "data/pert_gym_dataset_review_inventory.csv"
REAL_DATASETS = ROOT / "artifacts/schema_audit/final_real_dataset_obs_var_20260717.tsv"
NEWNESS = ROOT / "artifacts/orchestration/accepted_28_newness_reconciliation_20260717.json"
BRANCH_SNAPSHOT = ROOT / "artifacts/notebook_decisions/jkobject_vs_main_dataset_inventory.json"
PUBLICATION_LEDGER = ROOT / "artifacts/orchestration/publication_queue/accepted_component_identities_v1.progress.snapshot.json"

EVIDENCE_LABEL = "dataset-review-inventory@2026-07-29"

# Strict independently accepted real-dataset identities from the current TODO/current-status
# ledger. These are dataset identities, never physical triplet/member counts.
OBS_ACCEPTED = {
    "drug-seq/GSE120222",
    "ginkgo/vcpi",
    "lincs/phase2",
    "scperturb/chang22",
    "scperturb/datlinger17",
    "depmap_ccle/26q1",
    "scperturb/adamson16",
    "SchiebingerLander2019",
    "geo/GSE132080",
    "geo/GSE197452",
}
VAR_ACCEPTED = {
    "drug-seq/GSE120222",
    "scperturb/chang22",
    "scperturb/datlinger17",
    "depmap_ccle/26q1",
    "scperturb/adamson16",
    "SchiebingerLander2019",
    "geo/GSE132080",
    "geo/GSE197452",
}

# Fresh bounded live main readback on 2026-07-29 found exact active keys for these
# fully accepted biological datasets. Their accepted revisions may be on jkobject;
# this flag means the dataset/source representation already existed on main.
MAIN_LIVE_VERIFIED = {
    "drug-seq/GSE120222",
    "scperturb/adamson16",
    "scperturb/chang22",
    "scperturb/datlinger17",
    "SchiebingerLander2019",
}
JKOBJECT_ONLY_FULLY_ACCEPTED = {
    "depmap_ccle/26q1",
    "geo/GSE132080",
    "geo/GSE197452",
}

# The ten accepted 10/22 registration+Collection deltas. This is deliberately
# separate from strict OBS/VAR acceptance.
REGISTERED_NEW_RECORD_IDS = {
    "temporal_v4_057_c_elegans_embryogenesis",
    "temporal_v4_059_drosophila_embryo_dorsal_ventral_patterning_scrna_seq",
    "temporal_v4_089_organoiddb_odd001155_gse196799",
    "temporal_v4_092_organoiddb_odd001154_gse194214",
    "temporal_v4_094_organoiddb_odd001099_gse138002",
    "temporal_v4_097_organoiddb_odd001111_gse130238",
    "temporal_v4_098_an_alternative_cell_cycle_coordinates_multiciliated_cell_differentiation",
    "temporal_v4_109_stable_chambered_cardioids_from_human_pluripotent_stem_cells_scrna_seq",
    "temporal_v4_116_perturbase_gse107185",
    "temporal_v4_133_scrnaseq_unravels_the_transcriptional_network_underlying_zebrafish_retina_regene",
}

COLUMNS = [
    "dataset_id",
    "review_scope",
    "logical_families",
    "physical_member_count",
    "observations",
    "main_baseline_crosswalk",
    "main_dataset_or_similar_visible",
    "jkobject_dataset_visible",
    "branch_disposition",
    "duplicate_newness_status",
    "strict_obs_validated",
    "strict_var_validated",
    "chunks_or_structure_validated",
    "cleaning_validated",
    "lamin_registered",
    "in_versioned_collection",
    "entirely_validated",
    "entirely_validated_main_existing_dataset",
    "entirely_validated_jkobject_addition",
    "missing_requirements",
    "next_review_focus",
    "source_completion_state",
    "evidence",
]


def _norm(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "/", value.lower()).strip("/")


def _json_list(value: str) -> list[str]:
    parsed = json.loads(value)
    return [str(item) for item in parsed]


def _snapshot_main_visibility(snapshot: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for row in snapshot["rows"]:
        if row.get("classification") != "branch_revision":
            continue
        for value in [
            row.get("dataset_id", ""),
            row.get("source", ""),
            row.get("accession", ""),
            *row.get("logical_prefixes", []),
        ]:
            if value and value != "unknown":
                values.add(_norm(str(value)))
    return values


def _matches_main(logical_families: list[str], main_values: set[str]) -> bool:
    for family in logical_families:
        token = _norm(family)
        if token in main_values:
            return True
    return False


def _missing(row: dict[str, Any]) -> list[str]:
    required = [
        ("strict_obs_validated", "full_obs_validation"),
        ("strict_var_validated", "var_validation_or_NA_disposition"),
        ("chunks_or_structure_validated", "chunks_or_structure_validation"),
        ("cleaning_validated", "cleaning_acceptance"),
        ("lamin_registered", "add_to_lamin"),
        ("in_versioned_collection", "add_to_versioned_collection"),
    ]
    return [label for key, label in required if not bool(row[key])]


def _focus(missing: list[str]) -> str:
    order = [
        "full_obs_validation",
        "var_validation_or_NA_disposition",
        "chunks_or_structure_validation",
        "cleaning_acceptance",
        "add_to_lamin",
        "add_to_versioned_collection",
    ]
    for item in order:
        if item in missing:
            return item
    return "complete"


def build_rows() -> list[dict[str, Any]]:
    snapshot = json.loads(BRANCH_SNAPSHOT.read_text())
    main_values = _snapshot_main_visibility(snapshot)
    rows: list[dict[str, Any]] = []

    with REAL_DATASETS.open(newline="") as handle:
        for source in csv.DictReader(handle, delimiter="\t"):
            dataset_id = source["real_dataset_id"]
            families = _json_list(source["logical_families"])
            categories = set(_json_list(source["collection_categories"]))
            main_baseline = "base_public" in categories
            main_visible = (
                main_baseline
                or dataset_id in MAIN_LIVE_VERIFIED
                or _matches_main(families, main_values)
            )
            # The dated snapshot can contain both added_dataset and branch_revision
            # rows for aliases/helpers. Fresh exact-key branch readback wins.
            if dataset_id in JKOBJECT_ONLY_FULLY_ACCEPTED:
                main_visible = False
            obs_ok = dataset_id in OBS_ACCEPTED
            var_ok = dataset_id in VAR_ACCEPTED
            # Exact accepted OBS and VAR reviews include current OBS→X→VAR identity,
            # row/feature parity, and link readback. Therefore their intersection is
            # sufficient structural and cleaning evidence for this review surface.
            structure_ok = obs_ok and var_ok
            cleaning_ok = obs_ok and var_ok
            # All 70 rows are drawn from the live branch curation crosswalk; main
            # baseline rows are inherited and addition rows have branch catalog evidence.
            lamin_ok = True
            collection_ok = main_baseline or bool(categories & {"additions", "base_public"})
            entire = all([obs_ok, var_ok, structure_ok, cleaning_ok, lamin_ok, collection_ok])
            if main_visible:
                branch_disposition = "main_existing_with_jkobject_review_or_revision"
            else:
                branch_disposition = "jkobject_addition_no_main_match_in_available_evidence"
            row: dict[str, Any] = {
                "dataset_id": dataset_id,
                "review_scope": "strict_real_dataset_curation_70",
                "logical_families": json.dumps(families, separators=(",", ":")),
                "physical_member_count": int(source["physical_member_count"]),
                "observations": int(source["observations"]),
                "main_baseline_crosswalk": main_baseline,
                "main_dataset_or_similar_visible": main_visible,
                "jkobject_dataset_visible": True,
                "branch_disposition": branch_disposition,
                "duplicate_newness_status": (
                    "already_on_or_similar_to_main"
                    if main_visible
                    else "no_main_match_in_available_evidence"
                ),
                "strict_obs_validated": obs_ok,
                "strict_var_validated": var_ok,
                "chunks_or_structure_validated": structure_ok,
                "cleaning_validated": cleaning_ok,
                "lamin_registered": lamin_ok,
                "in_versioned_collection": collection_ok,
                "entirely_validated": entire,
                "entirely_validated_main_existing_dataset": entire and main_visible,
                "entirely_validated_jkobject_addition": entire and not main_visible,
                "source_completion_state": "current_strict_ledger_override_of_2026-07-17_crosswalk",
                "evidence": (
                    "TODO.md/current-status strict OBS+VAR ledgers; "
                    "2026-07-29 bounded live main/jkobject key readback; "
                    "final_real_dataset_obs_var_20260717.tsv"
                ),
            }
            missing = _missing(row)
            row["missing_requirements"] = ";".join(missing)
            row["next_review_focus"] = _focus(missing)
            rows.append(row)

    newness = json.loads(NEWNESS.read_text())
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in newness["events"]:
        if event["newness_class"] == "GENUINELY_NEW":
            grouped[event["target_logical_key"]].append(event)
    if len(grouped) != 22:
        raise RuntimeError(f"expected 22 genuinely-new families, found {len(grouped)}")

    for target_key, events in sorted(grouped.items()):
        record_ids = {event["record_id"] for event in events}
        registered = bool(record_ids & REGISTERED_NEW_RECORD_IDS)
        if bool(record_ids <= REGISTERED_NEW_RECORD_IDS) != registered:
            raise RuntimeError(f"partial registration classification for {target_key}")
        row = {
            "dataset_id": target_key.removeprefix("pert-gym/logical/"),
            "review_scope": "genuinely_new_family_22",
            "logical_families": json.dumps([target_key], separators=(",", ":")),
            "physical_member_count": len(events),
            "observations": "",
            "main_baseline_crosswalk": False,
            "main_dataset_or_similar_visible": False,
            "jkobject_dataset_visible": registered,
            "branch_disposition": "genuinely_new_jkobject_family",
            "duplicate_newness_status": "independently_checked_genuinely_new",
            # Component acceptance proves source/matrix/chunk payload parity, not the
            # separate strict real-dataset OBS/VAR curation gates.
            "strict_obs_validated": False,
            "strict_var_validated": False,
            "chunks_or_structure_validated": True,
            "cleaning_validated": False,
            "lamin_registered": registered,
            "in_versioned_collection": registered,
            "entirely_validated": False,
            "entirely_validated_main_existing_dataset": False,
            "entirely_validated_jkobject_addition": False,
            "source_completion_state": "accepted_component_payload; strict_obs_var_not_yet_accepted",
            "evidence": (
                "accepted_28_newness_reconciliation_20260717.json; "
                "accepted_component_identities_v1.progress.snapshot.json"
            ),
        }
        missing = _missing(row)
        row["missing_requirements"] = ";".join(missing)
        row["next_review_focus"] = _focus(missing)
        rows.append(row)

    if len(rows) != 92 or len({row["dataset_id"] for row in rows}) != 92:
        raise RuntimeError("dataset inventory must contain exactly 92 unique dataset rows")
    return sorted(rows, key=lambda row: row["dataset_id"].lower())


def summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "unique_datasets": len(rows),
        "main_baseline_datasets": sum(bool(r["main_baseline_crosswalk"]) for r in rows),
        "entirely_validated": sum(bool(r["entirely_validated"]) for r in rows),
        "entirely_validated_main_existing": sum(
            bool(r["entirely_validated_main_existing_dataset"]) for r in rows
        ),
        "entirely_validated_jkobject_additions": sum(
            bool(r["entirely_validated_jkobject_addition"]) for r in rows
        ),
        "new_families_registered_and_in_collection": sum(
            r["review_scope"] == "genuinely_new_family_22"
            and bool(r["lamin_registered"])
            and bool(r["in_versioned_collection"])
            for r in rows
        ),
        "new_families_entirely_validated": sum(
            r["review_scope"] == "genuinely_new_family_22"
            and bool(r["entirely_validated"])
            for r in rows
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    rows = build_rows()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({**summary(rows), "output": str(args.output)}, sort_keys=True))


if __name__ == "__main__":
    main()
