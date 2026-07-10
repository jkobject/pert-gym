#!/usr/bin/env python3
"""Materialize a compact STRAND/PerturbQA loader-sidecar join table.

Inputs are already cached, audited source sidecars and the accepted v3 guide
reference.  This script intentionally never opens a matrix X or connects to
Lamin: it creates a perturbation-level projection with source obs provenance,
source-native split labels, and guide-target metadata.  It is not a viability
label manifest and it does not upgrade the guide-coordinate proxy into a
verified external genome/TSS annotation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
AUDIT_DIR = ROOT / "artifacts/schema_audit"
CACHE_DIR = (
    ROOT
    / ".lamin-cache/lamindb/lamin-us-west-2/H7d9vxvceBoh/pert-gym/auxiliary/strand"
    / "perturbqa_mappings_20260703"
)
GUIDE_REFERENCE = AUDIT_DIR / "strand_perturbqa_guide_reference_20260708_v3.parquet"
GAP_CLASSIFICATION = (
    AUDIT_DIR / "strand_perturbqa_guide_gap_classification_20260710_t_78160c62.json"
)
OUT_TSV = AUDIT_DIR / "model_ready_v2_strand_join_table_20260710.tsv"
OUT_JSON = AUDIT_DIR / "model_ready_v2_strand_join_table_20260710.json"

CLASSIFICATION_SCHEMA = (
    "strand_perturbqa_guide_gap_classification_20260710_t_78160c62.v1"
)
CLASSIFICATION_SHA256 = (
    "d0aba4524371b5aa3b9390f4aca849dbec4f32c668233da9253d8a0e1a2d24d4"
)
GUIDE_REFERENCE_SHA256 = (
    "705cbebf5f86716818d31b8f16d81600fe35ff4fc5fc69b891ab7e1ce828c1b1"
)
GW_MAPPING_SHA256 = "1cc4fb106cd5542751bdfb70df2040ceefe7a1b8aa0f03613c185b9f2c405a98"
EXPECTED_RESIDUALS_BY_DATASET = {"hepg2": 53, "jurkat": 66, "k562": 218, "rpe1": 66}
EXPECTED_EXCLUSIONS = {("k562-de.csv", "ELOB"), ("k562-dir.csv", "ELOB")}

TABLE_COLUMNS = [
    "join_row_id",
    "dataset_id",
    "perturbqa_mapping_file",
    "perturbqa_task",
    "perturbqa_perturbation",
    "perturbqa_guide_target_symbol",
    "perturbqa_split",
    "perturbqa_label_row_count",
    "perturbqa_target_label_count",
    "perturbqa_target_label_sample",
    "lamin_obs_key",
    "lamin_obs_uid",
    "lamin_obs_sha256",
    "lamin_obs_join_column",
    "lamin_obs_row_key_column",
    "cell_line",
    "source_family",
    "guide_reference_artifact_key",
    "guide_reference_id_sample",
    "guide_raw_token_count",
    "guide_parsed_non_control_count",
    "guide_control_token_count",
    "guide_tss_proxy_true_count",
    "guide_tss_proxy_status",
    "guide_genome_build",
    "guide_mapping_status",
    "guide_resolution_classification",
    "guide_resolution_evidence",
    "model_ready_status",
    "model_ready_reason",
]


def _task_from_file(path: Path) -> tuple[str, str]:
    stem = path.stem
    if stem.startswith("k562_set-"):
        return "k562", "set_" + stem.removeprefix("k562_set-").replace("-", "_")
    dataset, raw_task = stem.split("-", 1)
    return dataset, raw_task.replace("-", "_")


def _hash_row(values: list[str]) -> str:
    return hashlib.sha256("\x1f".join(values).encode()).hexdigest()[:20]


def _as_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_reviewed_alias_policy(
    guides: pd.DataFrame,
) -> tuple[dict[tuple[str, str], dict[str, str]], set[tuple[str, str]], dict[str, Any]]:
    """Load the one reviewed alias policy and revalidate it against local inputs."""
    if _sha256(GUIDE_REFERENCE) != GUIDE_REFERENCE_SHA256:
        raise ValueError("accepted guide reference SHA256 differs from reviewed input")
    gw_mapping_path = CACHE_DIR / "k562_gw_mapping_full.json"
    if _sha256(gw_mapping_path) != GW_MAPPING_SHA256:
        raise ValueError(
            "cached PerturbQA Ensembl bridge SHA256 differs from reviewed input"
        )
    if _sha256(GAP_CLASSIFICATION) != CLASSIFICATION_SHA256:
        raise ValueError(
            "reviewed gap-classification SHA256 differs from approved policy"
        )

    payload = json.loads(GAP_CLASSIFICATION.read_text())
    required_payload_keys = {
        "alias_map",
        "counts",
        "exclusions",
        "inputs",
        "per_dataset",
        "rows",
        "schema_version",
    }
    missing = sorted(required_payload_keys - set(payload))
    if missing or payload["schema_version"] != CLASSIFICATION_SCHEMA:
        raise ValueError(
            f"unrecognized reviewed alias-policy schema: missing={missing}"
        )
    if payload["inputs"].get("guide_reference_sha256") != GUIDE_REFERENCE_SHA256:
        raise ValueError("alias policy was not built from the reviewed guide reference")
    if payload["inputs"].get("local_alias_mapping_sha256") != GW_MAPPING_SHA256:
        raise ValueError(
            "alias policy was not built from the reviewed PerturbQA bridge"
        )

    expected_counts = {
        "strict_unmatched_perturbation_task_rows": 403,
        "exact_alias_rows": 401,
        "unresolved_or_excluded_rows": 2,
    }
    if {key: payload["counts"].get(key) for key in expected_counts} != expected_counts:
        raise ValueError("reviewed alias-policy count conservation changed")
    classifications = payload["counts"].get("classification_counts")
    if classifications != {
        "ambiguous_multi_target": 2,
        "control_sentinel": 0,
        "evidence_backed_exact_alias": 401,
        "exclusion": 0,
        "source_token_absent_unmaterialized": 0,
    }:
        raise ValueError("reviewed alias-policy classification counts changed")
    observed_residuals = {
        dataset: item.get("strict_unmatched_unique_perturbation_rows")
        for dataset, item in payload["per_dataset"].items()
    }
    if observed_residuals != EXPECTED_RESIDUALS_BY_DATASET:
        raise ValueError("reviewed alias-policy per-dataset residual counts changed")

    rows = payload["rows"]
    if not isinstance(rows, list) or len(rows) != 403:
        raise ValueError("reviewed alias policy must contain exactly 403 residual rows")
    row_keys = {
        (
            _as_text(row.get("dataset")),
            _as_text(row.get("perturbqa_mapping_file")),
            _as_text(row.get("perturbqa_perturbation")),
        )
        for row in rows
    }
    if len(row_keys) != 403:
        raise ValueError("reviewed alias policy has duplicate residual task rows")

    exclusions = {
        (
            _as_text(row.get("perturbqa_mapping_file")),
            _as_text(row.get("perturbqa_perturbation")),
        )
        for row in payload["exclusions"]
    }
    if exclusions != EXPECTED_EXCLUSIONS:
        raise ValueError(
            "reviewed alias policy must exclude exactly both k562 ELOB task rows"
        )
    if any(
        row.get("classification") != "ambiguous_multi_target"
        for row in payload["exclusions"]
    ):
        raise ValueError("reviewed ELOB exclusions must remain ambiguous_multi_target")

    aliases = payload["alias_map"]
    if not isinstance(aliases, dict) or len(aliases) != 157:
        raise ValueError(
            "reviewed alias policy must contain exactly 157 dataset|pert keys"
        )
    gw_mapping = json.loads(gw_mapping_path.read_text())
    if not isinstance(gw_mapping, dict):
        raise ValueError("cached PerturbQA Ensembl bridge must be an object")
    accepted_by_ensembl: dict[tuple[str, str], set[str]] = defaultdict(set)
    for _, row in guides.dropna(
        subset=["dataset", "ensembl_gene_id", "target_symbol"]
    ).iterrows():
        accepted_by_ensembl[
            (_as_text(row["dataset"]), _as_text(row["ensembl_gene_id"]))
        ].add(_as_text(row["target_symbol"]))

    policy_by_task: dict[tuple[str, str], dict[str, str]] = {}
    for key, alias in aliases.items():
        dataset = _as_text(alias.get("dataset"))
        perturbation = _as_text(alias.get("perturbqa_perturbation"))
        target = _as_text(alias.get("resolved_target_symbol"))
        ensembl_id = _as_text(alias.get("ensembl_gene_id"))
        if key != f"{dataset}|{perturbation}" or not all(
            (dataset, perturbation, target, ensembl_id)
        ):
            raise ValueError(f"malformed reviewed alias key: {key}")
        if gw_mapping.get(perturbation) != ensembl_id:
            raise ValueError(
                f"alias no longer matches exact cached Ensembl bridge: {key}"
            )
        if accepted_by_ensembl.get((dataset, ensembl_id)) != {target}:
            raise ValueError(
                f"alias is not a singular same-dataset accepted-v3 target: {key}"
            )
        if (dataset, perturbation) in policy_by_task:
            raise ValueError(f"conflicting reviewed alias key: {key}")
        policy_by_task[(dataset, perturbation)] = {
            "target": target,
            "ensembl_id": ensembl_id,
            "evidence": "reviewed_exact_cached_perturbqa_token_to_ensembl_to_singular_accepted_v3_target",
        }

    return (
        policy_by_task,
        exclusions,
        {"payload": payload, "residual_row_keys": row_keys},
    )


def build_table() -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not GUIDE_REFERENCE.exists():
        raise FileNotFoundError(
            f"accepted guide reference is missing: {GUIDE_REFERENCE}"
        )
    if not GAP_CLASSIFICATION.exists():
        raise FileNotFoundError(
            f"reviewed alias policy is missing: {GAP_CLASSIFICATION}"
        )
    if not CACHE_DIR.exists():
        raise FileNotFoundError(f"cached PerturbQA sidecars are missing: {CACHE_DIR}")

    guides = pd.read_parquet(GUIDE_REFERENCE)

    required_guide_columns = {
        "dataset",
        "target_symbol",
        "guide_reference_id",
        "source_obs_key",
        "source_obs_uid",
        "source_obs_sha256",
        "cell_line",
        "source_family",
        "raw_guide_token",
        "is_control_token",
        "parse_status",
        "tss_window_plus_minus_200bp_assignment",
        "tss_window_assignment_status",
        "genome_build",
    }
    missing_guide_columns = sorted(required_guide_columns - set(guides.columns))
    if missing_guide_columns:
        raise ValueError(f"guide reference is missing columns: {missing_guide_columns}")
    reviewed_aliases, reviewed_exclusions, policy = _load_reviewed_alias_policy(guides)

    guide_by_target: dict[tuple[str, str], dict[str, Any]] = {}
    for (dataset, target), group in guides.groupby(
        ["dataset", "target_symbol"], dropna=False
    ):
        if not isinstance(target, str) or not target:
            continue
        provenance_values = {
            column: sorted(
                {_as_text(value) for value in group[column] if _as_text(value)}
            )
            for column in (
                "source_obs_key",
                "source_obs_uid",
                "source_obs_sha256",
                "cell_line",
                "source_family",
                "genome_build",
            )
        }
        for column, values in provenance_values.items():
            if len(values) != 1:
                raise ValueError(
                    f"guide reference has ambiguous {column} for {dataset}/{target}: {values}"
                )
        guide_by_target[(str(dataset), target)] = {
            "guide_reference_id_sample": ";".join(
                sorted(group["guide_reference_id"].astype(str))[:3]
            ),
            "guide_raw_token_count": int(group["raw_guide_token"].nunique()),
            "guide_parsed_non_control_count": int(
                (
                    (~group["is_control_token"].astype(bool))
                    & (group["parse_status"] == "parsed_existing_obs_token")
                ).sum()
            ),
            "guide_control_token_count": int(
                group["is_control_token"].astype(bool).sum()
            ),
            "guide_tss_proxy_true_count": int(
                group["tss_window_plus_minus_200bp_assignment"].astype(bool).sum()
            ),
            "guide_tss_proxy_status": ";".join(
                sorted(
                    {
                        _as_text(value)
                        for value in group["tss_window_assignment_status"]
                        if _as_text(value)
                    }
                )
            ),
            "lamin_obs_key": provenance_values["source_obs_key"][0],
            "lamin_obs_uid": provenance_values["source_obs_uid"][0],
            "lamin_obs_sha256": provenance_values["source_obs_sha256"][0],
            "cell_line": provenance_values["cell_line"][0],
            "source_family": provenance_values["source_family"][0],
            "guide_genome_build": provenance_values["genome_build"][0],
        }

    rows: list[dict[str, str]] = []
    per_file: dict[str, Any] = {}
    unresolved_by_file: dict[str, list[dict[str, str]]] = {}
    gap_policy_counts: Counter[str] = Counter()
    initial_unmatched_count = 0
    observed_residual_keys: set[tuple[str, str, str]] = set()
    for csv_path in sorted(CACHE_DIR.glob("*.csv")):
        dataset, task = _task_from_file(csv_path)
        mapping = pd.read_csv(csv_path)
        required_mapping_columns = {"pert", "gene", "label", "split"}
        if missing := sorted(required_mapping_columns - set(mapping.columns)):
            raise ValueError(f"{csv_path.name} is missing columns: {missing}")
        split_counts = mapping.groupby("pert", dropna=False)["split"].nunique(
            dropna=False
        )
        conflicting = sorted(
            str(pert) for pert, count in split_counts.items() if count != 1
        )
        if conflicting:
            raise ValueError(
                f"{csv_path.name} assigns multiple source-native splits to perturbations: "
                + ", ".join(conflicting[:10])
            )
        grouped = mapping.groupby("pert", sort=True, dropna=False)
        unresolved: list[dict[str, str]] = []
        unmatched_before_resolution = 0
        matched = 0
        for pert, group in grouped:
            perturbation = _as_text(pert)
            guide = guide_by_target.get((dataset, perturbation))
            guide_target = perturbation
            resolution_classification = "direct_exact"
            resolution_evidence = "accepted_v3_target_symbol_exact"
            if guide is None:
                initial_unmatched_count += 1
                unmatched_before_resolution += 1
                residual_key = (dataset, csv_path.name, perturbation)
                observed_residual_keys.add(residual_key)
                if (csv_path.name, perturbation) in reviewed_exclusions:
                    gap_policy_counts["ambiguous_multi_target_excluded"] += 1
                    unresolved.append(
                        {
                            "perturbation": perturbation,
                            "classification": "ambiguous_multi_target",
                            "reason": (
                                "Excluded by the reviewed alias policy: exact cached PerturbQA "
                                "token maps to accepted-v3 TCEB1 and TCEB2; no target is selected."
                            ),
                        }
                    )
                    continue
                reviewed_alias = reviewed_aliases.get((dataset, perturbation))
                if reviewed_alias is None:
                    raise ValueError(
                        f"strict residual absent from reviewed allow/deny policy: {residual_key}"
                    )
                guide_target = reviewed_alias["target"]
                guide = guide_by_target.get((dataset, guide_target))
                if guide is None:
                    raise ValueError(
                        f"reviewed alias target disappeared from accepted-v3 guide reference: {residual_key}"
                    )
                resolution_classification = "evidence_backed_exact_alias"
                resolution_evidence = reviewed_alias["evidence"]
                gap_policy_counts[resolution_classification] += 1
            matched += 1
            target_labels = sorted(
                {_as_text(value) for value in group["gene"] if _as_text(value)}
            )
            split_values = sorted(
                {_as_text(value) for value in group["split"] if _as_text(value)}
            )
            assert len(split_values) == 1
            row = {
                "dataset_id": f"strand_perturbqa_{dataset}",
                "perturbqa_mapping_file": csv_path.name,
                "perturbqa_task": task,
                "perturbqa_perturbation": perturbation,
                "perturbqa_guide_target_symbol": guide_target,
                "perturbqa_split": split_values[0],
                "perturbqa_label_row_count": str(len(group)),
                "perturbqa_target_label_count": str(len(target_labels)),
                "perturbqa_target_label_sample": ";".join(target_labels[:5]),
                "lamin_obs_join_column": "pert_target",
                "lamin_obs_row_key_column": "obs_uuid",
                "guide_reference_artifact_key": (
                    "pert-gym/auxiliary/strand/"
                    "perturbqa_guide_reference_20260708_v3.parquet"
                ),
                "guide_mapping_status": (
                    "matched_to_accepted_v3_source_obs_guide_reference"
                    if resolution_classification == "direct_exact"
                    else "matched_to_accepted_v3_via_reviewed_exact_alias_policy"
                ),
                "guide_resolution_classification": resolution_classification,
                "guide_resolution_evidence": resolution_evidence,
                "model_ready_status": "loader_projectable_only",
                "model_ready_reason": (
                    "Expression/guide/split mapping sidecar only; no direct viability label, "
                    "and source-native split is not yet leakage-audited."
                ),
            }
            row.update({key: _as_text(value) for key, value in guide.items()})
            row["join_row_id"] = _hash_row(
                [
                    row["dataset_id"],
                    row["perturbqa_mapping_file"],
                    row["perturbqa_perturbation"],
                    row["perturbqa_split"],
                ]
            )
            rows.append({column: row.get(column, "") for column in TABLE_COLUMNS})
        unresolved_by_file[csv_path.name] = unresolved
        per_file[csv_path.name] = {
            "dataset": dataset,
            "task": task,
            "source_rows": int(len(mapping)),
            "source_unique_perturbations": int(mapping["pert"].nunique()),
            "matched_unique_perturbations": matched,
            "source_unmatched_before_resolution": unmatched_before_resolution,
            "unresolved_unique_perturbations": len(unresolved),
            "source_native_splits": sorted(
                mapping["split"].dropna().astype(str).unique()
            ),
            "all_perts_have_one_split": True,
            "source_native_perturbation_split_overlap": conflicting,
            "source_native_perturbation_split_overlap_count": len(conflicting),
        }

    if observed_residual_keys != policy["residual_row_keys"]:
        missing = sorted(policy["residual_row_keys"] - observed_residual_keys)
        extra = sorted(observed_residual_keys - policy["residual_row_keys"])
        raise ValueError(
            "strict residual conservation against reviewed policy failed: "
            f"missing={missing[:3]} extra={extra[:3]}"
        )
    if initial_unmatched_count != 403 or gap_policy_counts != Counter(
        {
            "evidence_backed_exact_alias": 401,
            "ambiguous_multi_target_excluded": 2,
        }
    ):
        raise ValueError("reviewed alias allow/deny count conservation failed")
    policy_per_dataset = {
        dataset: {
            "accepted_alias_rows": item["classification_counts"].get(
                "evidence_backed_exact_alias", 0
            ),
            "excluded_rows": item["strict_unmatched_unique_perturbation_rows"]
            - item["classification_counts"].get("evidence_backed_exact_alias", 0),
        }
        for dataset, item in policy["payload"]["per_dataset"].items()
    }
    if policy_per_dataset != {
        "hepg2": {"accepted_alias_rows": 53, "excluded_rows": 0},
        "jurkat": {"accepted_alias_rows": 66, "excluded_rows": 0},
        "k562": {"accepted_alias_rows": 216, "excluded_rows": 2},
        "rpe1": {"accepted_alias_rows": 66, "excluded_rows": 0},
    }:
        raise ValueError("reviewed alias policy per-dataset allow/deny counts changed")
    if not rows:
        raise ValueError("no STRAND/PerturbQA rows were materialized")
    duplicate_ids = [
        key
        for key, count in Counter(row["join_row_id"] for row in rows).items()
        if count > 1
    ]
    if duplicate_ids:
        raise ValueError(f"duplicate join_row_id values: {duplicate_ids[:10]}")

    metadata = {
        "schema_version": "model_ready_v2_strand_join_table_20260710.v1",
        "purpose": (
            "Compact loader sidecar joining PerturbQA source-native task/split rows to "
            "accepted STRAND guide-reference metadata and Lamin obs provenance."
        ),
        "inputs": {
            "guide_reference": str(GUIDE_REFERENCE.relative_to(ROOT)),
            "guide_reference_sha256": hashlib.sha256(
                GUIDE_REFERENCE.read_bytes()
            ).hexdigest(),
            "perturbqa_cache_dir": str(CACHE_DIR.relative_to(ROOT)),
            "reviewed_alias_policy": str(GAP_CLASSIFICATION.relative_to(ROOT)),
            "reviewed_alias_policy_sha256": _sha256(GAP_CLASSIFICATION),
        },
        "join_semantics": {
            "perturbqa_to_lamin": "PerturbQA pert -> accepted v3 target_symbol -> source Lamin obs pert_target",
            "lamin_obs_join_column": "pert_target",
            "lamin_obs_row_key_column": "obs_uuid",
            "split_source": "PerturbQA source-native CSV split",
            "guide_reference_scope": (
                "Guide tokens parsed from source obs; coordinate/TSS proxy is not recomputed "
                "against external annotation and genome build remains source-not-recorded."
            ),
        },
        "counts": {
            "table_rows": len(rows),
            "files": len(per_file),
            "matched_unique_perturbation_rows_after_resolution": sum(
                stats["matched_unique_perturbations"] for stats in per_file.values()
            ),
            "unmatched_unique_perturbation_rows_before_resolution": initial_unmatched_count,
            "unresolved_unique_perturbation_rows_after_resolution": sum(
                stats["unresolved_unique_perturbations"] for stats in per_file.values()
            ),
            "resolved_unique_perturbation_rows": initial_unmatched_count
            - sum(
                stats["unresolved_unique_perturbations"] for stats in per_file.values()
            ),
            "reviewed_alias_policy_rows": dict(sorted(gap_policy_counts.items())),
            "reviewed_alias_policy_keys": len(reviewed_aliases),
            "reviewed_alias_policy_per_dataset": policy_per_dataset,
        },
        "per_file": per_file,
        "split_leakage_audit": {
            "scope": (
                "Perturbation overlap is audited within each source-native mapping file. "
                "Do not aggregate splits across different PerturbQA tasks as a benchmark split."
            ),
            "all_mapping_files_have_zero_within_file_perturbation_overlap": all(
                not stats["source_native_perturbation_split_overlap"]
                for stats in per_file.values()
            ),
            "per_file_overlap_counts": {
                name: stats["source_native_perturbation_split_overlap_count"]
                for name, stats in per_file.items()
            },
        },
        "loader_exclusions": {
            "policy": (
                "Only reviewed evidence_backed_exact_alias residual rows are included; all "
                "other residual classes are denied. Both ambiguous k562 ELOB task rows are "
                "excluded from the TSV and must not be adapted as mapping sidecars."
            ),
            "unresolved_by_file": unresolved_by_file,
        },
        "limitations": [
            "This is a mapping/pretraining sidecar, not direct viability/survival supervision.",
            "The source-native PerturbQA split is preserved but not claimed leakage-safe for a future benchmark.",
            "The v3 guide reference carries source-token coordinates/proxy status, not externally verified genome-build or TSS-window proof.",
            "Table rows are perturbation-task aggregates; use lamin_obs_key + pert_target to select actual obs rows and obs_uuid as their stable row key.",
            "Unresolved source-version/identifier gaps are explicit loader exclusions; do not infer substitutions from spelling.",
        ],
    }
    return rows, metadata


def write_outputs(rows: list[dict[str, str]], metadata: dict[str, Any]) -> None:
    OUT_TSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_TSV.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TABLE_COLUMNS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    metadata["outputs"] = {
        "table_tsv": str(OUT_TSV.relative_to(ROOT)),
        "table_tsv_sha256": hashlib.sha256(OUT_TSV.read_bytes()).hexdigest(),
    }
    OUT_JSON.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="build and validate without writing"
    )
    args = parser.parse_args()
    rows, metadata = build_table()
    if args.check:
        print(json.dumps(metadata["counts"], sort_keys=True))
        return
    write_outputs(rows, metadata)
    print(
        json.dumps(
            {
                "table": str(OUT_TSV.relative_to(ROOT)),
                "metadata": str(OUT_JSON.relative_to(ROOT)),
                "counts": metadata["counts"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
