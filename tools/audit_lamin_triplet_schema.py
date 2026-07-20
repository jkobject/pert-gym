#!/usr/bin/env python3
"""Read-only schema and triplet audit for laminlabs/pertdata.

This script connects through tools.lamin_context.connect_pertdata(), reads Lamin
metadata, optionally samples obs/var payloads, and writes local TSV reports under
artifacts/schema_audit/. It must not create, modify, delete, or link Lamin
artifacts.
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from tools.lamin_context import connect_pertdata, ensure_project_cache

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "artifacts" / "schema_audit"
SCHEMA_FEATURE_NAME_CACHE: dict[str, list[str]] = {}

CANONICAL_OBS_COLUMNS = [
    "dataset",
    "sample",
    "cell_id",
    "donor_id",
    "batch",
    "cell_type",
    "cell_line",
    "disease",
    "tissue_type",
    "organism",
    "sex",
    "age",
    "ethnicity",
    "sequencer",
    "technology",
    "assay",
    "modality",
    "media",
    "is_bulk",
    "is_pseudobulk",
    "perturbation",
    "perturbation_type",
    "perturbation_technology",
    "perturbation_library",
    "guide_id",
    "guide_sequence",
    "perturbation_target",
    "perturbation_target_id",
    "is_control",
    "dose",
    "dose_unit",
    "timepoint",
    "trajectory_id",
    "pseudotime",
    "is_baseline",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
    "n_counts",
    "n_genes",
    "pct_mito",
    "pct_ribo",
    "is_low_quality",
]

SYNONYMS = {
    "perturbation": ["pert_name", "pert_target", "condition", "drugname_drugconc"],
    "perturbation_type": ["pert_type", "pert_type_original"],
    "dose": ["pert_dose", "pert_dose_original", "compound_concentration"],
    "timepoint": ["pert_time", "pert_time_original", "time"],
    "cell_type": ["celltype", "cell_type_from_author"],
    "ethnicity": ["self_reported_ethnicity"],
    "n_counts": ["ncounts", "total_counts", "UMI count", "tscp_count"],
    "n_genes": ["ngenes", "n_genes_by_counts", "gene_count"],
    "pct_mito": ["percent_mito", "pct_counts_mt", "pcnt_mito"],
}

CONTROL_STRICT_FIELDS = [
    "dataset",
    "cell_type",
    "cell_line",
    "disease",
    "sex",
    "age",
    "ethnicity",
    "sequencer",
    "organism",
    "donor_id",
    "batch",
    "timepoint",
]


@dataclass(frozen=True)
class ArtifactRow:
    key: str
    uid: str
    suffix: str
    otype: str
    size: int | None
    n_observations: int | None


def artifact_to_row(artifact: Any) -> ArtifactRow:
    return ArtifactRow(
        key=getattr(artifact, "key", "") or "",
        uid=getattr(artifact, "uid", "") or "",
        suffix=getattr(artifact, "suffix", "") or "",
        otype=getattr(artifact, "otype", "") or "",
        size=getattr(artifact, "size", None),
        n_observations=getattr(artifact, "n_observations", None),
    )


def prefix_for_key(key: str) -> str | None:
    for suffix in ("/obs.parquet", "/X.h5ad", "/var.parquet"):
        if key.endswith(suffix):
            return key[: -len(suffix)]
    return None


def family_for_prefix(prefix: str) -> str:
    return prefix.split("/", 1)[0] if prefix else ""


def logical_dataset_for_prefix(prefix: str) -> str:
    parts = prefix.split("/")
    if not parts:
        return prefix
    fam = parts[0]
    if fam == "prism_collection" and len(parts) >= 2:
        return "/".join(parts[:2])
    if fam == "viperturb" and len(parts) >= 2:
        return "/".join(parts[:2])
    if fam == "arc_vcc" and len(parts) >= 3:
        return "/".join(parts[:3])
    if fam == "tahoe100m" and len(parts) >= 2:
        return "/".join(parts[:2])
    return prefix


def source_accession_for_prefix(prefix: str) -> str:
    """Return first GEO-like accession embedded in a prefix, if any."""
    lower = prefix.lower()
    if lower.startswith("lincs/phase1/"):
        return "GSE92742"
    if lower.startswith("lincs/phase2/"):
        return "GSE70138"
    match = re.search(r"\bGSE\d+\b", prefix, flags=re.IGNORECASE)
    return match.group(0).upper() if match else ""


def infer_prefix_role(prefix: str, has_obs: bool, has_x: bool, has_var: bool) -> str:
    """Classify a prefix without loading payloads.

    This is intentionally conservative. It separates true loadable triplets from
    known sidecar/non-expression/orphan patterns so repair plans do not blindly
    create obs/var for every partial prefix.
    """
    lower = prefix.lower()
    complete = has_obs and has_x and has_var
    if "properseq/chimeric_read_pairs" in lower:
        return "excluded_legacy_dataset"
    if "tutorial/" in lower:
        return "demo_or_model_sidecar"
    if lower == "tahoe100m/2025-02-25":
        return "shared_var_reference"
    if any(token in lower for token in ["protein_counts", "atac_counts"]):
        return "auxiliary_matrix_sidecar"
    if "filtered_feature_bc_matrix" in lower and not has_x:
        return "feature_reference_sidecar"
    if "minified_anndata" in lower or "scvi" in lower:
        return "model_embedding_sidecar"
    if complete:
        if re.search(r"/chunk_\d+$", prefix):
            return "chunk_triplet"
        if re.search(r"/plate\d+_", prefix):
            return "plate_triplet"
        return "canonical_triplet"
    if has_obs and has_x and not has_var:
        if re.search(r"/plate\d+_", prefix):
            return "plate_triplet_missing_var"
        return "canonical_triplet_missing_var"
    if has_x and not has_obs:
        return "orphan_X_or_sidecar"
    if has_var and not has_obs and not has_x:
        return "shared_var_or_feature_reference"
    if has_obs and not has_x:
        return "orphan_obs_or_demo"
    return "incomplete_unknown"


def is_training_candidate_role(role: str) -> bool:
    """Return whether a role represents a primary trainable expression/readout X."""
    return role in {"canonical_triplet", "chunk_triplet", "plate_triplet"}


def infer_variant_type(prefix: str, role: str) -> str:
    lower = prefix.lower()
    if "properseq/chimeric_read_pairs" in lower:
        return "excluded_legacy_gse150818"
    if "protein" in lower:
        return "protein"
    if "atac" in lower:
        return "atac"
    if "raw_counts" in lower or "raw" in lower:
        return "raw_counts"
    if "normalized" in lower:
        return "normalized_expression"
    if "delta" in lower:
        return "delta_expression"
    if "pseudobulk" in lower:
        return "pseudobulk"
    if "vscore" in lower:
        return "variant_score"
    if "minified" in lower or "scvi" in lower:
        return "model_or_embedding"
    if "screen" in lower or "gdsc" in lower or "score" in lower:
        return "screen_response"
    if "sidecar" in role or "orphan" in role or "reference" in role:
        return "unknown_sidecar"
    return "expression"


def infer_modality(prefix: str, role: str, variant: str) -> str:
    lower = prefix.lower()
    if "properseq/chimeric_read_pairs" in lower:
        return "excluded_legacy"
    if variant == "protein":
        return "protein"
    if variant == "atac":
        return "atac"
    if "rxrx" in lower or "cellpainting" in lower:
        return "image"
    if any(token in lower for token in ["gdsc", "score_crispr", "repurposing"]):
        return "screen"
    if "lincs" in lower:
        return "L1000"
    if "drug-seq" in lower:
        return "bulk_RNA"
    if "sidecar" in role and variant == "unknown_sidecar":
        return "unknown_sidecar"
    return "RNA"


def recommended_repair(prefix: str, role: str, missing: list[str]) -> tuple[str, str]:
    """Return priority and action for an incomplete prefix."""
    lower = prefix.lower()
    missing_text = ",".join(missing)
    if role in {"canonical_triplet_missing_var", "plate_triplet_missing_var"}:
        if lower.startswith("tahoe100m/2025-02-25/plate"):
            return (
                "urgent_triplet_repair",
                "link/copy shared tahoe100m/2025-02-25/var.parquet after confirming X.n_vars matches",
            )
        if lower == "lincs/phase1/level2_gex_delta_n49216x978":
            return (
                "urgent_triplet_repair",
                "reuse LINCS Level2 epsilon var if X has 978 features and identifiers match",
            )
        if lower == "cellarity/gse305979/gse305979_day1-7_raw_counts":
            return (
                "urgent_triplet_repair",
                "compare sibling Cellarity GSE305979 var artifacts, then create/link matching var",
            )
        if lower == "scperturb/replogle22_rpe1":
            return (
                "urgent_triplet_repair",
                "inspect RPE1 X.h5ad .var or sibling Replogle var; only link if feature index matches",
            )
        return (
            "urgent_triplet_repair",
            f"repair missing {missing_text} only after dimension and feature-identity check",
        )
    if role == "auxiliary_matrix_sidecar":
        if "protein_counts" in lower:
            return (
                "standardize_auxiliary_artifact",
                "treat as matrix-like auxiliary modality; target naming is X_protein.h5ad + var_protein.parquet after obs/feature identity checks",
            )
        if "atac_counts" in lower:
            return (
                "standardize_auxiliary_artifact",
                "treat as matrix-like auxiliary modality; target naming is X_atac.h5ad + var_atac.parquet after obs/feature identity checks",
            )
        return (
            "standardize_auxiliary_artifact",
            "treat as matrix-like auxiliary modality X_<name>.h5ad + var_<name>.parquet after obs/feature identity checks",
        )
    if role == "model_embedding_sidecar":
        return (
            "standardize_auxiliary_artifact",
            "treat as model/embedding payload; target naming is obsm_<name> or a typed model artifact, not a primary expression triplet",
        )
    if role in {
        "feature_reference_sidecar",
        "shared_var_reference",
        "demo_or_model_sidecar",
        "non_expression_sidecar",
        "excluded_legacy_dataset",
    }:
        if role == "excluded_legacy_dataset":
            return (
                "excluded_legacy_dataset",
                "archive/delete from pert-gym branch; do not use as PRoPER-seq substitute",
            )
        return (
            "classify_sidecar",
            "do not force into canonical expression triplet; record sidecar role",
        )
    if role == "orphan_X_or_sidecar":
        return (
            "orphan_review",
            "inspect whether X.h5ad still contains obs/var; migrate only if intended as loadable dataset",
        )
    if role == "orphan_obs_or_demo":
        return (
            "orphan_review",
            "classify as demo/obs-only sidecar or find matching X before repair",
        )
    return (
        "manual_review",
        f"review incomplete prefix with missing parts: {missing_text}",
    )


def write_tsv(
    path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        df = pd.DataFrame(rows)
        if columns is not None:
            df = df.reindex(columns=columns)
    elif columns is not None:
        df = pd.DataFrame({column: pd.Series(dtype="object") for column in columns})
    else:
        df = pd.DataFrame()
    df.to_csv(path, sep="\t", index=False)


def bool_str(value: bool) -> str:
    return "true" if value else "false"


def classify_var_index(index_values: list[str]) -> str:
    if not index_values:
        return "empty"
    ens = sum(bool(re.match(r"^ENS[A-Z]*G\d+", str(x))) for x in index_values)
    if ens == len(index_values):
        return "ensembl"
    if ens > 0:
        return "mixed"
    if all(str(x).strip() for x in index_values):
        return "symbol_or_author_id"
    return "unknown"


def infer_x_semantics(prefix: str, obs_columns: set[str]) -> str:
    lower = prefix.lower()
    if any(x in lower for x in ["gdsc", "score", "repurposing"]):
        return "empty_or_screen_response"
    if "lincs" in lower and "level2_gex" in lower:
        return "normalized_expression"
    if "lincs" in lower and "delta" in lower:
        return "delta_expression"
    if "properseq/chimeric" in lower:
        return "excluded_legacy_dataset"
    if "protein_counts" in lower:
        return "protein_counts_auxiliary"
    if "atac_counts" in lower:
        return "atac_counts_auxiliary"
    if "raw_counts" in lower or lower.endswith("_raw") or "/raw" in lower:
        return "raw_counts"
    if "normalized" in lower:
        return "normalized_expression"
    if "log1p" in lower or "lognorm" in lower:
        return "log1p_expression"
    if "delta" in lower or "signature" in lower:
        return "delta_expression"
    if "lfc" in obs_columns:
        return "expression_plus_response_obs"
    return "unknown_expression"


def summarize_control_availability(df: pd.DataFrame) -> dict[str, Any]:
    if "is_control" not in df.columns:
        return {
            "has_is_control": False,
            "n_controls": None,
            "strict_control_groups": None,
            "dataset_control_available": None,
            "notes": "missing is_control",
        }
    controls = df[df["is_control"].astype(str).str.lower().isin(["true", "1", "yes"])]
    dataset_control_available = len(controls) > 0
    fields = [c for c in CONTROL_STRICT_FIELDS if c in df.columns]
    strict_groups = None
    if dataset_control_available and fields:
        strict_groups = controls[fields].drop_duplicates().shape[0]
    return {
        "has_is_control": True,
        "n_controls": int(len(controls)),
        "strict_control_groups": strict_groups,
        "dataset_control_available": dataset_control_available,
        "notes": "sampled rows only" if len(df) else "empty obs",
    }


def load_dataframe_artifact(ln: Any, key: str) -> pd.DataFrame:
    artifact = ln.Artifact.get(key=key)
    loaded = artifact.load()
    if isinstance(loaded, pd.DataFrame):
        return loaded
    return pd.DataFrame(loaded)


def auxiliary_name_for_prefix(prefix: str, role: str, variant: str) -> str:
    lower = prefix.lower()
    if "protein_counts" in lower or variant == "protein":
        return "protein"
    if "atac_counts" in lower or variant == "atac":
        return "atac"
    if "lfc" in lower or "delta" in lower:
        return "lfc"
    if "cnv" in lower:
        return "cnv"
    if "mutation" in lower or "mut" in lower:
        return "mutation"
    if "scvi" in lower or "minified_anndata" in lower or "embedding" in role:
        return "scvi"
    if "filtered_feature_bc_matrix" in lower:
        return "feature_reference"
    return variant if variant not in {"unknown_sidecar", "expression"} else "review"


def auxiliary_artifact_plan(row: dict[str, Any]) -> dict[str, Any] | None:
    role = row["prefix_role"]
    if not (
        "sidecar" in role
        or "reference" in role
        or role in {"orphan_X_or_sidecar", "orphan_obs_or_demo"}
    ):
        return None

    prefix = row["prefix"]
    name = auxiliary_name_for_prefix(prefix, role, row["variant_type"])
    target_prefix = ""
    target_x_key = ""
    target_var_key = ""
    target_obsm_key = ""
    standard_kind = "manual_review"
    feasible_now = "false"
    safe_next_action = "manual review before rewriting or linking"

    if role == "auxiliary_matrix_sidecar":
        standard_kind = "matrix_auxiliary"
        if "GSE305370_citeseq" in prefix:
            target_prefix = "cellarity/GSE305370/GSE305370_citeseq_alldonors_alldays"
        elif "GSE305370_multiome" in prefix:
            target_prefix = "cellarity/GSE305370/GSE305370_multiome_alldonors_alldays"
        key_prefix = target_prefix or "<primary-prefix>"
        target_x_key = f"{key_prefix}/X_{name}.h5ad"
        target_var_key = f"{key_prefix}/var_{name}.parquet"
        safe_next_action = (
            "bounded obs-index and feature identity check before aliasing/linking"
        )
    elif role == "model_embedding_sidecar":
        standard_kind = "model_or_embedding_artifact"
        target_prefix = row["logical_dataset"]
        target_obsm_key = f"{target_prefix}/obsm_{name}.h5ad"
        safe_next_action = "dedicated backed inspection plan; payload is too large for opportunistic load/rewrite"
    elif role == "shared_var_reference":
        standard_kind = "shared_feature_reference"
        target_var_key = f"{prefix}/var.parquet"
        feasible_now = "true"
        safe_next_action = "leave as top-level shared var reference; plate-level vars are already same-prefix"
    elif role == "feature_reference_sidecar":
        standard_kind = "feature_reference"
        safe_next_action = (
            "keep as feature-barcode reference unless paired with a primary X"
        )
    elif role == "non_expression_sidecar":
        standard_kind = "non_expression_readout"
        safe_next_action = "keep typed outside canonical RNA triplet"
    else:
        standard_kind = "orphan_review"
        safe_next_action = "inspect backed obs/var and decide whether this is a canonical X or auxiliary payload"

    return {
        "prefix": prefix,
        "logical_dataset": row["logical_dataset"],
        "current_role": role,
        "standard_kind": standard_kind,
        "auxiliary_name": name,
        "target_primary_prefix": target_prefix,
        "target_X_key": target_x_key,
        "target_var_key": target_var_key,
        "target_obsm_key": target_obsm_key,
        "feasible_without_payload_load": feasible_now,
        "safe_next_action": safe_next_action,
    }


def feature_link_key(artifact: Any, feature_name: str) -> str:
    """Return linked artifact key for a feature link, without loading payloads."""
    if artifact is None:
        return ""
    try:
        value = artifact.features.get_values().get(feature_name)
    except Exception:  # noqa: BLE001 - audit should keep going
        return ""
    return getattr(value, "key", str(value)) if value is not None else ""


def schema_feature_names(artifact: Any) -> list[str]:
    """Return registered dataset schema feature names without loading payloads.

    Lamin stores validated dataframe columns on ``artifact.schema``. Reading this
    registry metadata is safe for Tahoe-scale obs tables because it does not
    download the parquet payload or open the matrix.
    """
    if artifact is None:
        return []
    schema = getattr(artifact, "schema", None)
    if schema is None:
        return []
    cache_key = str(
        getattr(schema, "uid", "") or getattr(schema, "id", "") or id(schema)
    )
    if cache_key in SCHEMA_FEATURE_NAME_CACHE:
        return SCHEMA_FEATURE_NAME_CACHE[cache_key]
    try:
        members = list(schema.members.all())
    except Exception:  # noqa: BLE001 - audit should keep going
        try:
            members = list(schema.features.all())
        except Exception:  # noqa: BLE001
            SCHEMA_FEATURE_NAME_CACHE[cache_key] = []
            return []
    names = sorted(
        {
            str(getattr(member, "name", ""))
            for member in members
            if getattr(member, "name", "")
        }
    )
    SCHEMA_FEATURE_NAME_CACHE[cache_key] = names
    return names


def obs_columns_from_registry(artifact: Any) -> set[str]:
    """Return registered obs column names from schema metadata only."""
    return set(schema_feature_names(artifact))


def alias_available_for_columns(obs_columns: set[str]) -> dict[str, list[str]]:
    return {
        canonical: [alias for alias in aliases if alias in obs_columns]
        for canonical, aliases in SYNONYMS.items()
        if canonical not in obs_columns
        and any(alias in obs_columns for alias in aliases)
    }


def classify_var_from_columns(columns: set[str], prefix: str) -> str:
    """Classify var ID availability from registry columns without loading values."""
    lower_cols = {c.lower() for c in columns}
    if not columns:
        return "unknown_metadata_only"
    if any(c in lower_cols for c in {"ensembl_id", "ensembl", "gene_ids", "gene_id"}):
        return "id_columns_present_metadata_only"
    if any(
        c in lower_cols for c in {"gene_symbol", "symbol", "gene_name", "gene_symbols"}
    ):
        return "symbol_columns_present_metadata_only"
    if any(token in prefix.lower() for token in ["gdsc", "score", "repurposing"]):
        return "screen_feature_metadata_only"
    return "unknown_metadata_only"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--sample-payloads",
        action="store_true",
        help="Load obs/var payloads for a bounded number of prefixes.",
    )
    parser.add_argument(
        "--max-prefixes",
        type=int,
        default=80,
        help="Maximum complete/incomplete triplet prefixes to sample when --sample-payloads is set.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=5000,
        help="Maximum obs rows to inspect per sampled dataset after loading.",
    )
    args = parser.parse_args()

    ensure_project_cache()
    ln = connect_pertdata()
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    raw_artifacts = list(ln.Artifact.filter().all())
    artifacts = [artifact_to_row(a) for a in raw_artifacts]
    artifact_rows = [row.__dict__ for row in artifacts]
    write_tsv(out / "artifact_inventory.tsv", artifact_rows)

    suffix_by_prefix: dict[str, set[str]] = defaultdict(set)
    row_by_key = {a.key: a for a in artifacts if a.key}
    artifact_by_key = {
        getattr(a, "key", ""): a for a in raw_artifacts if getattr(a, "key", "")
    }
    for row in artifacts:
        prefix = prefix_for_key(row.key)
        if prefix is not None:
            suffix_by_prefix[prefix].add(row.key.rsplit("/", 1)[1])

    triplet_rows: list[dict[str, Any]] = []
    for prefix in sorted(suffix_by_prefix):
        suffixes = suffix_by_prefix[prefix]
        has_obs = "obs.parquet" in suffixes
        has_x = "X.h5ad" in suffixes
        has_var = "var.parquet" in suffixes
        complete = has_obs and has_x and has_var
        role = infer_prefix_role(prefix, has_obs, has_x, has_var)
        variant = infer_variant_type(prefix, role)
        modality = infer_modality(prefix, role, variant)
        obs_row = row_by_key.get(f"{prefix}/obs.parquet")
        x_row = row_by_key.get(f"{prefix}/X.h5ad")
        var_row = row_by_key.get(f"{prefix}/var.parquet")
        linked_obs_key = ""
        linked_var_key = ""
        if not complete:
            linked_obs_key = feature_link_key(
                artifact_by_key.get(f"{prefix}/X.h5ad"), "obs"
            )
            linked_var_key = feature_link_key(
                artifact_by_key.get(f"{prefix}/X.h5ad"), "var"
            )
        triplet_rows.append(
            {
                "prefix": prefix,
                "family": family_for_prefix(prefix),
                "logical_dataset": logical_dataset_for_prefix(prefix),
                "source_accession": source_accession_for_prefix(prefix),
                "prefix_role": role,
                "variant_type": variant,
                "modality_guess": modality,
                "has_obs": bool_str(has_obs),
                "has_X": bool_str(has_x),
                "has_var": bool_str(has_var),
                "linked_obs_key": linked_obs_key,
                "linked_var_key": linked_var_key,
                "has_linked_var": bool_str(bool(linked_var_key)),
                "is_complete_triplet": bool_str(complete),
                "is_training_candidate": bool_str(
                    complete and is_training_candidate_role(role)
                ),
                "obs_n_observations": getattr(obs_row, "n_observations", None),
                "X_n_observations": getattr(x_row, "n_observations", None),
                "var_n_observations": getattr(var_row, "n_observations", None),
                "obs_size": getattr(obs_row, "size", None),
                "X_size": getattr(x_row, "size", None),
                "var_size": getattr(var_row, "size", None),
            }
        )
    write_tsv(out / "triplet_integrity.tsv", triplet_rows)
    write_tsv(
        out / "prefix_classification.tsv",
        triplet_rows,
        columns=[
            "prefix",
            "family",
            "logical_dataset",
            "source_accession",
            "prefix_role",
            "variant_type",
            "modality_guess",
            "is_complete_triplet",
            "is_training_candidate",
            "has_obs",
            "has_X",
            "has_var",
            "linked_obs_key",
            "linked_var_key",
            "has_linked_var",
        ],
    )

    logical_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in triplet_rows:
        logical_groups[row["logical_dataset"]].append(row)
    logical_rows: list[dict[str, Any]] = []
    for logical, rows in sorted(logical_groups.items()):
        complete_count = sum(r["is_complete_triplet"] == "true" for r in rows)
        role_counts = defaultdict(int)
        variant_counts = defaultdict(int)
        modality_counts = defaultdict(int)
        accessions = sorted(
            {r["source_accession"] for r in rows if r["source_accession"]}
        )
        for row in rows:
            role_counts[row["prefix_role"]] += 1
            variant_counts[row["variant_type"]] += 1
            modality_counts[row["modality_guess"]] += 1
        logical_rows.append(
            {
                "logical_dataset": logical,
                "family": family_for_prefix(logical),
                "source_accessions": ",".join(accessions),
                "n_prefixes": len(rows),
                "n_complete_triplets": complete_count,
                "n_incomplete_prefixes": len(rows) - complete_count,
                "n_training_candidate_prefixes": sum(
                    r["is_training_candidate"] == "true" for r in rows
                ),
                "is_chunked_or_sharded": bool_str(len(rows) > 1),
                "prefix_roles": repr(dict(sorted(role_counts.items()))),
                "variant_types": repr(dict(sorted(variant_counts.items()))),
                "modality_guesses": repr(dict(sorted(modality_counts.items()))),
                "example_prefix": rows[0]["prefix"],
            }
        )
    write_tsv(out / "logical_dataset_manifest.tsv", logical_rows)

    repair_rows = []
    for row in triplet_rows:
        if row["is_complete_triplet"] == "true":
            continue
        missing = [
            name
            for name, flag in [
                ("obs.parquet", row["has_obs"]),
                ("X.h5ad", row["has_X"]),
                ("var.parquet", row["has_var"]),
            ]
            if flag != "true"
        ]
        priority, action = recommended_repair(
            row["prefix"], row["prefix_role"], missing
        )
        if "var.parquet" in missing and row["has_linked_var"] == "true":
            priority = "local_var_artifact_missing_but_linked"
            action = (
                "X already links to a shared var artifact; decide whether to keep shared-var contract "
                "or create same-prefix var alias/copy for strict triplet convention"
            )
        repair_rows.append(
            {
                "prefix": row["prefix"],
                "family": row["family"],
                "logical_dataset": row["logical_dataset"],
                "source_accession": row["source_accession"],
                "prefix_role": row["prefix_role"],
                "variant_type": row["variant_type"],
                "modality_guess": row["modality_guess"],
                "linked_obs_key": row["linked_obs_key"],
                "linked_var_key": row["linked_var_key"],
                "has_linked_var": row["has_linked_var"],
                "missing_parts": ",".join(missing),
                "priority": priority,
                "suggested_action": action,
            }
        )
    write_tsv(out / "repair_plan.tsv", repair_rows)

    auxiliary_rows = [
        plan
        for row in triplet_rows
        if (plan := auxiliary_artifact_plan(row)) is not None
    ]
    write_tsv(
        out / "auxiliary_artifact_plan.tsv",
        auxiliary_rows,
        columns=[
            "prefix",
            "logical_dataset",
            "current_role",
            "standard_kind",
            "auxiliary_name",
            "target_primary_prefix",
            "target_X_key",
            "target_var_key",
            "target_obsm_key",
            "feasible_without_payload_load",
            "safe_next_action",
        ],
    )

    if args.sample_payloads:
        sample_prefixes = [r["prefix"] for r in triplet_rows if r["has_obs"] == "true"]
        sample_prefixes = sample_prefixes[: args.max_prefixes]
    else:
        sample_prefixes = []

    obs_coverage_rows: list[dict[str, Any]] = []
    var_rows: list[dict[str, Any]] = []
    control_rows: list[dict[str, Any]] = []
    x_semantics_rows: list[dict[str, Any]] = []

    # Metadata-only schema harmonization reports for all prefixes. These use
    # Lamin registry schemas/features and artifact row counts, not payload loads.
    for row in triplet_rows:
        prefix = row["prefix"]
        obs_artifact = artifact_by_key.get(f"{prefix}/obs.parquet")
        var_artifact = artifact_by_key.get(f"{prefix}/var.parquet")
        obs_columns = (
            obs_columns_from_registry(obs_artifact)
            if row["has_obs"] == "true"
            else set()
        )
        present = [c for c in CANONICAL_OBS_COLUMNS if c in obs_columns]
        missing = [c for c in CANONICAL_OBS_COLUMNS if c not in obs_columns]
        alias_available = alias_available_for_columns(obs_columns)
        if row["has_obs"] == "true":
            obs_coverage_rows.append(
                {
                    "prefix": prefix,
                    "logical_dataset": row["logical_dataset"],
                    "obs_shape": f"{row['obs_n_observations']}xunknown_columns_metadata_only",
                    "n_obs_columns": len(obs_columns),
                    "present_columns": ",".join(present),
                    "missing_columns": ",".join(missing),
                    "alias_available": repr(alias_available),
                    "load_error": "metadata_only_registry_no_payload_load",
                }
            )
            control_rows.append(
                {
                    "prefix": prefix,
                    "has_is_control": "is_control" in obs_columns,
                    "n_controls": None,
                    "strict_control_groups": None,
                    "dataset_control_available": None,
                    "notes": "metadata_only_registry_no_payload_load",
                }
            )
        else:
            control_rows.append(
                {
                    "prefix": prefix,
                    "has_is_control": None,
                    "n_controls": None,
                    "strict_control_groups": None,
                    "dataset_control_available": None,
                    "notes": "missing obs.parquet",
                }
            )

        var_columns = (
            set(schema_feature_names(var_artifact))
            if row["has_var"] == "true"
            else set()
        )
        if row["has_var"] == "true":
            var_rows.append(
                {
                    "prefix": prefix,
                    "logical_dataset": row["logical_dataset"],
                    "var_shape": f"{row['var_n_observations']}xunknown_columns_metadata_only",
                    "var_index_class": classify_var_from_columns(var_columns, prefix),
                    "var_columns": ",".join(sorted(var_columns)),
                    "load_error": "metadata_only_registry_no_payload_load",
                }
            )
        else:
            var_rows.append(
                {
                    "prefix": prefix,
                    "logical_dataset": row["logical_dataset"],
                    "var_shape": "",
                    "var_index_class": "missing",
                    "var_columns": "",
                    "load_error": "missing var.parquet",
                }
            )

        x_semantics_rows.append(
            {
                "prefix": prefix,
                "logical_dataset": row["logical_dataset"],
                "x_semantics_guess": infer_x_semantics(prefix, obs_columns),
                "needs_manual_review": bool_str(
                    row["modality_guess"] in {"unknown_sidecar", "RNA"}
                ),
            }
        )

    for prefix in sample_prefixes:
        obs_key = f"{prefix}/obs.parquet"
        var_key = f"{prefix}/var.parquet"
        obs_columns: set[str] = set()
        obs_shape = None
        load_error = ""
        try:
            obs_df = load_dataframe_artifact(ln, obs_key)
            obs_shape = str(obs_df.shape)
            if args.max_rows and len(obs_df) > args.max_rows:
                obs_sample = obs_df.head(args.max_rows).copy()
            else:
                obs_sample = obs_df
            obs_columns = set(map(str, obs_df.columns))
            present = [c for c in CANONICAL_OBS_COLUMNS if c in obs_columns]
            missing = [c for c in CANONICAL_OBS_COLUMNS if c not in obs_columns]
            alias_available = {
                c: [s for s in SYNONYMS.get(c, []) if s in obs_columns] for c in missing
            }
            obs_coverage_rows.append(
                {
                    "prefix": prefix,
                    "logical_dataset": logical_dataset_for_prefix(prefix),
                    "obs_shape": obs_shape,
                    "n_obs_columns": len(obs_columns),
                    "present_columns": ",".join(present),
                    "missing_columns": ",".join(missing),
                    "alias_available": repr(
                        {k: v for k, v in alias_available.items() if v}
                    ),
                    "load_error": "",
                }
            )
            ctrl = summarize_control_availability(obs_sample)
            control_rows.append({"prefix": prefix, **ctrl})
        except Exception as exc:  # noqa: BLE001 - audit must keep going
            load_error = f"{type(exc).__name__}: {exc}"
            obs_coverage_rows.append(
                {
                    "prefix": prefix,
                    "logical_dataset": logical_dataset_for_prefix(prefix),
                    "obs_shape": obs_shape,
                    "n_obs_columns": None,
                    "present_columns": "",
                    "missing_columns": ",".join(CANONICAL_OBS_COLUMNS),
                    "alias_available": "{}",
                    "load_error": load_error,
                }
            )
            control_rows.append(
                {
                    "prefix": prefix,
                    "has_is_control": None,
                    "n_controls": None,
                    "strict_control_groups": None,
                    "dataset_control_available": None,
                    "notes": load_error,
                }
            )

        x_semantics_rows.append(
            {
                "prefix": prefix,
                "logical_dataset": logical_dataset_for_prefix(prefix),
                "x_semantics_guess": infer_x_semantics(prefix, obs_columns),
                "needs_manual_review": "true",
            }
        )

        try:
            if f"{prefix}/var.parquet" in row_by_key:
                var_df = load_dataframe_artifact(ln, var_key)
                index_values = [str(x) for x in list(var_df.index[:1000])]
                var_rows.append(
                    {
                        "prefix": prefix,
                        "logical_dataset": logical_dataset_for_prefix(prefix),
                        "var_shape": str(var_df.shape),
                        "var_index_class": classify_var_index(index_values),
                        "var_columns": ",".join(map(str, var_df.columns)),
                        "load_error": "",
                    }
                )
            else:
                var_rows.append(
                    {
                        "prefix": prefix,
                        "logical_dataset": logical_dataset_for_prefix(prefix),
                        "var_shape": "",
                        "var_index_class": "missing",
                        "var_columns": "",
                        "load_error": "missing var.parquet",
                    }
                )
        except Exception as exc:  # noqa: BLE001
            var_rows.append(
                {
                    "prefix": prefix,
                    "logical_dataset": logical_dataset_for_prefix(prefix),
                    "var_shape": "",
                    "var_index_class": "unknown",
                    "var_columns": "",
                    "load_error": f"{type(exc).__name__}: {exc}",
                }
            )

    write_tsv(
        out / "obs_column_coverage.tsv",
        obs_coverage_rows,
        columns=[
            "prefix",
            "logical_dataset",
            "obs_shape",
            "n_obs_columns",
            "present_columns",
            "missing_columns",
            "alias_available",
            "load_error",
        ],
    )
    write_tsv(
        out / "var_alignment.tsv",
        var_rows,
        columns=[
            "prefix",
            "logical_dataset",
            "var_shape",
            "var_index_class",
            "var_columns",
            "load_error",
        ],
    )
    write_tsv(
        out / "control_availability.tsv",
        control_rows,
        columns=[
            "prefix",
            "has_is_control",
            "n_controls",
            "strict_control_groups",
            "dataset_control_available",
            "notes",
        ],
    )
    write_tsv(
        out / "x_semantics.tsv",
        x_semantics_rows,
        columns=[
            "prefix",
            "logical_dataset",
            "x_semantics_guess",
            "needs_manual_review",
        ],
    )
    duplicate_rows: list[dict[str, Any]] = []
    logical_by_accession: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in logical_rows:
        for accession in str(row.get("source_accessions", "")).split(","):
            if accession:
                logical_by_accession[accession].append(row)
    for accession, rows in sorted(logical_by_accession.items()):
        if len(rows) < 2:
            continue
        for i, left in enumerate(rows):
            for right in rows[i + 1 :]:
                relationship = "same_accession_candidate"
                if left["family"] == right["family"]:
                    relationship = "same_family_variant_or_shard_candidate"
                duplicate_rows.append(
                    {
                        "candidate_a": left["logical_dataset"],
                        "candidate_b": right["logical_dataset"],
                        "logical_a": left["logical_dataset"],
                        "logical_b": right["logical_dataset"],
                        "evidence_type": "same_accession",
                        "evidence": accession,
                        "relationship": relationship,
                        "recommended_decision": "manual_review",
                        "decision_status": "open",
                    }
                )
    write_tsv(
        out / "duplicate_candidates.tsv",
        duplicate_rows,
        columns=[
            "candidate_a",
            "candidate_b",
            "logical_a",
            "logical_b",
            "evidence_type",
            "evidence",
            "relationship",
            "recommended_decision",
            "decision_status",
        ],
    )

    summary = {
        "instance": ln.setup.settings.instance.slug,
        "branch": ln.setup.settings.branch.name,
        "artifacts": len(artifacts),
        "triplet_prefixes": len(triplet_rows),
        "complete_triplets": sum(
            r["is_complete_triplet"] == "true" for r in triplet_rows
        ),
        "incomplete_triplets": sum(
            r["is_complete_triplet"] != "true" for r in triplet_rows
        ),
        "training_candidate_prefixes": sum(
            r["is_training_candidate"] == "true" for r in triplet_rows
        ),
        "logical_datasets": len(logical_rows),
        "urgent_triplet_repairs": sum(
            r["priority"] == "urgent_triplet_repair" for r in repair_rows
        ),
        "local_var_missing_but_linked": sum(
            r["priority"] == "local_var_artifact_missing_but_linked"
            for r in repair_rows
        ),
        "sidecar_classification_items": sum(
            r["priority"] == "classify_sidecar" for r in repair_rows
        ),
        "auxiliary_standardization_items": sum(
            r["priority"] == "standardize_auxiliary_artifact" for r in repair_rows
        ),
        "auxiliary_artifact_plan_items": len(auxiliary_rows),
        "orphan_review_items": sum(
            r["priority"] == "orphan_review" for r in repair_rows
        ),
        "duplicate_candidates": len(duplicate_rows),
        "metadata_obs_prefixes": len(obs_coverage_rows) - len(sample_prefixes),
        "metadata_var_prefixes": len(var_rows) - len(sample_prefixes),
        "metadata_x_semantics_prefixes": len(x_semantics_rows) - len(sample_prefixes),
        "obs_prefixes_with_derivable_aliases": sum(
            row["alias_available"] != "{}" for row in obs_coverage_rows
        ),
        "var_prefixes_with_id_or_symbol_columns": sum(
            row["var_index_class"]
            in {
                "id_columns_present_metadata_only",
                "symbol_columns_present_metadata_only",
            }
            for row in var_rows
        ),
        "sampled_payload_prefixes": len(sample_prefixes),
        "output_dir": str(out),
    }
    (out / "summary.txt").write_text(
        "\n".join(f"{k}={v}" for k, v in summary.items()) + "\n"
    )
    for key, value in summary.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
