#!/usr/bin/env python3
"""Append-only DATASET_E2E_V3 curation for scPerturb Datlinger17."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import anndata as ad
import numpy as np
import pandas as pd
from anndata.utils import make_index_unique
from pandas.testing import assert_frame_equal

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_60ced1f1"
REAL_DATASET_ID = "scperturb/datlinger17"
PREFIX = REAL_DATASET_ID
EXPECTED_N_OBS = 5_905
EXPECTED_N_VARS = 24_389
EXPECTED_OBS = {"uid": "sitiyL4128YBC8BS0004", "hash": "gU48Qsw1u6MbLvJMIshIiA"}
EXPECTED_X = {"uid": "AVlPOzYBdcrplGXk0000", "hash": "xZOVQ8LeticwYF_-a_Ije0"}
EXPECTED_VAR = {"uid": "AYnivbGN3JCRzkN70001", "hash": "5-S_Oe8xYvmUro9XcE2mAw"}
FROZEN_INPUT_BINDINGS_PATH = Path(__file__).with_name("frozen_inputs") / "bindings.json"
SOURCE_SPECS = {
    "digital_expression.csv.gz": {
        "url": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE92nnn/GSE92872/suppl/GSE92872_CROP-seq_Jurkat_TCR.digital_expression.csv.gz",
        "size": 18_181_357,
        "sha256": "3e0bb8554fdd6f732ec039e703f685631334e9c06029864e81594babc8def0af",
    },
    "supp_table2.xlsx": {
        "url": "https://static-content.springer.com/esm/art%3A10.1038%2Fnmeth.4177/MediaObjects/41592_2017_BFnmeth4177_MOESM268_ESM.xlsx",
        "size": 21_918,
        "sha256": "3acaf07ca5b5cb2fde9b957ae9e6f0b27a6df267b013ba9d818931b11ce54c44",
    },
}
SAMPLE_BY_CONDITION_REPLICATE = {
    **{("stimulated", str(index)): f"GSM24390{79 + index}" for index in range(1, 7)},
    **{("unstimulated", str(index)): f"GSM24390{85 + index}" for index in range(1, 6)},
}
CANONICAL_OBS_FIELDS = (
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
    "source",
    "source_accession",
    "control_availability",
    "x_semantics",
)
NOT_APPLICABLE_FIELDS = {
    "dose",
    "dose_unit",
    "trajectory_id",
    "pseudotime",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
}
UNKNOWN_FIELDS = {"donor_id", "age", "ethnicity", "perturbation_target_id"}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_values_sha256(values: pd.Index) -> str:
    return sha256_bytes("\n".join(values.astype(str)).encode())


def load_frozen_input_bindings() -> dict[str, Any]:
    manifest = json.loads(FROZEN_INPUT_BINDINGS_PATH.read_text())
    if manifest.get("format") != "pert-gym.frozen-input-bindings/v1":
        raise AssertionError("frozen input format drift")
    repository_root = Path(__file__).parents[5]
    for entry in manifest["inputs"]:
        compressed = (repository_root / entry["binding_path"]).read_bytes()
        if sha256_bytes(compressed) != entry["gzip_sha256"]:
            raise AssertionError("frozen gzip hash drift")
        raw = gzip.decompress(compressed)
        if (
            len(raw) != entry["uncompressed_bytes"]
            or sha256_bytes(raw) != entry["uncompressed_sha256"]
        ):
            raise AssertionError("frozen input identity drift")
    if len(manifest["inputs"]) != 2:
        raise AssertionError("frozen input coverage drift")
    return manifest


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": str(artifact.version),
        "size": int(artifact.size),
        "n_observations": getattr(artifact, "n_observations", None),
        "created_at": str(artifact.created_at),
        "description": str(artifact.description),
        "run_uid": str(getattr(getattr(artifact, "run", None), "uid", None)),
    }


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    if not records:
        raise AssertionError(f"missing Artifact history: {key}")
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not bool(records[-1].is_latest):
        raise AssertionError(f"newest Artifact is not latest: {key}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    records = list(ln.Artifact.filter(key=value).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"cannot resolve feature Artifact: {value}")
    return records[-1]


def download_sources() -> tuple[dict[str, Path], dict[str, Any]]:
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-datlinger17-sources"
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    receipts: dict[str, Any] = {}
    for name, spec in SOURCE_SPECS.items():
        path = root / name
        if not path.exists() or path.stat().st_size != spec["size"]:
            subprocess.run(
                [
                    "curl",
                    "--silent",
                    "--show-error",
                    "--location",
                    "--fail",
                    "--retry",
                    "3",
                    "--output",
                    str(path),
                    str(spec["url"]),
                ],
                check=True,
                timeout=600,
            )
        if path.stat().st_size != spec["size"] or sha256_file(path) != spec["sha256"]:
            raise AssertionError(f"source payload identity drift: {name}")
        paths[name] = path
        receipts[name] = dict(spec)
    if sum(int(item["size"]) for item in receipts.values()) > 1_073_741_824:
        raise AssertionError("source payload exceeds bounded-card admission")
    return paths, receipts


def load_sources(*, load_matrix: bool = False) -> dict[str, Any]:
    paths, receipts = download_sources()
    source_obs = pd.read_csv(
        paths["digital_expression.csv.gz"], header=None, index_col=0, nrows=5
    ).T.set_index("cell")
    source_obs.index = make_index_unique(source_obs.index.astype(str))
    guides = pd.read_excel(
        paths["supp_table2.xlsx"], dtype={"gRNA_ID": "string", "Sequence": "string"}
    )
    if len(source_obs) != EXPECTED_N_OBS or not source_obs.index.is_unique:
        raise AssertionError("source OBS denominator/identity drift")
    if len(guides) != 116 or not guides["gRNA_ID"].is_unique:
        raise AssertionError("supplement guide denominator/identity drift")
    if (
        not guides["Sequence"]
        .astype("string")
        .str.fullmatch(r"[ACGT]{20}", na=False)
        .all()
    ):
        raise AssertionError("supplement guide sequence drift")
    if set(source_obs["grna"].astype(str)) - set(guides["gRNA_ID"].astype(str)):
        raise AssertionError(
            "source OBS contains guides absent from Supplementary Table 2"
        )
    if int(source_obs["grna"].astype(str).str.startswith("CTRL").sum()) != 1_320:
        raise AssertionError("source control-row denominator drift")
    result: dict[str, Any] = {
        "obs": source_obs,
        "guides": guides,
        "receipts": receipts,
    }
    if load_matrix:
        matrix = pd.read_csv(
            paths["digital_expression.csv.gz"], skiprows=6, header=None, index_col=0
        ).T
        if len(matrix) != len(source_obs):
            raise AssertionError("source matrix/OBS row denominator drift")
        # The source script binds the transposed expression frame positionally to
        # the metadata-derived OBS axis; pandas otherwise leaves a numeric index.
        # It then applies AnnData.var_names_make_unique() before publication.
        matrix.index = source_obs.index.copy()
        matrix.columns = make_index_unique(matrix.columns.astype(str))
        result["matrix"] = matrix
    return result


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing_series(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def exact_source_join(
    obs: pd.DataFrame, source_obs: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    keys = pd.Index(obs["original_obs_index"].astype(str))
    if not keys.is_unique or not keys.equals(source_obs.index):
        raise AssertionError("source/current OBS ordered identity drift")
    joined = source_obs.reindex(keys).set_axis(obs.index)
    comparisons = {"replicate": "replicate", "condition": "perturbation_2"}
    mismatches: dict[str, int] = {}
    for source_column, current_column in comparisons.items():
        left = joined[source_column].astype("string")
        right = obs[current_column].astype("string")
        equal = (left.isna() & right.isna()) | left.fillna("").eq(right.fillna(""))
        mismatches[current_column] = int((~equal).sum())
    collapsed = (
        joined["grna"]
        .where(~joined["grna"].str.startswith("CTRL"), "control")
        .astype("string")
    )
    mismatches["perturbation"] = int(
        (~collapsed.eq(obs["perturbation"].astype("string"))).sum()
    )
    if any(mismatches.values()):
        raise AssertionError(f"source OBS semantic mismatch: {mismatches}")
    return joined, {
        "source_rows": len(source_obs),
        "current_rows": len(obs),
        "join_mismatch_count": 0,
        "column_mismatches": mismatches,
        "joined_order_sha256": ordered_values_sha256(keys),
        "join_semantics": "exact source barcode order after AnnData make_index_unique",
    }


def curate_obs(
    obs: pd.DataFrame, source: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    original = obs.copy(deep=True)
    joined, join_receipt = exact_source_join(obs, source["obs"])
    mapping = source["guides"].set_index("gRNA_ID")
    guide_id = joined["grna"].astype("string")
    guide_sequence = guide_id.map(mapping["Sequence"]).astype("string")
    target = joined["gene"].astype("string").replace({"CTRL": pd.NA})
    controls = guide_id.str.startswith("CTRL", na=False).astype("boolean")
    if guide_sequence.isna().any():
        raise AssertionError("guide/control source join drift")
    samples = pd.Series(
        [
            SAMPLE_BY_CONDITION_REPLICATE.get((str(condition), str(replicate)))
            for condition, replicate in zip(
                joined["condition"], joined["replicate"], strict=True
            )
        ],
        index=obs.index,
        dtype="string",
    )
    if samples.isna().any() or samples.nunique() != len(
        set(SAMPLE_BY_CONDITION_REPLICATE.values())
    ):
        raise AssertionError("GEO sample mapping drift")

    curated = obs.copy(deep=True)
    for field in CANONICAL_OBS_FIELDS:
        source_original = f"source_original_{field}"
        if source_original not in original:
            curated[source_original] = (
                original[field] if field in original else missing_series(original.index)
            )
    set_field(curated, "dataset", PREFIX, "known", "canonical logical family")
    set_field(
        curated,
        "sample",
        samples,
        "known",
        "GEO CROP-seq run by condition and replicate",
    )
    set_field(
        curated,
        "cell_id",
        original["original_obs_index"].astype("string"),
        "known",
        "GEO digital-expression cell barcode",
    )
    set_field(
        curated,
        "batch",
        "replicate_" + joined["replicate"].astype(str),
        "known",
        "GEO digital-expression replicate",
    )
    set_field(
        curated, "cell_type", "T cell", "known", "publication and accepted triplet"
    )
    set_field(curated, "cell_line", "Jurkat", "known", "GEO and publication")
    set_field(
        curated,
        "disease",
        "T-cell acute lymphoblastic leukemia",
        "known",
        "Jurkat accepted triplet context",
    )
    set_field(
        curated,
        "tissue_type",
        "cell culture",
        "known",
        "publication experimental design",
    )
    set_field(
        curated, "organism", "Homo sapiens", "known", "GEO CROP-seq Jurkat samples"
    )
    set_field(curated, "sex", "male", "known", "accepted Jurkat triplet annotation")
    set_field(
        curated, "sequencer", "Illumina HiSeq 4000", "known", "GEO platform GPL20301"
    )
    set_field(
        curated, "technology", "Drop-seq", "known", "publication CROP-seq protocol"
    )
    set_field(curated, "assay", "CROP-seq", "known", "publication and GEO")
    set_field(curated, "modality", "scRNA-seq", "known", "GEO experiment type")
    set_field(
        curated,
        "media",
        "RPMI with penicillin/streptomycin and 10% FCS",
        "known",
        "publication Methods",
    )
    set_field(curated, "is_bulk", False, "known", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "known", "single-cell source")
    set_field(
        curated,
        "perturbation",
        original["perturbation"].astype("string"),
        "known",
        "accepted scPerturb guide-level perturbation",
    )
    set_field(
        curated,
        "perturbation_type",
        "CRISPRko",
        "known",
        "publication knockout signatures and Cas9 genome editing",
    )
    set_field(
        curated,
        "perturbation_technology",
        "CROPseq-Guide-Puro CRISPR-Cas9 knockout",
        "known",
        "publication Methods",
    )
    set_field(
        curated,
        "perturbation_library",
        "Datlinger et al. TCR gRNA library",
        "known",
        "publication Supplementary Table 2",
    )
    set_field(
        curated, "guide_id", guide_id, "known", "GEO digital-expression grna header"
    )
    set_field(
        curated,
        "guide_sequence",
        guide_sequence,
        "known",
        "publication Supplementary Table 2 exact gRNA_ID join",
    )
    set_field(
        curated,
        "perturbation_target",
        target,
        np.where(controls, "not_applicable", "known"),
        "GEO digital-expression gene header",
    )
    set_field(curated, "is_control", controls, "known", "source CTRL guide identity")
    set_field(
        curated,
        "timepoint",
        240.0,
        "known",
        "publication Methods: 4 h stimulation or continued starvation",
    )
    set_field(
        curated,
        "is_baseline",
        joined["condition"].eq("unstimulated").astype("boolean"),
        "known",
        "GEO naive/unstimulated condition",
    )
    set_field(
        curated,
        "n_counts",
        original["ncounts"].astype("Float64"),
        "known",
        "accepted source QC alias ncounts",
    )
    set_field(
        curated,
        "n_genes",
        original["ngenes"].astype("Int64"),
        "known",
        "accepted source QC alias ngenes",
    )
    set_field(
        curated,
        "pct_mito",
        original["percent_mito"].astype("Float64"),
        "known",
        "accepted source QC alias percent_mito",
    )
    set_field(
        curated,
        "pct_ribo",
        original["percent_ribo"].astype("Float64"),
        "known",
        "accepted source QC alias percent_ribo",
    )
    set_field(
        curated,
        "is_low_quality",
        False,
        "known",
        "GEO states all 5,905 are high-quality assigned-gRNA transcriptomes",
    )
    set_field(curated, "source", "scPerturb", "known", "accepted collection source")
    set_field(
        curated,
        "source_accession",
        "GSE92872",
        "known",
        "GEO series accession",
    )
    set_field(
        curated,
        "control_availability",
        "dataset_control_available",
        "known",
        "1,320 source CTRL-guide rows",
    )
    set_field(
        curated,
        "x_semantics",
        "raw_counts",
        "known",
        "exact source digital-expression matrix parity",
    )
    curated["source_geo_accession"] = "GSE92872"
    curated["source_tcr_condition"] = joined["condition"].astype("string")
    curated["source_tcr_stimulation_minutes"] = 240.0
    for field in NOT_APPLICABLE_FIELDS:
        dtype = (
            "Float64" if field in {"dose", "pseudotime", "response_value"} else "string"
        )
        set_field(
            curated,
            field,
            missing_series(curated.index, dtype),
            "not_applicable",
            "dataset design",
        )
    for field in UNKNOWN_FIELDS:
        dtype = "Float64" if field == "age" else "string"
        set_field(
            curated,
            field,
            missing_series(curated.index, dtype),
            "unknown",
            "source-exhaustive search found no defensible row value",
        )
    if len(curated) != EXPECTED_N_OBS or not curated.index.equals(original.index):
        raise AssertionError("OBS row count/order drift")
    if not curated["obs_uuid"].is_unique or not curated["original_obs_index"].is_unique:
        raise AssertionError("OBS identity uniqueness drift")
    predecessor_accessions = curated["source_original_source_accession"].dropna()
    return curated, {
        **join_receipt,
        "guide_table_rows": len(source["guides"]),
        "guide_sequence_known_rows": int(guide_sequence.notna().sum()),
        "control_rows": int(controls.sum()),
        "geo_samples": int(samples.nunique()),
        "canonical_source_accession": "GSE92872",
        "preserved_source_accession_rows": len(predecessor_accessions),
        "preserved_source_accession_values": sorted(
            predecessor_accessions.astype(str).unique().tolist()
        ),
    }


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        state_column = f"{field}_state"
        if field not in frame or state_column not in frame:
            raise AssertionError(f"canonical OBS disposition absent: {field}")
        states = frame[state_column].astype("string")
        known = int(frame[field].notna().sum())
        if states.eq("not_applicable").all():
            disposition = "not_applicable"
        elif states.eq("unknown").all():
            disposition = "unknown"
        elif known == len(frame):
            disposition = "materialized_complete"
        else:
            disposition = "materialized_partial"
        result[field] = {
            "disposition": disposition,
            "materialized": True,
            "known_rows": known,
            "unknown_rows": len(frame) - known,
            "source_bound": disposition.startswith("materialized"),
        }
    return result


def verify_obs_semantics(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    if not actual.index.equals(expected.index) or list(actual.columns) != list(
        expected.columns
    ):
        raise AssertionError("OBS schema/order mismatch")
    try:
        assert_frame_equal(actual, expected, check_categorical=False)
    except AssertionError as error:
        raise AssertionError("OBS source semantic mismatch") from error


def obs_matches_expected_semantics(actual: pd.DataFrame, expected: pd.DataFrame) -> bool:
    try:
        verify_obs_semantics(actual, expected)
    except AssertionError:
        return False
    return True


def verify_var(var: pd.DataFrame, x_axis: pd.Index) -> dict[str, Any]:
    if len(var) != EXPECTED_N_VARS or not var.index.astype(str).equals(
        x_axis.astype(str)
    ):
        raise AssertionError("VAR/X feature-axis count/order drift")
    stable = var["stable_feature_id"].astype("string")
    if not stable.str.fullmatch(r"ENSG\d{11}", na=False).all() or not stable.is_unique:
        raise AssertionError("VAR exact unique human ENSG contract drift")
    wrong_species = int((~stable.str.startswith("ENSG", na=False)).sum())
    return {
        "rows": len(var),
        "stable_feature_id_unique": True,
        "human_ensg_rows": int(stable.notna().sum()),
        "wrong_species_rows": wrong_species,
        "axis_count_parity": True,
        "axis_order_parity": True,
        "axis_order_sha256": ordered_values_sha256(var.index),
        "stable_id_order_sha256": ordered_values_sha256(pd.Index(stable)),
        "shared_var_identity": "one physical member links to one dataset-level same-prefix VAR",
        "needs_revision": False,
        "mismatch_count": 0,
    }


def verify_x_source_parity(
    artifact: Any,
    var: pd.DataFrame,
    source: dict[str, Any],
    expected_obs_order: pd.Index,
) -> tuple[pd.Index, dict[str, Any]]:
    if {"uid": str(artifact.uid), "hash": str(artifact.hash)} != EXPECTED_X:
        raise AssertionError("accepted X identity drift")
    path = Path(artifact.cache())
    backed = ad.read_h5ad(path, backed="r")
    if (backed.n_obs, backed.n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
        raise AssertionError("X shape drift")
    x_obs = backed.obs_names.astype(str)
    x_var = backed.var_names.astype(str)
    if not x_obs.equals(expected_obs_order.astype(str)):
        raise AssertionError("X/OBS row-axis drift")
    matrix = source.get("matrix")
    if matrix is None:
        raise AssertionError("source matrix required for X parity")
    if not var.index.astype(str).equals(x_var):
        raise AssertionError("VAR/X feature-axis order drift before source parity")
    source_columns = pd.Index(matrix.columns.astype(str))
    raw_axis_candidates = {"__index__": pd.Index(var.index.astype(str))}
    raw_axis_candidates.update(
        {str(column): pd.Index(var[column].astype(str)) for column in var.columns}
    )
    axis_candidates = {
        name: make_index_unique(axis) for name, axis in raw_axis_candidates.items()
    }
    candidate_evidence = {
        name: {
            "raw_unique": bool(raw_axis_candidates[name].is_unique),
            "normalized_unique": bool(axis.is_unique),
            "source_overlap": int(axis.isin(source_columns).sum()),
        }
        for name, axis in axis_candidates.items()
    }
    preference = (
        "gene_symbol",
        "symbol",
        "gene_name",
        "feature_name",
        "pert_gym_original_var_index",
        "__index__",
    )
    full_matches = [
        name
        for name, evidence in candidate_evidence.items()
        if evidence["normalized_unique"]
        and evidence["source_overlap"] == EXPECTED_N_VARS
    ]
    full_matches.sort(
        key=lambda name: (
            preference.index(name) if name in preference else len(preference)
        )
    )
    if not full_matches:
        best = sorted(
            candidate_evidence.items(),
            key=lambda item: int(item[1]["source_overlap"]),
            reverse=True,
        )[:10]
        raise AssertionError(
            f"no VAR column reproduces the source feature axis: best={best}"
        )
    source_feature_axis_column = full_matches[0]
    source_feature_axis = axis_candidates[source_feature_axis_column]
    selected = matrix.loc[:, source_feature_axis]
    mismatch_count = 0
    for start in range(0, EXPECTED_N_OBS, 256):
        stop = min(EXPECTED_N_OBS, start + 256)
        actual = backed.X[start:stop]
        if hasattr(actual, "toarray"):
            actual = actual.toarray()
        expected = selected.iloc[start:stop].to_numpy()
        mismatch_count += int(np.count_nonzero(np.asarray(actual) != expected))
    receipt = {
        **EXPECTED_X,
        "shape": [backed.n_obs, backed.n_vars],
        "var_names_sha256": ordered_values_sha256(x_var),
        "source_feature_axis_sha256": ordered_values_sha256(source_feature_axis),
        "source_feature_axis_column": source_feature_axis_column,
        "source_feature_axis_candidates": candidate_evidence,
        "source_matrix_shape": list(matrix.shape),
        "selected_source_shape": list(selected.shape),
        "source_value_mismatch_count": mismatch_count,
        "raw_counts": mismatch_count == 0,
        "backed_x_dtype": str(backed.X.dtype),
        "source_selected_min": float(selected.min().min()),
        "source_selected_integral": bool(
            np.equal(np.mod(selected.to_numpy(), 1), 0).all()
        ),
    }
    backed.file.close()
    if (
        mismatch_count
        or receipt["source_selected_min"] < 0
        or not receipt["source_selected_integral"]
    ):
        raise AssertionError("X/source raw-count parity drift")
    return x_var, receipt


def collection_snapshot(ln: Any) -> dict[str, Any]:
    snapshots: dict[str, Any] = {}
    for key, expected_uid, expected_count in (
        ("pert-gym/base-public/20260621", "SM2AjpxEQZ1pEfF30000", 60),
        ("pert-gym/canonical/20260621", "aEWBxMlcWx2d7Cd80000", 1056),
    ):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matches = [
            {"uid": str(item.uid), "key": str(item.key)}
            for item in members
            if str(item.key) == f"{PREFIX}/obs.parquet"
        ]
        if (
            str(collection.uid) != expected_uid
            or len(members) != expected_count
            or len(matches) != 1
        ):
            raise AssertionError(f"target Collection membership drift: {key}")
        snapshots[key] = {
            "uid": str(collection.uid),
            "hash": str(collection.hash),
            "member_count": len(members),
            "target_key_matches": matches,
        }
    return snapshots


def verify_current(ln: Any, source: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    obs_artifact, obs_history = latest_artifact(ln, f"{PREFIX}/obs.parquet")
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    if {"uid": str(x_artifact.uid), "hash": str(x_artifact.hash)} != EXPECTED_X:
        raise AssertionError("X identity drift")
    if {"uid": str(var_artifact.uid), "hash": str(var_artifact.hash)} != EXPECTED_VAR:
        raise AssertionError("VAR identity drift")
    if len(obs) != EXPECTED_N_OBS or len(obs) > 50_000:
        raise AssertionError("OBS bounded denominator drift")
    expected_obs, join_receipt = curate_obs(obs, source)
    var = var_artifact.load()
    x_axis, x_receipt = verify_x_source_parity(
        x_artifact,
        var,
        source,
        pd.Index(obs["original_obs_index"].astype(str)),
    )
    var_verdict = verify_var(var, x_axis)
    obs_curated = str(obs_artifact.description).startswith(
        f"{TASK_ID}: source-exhaustive Datlinger17 OBS"
    ) and obs_matches_expected_semantics(obs, expected_obs)
    return {
        "obs_before": artifact_identity(obs_artifact),
        "obs_history": [artifact_identity(item) for item in obs_history],
        "x": artifact_identity(x_artifact),
        "x_source_parity": x_receipt,
        "var": artifact_identity(var_artifact),
        "var_verdict": var_verdict,
        "source_manifest": source["receipts"],
        "source_join": join_receipt,
        "canonical_field_dispositions": field_dispositions(
            obs if obs_curated else expected_obs
        ),
        "chunk_decision": {
            "physical_members": 1,
            "x_bytes": int(x_artifact.size),
            "decision": "no_rechunk",
            "reason": "single 129.6 MB member is already below the target envelope; rewrite would add fragmentation without a defect",
        },
        "already_curated_obs": obs_curated,
        "curated_obs": expected_obs,
        "obs_artifact": obs_artifact,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
    }, obs_curated


def publish(ln: Any, result: dict[str, Any], helper_sha256: str) -> list[Any]:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-datlinger17-publish-"))
    ln.track(
        key=f"pert-gym/real-dataset-curation/{REAL_DATASET_ID}/{TASK_ID}",
        kind="script",
        params={"task_id": TASK_ID, "helper_sha256": helper_sha256},
        new_run=True,
        pypackages=False,
        stream_tracking=False,
    )
    path = root / "obs.parquet"
    result["curated_obs"].to_parquet(path)
    obs = ln.Artifact.from_dataframe(
        path,
        key=f"{PREFIX}/obs.parquet",
        revises=result["obs_artifact"],
        description=f"{TASK_ID}: source-exhaustive Datlinger17 OBS; canonical GSE92872 accession, exact 5905-cell GEO join, all 116 guide IDs/sequences mapped, 11 GSM runs, CRISPRko and 4 h TCR condition semantics",
    ).save()
    obs.features.set_values({"X": result["x_artifact"]})
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()
    return [obs]


def strip_runtime(result: dict[str, Any]) -> dict[str, Any]:
    hidden = {"curated_obs", "obs_artifact", "x_artifact", "var_artifact"}
    return {key: value for key, value in result.items() if key not in hidden}


def emit_product(phase: str, current: int) -> None:
    print(
        "PRODUCT_EXECUTION="
        + canonical(
            {
                "product_execution": {
                    "host": os.uname().nodename,
                    "pid": os.getpid(),
                    "phase": phase,
                    "payload_heartbeat_at": int(time.time()),
                    "metric": "dataset_e2e_v3",
                    "current": current,
                    "denominator": 1,
                    "unit": "biological_dataset",
                }
            }
        ),
        flush=True,
    )


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    helper_sha256 = sha256_file(Path(__file__))
    frozen = load_frozen_input_bindings()
    capacity = preflight()
    if capacity.available_memory_bytes < 8 * 1024**3:
        raise RuntimeError("MemAvailable below 8 GiB")
    emit_product("preflight", 0)
    source = load_sources(load_matrix=True)
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    result, already_curated = verify_current(ln, source)
    collections_before = collection_snapshot(ln)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    writes: list[Any] = []
    if mode == "mutate" and not already_curated:
        metadata = {
            "run_id": TASK_ID,
            "pid": os.getpid(),
            "host": capacity.hostname,
            "project": capacity.project,
            "zone": capacity.zone,
            "branch": ln.setup.settings.branch.name,
            "started_at": time.time(),
        }
        with ExitStack() as stack:
            stack.enter_context(
                lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity)
            )
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            fresh, fresh_curated = verify_current(ln, source)
            if fresh_curated:
                result, already_curated = fresh, True
            else:
                result = fresh
                writes = publish(ln, fresh, helper_sha256)
    elif mode == "verify" and not already_curated:
        raise AssertionError("verify requested before exact OBS successor exists")
    final, final_curated = verify_current(ln, source)
    if mode in {"mutate", "verify"} and not final_curated:
        raise AssertionError("terminal OBS readback failed")
    collections_after = collection_snapshot(ln)
    if collections_after != collections_before:
        raise AssertionError("Collection drift")
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    receipt = {
        "format": "pert-gym.dataset-e2e-v3/v1",
        "contract_sha256": "bb786f6619c8a395575fe88c67f3c75bd4c6a545107c1535b02db88be02505a6",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "frozen_inputs": frozen["inputs"],
        "source_denominator": {
            "biological_datasets": 1,
            "logical_families": 1,
            "physical_members": 1,
            "observations": EXPECTED_N_OBS,
            "features": EXPECTED_N_VARS,
            "guide_library_rows": 116,
        },
        "member_before": strip_runtime(result),
        "member_after": strip_runtime(final),
        "collections": collections_after,
        "writes": {
            "obs_revisions": len(writes),
            "var_revisions": 0,
            "x_revisions": 0,
            "collection_writes": 0,
            "deletions": 0,
            "artifacts": [artifact_identity(item) for item in writes],
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {
            "hostname": capacity.hostname,
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "rollback": {
            "preserved_predecessor_uid": EXPECTED_OBS["uid"],
            "preserved_x_uid": EXPECTED_X["uid"],
            "preserved_var_uid": EXPECTED_VAR["uid"],
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = sha256_bytes(canonical(receipt).encode())
    emit_product("checkpointing", 1)
    print("DATLINGER17_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
