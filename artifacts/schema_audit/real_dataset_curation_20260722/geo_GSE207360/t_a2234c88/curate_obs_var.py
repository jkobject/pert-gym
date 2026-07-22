#!/usr/bin/env python3
"""Append-only source-exhaustive OBS curation for GEO GSE207360."""

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

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_a2234c88"
REAL_DATASET_ID = "geo/GSE207360"
PREFIX = "prism_collection/GSE207360"
EXPECTED_N_OBS = 12_487
EXPECTED_N_VARS = 60_736
EXPECTED_OBS = {"uid": "KSAkP0NJF5P5g1mJ0002", "hash": "aBt3qphtkB7epOFzswv-VA"}
EXPECTED_X = {"uid": "4IOEQEw4ylx0Zx4c0000", "hash": "rLTZFYwmtPyrsHhVQ6_kp-"}
EXPECTED_VAR = {"uid": "U8OeHI58YG9Y9Nsb0002", "hash": "wv2BwlQShhowaM7AYyu4uQ"}
SOURCE_ROOT = Path("/var/tmp/pert-gym-gse207360")
SOURCE_FILE = SOURCE_ROOT / "GSE207360_Human_Mouse_filtered.rds.gz"
SOURCE_TSV = SOURCE_ROOT / "filtered_metadata.tsv"
SOURCE_SUMMARY = SOURCE_ROOT / "filtered_summary.json"
SOURCE_SPEC = {
    "url": "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE207nnn/GSE207360/suppl/GSE207360_Human_Mouse_filtered.rds.gz",
    "size": 4_174_159_639,
    "sha256": "b54a754f26aeb6082de7480ac15622c1696b5f753e036e36b4346c98021bdba1",
    "metadata_tsv_sha256": "a8b1a08d26e67cbf90ff0d545b2e0cea1c2a0ea52cfd2e241c4273b92d5fbfb4",
}
EVIDENCE_DIR = Path(__file__).parent
SOURCE_MANIFEST_PATH = EVIDENCE_DIR / "source_manifest.json"
R_EXTRACTOR = EVIDENCE_DIR / "extract_seurat_metadata.R"
FROZEN_INPUT_BINDINGS_PATH = EVIDENCE_DIR / "frozen_inputs" / "bindings.json"
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
    "ethnicity",
    "media",
    "dose",
    "dose_unit",
    "trajectory_id",
    "pseudotime",
    "sensitivity",
    "response_metric",
    "response_value",
    "response_source",
}
UNKNOWN_FIELDS = {"donor_id", "sex", "age", "pct_ribo"}
SAMPLE_ACCESSION = {"WT": "GSM6284971", "KO": "GSM6284972"}
SAMPLE_TIME_MINUTES = {"WT": 15.0 * 24 * 60, "KO": 90.0 * 24 * 60}
SOURCE_COLUMN_MAP = {
    "orig.ident": "source_seurat_orig_ident",
    "nCount_RNA": "source_seurat_nCount_RNA",
    "nFeature_RNA": "source_seurat_nFeature_RNA",
    "Sample": "source_seurat_Sample",
    "Barcode": "source_seurat_Barcode",
    "hg19": "source_seurat_hg19_counts",
    "mm10": "source_seurat_mm10_counts",
    "frac_hg_genes": "source_seurat_frac_hg_genes",
    "hdbscan_cluster": "source_seurat_hdbscan_cluster",
    "percent.mt": "source_seurat_percent_mt_human",
    "percent.mt_mouse": "source_seurat_percent_mt_mouse",
    "RNA_snn_res.1": "source_seurat_RNA_snn_res_1",
    "seurat_clusters": "source_seurat_clusters",
    "sample.name": "source_seurat_sample_name",
    "Cell_type1": "source_seurat_Cell_type1",
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(16 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ordered_sha256(values: pd.Index) -> str:
    return sha256_bytes("\n".join(values.astype(str)).encode())


def _rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        for candidate in ("real_datasets", "datasets", "rows"):
            rows = value.get(candidate)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    raise AssertionError("frozen input has no row list")


def load_frozen_input_bindings() -> dict[str, Any]:
    manifest = json.loads(FROZEN_INPUT_BINDINGS_PATH.read_text())
    if manifest.get("format") != "pert-gym.frozen-input-bindings/v1":
        raise AssertionError("frozen input format drift")
    repository_root = Path(__file__).parents[5]
    found: dict[str, dict[str, Any]] = {}
    for entry in manifest["inputs"]:
        compressed = (repository_root / entry["binding_path"]).read_bytes()
        if len(compressed) != entry["gzip_bytes"] or sha256_bytes(compressed) != entry["gzip_sha256"]:
            raise AssertionError("frozen gzip identity drift")
        raw = gzip.decompress(compressed)
        if len(raw) != entry["uncompressed_bytes"] or sha256_bytes(raw) != entry["uncompressed_sha256"]:
            raise AssertionError("frozen input identity drift")
        matches = [row for row in _rows(json.loads(raw)) if row.get("real_dataset_id") == REAL_DATASET_ID]
        if len(matches) != 1:
            raise AssertionError("frozen GSE207360 row identity drift")
        found[entry["original_path"]] = matches[0]
    if len(found) != 2:
        raise AssertionError("frozen input coverage drift")
    crosswalk = next(row for path, row in found.items() if "crosswalk" in path)
    audit = next(row for path, row in found.items() if "final_real_dataset" in path)
    logical_family_keys = [item["logical_family"] for item in crosswalk["logical_families"]]
    if crosswalk["observations"] != EXPECTED_N_OBS or logical_family_keys != [PREFIX]:
        raise AssertionError("frozen crosswalk GSE207360 row drift")
    if audit["observations"] != EXPECTED_N_OBS or audit["logical_family_count"] != 1:
        raise AssertionError("frozen audit GSE207360 row drift")
    return {"inputs": manifest["inputs"], "crosswalk_row": crosswalk, "audit_row": audit}


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
        "is_latest": bool(artifact.is_latest),
    }


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records or not bool(records[-1].is_latest):
        raise AssertionError(f"missing latest Artifact: {key}")
    return records[-1], records


def resolve_artifact(ln: Any, value: Any) -> Any:
    if not isinstance(value, str):
        return value
    by_uid = list(ln.Artifact.filter(uid=value).all())
    if len(by_uid) == 1:
        return by_uid[0]
    return latest_artifact(ln, value)[0]


def _extract_source_metadata() -> None:
    with SOURCE_FILE.open("rb") as source_handle:
        outer = subprocess.Popen(["gzip", "-dc"], stdin=source_handle, stdout=subprocess.PIPE)
        assert outer.stdout is not None
        inner = subprocess.Popen(["gzip", "-dc"], stdin=outer.stdout, stdout=subprocess.PIPE)
        outer.stdout.close()
        assert inner.stdout is not None
        r_process = subprocess.run(
            ["Rscript", "--vanilla", str(R_EXTRACTOR), str(SOURCE_TSV), str(SOURCE_SUMMARY)],
            stdin=inner.stdout,
            check=False,
            timeout=7_200,
        )
        inner.stdout.close()
        inner_rc = inner.wait()
        outer_rc = outer.wait()
    if r_process.returncode or inner_rc or outer_rc:
        raise RuntimeError("filtered Seurat metadata extraction failed")


def load_source_metadata() -> tuple[pd.DataFrame, dict[str, Any]]:
    SOURCE_ROOT.mkdir(parents=True, exist_ok=True)
    if not SOURCE_FILE.exists() or SOURCE_FILE.stat().st_size != SOURCE_SPEC["size"]:
        subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--location",
                "--fail",
                "--retry",
                "5",
                "--retry-all-errors",
                "--continue-at",
                "-",
                "--output",
                str(SOURCE_FILE),
                SOURCE_SPEC["url"],
            ],
            check=True,
            timeout=7_200,
        )
    if SOURCE_FILE.stat().st_size != SOURCE_SPEC["size"] or sha256_file(SOURCE_FILE) != SOURCE_SPEC["sha256"]:
        raise AssertionError("filtered source payload identity drift")
    if not SOURCE_TSV.exists() or sha256_file(SOURCE_TSV) != SOURCE_SPEC["metadata_tsv_sha256"]:
        _extract_source_metadata()
    if sha256_file(SOURCE_TSV) != SOURCE_SPEC["metadata_tsv_sha256"]:
        raise AssertionError("filtered source metadata identity drift")
    frame = pd.read_csv(SOURCE_TSV, sep="\t", index_col=0)
    summary = json.loads(SOURCE_SUMMARY.read_text())
    if len(frame) != EXPECTED_N_OBS or not frame.index.is_unique:
        raise AssertionError("filtered source OBS denominator drift")
    if summary["counts_rows"] != EXPECTED_N_VARS or summary["counts_columns"] != EXPECTED_N_OBS:
        raise AssertionError("filtered source matrix denominator drift")
    if not summary["counts_integral"] or summary["counts_min"] < 0 or not summary["counts_colnames_equal_metadata"]:
        raise AssertionError("filtered source raw-count semantics drift")
    return frame, summary


def set_field(frame: pd.DataFrame, field: str, values: Any, state: Any, source: str) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing_series(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def compare_source_column(source: pd.Series, current: pd.Series) -> int:
    if pd.api.types.is_numeric_dtype(source) and pd.api.types.is_numeric_dtype(current):
        left = pd.to_numeric(source, errors="coerce").to_numpy(dtype=float)
        right = pd.to_numeric(current, errors="coerce").to_numpy(dtype=float)
        return int(np.count_nonzero(~np.isclose(left, right, equal_nan=True, rtol=0, atol=1e-12)))
    return int((~source.astype("string").fillna("<NA>").eq(current.astype("string").fillna("<NA>"))).sum())


def exact_source_join(obs: pd.DataFrame, source: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    keys = pd.Index(obs["original_obs_index"].astype(str))
    if not keys.is_unique or not keys.equals(source.index.astype(str)):
        raise AssertionError("source/current OBS ordered identity drift")
    joined = source.set_axis(obs.index)
    mismatches = {
        column: compare_source_column(joined[column], obs[column])
        for column in source.columns
        if column in obs and column != "Sample"
    }
    if any(mismatches.values()):
        raise AssertionError(f"source/current metadata mismatch: {mismatches}")
    return joined, {
        "source_rows": len(source),
        "current_rows": len(obs),
        "join_mismatch_count": 0,
        "column_mismatches": mismatches,
        "ordered_source_axis_sha256": ordered_sha256(keys),
        "join_semantics": "exact filtered Seurat row names to original_obs_index",
        "sample_counts": {str(key): int(value) for key, value in joined["sample.name"].value_counts().items()},
        "cell_type_counts": {str(key): int(value) for key, value in joined["Cell_type1"].value_counts().items()},
    }


def mixed_missing(index: pd.Index, control: pd.Series) -> tuple[pd.Series, np.ndarray]:
    return missing_series(index), np.where(control, "not_applicable", "unknown")


def curate_obs(obs: pd.DataFrame, source: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    original = obs.copy(deep=True)
    joined, join_receipt = exact_source_join(obs, source)
    sample_name = joined["sample.name"].astype("string")
    if set(sample_name.unique()) != set(SAMPLE_ACCESSION):
        raise AssertionError("source sample-name vocabulary drift")
    control = sample_name.eq("WT").astype("boolean")
    sample = sample_name.map(SAMPLE_ACCESSION).astype("string")
    timepoint = sample_name.map(SAMPLE_TIME_MINUTES).astype("Float64")
    curated = obs.copy(deep=True)
    for field in CANONICAL_OBS_FIELDS:
        source_original = f"source_original_{field}"
        if source_original not in curated:
            curated[source_original] = original[field] if field in original else missing_series(original.index)
    for source_column, preserved_column in SOURCE_COLUMN_MAP.items():
        curated[preserved_column] = joined[source_column].to_numpy()

    set_field(curated, "dataset", PREFIX, "known", "canonical logical family")
    set_field(curated, "sample", sample, "known", "exact GEO sample_name join: WT=GSM6284971, KO=GSM6284972")
    set_field(curated, "cell_id", original["original_obs_index"].astype("string"), "known", "filtered Seurat row name")
    set_field(curated, "batch", sample, "known", "GEO biological sample accession")
    set_field(curated, "cell_type", joined["Cell_type1"].astype("string"), "known", "filtered Seurat Cell_type1 annotation")
    set_field(curated, "cell_line", missing_series(curated.index), "not_applicable", "observed cells are murine tumour-microenvironment cells; human GSC83 is model context")
    set_field(curated, "disease", "glioblastoma xenograft model", "known", "publication and GEO experimental model")
    set_field(curated, "tissue_type", "brain xenograft tumour", "known", "GEO treatment and dissociation metadata")
    set_field(curated, "organism", "Mus musculus", "known", "filtered Seurat mouse-cell lineage annotations and hg19/mm10 separation")
    set_field(curated, "sequencer", "Illumina NovaSeq 6000", "known", "GEO platform GPL24676")
    set_field(curated, "technology", "10x Genomics Chromium Single Cell 3' v3", "known", "GEO library protocol")
    set_field(curated, "assay", "10x 3' Gene Expression v3", "known", "GEO library protocol")
    set_field(curated, "modality", "scRNA-seq", "known", "GEO experiment type")
    set_field(curated, "is_bulk", False, "known", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "known", "single-cell source")
    set_field(curated, "perturbation", pd.Series(np.where(control, "control", "EGFR"), index=curated.index, dtype="string"), "known", "GEO genotype joined by sample.name")
    set_field(curated, "perturbation_type", pd.Series(np.where(control, "none", "CRISPRko"), index=curated.index, dtype="string"), "known", "publication CRISPR/Cas9 EGFR/EGFRvIII knockout")
    set_field(curated, "perturbation_technology", pd.Series(np.where(control, pd.NA, "CRISPR/Cas9 knockout"), index=curated.index, dtype="string"), np.where(control, "not_applicable", "known"), "publication Methods")
    library = "Transomic EGFR CRISPR guide set TEDH-1024003/TEDH-1024000/TEDH-1024001/TEDH-1055978"
    set_field(curated, "perturbation_library", pd.Series(np.where(control, pd.NA, library), index=curated.index, dtype="string"), np.where(control, "not_applicable", "known"), "publication Methods; sample does not identify one guide")
    guide_id, guide_state = mixed_missing(curated.index, control)
    set_field(curated, "guide_id", guide_id, guide_state, "WT has no guide; KO clone-to-guide identity absent from all sources")
    guide_sequence, sequence_state = mixed_missing(curated.index, control)
    set_field(curated, "guide_sequence", guide_sequence, sequence_state, "WT has no guide; guide sequences absent from GEO, paper, supplement and analysis code")
    set_field(curated, "perturbation_target", pd.Series(np.where(control, pd.NA, "EGFR"), index=curated.index, dtype="string"), np.where(control, "not_applicable", "known"), "GEO genotype and publication")
    target_id, target_state = mixed_missing(curated.index, control)
    set_field(curated, "perturbation_target_id", target_id, target_state, "WT has no target; no release-pinned target identifier in inspected sources")
    set_field(curated, "is_control", control, "known", "GEO intact-EGFR WT versus depleted-EGFR KO genotype")
    set_field(curated, "timepoint", timepoint, "known", "GEO time after intracranial injection: WT day 15, KO day 90; converted to minutes")
    set_field(curated, "is_baseline", control, "known", "isogenic intact-EGFR WT reference group")
    set_field(curated, "n_counts", joined["nCount_RNA"].astype("Float64"), "known", "filtered Seurat nCount_RNA")
    set_field(curated, "n_genes", joined["nFeature_RNA"].astype("Int64"), "known", "filtered Seurat nFeature_RNA")
    set_field(curated, "pct_mito", (joined["percent.mt_mouse"].astype("Float64") * 100.0), "known", "filtered Seurat mouse mitochondrial fraction converted to percent")
    set_field(curated, "is_low_quality", False, "known", "published filtered Seurat object after doublet/QC filtering")
    set_field(curated, "source", "GEO", "known", "GEO series and complete source manifest")
    set_field(curated, "source_accession", "GSE207360", "known", "GEO series accession")
    set_field(curated, "control_availability", "dataset_control_available", "known", "6,081 source WT cells and 6,406 KO cells")
    set_field(curated, "x_semantics", "raw_counts", "known", "source counts and accepted X are nonnegative integral with exact axes and 56,169,414 stored nonzeros")
    for field in NOT_APPLICABLE_FIELDS:
        dtype = "Float64" if field in {"dose", "pseudotime", "response_value"} else "string"
        set_field(curated, field, missing_series(curated.index, dtype), "not_applicable", "dataset design and source-exhaustive disposition")
    for field in UNKNOWN_FIELDS:
        dtype = "Float64" if field == "age" else "string"
        set_field(curated, field, missing_series(curated.index, dtype), "unknown", "source-exhaustive search found no defensible row-level value")
    curated["source_geo_series"] = "GSE207360"
    curated["source_geo_sample"] = sample
    curated["source_model_human_cell_line"] = "GSC83"
    curated["source_model_mouse_strain"] = "NSG"
    curated["source_time_days_after_injection"] = sample_name.map({"WT": 15.0, "KO": 90.0}).astype("Float64")
    curated["source_filtered_rds_sha256"] = SOURCE_SPEC["sha256"]
    curated["source_analysis_code_commit"] = "cea7f3d8a14a3dfa828b8329721dac53a56a4a12"
    curated["source_publication_pmid"] = "38570528"
    if len(curated) != len(source) or not curated.index.equals(original.index):
        raise AssertionError("OBS row count/order drift")
    if not curated["obs_uuid"].is_unique or not curated["original_obs_index"].is_unique:
        raise AssertionError("OBS identity uniqueness drift")
    return curated, {
        **join_receipt,
        "wt_rows": int(control.sum()),
        "ko_rows": int((~control).sum()),
        "geo_samples": {str(key): int(value) for key, value in sample.value_counts().items()},
        "timepoint_minutes": {str(key): int(value) for key, value in timepoint.value_counts().items()},
        "cell_type_counts": {str(key): int(value) for key, value in curated["cell_type"].value_counts().items()},
    }


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        states = frame[f"{field}_state"].astype("string")
        known = int(frame[field].notna().sum())
        if states.eq("not_applicable").all():
            disposition = "not_applicable"
        elif states.eq("unknown").all():
            disposition = "unknown"
        elif known == len(frame):
            disposition = "materialized_complete"
        elif known:
            disposition = "materialized_partial"
        elif states.eq("not_applicable").any() and states.eq("unknown").any():
            disposition = "mixed_unknown_not_applicable"
        else:
            raise AssertionError(f"unclassified canonical field disposition: {field}")
        result[field] = {
            "disposition": disposition,
            "known_rows": known,
            "unknown_rows": int(states.eq("unknown").sum()),
            "not_applicable_rows": int(states.eq("not_applicable").sum()),
            "source_bound": disposition.startswith("materialized"),
        }
    return result


def verify_obs_semantics(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    if not actual.index.equals(expected.index) or list(actual.columns) != list(expected.columns):
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


def verify_var(var: pd.DataFrame) -> dict[str, Any]:
    stable = var["stable_feature_id"].astype("string")
    verdict = {
        "uid": EXPECTED_VAR["uid"],
        "rows": len(var),
        "human_ensg": int(stable.str.fullmatch(r"ENSG\d+", na=False).sum()),
        "mouse_ensmusg": int(stable.str.fullmatch(r"ENSMUSG\d+", na=False).sum()),
        "stable_feature_id_unique": bool(stable.is_unique),
        "needs_revision": False,
        "mismatch_count": 0,
    }
    if verdict != {
        "uid": "U8OeHI58YG9Y9Nsb0002",
        "rows": 60_736,
        "human_ensg": 32_738,
        "mouse_ensmusg": 27_998,
        "stable_feature_id_unique": True,
        "needs_revision": False,
        "mismatch_count": 0,
    }:
        raise AssertionError(f"accepted mixed-species VAR verdict drift: {verdict}")
    return verdict


def collection_snapshot(ln: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, uid, count in (
        ("pert-gym/additions/20260621", "kYkBznC2fuGmbUbg0000", 996),
        ("pert-gym/canonical/20260621", "aEWBxMlcWx2d7Cd80000", 1056),
    ):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matches = [{"uid": str(item.uid), "key": str(item.key)} for item in members if str(item.key) == f"{PREFIX}/obs.parquet"]
        if str(collection.uid) != uid or len(members) != count or len(matches) != 1:
            raise AssertionError(f"Collection membership drift: {key}")
        result[key] = {"uid": uid, "hash": str(collection.hash), "member_count": len(members), "target_key_matches": matches}
    return result


def verify_current(ln: Any, source: pd.DataFrame, source_summary: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    obs_artifact, history = latest_artifact(ln, f"{PREFIX}/obs.parquet")
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    if {"uid": str(x_artifact.uid), "hash": str(x_artifact.hash)} != EXPECTED_X:
        raise AssertionError("accepted X identity drift")
    if {"uid": str(var_artifact.uid), "hash": str(var_artifact.hash)} != EXPECTED_VAR:
        raise AssertionError("accepted VAR identity drift")
    if len(obs) != EXPECTED_N_OBS:
        raise AssertionError("OBS denominator drift")
    expected_obs, join_receipt = curate_obs(obs, source)
    var = var_artifact.load()
    var_verdict = verify_var(var)
    curated = str(obs_artifact.description).startswith(f"{TASK_ID}: source-exhaustive GSE207360 OBS") and obs_matches_expected_semantics(obs, expected_obs)
    return {
        "obs_before": artifact_identity(obs_artifact),
        "obs_history": [artifact_identity(item) for item in history],
        "x": artifact_identity(x_artifact),
        "x_semantics": {
            **EXPECTED_X,
            "shape": [EXPECTED_N_OBS, EXPECTED_N_VARS],
            "dtype": "float32",
            "stored_nonzero": 56_169_414,
            "minimum_stored_value": 1.0,
            "maximum_stored_value": 2250.0,
            "non_integral_stored_values": 0,
            "raw_counts": True,
            "obs_names_sha256": "65de59c0c005cbdbbdd3a08d6aba68efb7a0667bd12f7a393cd396a80eeb193b",
            "var_names_sha256": "aa592b3ef2d217646eb95395c0207af7ae0a42c7b27c363710b722982cc1ffb3",
        },
        "var": artifact_identity(var_artifact),
        "var_verdict": var_verdict,
        "source": {
            **SOURCE_SPEC,
            "metadata_rows": len(source),
            "metadata_columns": list(source.columns),
            "r_summary": source_summary,
            "manifest_sha256": sha256_file(SOURCE_MANIFEST_PATH),
        },
        "source_join": join_receipt,
        "canonical_field_dispositions": field_dispositions(obs if curated else expected_obs),
        "already_curated_obs": curated,
        "curated_obs": expected_obs,
        "obs_artifact": obs_artifact,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
    }, curated


def publish(ln: Any, result: dict[str, Any], helper_sha256: str) -> list[Any]:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-gse207360-publish-"))
    ln.track(
        key=f"pert-gym/real-dataset-curation/{REAL_DATASET_ID}/{TASK_ID}",
        kind="script",
        params={"task_id": TASK_ID, "helper_sha256": helper_sha256, "source_sha256": SOURCE_SPEC["sha256"]},
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
        description=f"{TASK_ID}: source-exhaustive GSE207360 OBS; exact 12,487-cell filtered-Seurat join, six murine cell types, WT/KO GSM and day-15/day-90 model semantics, complete 48-field dispositions; source sha256 {SOURCE_SPEC['sha256']}",
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
    print("PRODUCT_EXECUTION=" + canonical({"product_execution": {
        "host": os.uname().nodename,
        "pid": os.getpid(),
        "phase": phase,
        "payload_heartbeat_at": int(time.time()),
        "metric": "accepted_obs_datasets",
        "current": current,
        "denominator": 70,
        "unit": "biological_dataset",
    }}), flush=True)


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    helper_sha256 = sha256_file(Path(__file__))
    extractor_sha256 = sha256_file(R_EXTRACTOR)
    frozen = load_frozen_input_bindings()
    source_manifest = json.loads(SOURCE_MANIFEST_PATH.read_text())
    if source_manifest["real_dataset_id"] != REAL_DATASET_ID:
        raise AssertionError("source manifest identity drift")
    capacity = preflight()
    emit_product("preflight", 9)
    source, source_summary = load_source_metadata()
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin target")
    result, already_curated = verify_current(ln, source, source_summary)
    collections_before = collection_snapshot(ln)
    counts_before = {"artifacts": ln.Artifact.filter().count(), "collections": ln.Collection.filter().count()}
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
            stack.enter_context(lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity))
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            fresh, fresh_curated = verify_current(ln, source, source_summary)
            if fresh_curated:
                result, already_curated = fresh, True
            else:
                if {"uid": str(fresh["obs_artifact"].uid), "hash": str(fresh["obs_artifact"].hash)} != EXPECTED_OBS:
                    raise AssertionError("OBS predecessor changed after inspection; refusing race")
                result = fresh
                writes = publish(ln, fresh, helper_sha256)
    elif mode == "verify" and not already_curated:
        raise AssertionError("verify requested before exact OBS successor exists")
    final, final_curated = verify_current(ln, source, source_summary)
    if mode in {"mutate", "verify"} and not final_curated:
        raise AssertionError("terminal OBS readback failed")
    collections_after = collection_snapshot(ln)
    if collections_after != collections_before:
        raise AssertionError("Collection drift")
    counts_after = {"artifacts": ln.Artifact.filter().count(), "collections": ln.Collection.filter().count()}
    receipt = {
        "format": "pert-gym.real-dataset-curation-v2/v1",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "extractor_sha256": extractor_sha256,
        "source_manifest_sha256": sha256_file(SOURCE_MANIFEST_PATH),
        "frozen_inputs": frozen["inputs"],
        "source_denominator": {
            "biological_datasets": 1,
            "logical_families": 1,
            "physical_members": 1,
            "observations": EXPECTED_N_OBS,
            "features": EXPECTED_N_VARS,
            "geo_scrna_samples": 2,
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
    emit_product("checkpointing", 10 if mode in {"mutate", "verify"} else 9)
    print("GSE207360_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
