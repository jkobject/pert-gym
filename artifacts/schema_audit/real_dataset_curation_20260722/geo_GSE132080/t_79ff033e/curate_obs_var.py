#!/usr/bin/env python3
"""Append-only source-exhaustive OBS+VAR curation for GEO GSE132080."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import re
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
from pandas.testing import assert_frame_equal

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_79ff033e"
REAL_DATASET_ID = "geo/GSE132080"
PREFIX = "prism_collection/GSE132080"
EXPECTED_N_OBS = 23_608
EXPECTED_SOURCE_N_OBS = 23_633
EXPECTED_N_VARS = 33_694
EXPECTED_X = {"uid": "NEbod0p6ws0H5wug0000", "hash": "gbMxw1JnmmLnKzWKTWdC_V"}
FROZEN_INPUT_BINDINGS_PATH = Path(__file__).with_name("frozen_inputs") / "bindings.json"
SOURCE_ROOT = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE132nnn/GSE132080/suppl"
SOURCE_SPECS = {
    "GSE132080_10X_barcodes.tsv.gz": {
        "size": 94_141,
        "sha256": "5ac798d460d21a0ca190f3c11d952ea98fa15ec2f4b2a0ec6d1cde2e7202aa65",
        "last_modified": "Mon, 03 Jun 2019 11:11:26 GMT",
    },
    "GSE132080_10X_genes.tsv.gz": {
        "size": 264_786,
        "sha256": "cb459e6f14a8008e68f2ae535ac5411b6651d278355e9b517ff6a7e43587e603",
        "last_modified": "Mon, 03 Jun 2019 11:11:26 GMT",
    },
    "GSE132080_10X_matrix.mtx.gz": {
        "size": 352_247_428,
        "sha256": "7c908fbb76feaccac285209bf2578970873f8b8ec819e2a88ac6b83e3553239d",
        "last_modified": "Mon, 03 Jun 2019 11:12:49 GMT",
    },
    "GSE132080_cell_identities.csv.gz": {
        "size": 366_158,
        "sha256": "db9a2bab685004537fccd7aa2b5e19c6c1fc782eca9a4eb1d9275ffe2a19b0a0",
        "last_modified": "Mon, 03 Jun 2019 11:12:49 GMT",
    },
    "GSE132080_sgRNA_barcode_sequences_and_phenotypes.csv.gz": {
        "size": 4_463,
        "sha256": "9ddc95fe2b668fe2bad0acd74c9c65daac6d88b4b414c41a363f240fadb2b505",
        "last_modified": "Thu, 27 Jun 2019 17:07:28 GMT",
    },
}
SAMPLE_BY_GEMGROUP = {1: "GSM3842207", 2: "GSM3842208", 3: "GSM3842209"}

CANONICAL_OBS_FIELDS = (
    "dataset", "sample", "cell_id", "donor_id", "batch", "cell_type", "cell_line",
    "disease", "tissue_type", "organism", "sex", "age", "ethnicity", "sequencer",
    "technology", "assay", "modality", "media", "is_bulk", "is_pseudobulk",
    "perturbation", "perturbation_type", "perturbation_technology",
    "perturbation_library", "guide_id", "guide_sequence", "perturbation_target",
    "perturbation_target_id", "is_control", "dose", "dose_unit", "timepoint",
    "trajectory_id", "pseudotime", "is_baseline", "sensitivity", "response_metric",
    "response_value", "response_source", "n_counts", "n_genes", "pct_mito", "pct_ribo",
    "is_low_quality", "source", "source_accession", "control_availability", "x_semantics",
)
NOT_APPLICABLE_FIELDS = {
    "dose", "dose_unit", "trajectory_id", "pseudotime", "sensitivity",
    "response_metric", "response_value", "response_source",
}
UNKNOWN_FIELDS = {
    "donor_id", "sex", "age", "ethnicity", "perturbation_target_id", "is_baseline",
    "n_counts", "n_genes", "pct_mito", "pct_ribo",
}


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
        if len(raw) != entry["uncompressed_bytes"] or sha256_bytes(raw) != entry["uncompressed_sha256"]:
            raise AssertionError("frozen input identity drift")
    if len(manifest["inputs"]) != 2:
        raise AssertionError("frozen input coverage drift")
    return manifest


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid), "key": str(artifact.key), "hash": str(artifact.hash),
        "version": str(artifact.version), "size": int(artifact.size),
        "n_observations": getattr(artifact, "n_observations", None),
        "created_at": str(artifact.created_at), "description": str(artifact.description),
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


def parse_headers(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        if key in {"last-modified", "content-length", "etag"}:
            result[key] = value.strip()
    return result


def download_sources() -> tuple[dict[str, Path], dict[str, Any]]:
    root = Path(tempfile.gettempdir()) / f"{TASK_ID}-gse132080-sources"
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    receipts: dict[str, Any] = {}
    for name, spec in SOURCE_SPECS.items():
        url = f"{SOURCE_ROOT}/{name}"
        head = subprocess.run(
            ["curl", "--silent", "--show-error", "--location", "--fail", "--head", url],
            check=True, capture_output=True, text=True, timeout=120,
        )
        headers = parse_headers(head.stdout)
        if headers.get("content-length") != str(spec["size"]) or headers.get("last-modified") != spec["last_modified"]:
            raise AssertionError(f"source HTTP identity drift: {name}={headers}")
        path = root / name
        if not path.exists() or path.stat().st_size != spec["size"]:
            subprocess.run(
                ["curl", "--silent", "--show-error", "--location", "--fail", "--retry", "3", "--output", str(path), url],
                check=True, timeout=3600,
            )
        if path.stat().st_size != spec["size"] or sha256_file(path) != spec["sha256"]:
            raise AssertionError(f"source payload identity drift: {name}")
        paths[name] = path
        receipts[name] = {"url": url, **spec, "headers": headers}
    return paths, receipts


def read_matrix_header(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt") as handle:
        banner = handle.readline().strip()
        while True:
            line = handle.readline().strip()
            if not line.startswith("%"):
                break
    rows, columns, entries = map(int, line.split())
    if (rows, columns) != (EXPECTED_N_VARS, EXPECTED_SOURCE_N_OBS):
        raise AssertionError("source MatrixMarket shape drift")
    return {"banner": banner, "rows": rows, "columns": columns, "entries": entries}


def load_sources() -> dict[str, Any]:
    paths, receipts = download_sources()
    barcodes = pd.Index(
        pd.read_csv(paths["GSE132080_10X_barcodes.tsv.gz"], sep="\t", header=None, dtype="string").iloc[:, 0].astype(str)
    )
    genes = pd.read_csv(paths["GSE132080_10X_genes.tsv.gz"], sep="\t", header=None, dtype="string")
    genes.columns = ["ensembl_gene_id", "gene_symbol"]
    identities = pd.read_csv(paths["GSE132080_cell_identities.csv.gz"], index_col=0)
    identities.index = identities.index.astype(str)
    guides = pd.read_csv(paths["GSE132080_sgRNA_barcode_sequences_and_phenotypes.csv.gz"])
    matrix = read_matrix_header(paths["GSE132080_10X_matrix.mtx.gz"])
    if len(barcodes) != EXPECTED_SOURCE_N_OBS or not barcodes.is_unique:
        raise AssertionError("source barcode denominator/uniqueness drift")
    if len(genes) != EXPECTED_N_VARS or not genes["ensembl_gene_id"].is_unique:
        raise AssertionError("source gene denominator/Ensembl uniqueness drift")
    if len(identities) != EXPECTED_N_OBS or not identities.index.is_unique:
        raise AssertionError("source identity denominator/uniqueness drift")
    excluded = barcodes.difference(identities.index)
    if len(excluded) != 25 or len(identities.index.difference(barcodes)):
        raise AssertionError("source barcode-to-identity accounting drift")
    if len(guides) != 128 or not guides["sgRNA_name"].is_unique or not guides["sequence"].astype(str).str.fullmatch(r"[ACGT]{20}").all():
        raise AssertionError("source sgRNA phenotype table drift")
    return {
        "barcodes": barcodes, "genes": genes, "identities": identities,
        "guides": guides, "matrix": matrix, "receipts": receipts,
        "excluded_barcodes": excluded,
    }


def exact_source_join(obs: pd.DataFrame, identities: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if "original_obs_index" not in obs:
        raise AssertionError("current OBS lacks original_obs_index")
    keys = pd.Index(obs["original_obs_index"].astype(str))
    if not keys.is_unique or set(keys) != set(identities.index):
        raise AssertionError("source/current OBS identity set drift")
    joined = identities.reindex(keys)
    joined.index = obs.index
    mismatches: dict[str, int] = {}
    for column in identities.columns:
        if column not in obs:
            continue
        left = joined[column].astype("string")
        right = obs[column].astype("string")
        equal = (left.isna() & right.isna()) | left.fillna("").eq(right.fillna(""))
        mismatches[str(column)] = int((~equal).sum())
    if any(mismatches.values()):
        raise AssertionError(f"source OBS semantic mismatch: {mismatches}")
    return joined, {
        "source_rows": len(identities), "current_rows": len(obs),
        "identity_set_match": True, "join_mismatch_count": sum(mismatches.values()),
        "column_mismatches": mismatches,
        "joined_order_sha256": ordered_values_sha256(keys),
        "join_semantics": "set-equal cell identity then reindex source cell_identities.csv to original_obs_index order",
    }


def source_guide_name(guide_identity: str) -> str | None:
    if guide_identity.startswith("neg_ctrl_"):
        return None
    first, separator, remainder = guide_identity.partition("_")
    if not separator or not remainder.startswith(first + "_"):
        raise AssertionError(f"unexpected guide identity syntax: {guide_identity}")
    return remainder


def guide_mapping(guides: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {str(row["sgRNA_name"]): row.to_dict() for _, row in guides.iterrows()}


def set_field(frame: pd.DataFrame, field: str, values: Any, state: Any, source: str) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing_series(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def curate_obs(obs: pd.DataFrame, source: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    original = obs.copy(deep=True)
    joined, join_receipt = exact_source_join(obs, source["identities"])
    mapping = guide_mapping(source["guides"])
    source_names = joined["guide_identity"].astype(str).map(source_guide_name)
    mapped_rows = [mapping.get(name) if name is not None else None for name in source_names]
    noncontrol_unmapped = sum(name is not None and row is None for name, row in zip(source_names, mapped_rows, strict=True))
    if noncontrol_unmapped:
        raise AssertionError(f"unmapped non-control guides: {noncontrol_unmapped}")

    curated = obs.copy(deep=True)
    for field in ("dataset", "cell_line", "disease", "tissue_type", "organism", "is_control", "perturbation", "perturbation_type", "assay", "modality"):
        if field in original:
            curated[f"source_original_{field}"] = original[field]
    gemgroup = pd.to_numeric(joined["gemgroup"], errors="raise").astype(int)
    samples = gemgroup.map(SAMPLE_BY_GEMGROUP).astype("string")
    if samples.isna().any():
        raise AssertionError("unknown gemgroup")
    guide_id = joined["guide_identity"].astype("string")
    sequence = pd.Series(
        [pd.NA if row is None else row["sequence"] for row in mapped_rows],
        index=curated.index, dtype="string",
    )
    target = original["perturbation"].astype("string")
    controls = original["is_control"].astype("boolean")
    expected_controls = guide_id.str.startswith("neg_ctrl_")
    if not controls.astype(bool).equals(expected_controls.astype(bool)):
        raise AssertionError("source control semantics drift")

    set_field(curated, "dataset", PREFIX, "known", "canonical logical family")
    set_field(curated, "sample", samples, "known", "GEO GSM by source gemgroup")
    set_field(curated, "cell_id", original["original_obs_index"].astype("string"), "known", "GEO 10X barcode")
    set_field(curated, "batch", "gemgroup_" + gemgroup.astype(str), "known", "source cell identities gemgroup")
    set_field(curated, "cell_type", "K-562 myeloid leukemia cell", "known", "GEO sample source")
    set_field(curated, "cell_line", "K-562", "known", "GEO sample characteristic")
    set_field(curated, "disease", "chronic myelogenous leukemia", "known", "GEO sample characteristic")
    set_field(curated, "tissue_type", "cell culture", "known", "GEO experimental design")
    set_field(curated, "organism", "Homo sapiens", "known", "GEO GSE132080")
    set_field(curated, "sequencer", "Illumina HiSeq 4000", "known", "GEO platform GPL20301")
    set_field(curated, "technology", "10x Genomics Chromium Single Cell 3' v2", "known", "GEO extraction protocol")
    set_field(curated, "assay", "Perturb-seq", "known", "GEO overall design")
    set_field(curated, "modality", "scRNA-seq", "known", "GEO experiment type")
    set_field(curated, "media", "RPMI + FBS + Pen/Strep/Gln", "known", "GEO growth protocol")
    set_field(curated, "is_bulk", False, "known", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "known", "single-cell source")
    set_field(curated, "perturbation", target, "known", "source cell identity and sgRNA phenotype table")
    set_field(curated, "perturbation_type", "CRISPRi", "known", "GEO experimental design")
    set_field(curated, "perturbation_technology", "CRISPR interference", "known", "GEO experimental design")
    set_field(curated, "perturbation_library", "Jost et al. attenuated sgRNA allelic series", "known", "GEO publication")
    set_field(curated, "guide_id", guide_id, "known", "source cell identities")
    set_field(curated, "guide_sequence", sequence, np.where(sequence.notna(), "known", "unknown"), "source sgRNA phenotype table; control sequences not supplied")
    set_field(curated, "perturbation_target", target, "known", "source cell identity and sgRNA phenotype table")
    set_field(curated, "is_control", controls, "known", "source non-targeting guide identity")
    set_field(curated, "timepoint", 7_200.0, "known", "GEO growth protocol: FACS day 3 plus 2 additional days")
    set_field(curated, "is_low_quality", ~joined["good_coverage"].astype(bool), "known", "inverse source good_coverage flag")
    set_field(curated, "source", "GEO", "known", "processed source authority")
    set_field(curated, "source_accession", "GSE132080", "known", "GEO series")
    set_field(curated, "control_availability", "strict_control_available", "known", "source non-targeting guides")
    set_field(curated, "x_semantics", "raw_counts", "known", "GEO Cell Ranger UMI matrix description")

    phenotype_columns = ("gamma_day5", "gamma_day10", "relative_activity_day5", "relative_activity_day10")
    for field in phenotype_columns:
        curated[f"source_guide_{field}"] = pd.Series(
            [pd.NA if row is None else row[field] for row in mapped_rows], index=curated.index, dtype="Float64"
        )
    for field in NOT_APPLICABLE_FIELDS:
        dtype = "Float64" if field in {"dose", "pseudotime", "response_value"} else "string"
        set_field(curated, field, missing_series(curated.index, dtype), "not_applicable", "dataset design")
    for field in UNKNOWN_FIELDS:
        dtype = "Float64" if field in {"age", "n_counts", "n_genes", "pct_mito", "pct_ribo"} else "string"
        set_field(curated, field, missing_series(curated.index, dtype), "unknown", "source-exhaustive search found no defensible row value")
    if len(curated) != EXPECTED_N_OBS or not curated.index.equals(original.index):
        raise AssertionError("OBS row count/order drift")
    if not curated["obs_uuid"].is_unique or not curated["original_obs_index"].is_unique:
        raise AssertionError("OBS identity uniqueness drift")
    return curated, {
        **join_receipt,
        "source_barcode_rows": len(source["barcodes"]),
        "source_identity_rows": len(source["identities"]),
        "excluded_unassigned_barcodes": len(source["excluded_barcodes"]),
        "guide_table_rows": len(source["guides"]),
        "guide_sequence_known_rows": int(sequence.notna().sum()),
        "guide_sequence_unknown_control_rows": int(sequence.isna().sum()),
        "control_rows": int(controls.sum()),
    }


def verify_obs_semantics(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    if not actual.index.equals(expected.index) or list(actual.columns) != list(expected.columns):
        raise AssertionError("OBS schema/order mismatch")
    try:
        assert_frame_equal(actual, expected, check_categorical=True)
    except AssertionError as error:
        raise AssertionError("OBS source semantic mismatch") from error


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
            "disposition": disposition, "materialized": True,
            "known_rows": known, "unknown_rows": len(frame) - known,
            "source_bound": disposition.startswith("materialized"),
        }
    return result


def curate_var(var: pd.DataFrame, genes: pd.DataFrame) -> pd.DataFrame:
    original = var.copy(deep=True)
    stable = var["stable_feature_id"].astype("string")
    source_stable = genes["ensembl_gene_id"].astype("string").set_axis(var.index)
    source_symbols = genes["gene_symbol"].astype("string").set_axis(var.index)
    if not stable.equals(source_stable):
        raise AssertionError("VAR stable IDs differ from GEO gene axis")
    if not var.index.astype(str).equals(pd.Index(source_symbols.astype(str))):
        raise AssertionError("VAR symbols differ from GEO gene axis")
    curated = var.copy(deep=True)
    curated["stable_feature_id_namespace"] = "Ensembl stable gene ID"
    curated["organism"] = "Homo sapiens"
    curated["feature_index"] = stable
    curated["feature_index_source"] = "GEO 10X genes.tsv Ensembl column"
    assert_frame_equal(curated.loc[:, original.columns], original)
    if not curated.index.equals(original.index):
        raise AssertionError("VAR row order drift")
    return curated


def verify_var(var: pd.DataFrame, genes: pd.DataFrame, x_axis: pd.Index) -> dict[str, Any]:
    required = {"stable_feature_id", "stable_feature_id_namespace", "stable_feature_id_mapping_status", "organism", "feature_index", "feature_index_source"}
    if required - set(var.columns):
        raise AssertionError(f"VAR contract absent: {sorted(required - set(var.columns))}")
    if len(var) != EXPECTED_N_VARS or not var.index.astype(str).equals(x_axis.astype(str)):
        raise AssertionError("VAR/X feature-axis count/order drift")
    stable = var["stable_feature_id"].astype("string")
    source_stable = genes["ensembl_gene_id"].astype("string").set_axis(var.index)
    if not stable.equals(source_stable) or not stable.str.fullmatch(r"ENSG\d{11}", na=False).all() or not stable.is_unique:
        raise AssertionError("VAR exact unique human ENSG contract drift")
    if not var["feature_index"].astype("string").equals(stable):
        raise AssertionError("VAR feature_index drift")
    if not var["stable_feature_id_namespace"].astype("string").eq("Ensembl stable gene ID").all():
        raise AssertionError("VAR namespace drift")
    if not var["organism"].astype("string").eq("Homo sapiens").all():
        raise AssertionError("VAR organism drift")
    index_unique = bool(var.index.is_unique)
    return {
        "rows": len(var), "stable_feature_id_unique": True,
        "index_unique": index_unique, "duplicate_index_rows": int(var.index.duplicated(keep=False).sum()),
        "stable_id_or_index_uniqueness": bool(stable.is_unique or index_unique),
        "axis_count_parity": True, "axis_order_parity": True,
        "axis_order_sha256": ordered_values_sha256(var.index),
        "stable_id_order_sha256": ordered_values_sha256(pd.Index(stable)),
        "organism_values": ["Homo sapiens"], "needs_revision": False, "mismatch_count": 0,
    }


def x_axis(artifact: Any) -> tuple[pd.Index, dict[str, Any]]:
    if {"uid": str(artifact.uid), "hash": str(artifact.hash)} != EXPECTED_X:
        raise AssertionError("accepted X identity drift")
    path = Path(artifact.cache())
    backed = ad.read_h5ad(path, backed="r")
    if (backed.n_obs, backed.n_vars) != (EXPECTED_N_OBS, EXPECTED_N_VARS):
        raise AssertionError("X shape drift")
    axis = backed.var_names.astype(str).copy()
    receipt = {
        **EXPECTED_X, "shape": [backed.n_obs, backed.n_vars],
        "var_names_sha256": ordered_values_sha256(axis), "backed_only": True,
    }
    backed.file.close()
    return axis, receipt


def collection_snapshot(ln: Any) -> dict[str, Any]:
    snapshots: dict[str, Any] = {"historical_manifest_identity": "jkobject:GCjqQtGwPzkY"}
    for key in ("pert-gym/additions/20260621", "pert-gym/canonical/20260621"):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matches = [{"uid": str(item.uid), "key": str(item.key)} for item in members if str(item.key) == f"{PREFIX}/obs.parquet"]
        if len(matches) != 1:
            raise AssertionError(f"target Collection membership drift: {key}")
        snapshots[key] = {"uid": str(collection.uid), "hash": str(collection.hash), "member_count": len(members), "target_key_matches": matches}
    return snapshots


def verify_current(ln: Any, source: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    obs_artifact, obs_history = latest_artifact(ln, f"{PREFIX}/obs.parquet")
    obs = obs_artifact.load()
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    x_var_axis, x_receipt = x_axis(x_artifact)
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    var = var_artifact.load()
    curated_obs, join_receipt = curate_obs(obs, source)
    curated_var = curate_var(var, source["genes"])
    obs_curated = str(obs_artifact.description).startswith(f"{TASK_ID}: source-exhaustive GSE132080 OBS")
    var_curated = str(var_artifact.description).startswith(f"{TASK_ID}: GSE132080 human VAR")
    if obs_curated:
        verify_obs_semantics(obs, curated_obs)
    if var_curated:
        var_verdict = verify_var(var, source["genes"], x_var_axis)
    else:
        var_verdict = {
            "needs_revision": True,
            "missing_columns": sorted({"stable_feature_id_namespace", "organism", "feature_index", "feature_index_source"} - set(var.columns)),
            "stable_feature_id_unique": bool(var["stable_feature_id"].astype("string").is_unique),
            "index_unique": bool(var.index.is_unique),
        }
    return {
        "obs_before": artifact_identity(obs_artifact), "obs_history_count": len(obs_history),
        "x": artifact_identity(x_artifact), "x_axis": x_receipt,
        "var_before": artifact_identity(var_artifact), "rows": len(obs),
        "source_manifest": source["receipts"], "source_matrix": source["matrix"],
        "source_join": join_receipt,
        "source_denominator_accounting": {
            "matrix_barcodes": len(source["barcodes"]), "assigned_cell_identities": len(source["identities"]),
            "excluded_unassigned_barcodes": len(source["excluded_barcodes"]),
            "features": len(source["genes"]), "sgRNA_phenotypes": len(source["guides"]),
        },
        "canonical_field_dispositions": field_dispositions(obs if obs_curated else curated_obs),
        "var_verdict": var_verdict, "already_curated_obs": obs_curated,
        "already_curated_var": var_curated, "curated_obs": curated_obs, "curated_var": curated_var,
        "obs_artifact": obs_artifact, "x_artifact": x_artifact, "var_artifact": var_artifact,
        "x_var_axis": x_var_axis,
    }, obs_curated and var_curated


def publish(ln: Any, result: dict[str, Any], helper_sha256: str) -> dict[str, list[Any]]:
    writes: dict[str, list[Any]] = {"obs": [], "var": []}
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-gse132080-publish-"))
    ln.track(
        key=f"pert-gym/real-dataset-curation/{REAL_DATASET_ID}/{TASK_ID}", kind="script",
        params={"task_id": TASK_ID, "helper_sha256": helper_sha256}, new_run=True,
        pypackages=False, stream_tracking=False,
    )
    if not result["already_curated_var"]:
        path = root / "var.parquet"
        result["curated_var"].to_parquet(path)
        var = ln.Artifact.from_dataframe(
            path, key=f"{PREFIX}/var.parquet", revises=result["var_artifact"],
            description=f"{TASK_ID}: GSE132080 human VAR; preserves 33694-feature X order and duplicate source symbols; binds exact unique ENSG feature_index, namespace and organism",
        ).save()
        result["x_artifact"].features.set_values({"var": var})
        writes["var"].append(var)
    if not result["already_curated_obs"]:
        path = root / "obs.parquet"
        result["curated_obs"].to_parquet(path)
        obs = ln.Artifact.from_dataframe(
            path, key=f"{PREFIX}/obs.parquet", revises=result["obs_artifact"],
            description=f"{TASK_ID}: source-exhaustive GSE132080 OBS; exact 23608-cell identity join, 25 unassigned source barcodes excluded, 128-guide phenotype table mapped",
        ).save()
        obs.features.set_values({"X": result["x_artifact"]})
        writes["obs"].append(obs)
    try:
        ln.finish()
    except AttributeError:
        ln.context.finish()
    return writes


def strip_runtime(result: dict[str, Any]) -> dict[str, Any]:
    hidden = {"curated_obs", "curated_var", "obs_artifact", "x_artifact", "var_artifact", "x_var_axis"}
    return {key: value for key, value in result.items() if key not in hidden}


def emit_product(phase: str, current: int) -> None:
    print("PRODUCT_EXECUTION=" + canonical({"product_execution": {
        "host": os.uname().nodename, "pid": os.getpid(), "phase": phase,
        "payload_heartbeat_at": int(time.time()), "metric": "real_dataset_obs_var",
        "current": current, "denominator": 1, "unit": "biological_dataset",
    }}), flush=True)


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    helper_sha256 = sha256_file(Path(__file__))
    frozen = load_frozen_input_bindings()
    capacity = preflight()
    emit_product("preflight", 0)
    source = load_sources()
    ln = connect_pertdata()
    if ln.setup.settings.instance.slug != "laminlabs/pertdata" or ln.setup.settings.branch.name != "jkobject":
        raise AssertionError("wrong Lamin target")
    result, all_curated = verify_current(ln, source)
    collections_before = collection_snapshot(ln)
    counts_before = {"artifacts": ln.Artifact.filter().count(), "collections": ln.Collection.filter().count()}
    writes: dict[str, list[Any]] = {"obs": [], "var": []}
    if mode == "mutate" and not all_curated:
        metadata = {
            "run_id": TASK_ID, "pid": os.getpid(), "host": capacity.hostname,
            "project": capacity.project, "zone": capacity.zone,
            "branch": ln.setup.settings.branch.name, "started_at": time.time(),
        }
        with ExitStack() as stack:
            stack.enter_context(lamin_writer_lease(run_id=TASK_ID, preflight_result=capacity))
            stack.enter_context(distributed_lamin_writer_lease(metadata))
            fresh, fresh_all = verify_current(ln, source)
            if fresh_all:
                result, all_curated = fresh, True
            else:
                result = fresh
                writes = publish(ln, fresh, helper_sha256)
    elif mode == "verify" and not all_curated:
        raise AssertionError("verify requested before exact OBS+VAR revisions exist")
    final, final_all = verify_current(ln, source)
    if mode in {"mutate", "verify"} and not final_all:
        raise AssertionError("terminal OBS+VAR readback failed")
    collections_after = collection_snapshot(ln)
    if collections_after != collections_before:
        raise AssertionError("Collection drift")
    counts_after = {"artifacts": ln.Artifact.filter().count(), "collections": ln.Collection.filter().count()}
    receipt = {
        "format": "pert-gym.real-dataset-obs-var-curation/v2", "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID, "dataset_id": PREFIX, "status": "PASS", "mode": mode,
        "helper_sha256": helper_sha256, "frozen_inputs": frozen["inputs"],
        "source_denominator": {"biological_datasets": 1, "logical_families": 1, "physical_members": 1, "source_matrix_observations": EXPECTED_SOURCE_N_OBS, "curated_observations": EXPECTED_N_OBS, "features": EXPECTED_N_VARS},
        "member_before": strip_runtime(result), "member_after": strip_runtime(final),
        "collections": collections_after,
        "writes": {
            "obs_revisions": len(writes["obs"]), "var_revisions": len(writes["var"]),
            "x_revisions": 0, "collection_writes": 0, "deletions": 0,
            "artifacts": {role: [artifact_identity(item) for item in items] for role, items in writes.items()},
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {"hostname": capacity.hostname, "available_memory_bytes": capacity.available_memory_bytes, "free_disk_bytes": capacity.free_disk_bytes},
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = sha256_bytes(canonical(receipt).encode())
    emit_product("checkpointing", 1)
    print("GSE132080_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
