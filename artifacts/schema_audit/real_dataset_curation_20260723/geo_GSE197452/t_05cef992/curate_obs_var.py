#!/usr/bin/env python3
"""Source-exhaustive, append-only GSE197452 OBS curation and verification."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
import tempfile
import time
import urllib.request
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import anndata as ad
import h5py
import numpy as np
import pandas as pd
from inspect_sources import (
    download_sources,
    feature_assignments,
    load_feature_table,
    matrix_market_header,
    ordered_sha256,
    read_gene_table,
    read_one_column,
    sha256_file,
)
from pandas.testing import assert_frame_equal
from scipy import sparse

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_05cef992"
REAL_DATASET_ID = "geo:GSE197452"
PREFIX = "prism_collection/GSE197452_Perturb-seq"
OBS_KEY = f"{PREFIX}/obs.parquet"
X_KEY = f"{PREFIX}/X.h5ad"
VAR_KEY = f"{PREFIX}/var.parquet"
EXPECTED_OBS_UID = "6UsaktwOJjkXPM3L0002"
EXPECTED_X_UID = "GYQBTGssvyua7wmc0000"
EXPECTED_X_HASH = "1fnOGlEvms7NaqTV_Bc76o"
EXPECTED_VAR_UID = "eJMdIf8H75RMWWK90001"
EXPECTED_N_OBS = 20_811
EXPECTED_N_VARS = 33_694
PREDECESSOR_COLLECTION_UID = "Ltjv1RYDCnuxqqRT0000"
PREDECESSOR_COLLECTION_KEY = "pert-gym/additions/20260721-drug-seq-gse120222-e2e"
PREDECESSOR_MEMBER_COUNT = 1_018
SUCCESSOR_COLLECTION_KEY = "pert-gym/additions/20260723-gse197452-e2e"
HERE = Path(__file__).resolve().parent
MANIFEST_PATH = HERE / "source_manifest.json"
JOURNAL_PATH = Path.home() / ".cache/pert-gym/curation_journal" / f"{TASK_ID}.json"

CANONICAL_OBS_FIELDS = (
    "dataset",
    "sample",
    "cell_id",
    "batch",
    "donor_id",
    "replicate",
    "plate_id",
    "well_id",
    "cell_type",
    "cell_line",
    "tissue_type",
    "organism",
    "disease",
    "age",
    "sex",
    "ethnicity",
    "sequencer",
    "technology",
    "assay",
    "modality",
    "media",
    "treatment",
    "perturbation",
    "perturbation_type",
    "perturbation_technology",
    "perturbation_library",
    "guide_id",
    "guide_sequence",
    "perturbation_target",
    "dose",
    "dose_unit",
    "timepoint",
    "timepoint_unit",
    "condition",
    "is_control",
    "control_availability",
    "n_counts",
    "n_genes",
    "pct_mito",
    "pct_ribo",
    "is_low_quality",
    "pseudotime",
    "response_value",
    "response_type",
)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def frame_sha256(frame: pd.DataFrame) -> str:
    payload = pd.util.hash_pandas_object(frame, index=True).values.tobytes()
    schema = canonical(
        [(str(column), str(dtype)) for column, dtype in frame.dtypes.items()]
    )
    return sha256_bytes(schema.encode() + payload)


def write_journal(phase: str, **extra: Any) -> None:
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "format": "pert-gym.gse197452-curation-journal/v1",
        "task_id": TASK_ID,
        "phase": phase,
        "updated_at": int(time.time()),
        **extra,
    }
    temp = JOURNAL_PATH.with_suffix(".tmp")
    temp.write_text(canonical(entry) + "\n", encoding="utf-8")
    temp.replace(JOURNAL_PATH)


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def missing(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def latest_artifact(ln: Any, key: str) -> tuple[Any, list[Any]]:
    records = list(ln.Artifact.filter(key=key).all())
    records.sort(key=lambda item: (str(item.created_at), str(item.uid)))
    if not records:
        raise AssertionError(f"artifact absent: {key}")
    return records[-1], records


def artifact_by_uid(ln: Any, uid: str) -> Any:
    records = [
        record for record in ln.Artifact.filter(uid=uid).all() if str(record.uid) == uid
    ]
    if len(records) != 1:
        raise AssertionError(f"artifact identity drift: {uid}")
    return records[0]


def collection_by_uid(ln: Any, uid: str) -> Any:
    records = [
        record
        for record in ln.Collection.filter(uid=uid).all()
        if str(record.uid) == uid
    ]
    if len(records) != 1:
        raise AssertionError(f"Collection identity drift: {uid}")
    return records[0]


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "size": int(artifact.size) if artifact.size is not None else None,
        "created_at": str(artifact.created_at),
    }


def member_identity(members: list[Any]) -> list[dict[str, str]]:
    return sorted(
        ({"uid": str(item.uid), "key": str(item.key)} for item in members),
        key=lambda item: (item["key"], item["uid"]),
    )


def membership_sha256(members: list[Any]) -> str:
    return sha256_bytes(canonical(member_identity(members)).encode())


def verify_frozen_manifest() -> dict[str, Any]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if (
        manifest["task_id"] != TASK_ID
        or manifest["real_dataset_id"] != REAL_DATASET_ID
        or manifest["dataset_id"] != PREFIX
    ):
        raise AssertionError("source manifest identity drift")
    frozen = manifest["frozen_lamin_inputs"]
    expected = {
        "obs": EXPECTED_OBS_UID,
        "x": EXPECTED_X_UID,
        "var": EXPECTED_VAR_UID,
        "additions_predecessor": PREDECESSOR_COLLECTION_UID,
    }
    for role, uid in expected.items():
        if frozen[role]["uid"] != uid:
            raise AssertionError(f"frozen {role} identity drift")
    return manifest


def verify_small_authorities(manifest: dict[str, Any]) -> dict[str, Any]:
    receipts: dict[str, Any] = {}
    authorities = manifest["source_authorities"]
    sources = {
        "GSE197452_family.soft.gz": authorities["geo"]["family_soft"],
        "PMC9931582_fullTextXML": authorities["publication"]["full_text_xml"],
    }
    for name, expected in sources.items():
        payload = urllib.request.urlopen(expected["url"], timeout=120).read()
        actual = {
            "url": expected["url"],
            "size": len(payload),
            "sha256": sha256_bytes(payload),
        }
        if actual["size"] != expected["size"] or actual["sha256"] != expected["sha256"]:
            raise AssertionError(f"authority identity drift: {name}")
        receipts[name] = actual
    return receipts


def load_sources(manifest: dict[str, Any]) -> dict[str, Any]:
    paths, receipts = download_sources()
    for name, expected in manifest["payloads"].items():
        actual = receipts[name]
        if actual["size"] != expected["size"] or actual["sha256"] != expected["sha256"]:
            raise AssertionError(f"source payload identity drift: {name}")
    feature_table, feature_receipt = load_feature_table()
    expected_table = manifest["source_authorities"]["publication"][
        "supplementary_table_4"
    ]
    if (
        feature_receipt["member_sha256"] != expected_table["member_sha256"]
        or feature_receipt["member_size"] != expected_table["member_size"]
        or feature_receipt["rows"] != expected_table["rows"]
    ):
        raise AssertionError("Supplementary Table 4 drift")
    cells_ill = read_one_column(paths["GSM6297384_cells_counts_Pert_Ill.txt.gz"])
    cells_ult = read_one_column(paths["GSM6297385_cells_counts_Pert_Ult.txt.gz"])
    genes_ill = read_gene_table(paths["GSM6297384_genes_counts_Pert_Ill.txt.gz"])
    genes_ult = read_gene_table(paths["GSM6297385_genes_counts_Pert_Ult.txt.gz"])
    matrix_ill = matrix_market_header(
        paths["GSM6297384_expression_counts_Pert_Ill.txt.gz"]
    )
    matrix_ult = matrix_market_header(
        paths["GSM6297385_expression_counts_Pert_Ult.txt.gz"]
    )
    if (
        matrix_ill["rows"] != EXPECTED_N_VARS
        or matrix_ill["columns"] != EXPECTED_N_OBS
        or matrix_ult["rows"] != EXPECTED_N_VARS
        or matrix_ult["columns"] != 20_936
        or not genes_ill.equals(genes_ult)
    ):
        raise AssertionError("source expression denominator drift")
    return {
        "paths": paths,
        "receipts": receipts,
        "feature_table": feature_table,
        "feature_receipt": feature_receipt,
        "cells_ill": cells_ill,
        "cells_ult": cells_ult,
        "genes": genes_ill,
        "matrix_ill": matrix_ill,
        "matrix_ult": matrix_ult,
        "authority_receipts": verify_small_authorities(manifest),
    }


def normalized_csr(matrix: Any) -> sparse.csr_matrix:
    result = sparse.csr_matrix(matrix, copy=True)
    result.sum_duplicates()
    result.eliminate_zeros()
    result.sort_indices()
    return result


def accepted_symbol_axis_matches(
    source_names: pd.Index, stable_ids: pd.Index, accepted_names: pd.Index
) -> bool:
    if not (len(source_names) == len(stable_ids) == len(accepted_names)):
        return False
    return all(
        accepted == source or accepted == f"{source}_{stable}"
        for source, stable, accepted in zip(
            source_names, stable_ids, accepted_names, strict=True
        )
    )


def matrix_and_qc(
    x_artifact: Any, var: pd.DataFrame, source: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    h5_path = source["paths"]["GSM6297388_filtered_feature_bc_matrix.pert.ill.h5"]
    with h5py.File(h5_path, "r") as handle:
        matrix = handle["matrix"]
        shape = tuple(int(value) for value in matrix["shape"][:])
        features = matrix["features"]
        feature_types = pd.Index(
            [
                item.decode() if isinstance(item, bytes) else str(item)
                for item in features["feature_type"][:]
            ]
        )
        feature_ids = pd.Index(
            [
                item.decode() if isinstance(item, bytes) else str(item)
                for item in features["id"][:]
            ]
        )
        feature_names = pd.Index(
            [
                item.decode() if isinstance(item, bytes) else str(item)
                for item in features["name"][:]
            ]
        )
        barcodes = pd.Index(
            [
                item.decode() if isinstance(item, bytes) else str(item)
                for item in matrix["barcodes"][:]
            ]
        )
        full = sparse.csc_matrix(
            (matrix["data"][:], matrix["indices"][:], matrix["indptr"][:]),
            shape=shape,
        )
    if shape != (39_849, EXPECTED_N_OBS):
        raise AssertionError("source feature-barcode shape drift")
    gene_positions = np.flatnonzero(feature_types == "Gene Expression")
    if not np.array_equal(gene_positions, np.arange(EXPECTED_N_VARS)):
        raise AssertionError("source gene feature positions drift")
    source_matrix = normalized_csr(full[:EXPECTED_N_VARS, :].transpose())
    stable = var["stable_feature_id"].astype(str)
    accepted_names = pd.Index(var.index.astype(str))
    if (
        not feature_ids[:EXPECTED_N_VARS].equals(pd.Index(stable))
        or not accepted_symbol_axis_matches(
            feature_names[:EXPECTED_N_VARS],
            feature_ids[:EXPECTED_N_VARS],
            accepted_names,
        )
        or not barcodes.equals(pd.Index(source["accepted_barcodes"]))
    ):
        raise AssertionError("source/X/VAR axis drift")
    x_path = Path(x_artifact.cache())
    adata = ad.read_h5ad(x_path)
    try:
        accepted = normalized_csr(adata.X)
        if adata.shape != (EXPECTED_N_OBS, EXPECTED_N_VARS):
            raise AssertionError("accepted X shape drift")
        if not pd.Index(adata.obs_names.astype(str)).equals(barcodes):
            raise AssertionError("accepted X observation axis drift")
        if not pd.Index(adata.var_names.astype(str)).equals(accepted_names):
            raise AssertionError("accepted X feature axis drift")
        if (
            not np.array_equal(source_matrix.indptr, accepted.indptr)
            or not np.array_equal(source_matrix.indices, accepted.indices)
            or not np.array_equal(source_matrix.data, accepted.data)
        ):
            raise AssertionError("accepted X values differ from source Illumina matrix")
    finally:
        del adata
    counts = np.asarray(source_matrix.sum(axis=1)).ravel().astype(np.int64)
    detected = source_matrix.getnnz(axis=1).astype(np.int64)
    mito = np.asarray(
        source_matrix[:, feature_names[:EXPECTED_N_VARS].str.startswith("MT-")].sum(
            axis=1
        )
    ).ravel()
    ribo_mask = feature_names[:EXPECTED_N_VARS].str.match(r"^RP[SL]")
    ribo = np.asarray(source_matrix[:, ribo_mask].sum(axis=1)).ravel()
    if (counts <= 0).any():
        raise AssertionError("zero-count accepted cell")
    qc = pd.DataFrame(
        {
            "n_counts": counts,
            "n_genes": detected,
            "pct_mito": mito / counts * 100.0,
            "pct_ribo": ribo / counts * 100.0,
        }
    )
    return qc, {
        "shape": [EXPECTED_N_OBS, EXPECTED_N_VARS],
        "nnz": int(source_matrix.nnz),
        "source_exact_value_parity": True,
        "source_barcode_axis_sha256": ordered_sha256(barcodes),
        "source_feature_id_axis_sha256": ordered_sha256(feature_ids[:EXPECTED_N_VARS]),
        "source_feature_name_axis_sha256": ordered_sha256(
            feature_names[:EXPECTED_N_VARS]
        ),
        "accepted_var_index_axis_sha256": ordered_sha256(
            pd.Index(var.index.astype(str))
        ),
        "x_semantics": "raw_counts",
        "x_rewrite_required": False,
    }


def curate_obs(
    baseline: pd.DataFrame, assignments: pd.DataFrame, qc: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if len(baseline) != EXPECTED_N_OBS or not baseline["original_obs_index"].is_unique:
        raise AssertionError("baseline OBS identity drift")
    curated = baseline.copy(deep=True)
    for field in (
        "dataset",
        "cell_line",
        "disease",
        "tissue_type",
        "organism",
        "is_control",
        "condition",
        "perturbation",
        "perturbation_type",
        "assay",
        "modality",
    ):
        if field in baseline and f"source_original_{field}" not in curated:
            curated[f"source_original_{field}"] = baseline[field]
    guide = baseline["guide"].astype("string")
    if not guide.fillna("").equals(assignments["source_guide_top"].fillna("")):
        raise AssertionError("accepted guide differs from exact source top guide")
    controls = guide.str.startswith("NO_SITE_", na=False).astype("boolean")
    if not baseline["is_control"].astype("boolean").equals(controls):
        raise AssertionError("accepted control semantics drift")
    condition = pd.Series(
        np.where(controls, "control", "test"), index=curated.index, dtype="string"
    )
    if not baseline["condition"].astype("string").equals(condition):
        raise AssertionError("accepted condition semantics drift")
    target = baseline["perturbation"].astype("string").replace({"unknown": pd.NA})
    sequence = assignments["source_guide_sequence"].astype("string")
    if int(sequence.notna().sum()) != int(guide.notna().sum()):
        raise AssertionError("guide sequence join incomplete")

    set_field(curated, "dataset", PREFIX, "known", "canonical logical family")
    set_field(
        curated,
        "sample",
        "GSM6297384",
        "known",
        "exact Illumina expression/barcode axis",
    )
    set_field(
        curated,
        "cell_id",
        baseline["original_obs_index"].astype("string"),
        "known",
        "GEO 10x barcode",
    )
    set_field(
        curated,
        "batch",
        missing(curated.index),
        "unknown",
        "published HTO count matrix lacks reviewed demultiplex assignment",
    )
    for field in ("donor_id", "age", "sex", "ethnicity"):
        dtype = "Float64" if field == "age" else "string"
        set_field(
            curated,
            field,
            missing(curated.index, dtype),
            "not_applicable",
            "A375 cell-line experiment",
        )
    set_field(
        curated,
        "replicate",
        missing(curated.index),
        "unknown",
        "source does not map HTO labels to reviewed replicate identities",
    )
    for field in ("plate_id", "well_id"):
        set_field(
            curated,
            field,
            missing(curated.index),
            "not_applicable",
            "droplet single-cell assay",
        )
    set_field(curated, "cell_type", "A375 melanoma cell", "known", "GEO sample source")
    set_field(curated, "cell_line", "A-375", "known", "GEO source name")
    set_field(curated, "tissue_type", "cell culture", "known", "GEO growth protocol")
    set_field(curated, "organism", "Homo sapiens", "known", "GEO organism")
    set_field(
        curated, "disease", "malignant melanoma", "known", "A375 cell-line provenance"
    )
    set_field(
        curated, "sequencer", "Illumina HiSeq X Ten", "known", "GEO platform GPL20795"
    )
    set_field(
        curated,
        "technology",
        "10x Genomics Chromium Single Cell 3' v3",
        "known",
        "GEO extraction protocol",
    )
    set_field(curated, "assay", "Perturb-seq", "known", "GEO experiment design")
    set_field(curated, "modality", "scRNA-seq", "known", "GEO experiment type")
    set_field(
        curated,
        "media",
        missing(curated.index),
        "unknown",
        "source-exhaustive review found no complete growth-medium formulation",
    )
    set_field(
        curated, "treatment", "interferon gamma", "known", "GEO treatment protocol"
    )
    set_field(
        curated,
        "perturbation",
        target,
        np.where(target.notna(), "known", "unknown"),
        "accepted source-derived target label",
    )
    set_field(
        curated,
        "perturbation_type",
        pd.Series(
            np.where(guide.notna(), "CRISPR", pd.NA),
            index=curated.index,
            dtype="string",
        ),
        np.where(guide.notna(), "known", "unknown"),
        "GEO CRISPR knockout design",
    )
    set_field(
        curated,
        "perturbation_technology",
        "CROP-seq CRISPR-Cas9 knockout",
        "known",
        "publication methods",
    )
    set_field(
        curated,
        "perturbation_library",
        "6127-guide transcription-factor and chromatin-modifier library",
        "known",
        "Supplementary Table 4",
    )
    set_field(
        curated,
        "guide_id",
        guide,
        np.where(guide.notna(), "known", "unknown"),
        "GEO feature-barcode matrix",
    )
    set_field(
        curated,
        "guide_sequence",
        sequence,
        np.where(sequence.notna(), "known", "unknown"),
        "publication Supplementary Table 4",
    )
    set_field(
        curated,
        "perturbation_target",
        target,
        np.where(target.notna(), "known", "unknown"),
        "accepted source-derived target label",
    )
    set_field(curated, "dose", 2.0, "known", "GEO treatment protocol")
    set_field(curated, "dose_unit", "ng/mL", "known", "GEO treatment protocol")
    set_field(
        curated,
        "timepoint",
        960.0,
        "known",
        "GEO treatment protocol: 16 hours IFN-gamma",
    )
    set_field(
        curated, "timepoint_unit", "minute", "known", "normalized source duration"
    )
    set_field(
        curated,
        "condition",
        condition,
        "known",
        "non-targeting guide control semantics",
    )
    set_field(curated, "is_control", controls, "known", "NO_SITE guide identity")
    set_field(
        curated,
        "control_availability",
        "strict_control_available",
        "known",
        "source non-targeting guides",
    )
    for field in ("n_counts", "n_genes", "pct_mito", "pct_ribo"):
        values = pd.Series(qc[field].to_numpy(), index=curated.index)
        set_field(
            curated, field, values, "known", "exact Illumina raw-count source matrix"
        )
    set_field(
        curated,
        "is_low_quality",
        missing(curated.index, "boolean"),
        "unknown",
        "source supplies no reviewed per-cell QC label",
    )
    set_field(
        curated,
        "pseudotime",
        missing(curated.index, "Float64"),
        "not_applicable",
        "non-trajectory experiment",
    )
    set_field(
        curated,
        "response_value",
        missing(curated.index, "Float64"),
        "not_applicable",
        "no per-cell scalar response endpoint",
    )
    set_field(
        curated,
        "response_type",
        missing(curated.index),
        "not_applicable",
        "no per-cell scalar response endpoint",
    )
    set_field(curated, "is_bulk", False, "known", "single-cell source")
    set_field(curated, "is_pseudobulk", False, "known", "single-cell source")
    set_field(curated, "source", "GEO", "known", "processed source authority")
    set_field(curated, "source_accession", "GSE197452", "known", "GEO series")
    set_field(
        curated, "x_semantics", "raw_counts", "known", "exact source matrix parity"
    )
    curated["combination_size"] = np.where(guide.notna(), 2, 1).astype(np.int16)
    curated["perturbation_2"] = "interferon gamma"
    curated["perturbation_type_2"] = "cytokine"
    curated["dose_2"] = 2.0
    curated["dose_unit_2"] = "ng/mL"
    curated["timepoint_2"] = 960.0
    curated["timepoint_unit_2"] = "minute"
    curated["combination_id"] = pd.Series(
        [
            f"CRISPR:{item}|IFNG:2ng/mL" if pd.notna(item) else "IFNG:2ng/mL"
            for item in guide
        ],
        index=curated.index,
        dtype="string",
    )
    for column in assignments.columns:
        curated[column] = assignments[column].to_numpy()
    if len(curated) != len(baseline) or not curated.index.equals(baseline.index):
        raise AssertionError("OBS row count/order drift")
    if not curated["obs_uuid"].is_unique or not curated["original_obs_index"].is_unique:
        raise AssertionError("OBS identity uniqueness drift")
    return curated, {
        "rows": len(curated),
        "guide_rows": int(guide.notna().sum()),
        "guide_sequence_rows": int(sequence.notna().sum()),
        "control_rows": int(controls.sum()),
        "hash_top_known_rows": int(assignments["source_hash_top"].notna().sum()),
        "hash_top_tie_rows": int((assignments["source_hash_top_ties"] > 1).sum()),
    }


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        state_column = f"{field}_state"
        if field not in frame or state_column not in frame:
            raise AssertionError(f"canonical OBS disposition absent: {field}")
        states = frame[state_column].astype("string")
        if states.eq("not_applicable").all():
            disposition = "not_applicable"
        elif states.eq("unknown").all():
            disposition = "unknown"
        elif frame[field].notna().all():
            disposition = "materialized_complete"
        else:
            disposition = "materialized_partial"
        result[field] = {
            "disposition": disposition,
            "known_rows": int(frame[field].notna().sum()),
            "unknown_rows": int(frame[field].isna().sum()),
        }
    return result


def verify_var(var: pd.DataFrame, source: dict[str, Any]) -> dict[str, Any]:
    stable = var["stable_feature_id"].astype("string")
    genes = source["genes"]
    expected_ids = pd.Index(source["feature_ids"])
    expected_names = pd.Index(source["feature_names"])
    if (
        len(var) != EXPECTED_N_VARS
        or not stable.is_unique
        or not stable.str.fullmatch(r"ENSG\d{11}", na=False).all()
        or not pd.Index(stable.astype(str)).equals(expected_ids)
        or not accepted_symbol_axis_matches(
            expected_names,
            expected_ids,
            pd.Index(var.index.astype(str)),
        )
        or not genes["gene_symbol"]
        .astype(str)
        .equals(pd.Series(expected_names, index=genes.index))
    ):
        raise AssertionError("VAR exact human ENSG/source axis drift")
    return {
        "rows": len(var),
        "stable_feature_id_unique": True,
        "species": "Homo sapiens",
        "source_axis_parity": True,
        "needs_revision": False,
        "rewrite_reason": "none; exact live human ENSG axis and X parity remain intact",
    }


def prepare(ln: Any, source: dict[str, Any]) -> dict[str, Any]:
    baseline_artifact = artifact_by_uid(ln, EXPECTED_OBS_UID)
    x_artifact = artifact_by_uid(ln, EXPECTED_X_UID)
    var_artifact = artifact_by_uid(ln, EXPECTED_VAR_UID)
    if (
        str(baseline_artifact.key) != OBS_KEY
        or str(x_artifact.key) != X_KEY
        or str(x_artifact.hash) != EXPECTED_X_HASH
        or str(var_artifact.key) != VAR_KEY
    ):
        raise AssertionError("frozen Lamin artifact drift")
    baseline = baseline_artifact.load()
    source["accepted_barcodes"] = baseline["original_obs_index"].astype(str).tolist()
    assignments, assignment_receipt = feature_assignments(
        source["paths"]["GSM6297388_filtered_feature_bc_matrix.pert.ill.h5"],
        baseline,
        source["feature_table"],
    )
    if assignment_receipt["current_differs_top_guide"] != 0:
        raise AssertionError("source guide assignment drift")
    with h5py.File(
        source["paths"]["GSM6297388_filtered_feature_bc_matrix.pert.ill.h5"], "r"
    ) as handle:
        features = handle["matrix/features"]
        source["feature_ids"] = [
            item.decode() if isinstance(item, bytes) else str(item)
            for item in features["id"][:EXPECTED_N_VARS]
        ]
        source["feature_names"] = [
            item.decode() if isinstance(item, bytes) else str(item)
            for item in features["name"][:EXPECTED_N_VARS]
        ]
    var = var_artifact.load()
    qc, x_receipt = matrix_and_qc(x_artifact, var, source)
    var_receipt = verify_var(var, source)
    expected_obs, obs_receipt = curate_obs(baseline, assignments, qc)
    expected_hash = frame_sha256(expected_obs)
    latest, history = latest_artifact(ln, OBS_KEY)
    if str(latest.uid) == EXPECTED_OBS_UID:
        obs_curated = False
    elif str(latest.description).startswith(
        f"{TASK_ID}: source-exhaustive GSE197452 OBS"
    ):
        actual = latest.load()
        assert_frame_equal(actual, expected_obs, check_categorical=True)
        if frame_sha256(actual) != expected_hash:
            raise AssertionError("curated OBS frame hash drift")
        obs_curated = True
    else:
        raise AssertionError(
            f"foreign OBS revision after frozen baseline: {latest.uid}"
        )
    return {
        "baseline_artifact": baseline_artifact,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
        "latest_obs_artifact": latest,
        "expected_obs": expected_obs,
        "expected_obs_sha256": expected_hash,
        "obs_curated": obs_curated,
        "obs_history_count": len(history),
        "obs_receipt": obs_receipt,
        "assignment_receipt": assignment_receipt,
        "x_receipt": x_receipt,
        "var_receipt": var_receipt,
        "field_dispositions": field_dispositions(expected_obs),
    }


def successor_description(
    new_obs: Any, predecessor: Any, before: list[Any], after: list[Any]
) -> str:
    return canonical(
        {
            "format": "pert-gym.append-only-dataset-e2e-successor/v1",
            "task_id": TASK_ID,
            "dataset_id": PREFIX,
            "predecessor_uid": str(predecessor.uid),
            "predecessor_key": str(predecessor.key),
            "predecessor_membership_sha256": membership_sha256(before),
            "replaced_obs_uid": EXPECTED_OBS_UID,
            "added_obs_uid": str(new_obs.uid),
            "member_count_before": len(before),
            "member_count_after": len(after),
            "resulting_membership_sha256": membership_sha256(after),
            "membership_rule": "immutable predecessor with same-key OBS replaced by exact source-curated OBS; no duplicate artifact keys",
            "rollback": f"select immutable predecessor Collection {predecessor.uid}",
        }
    )


def ensure_successor(
    ln: Any, new_obs: Any, *, allow_create: bool = False
) -> tuple[Any, bool, dict[str, Any]]:
    predecessor = collection_by_uid(ln, PREDECESSOR_COLLECTION_UID)
    if str(predecessor.key) != PREDECESSOR_COLLECTION_KEY:
        raise AssertionError("predecessor Collection key drift")
    before = list(predecessor.artifacts.all())
    if len(before) != PREDECESSOR_MEMBER_COUNT:
        raise AssertionError("predecessor Collection member count drift")
    matches = [item for item in before if str(item.key) == OBS_KEY]
    if len(matches) != 1 or str(matches[0].uid) != EXPECTED_OBS_UID:
        raise AssertionError("predecessor target OBS membership drift")
    after = [item for item in before if str(item.key) != OBS_KEY] + [new_obs]
    keys = [str(item.key) for item in after]
    if len(keys) != len(set(keys)) or len(after) != len(before):
        raise AssertionError("successor Collection key uniqueness/count drift")
    description = successor_description(new_obs, predecessor, before, after)
    existing = list(ln.Collection.filter(key=SUCCESSOR_COLLECTION_KEY).all())
    created = False
    if existing:
        if len(existing) != 1:
            raise AssertionError("successor Collection key collision")
        successor = existing[0]
    else:
        if not allow_create:
            raise AssertionError("required successor Collection absent")
        successor = ln.Collection(
            after,
            key=SUCCESSOR_COLLECTION_KEY,
            description=description,
            skip_hash_lookup=True,
        ).save()
        created = True
    actual = list(successor.artifacts.all())
    if str(successor.description) != description or member_identity(
        actual
    ) != member_identity(after):
        raise AssertionError("successor Collection readback drift")
    return (
        successor,
        created,
        {
            "predecessor": {
                "uid": str(predecessor.uid),
                "key": str(predecessor.key),
                "member_count": len(before),
                "membership_sha256": membership_sha256(before),
            },
            "successor": {
                "uid": str(successor.uid),
                "key": str(successor.key),
                "member_count": len(actual),
                "membership_sha256": membership_sha256(actual),
                "target_obs_uid": str(new_obs.uid),
            },
        },
    )


def publish(ln: Any, prepared: dict[str, Any], helper_sha256: str) -> tuple[Any, bool]:
    if prepared["obs_curated"]:
        return prepared["latest_obs_artifact"], False
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-publish-"))
    path = root / "obs.parquet"
    prepared["expected_obs"].to_parquet(path)
    description = (
        f"{TASK_ID}: source-exhaustive GSE197452 OBS; exact 20811-cell Illumina barcode "
        f"and raw-count parity; 20784 guide sequences from Supplementary Table 4; "
        f"frame_sha256={prepared['expected_obs_sha256']}; helper_sha256={helper_sha256}"
    )
    ln.track(
        key=f"pert-gym/real-dataset-curation/{REAL_DATASET_ID}/{TASK_ID}",
        kind="script",
        params={"task_id": TASK_ID, "helper_sha256": helper_sha256},
        new_run=True,
        pypackages=False,
        stream_tracking=False,
    )
    obs = ln.Artifact.from_dataframe(
        path,
        key=OBS_KEY,
        revises=prepared["baseline_artifact"],
        description=description,
    ).save()
    obs.features.set_values({"X": prepared["x_artifact"]})
    write_journal("obs_saved", obs_uid=str(obs.uid))
    return obs, True


def strip_runtime(prepared: dict[str, Any]) -> dict[str, Any]:
    hidden = {
        "baseline_artifact",
        "x_artifact",
        "var_artifact",
        "latest_obs_artifact",
        "expected_obs",
    }
    result = {key: value for key, value in prepared.items() if key not in hidden}
    result["latest_obs"] = artifact_identity(prepared["latest_obs_artifact"])
    result["x"] = artifact_identity(prepared["x_artifact"])
    result["var"] = artifact_identity(prepared["var_artifact"])
    return result


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
                    "metric": "real_dataset_end_to_end",
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
    manifest = verify_frozen_manifest()
    capacity = preflight()
    write_journal("preflight", mode=mode, helper_sha256=helper_sha256)
    emit_product("preflight", 0)
    source = load_sources(manifest)
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    prepared = prepare(ln, source)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    obs_created = False
    collection_created = False
    if mode == "mutate":
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
            fresh = prepare(ln, source)
            obs, obs_created = publish(ln, fresh, helper_sha256)
            successor, collection_created, collection_receipt = ensure_successor(
                ln, obs, allow_create=True
            )
            try:
                ln.finish()
            except AttributeError:
                ln.context.finish()
            write_journal(
                "published",
                obs_uid=str(obs.uid),
                collection_uid=str(successor.uid),
            )
    else:
        if not prepared["obs_curated"]:
            if mode == "verify":
                raise AssertionError("verify requested before curated OBS exists")
            collection_receipt = {"status": "not_evaluated_before_write"}
        else:
            _, _, collection_receipt = ensure_successor(
                ln, prepared["latest_obs_artifact"]
            )
    final = prepare(ln, source)
    if mode in {"mutate", "verify"} and not final["obs_curated"]:
        raise AssertionError("terminal curated OBS readback failed")
    if final["obs_curated"]:
        successor, _, collection_receipt = ensure_successor(
            ln, final["latest_obs_artifact"]
        )
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    receipt = {
        "format": "pert-gym.real-dataset-e2e-curation/v3",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "dataset_id": PREFIX,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "source_manifest_sha256": sha256_file(MANIFEST_PATH),
        "source_payloads": source["receipts"],
        "source_authorities": source["authority_receipts"],
        "supplementary_table_4": source["feature_receipt"],
        "member_before": strip_runtime(prepared),
        "member_after": strip_runtime(final),
        "collections": collection_receipt,
        "writes": {
            "obs_revisions": int(obs_created),
            "x_revisions": 0,
            "var_revisions": 0,
            "collection_writes": int(collection_created),
            "deletions": 0,
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {
            "hostname": capacity.hostname,
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = sha256_bytes(canonical(receipt).encode())
    write_journal(
        "verified" if mode == "verify" else "complete",
        obs_uid=str(final["latest_obs_artifact"].uid),
        receipt_sha256=receipt["canonical_sha256"],
    )
    emit_product("checkpointing", 1)
    print("GSE197452_CURATION_RECEIPT=" + canonical(receipt), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
