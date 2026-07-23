#!/usr/bin/env python3
"""Append-only source-exhaustive OBS curation for Cellarity public data."""

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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pandas as pd
from anndata.io import read_elem
from google.cloud import storage
from pandas.testing import assert_frame_equal

from tools.lamin_context import connect_pertdata
from tools.pert_gym_vm_runner import (
    BILLING_PROJECT,
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    preflight,
)

TASK_ID = "t_9c09e453"
REAL_DATASET_ID = "cellarity/public-collection"
SOURCE_MANIFEST_PATH = Path(__file__).with_name("source_manifest.json")
FROZEN_BINDINGS_PATH = Path(__file__).with_name("frozen_inputs") / "bindings.json"
STAGING_PREFIX = "pert-gym/staging/pert-gym/curation/cellarity/t_9c09e453"

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
    "timepoint_unit",
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

MEMBERS = (
    {
        "accession": "GSE305370",
        "prefix": "cellarity/GSE305370/GSE305370_citeseq_alldonors_alldays",
        "filename": "GSE305370_citeseq_alldonors_alldays.h5ad",
        "before_obs_uid": "dAFlb7abls3HQl5M0001",
        "collection_obs_uid": "dAFlb7abls3HQl5M0000",
        "x_uid": "afizT5wyemDxlpat0000",
        "x_hash": "fcUlS-UcwESGbUVIdS9_NQ",
        "var_uid": "iCyDpIOMsfFaSz6O0002",
        "n_obs": 134_583,
        "kind": "gse305370_citeseq",
        "source_columns": [
            "cell_type",
            "donor",
            "library",
            "n_counts",
            "n_genes",
            "pct_counts_mt",
            "time",
        ],
        "assay": "CITE-seq",
        "modality": "RNA + surface protein",
        "x_semantics": "unknown; source H5AD does not declare matrix transformation in row metadata",
    },
    {
        "accession": "GSE305370",
        "prefix": "cellarity/GSE305370/GSE305370_multiome_alldonors_alldays",
        "filename": "GSE305370_multiome_alldonors_alldays.h5ad",
        "before_obs_uid": "MLE2MEOCoBJT5kPE0001",
        "collection_obs_uid": "MLE2MEOCoBJT5kPE0000",
        "x_uid": "kUAJmEMAqFPqcQ6s0000",
        "x_hash": "mtiwFjvcNfDFLdIS2oYyhQ",
        "var_uid": "pE0RfeqVYLg08lPG0002",
        "n_obs": 164_462,
        "kind": "gse305370_multiome",
        "source_columns": [
            "cell_type_rna",
            "library",
            "n_counts",
            "n_genes",
            "pct_counts_mt_rna",
        ],
        "assay": "10x multiome",
        "modality": "RNA + chromatin accessibility",
        "x_semantics": "joint RNA/ATAC feature matrix; exact transform unknown",
    },
    {
        "accession": "GSE305370",
        "prefix": "cellarity/GSE305370/GSE305370_rna_combined_with_velocity_and_refined_annotations",
        "filename": "GSE305370_rna_combined_with_velocity_and_refined_annotations.h5ad",
        "before_obs_uid": "RJbcZEfscysBCeMj0001",
        "collection_obs_uid": "RJbcZEfscysBCeMj0000",
        "x_uid": "YarbYWCMuzxYlHi10000",
        "x_hash": "yI8iJQKMAPLA8xZIbea3Bw",
        "var_uid": "h9D2Ff07ruRv3kJe0002",
        "n_obs": 135_341,
        "kind": "gse305370_rna",
        "source_columns": [
            "cell_type",
            "day",
            "donor",
            "library",
            "n_counts",
            "n_genes",
            "pct_counts_mt",
        ],
        "assay": "10x multiome",
        "modality": "scRNA-seq with RNA velocity layers",
        "x_semantics": "RNA expression with velocity annotations; exact X transform unknown",
    },
    {
        "accession": "GSE305979",
        "prefix": "cellarity/GSE305979/GSE305979_day0-7_normalized_counts_with_celltype_annotations",
        "filename": "GSE305979_day0-7_normalized_counts_with_celltype_annotations.h5ad",
        "before_obs_uid": "Is3hhpzyzvMqRAZ20001",
        "collection_obs_uid": "Is3hhpzyzvMqRAZ20000",
        "x_uid": "6pRQ9KGEwDyKBt430000",
        "x_hash": "8nXZf1wehO3MuTIiKT5tHg",
        "var_uid": "FvUX0GmLYUIGty6e0001",
        "n_obs": 146_735,
        "kind": "gse305979_normalized",
        "source_columns": [
            "CELL_ID",
            "CONCENTRATION_UM",
            "LIBRARY_ID",
            "TIMEPOINT_HOURS",
            "cell_type",
            "compound_name",
            "n_counts",
            "n_genes",
            "percent_mito",
            "replicate",
        ],
        "assay": "10x 3' v3.1",
        "modality": "scRNA-seq",
        "x_semantics": "per-cell normalized to 10000 counts and log transformed",
    },
    {
        "accession": "GSE305979",
        "prefix": "cellarity/GSE305979/GSE305979_day0_raw_counts",
        "filename": "GSE305979_day0_raw_counts.h5ad",
        "before_obs_uid": "ugxvzG3isKtDchNG0001",
        "collection_obs_uid": "ugxvzG3isKtDchNG0000",
        "x_uid": "yZKh7KulUomkaK8w0000",
        "x_hash": "pfyiTUQpHeg3xFI-Pc68xA",
        "var_uid": "9AWpNsCAn10GS9ml0001",
        "n_obs": 2_875,
        "kind": "gse305979_day0_raw",
        "source_columns": [
            "CELL_ID",
            "CONCENTRATION_UM",
            "LIBRARY_ID",
            "TIMEPOINT_HOURS",
        ],
        "assay": "10x 3' v3.1",
        "modality": "scRNA-seq",
        "x_semantics": "raw counts",
    },
    {
        "accession": "GSE305979",
        "prefix": "cellarity/GSE305979/GSE305979_day1-7_demuxed_counts",
        "filename": "GSE305979_day1-7_demuxed_counts.h5ad",
        "before_obs_uid": "SVmujjG5GwXU0KZl0001",
        "collection_obs_uid": "SVmujjG5GwXU0KZl0000",
        "x_uid": "Tni8nLRSMPBqOklS0000",
        "x_hash": "ulS5LS-Qy6YFvQwJTYP7Fg",
        "var_uid": "Lg0yhjurEtaPPTP70001",
        "n_obs": 143_396,
        "kind": "gse305979_demuxed",
        "source_columns": [
            "CELL_ID",
            "CONCENTRATION_UM",
            "LIBRARY_ID",
            "TIMEPOINT_HOURS",
            "compound_name",
            "n_counts",
            "percent_mito",
            "sample_name",
        ],
        "assay": "10x 3' v3.1",
        "modality": "scRNA-seq",
        "x_semantics": "demultiplexed singlet raw counts",
    },
    {
        "accession": "GSE305979",
        "prefix": "cellarity/GSE305979/GSE305979_day1-7_raw_counts",
        "filename": "GSE305979_day1-7_raw_counts.h5ad",
        "before_obs_uid": "3gdXNHm2avLD3dLG0001",
        "collection_obs_uid": "3gdXNHm2avLD3dLG0000",
        "x_uid": "YVyGoCiaqFzitCBv0000",
        "x_hash": "iR8eZFqkXm5gpeiSQ0_pEA",
        "var_uid": "Zr3NlWlca2YIou4K0001",
        "n_obs": 223_971,
        "kind": "gse305979_raw",
        "source_columns": ["LIBRARY_ID", "n_counts", "percent_mito", "sample_name"],
        "assay": "10x 3' v3.1",
        "modality": "scRNA-seq",
        "x_semantics": "pre-demultiplexing raw counts; treatment assignment unavailable",
    },
    {
        "accession": "GSE306429",
        "prefix": "cellarity/GSE306429/GSE306429_combined_demuxed",
        "filename": "GSE306429_combined_demuxed.h5ad",
        "before_obs_uid": "DWsyQzOmlTS19kUr0001",
        "collection_obs_uid": "DWsyQzOmlTS19kUr0000",
        "x_uid": "s4Vsts7vMewJqf9n0000",
        "x_hash": "2D0Pn3t015BXVugK7d6Slg",
        "var_uid": "ezlGxRjcR0jqdW6E0001",
        "n_obs": 1_257_778,
        "kind": "gse306429_demuxed",
        "source_columns": [
            "bio_sample_id",
            "cell_id",
            "compound_name",
            "dose_uM",
            "library_id",
            "replicate",
            "timepoint_hr",
        ],
        "assay": "10x 3' v3",
        "modality": "scRNA-seq",
        "x_semantics": "demultiplexed single-cell counts",
    },
    {
        "accession": "GSE306429",
        "prefix": "cellarity/GSE306429/GSE306429_combined_pseudobulk",
        "filename": "GSE306429_combined_pseudobulk.h5ad",
        "before_obs_uid": "JL1I0jwLaqqNU1vu0002",
        "collection_obs_uid": "JL1I0jwLaqqNU1vu0001",
        "x_uid": "AmycM9CgrR1r551F0000",
        "x_hash": "BOJw3edfPSmNffOAyuYehw",
        "var_uid": "HzlKD1eIPXTvAQzu0001",
        "n_obs": 1_737,
        "kind": "gse306429_pseudobulk",
        "source_columns": [
            "bio_sample_id",
            "cell_id",
            "compound_name",
            "dose_uM",
            "library_id",
            "replicate",
            "timepoint_hr",
        ],
        "assay": "10x 3' v3",
        "modality": "pseudobulk scRNA-seq",
        "x_semantics": "sample-level pseudobulk expression",
    },
    {
        "accession": "GSE306429",
        "prefix": "cellarity/GSE306429/GSE306429_combined_vscores",
        "filename": "GSE306429_combined_vscores.h5ad",
        "before_obs_uid": "K0em7aNI1182HV8m0001",
        "collection_obs_uid": "K0em7aNI1182HV8m0000",
        "x_uid": "sofPAagXB7Nifke80000",
        "x_hash": "QrHcCBA9MqvFxbGosfWbzQ",
        "var_uid": "wQUnGNTREkmvE9XG0001",
        "n_obs": 1_563,
        "kind": "gse306429_vscores",
        "source_columns": [
            "cell_id",
            "compound_name",
            "dose_uM",
            "library_id",
            "replicate",
        ],
        "assay": "10x 3' v3",
        "modality": "pseudobulk perturbation response",
        "x_semantics": "gene-level v-score response matrix",
    },
)


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


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": str(artifact.version),
        "size": int(artifact.size),
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
    records = list(ln.Artifact.filter(uid=value).all())
    if len(records) == 1:
        return records[0]
    return latest_artifact(ln, value)[0]


def load_frozen_inputs() -> dict[str, Any]:
    bindings = json.loads(FROZEN_BINDINGS_PATH.read_text())
    if bindings.get("format") != "pert-gym.frozen-input-bindings/v1":
        raise AssertionError("frozen binding format drift")
    root = Path(__file__).parents[5]
    for entry in bindings["inputs"]:
        compressed = (root / entry["binding_path"]).read_bytes()
        if sha256_bytes(compressed) != entry["gzip_sha256"]:
            raise AssertionError("frozen gzip identity drift")
        raw = gzip.decompress(compressed)
        if (
            len(raw) != entry["uncompressed_bytes"]
            or sha256_bytes(raw) != entry["uncompressed_sha256"]
        ):
            raise AssertionError("frozen input identity drift")
    return bindings


def load_source_manifest() -> dict[str, Any]:
    manifest = json.loads(SOURCE_MANIFEST_PATH.read_text())
    if manifest.get("format") != "pert-gym.source-manifest/v1":
        raise AssertionError("source manifest format drift")
    objects = {item["filename"]: item for item in manifest["target_source_objects"]}
    if set(objects) != {item["filename"] for item in MEMBERS}:
        raise AssertionError("source manifest target coverage drift")
    if sum(item["n_obs"] for item in objects.values()) != 2_212_441:
        raise AssertionError("source manifest observation denominator drift")
    return manifest


def source_path(
    spec: dict[str, Any], manifest: dict[str, Any], root: Path
) -> tuple[Path, dict[str, Any]]:
    record = next(
        item
        for item in manifest["target_source_objects"]
        if item["filename"] == spec["filename"]
    )
    accession = spec["accession"]
    stem = accession[:6] + "nnn"
    expected_url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{stem}/{accession}/suppl/{spec['filename']}"
    if record["url"] != expected_url or record["n_obs"] != spec["n_obs"]:
        raise AssertionError(f"source manifest identity drift: {spec['filename']}")
    path = root / spec["filename"]
    if not path.exists() or path.stat().st_size != record["size"]:
        subprocess.run(
            [
                "curl",
                "--location",
                "--fail",
                "--retry",
                "5",
                "--continue-at",
                "-",
                "--output",
                str(path),
                expected_url,
            ],
            check=True,
            timeout=4 * 3600,
        )
    if path.stat().st_size != record["size"] or sha256_file(path) != record["sha256"]:
        raise AssertionError(f"source payload identity drift: {spec['filename']}")
    return path, {key: record[key] for key in ("url", "size", "sha256", "n_obs")}


def _frame_axis(group: h5py.Group) -> pd.Index:
    index_name = group.attrs["_index"]
    if isinstance(index_name, bytes):
        index_name = index_name.decode()
    values = read_elem(group[str(index_name)])
    return pd.Index(pd.Series(values, dtype="string").astype(str))


def load_source_inputs(path: Path, columns: list[str]) -> pd.DataFrame:
    with h5py.File(path, "r") as handle:
        obs = handle["obs"]
        if not isinstance(obs, h5py.Group):
            raise AssertionError("source H5AD obs encoding drift")
        missing = set(columns) - set(obs.keys())
        if missing:
            raise AssertionError(f"source columns missing: {sorted(missing)}")
        frame = pd.DataFrame(
            {column: pd.Series(read_elem(obs[column])) for column in columns}
        )
        frame.index = _frame_axis(obs)
    return frame


def series_equal(left: pd.Series, right: pd.Series) -> bool:
    lvalue = left.astype("string").fillna("<NA>").reset_index(drop=True)
    rvalue = right.astype("string").fillna("<NA>").reset_index(drop=True)
    return bool(lvalue.equals(rvalue))


def verify_source_join(
    obs: pd.DataFrame, source: pd.DataFrame, spec: dict[str, Any]
) -> dict[str, Any]:
    if len(obs) != spec["n_obs"] or len(source) != spec["n_obs"]:
        raise AssertionError("source/current row denominator drift")
    source_index = pd.Series(source.index, dtype="string").reset_index(drop=True)
    obs_index = pd.Series(obs.index, dtype="string").reset_index(drop=True)
    if not source_index.equals(obs_index):
        raise AssertionError(
            f"source/current exact index join failed: {spec['prefix']}"
        )
    aliases = {
        "cell_id": "cell_line",
        "cell_type": "cell_type_from_author",
        "cell_type_rna": "cell_type_from_author",
        "compound_name": "pert_name",
        "time": "time_from_author",
    }
    comparisons: dict[str, bool] = {}
    for column in source.columns:
        alias = aliases.get(column)
        target = alias if alias in obs else column
        if target not in obs:
            continue
        comparisons[f"{column}->{target}"] = series_equal(source[column], obs[target])
    required = [
        value
        for key, value in comparisons.items()
        if not key.startswith("compound_name->")
    ]
    if not all(required):
        raise AssertionError(f"source/current preserved-column mismatch: {comparisons}")
    if not source.index.is_unique:
        original_index = (
            pd.Series(obs["original_obs_index"], dtype="string").reset_index(drop=True)
            if "original_obs_index" in obs
            else pd.Series(dtype="string")
        )
        expected_columns = set(source.columns)
        compared_columns = {key.split("->", maxsplit=1)[0] for key in comparisons}
        obs_uuid_unique = "obs_uuid" in obs and bool(obs["obs_uuid"].is_unique)
        if (
            not source_index.equals(original_index)
            or compared_columns != expected_columns
            or not all(comparisons.values())
            or not obs_uuid_unique
        ):
            raise AssertionError(
                f"non-unique source index lacks exact row identity proof: {spec['prefix']}"
            )
    return {
        "rows": len(source),
        "exact_index_order_match": True,
        "index_unique": bool(source.index.is_unique),
        "column_equalities": comparisons,
        "join_semantics": (
            "exact source H5AD obs index equals accepted OBS index; duplicate indices "
            "additionally require exact original_obs_index order, unique obs_uuid, and "
            "equality of every selected source column"
        ),
    }


def nulls(index: pd.Index, dtype: str = "string") -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype=dtype)


def set_field(
    frame: pd.DataFrame, field: str, values: Any, state: Any, source: str
) -> None:
    frame[field] = values
    frame[f"{field}_state"] = state
    frame[f"{field}_source"] = source


def source_series(
    source: pd.DataFrame, name: str, index: pd.Index, dtype: str = "string"
) -> pd.Series:
    values = source[name].reset_index(drop=True)
    result = pd.Series(values.array, index=index)
    return result.astype(dtype)


def minutes_from_days(values: pd.Series) -> pd.Series:
    extracted = values.astype("string").str.extract(r"(\d+(?:\.\d+)?)", expand=False)
    return (pd.to_numeric(extracted, errors="coerce") * 24 * 60).astype("Float64")


def minutes_from_hours(values: pd.Series) -> pd.Series:
    return (pd.to_numeric(values, errors="coerce") * 60).astype("Float64")


def curate_obs(
    obs: pd.DataFrame, source: pd.DataFrame, spec: dict[str, Any]
) -> pd.DataFrame:
    curated = obs.copy(deep=True)
    index = curated.index
    unknown_reason = (
        "source-exhaustive GEO/source-H5AD review found no defensible row value"
    )
    for field in CANONICAL_OBS_FIELDS:
        set_field(curated, field, nulls(index), "unknown", unknown_reason)

    not_applicable = {
        "perturbation_library",
        "guide_id",
        "guide_sequence",
        "perturbation_target",
        "perturbation_target_id",
        "sensitivity",
    }
    for field in not_applicable:
        set_field(curated, field, nulls(index), "not_applicable", "dataset design")

    set_field(
        curated, "dataset", spec["prefix"], "known", "exact logical-family identity"
    )
    set_field(
        curated,
        "cell_id",
        pd.Series(index.astype(str), index=index, dtype="string"),
        "known",
        "exact source H5AD obs index",
    )
    set_field(
        curated, "organism", "Homo sapiens", "known", f"GEO {spec['accession']} species"
    )
    set_field(
        curated, "assay", spec["assay"], "known", f"GEO {spec['accession']} design"
    )
    set_field(
        curated, "modality", spec["modality"], "known", "target source-object identity"
    )
    set_field(
        curated,
        "technology",
        "10x Genomics",
        "known",
        f"GEO {spec['accession']} design",
    )
    set_field(
        curated,
        "is_bulk",
        False,
        "known",
        "single-cell assay; aggregation represented separately",
    )
    is_pseudobulk = spec["kind"] in {"gse306429_pseudobulk", "gse306429_vscores"}
    set_field(
        curated,
        "is_pseudobulk",
        is_pseudobulk,
        "known",
        "target source-object identity",
    )
    set_field(
        curated, "source", "Cellarity", "known", "accepted publication/source identity"
    )
    set_field(
        curated,
        "source_accession",
        spec["accession"],
        "known",
        "exact GEO source object",
    )
    set_field(
        curated,
        "x_semantics",
        spec["x_semantics"],
        "known",
        "source filename and GEO processing description",
    )

    if "library" in source:
        sample = source_series(source, "library", index)
    elif "LIBRARY_ID" in source:
        sample = source_series(source, "LIBRARY_ID", index)
    elif "bio_sample_id" in source:
        sample = source_series(source, "bio_sample_id", index)
    else:
        sample = source_series(source, "library_id", index)
    set_field(curated, "sample", sample, "known", "source H5AD sample/library identity")
    set_field(curated, "batch", sample, "known", "source H5AD sample/library identity")

    if "donor" in source:
        donor = source_series(source, "donor", index)
        set_field(curated, "donor_id", donor, "known", "source H5AD donor")
    if "cell_type" in curated and "cell_type_from_author" in obs:
        set_field(
            curated,
            "cell_type",
            obs["cell_type"],
            "known",
            "accepted ontology normalization of exact source author cell type",
        )
    elif spec["kind"] == "gse305370_multiome" and "cell_type" in obs:
        set_field(
            curated,
            "cell_type",
            obs["cell_type"],
            "known",
            "accepted ontology normalization of source cell_type_rna",
        )
    if "cell_line" in obs:
        set_field(
            curated,
            "cell_line",
            obs["cell_line"],
            "known",
            "accepted normalization of source cell_id",
        )
    elif "CELL_ID" in source:
        set_field(
            curated,
            "cell_line",
            source_series(source, "CELL_ID", index),
            "known",
            "source H5AD CELL_ID",
        )
    elif spec["accession"] == "GSE306429" and "cell_id" in source:
        set_field(
            curated,
            "cell_line",
            source_series(source, "cell_id", index),
            "known",
            "source H5AD cell_id label",
        )
    if "disease" in obs:
        set_field(
            curated,
            "disease",
            obs["disease"],
            "known",
            "accepted source-derived disease annotation",
        )
    if "tissue_type" in obs:
        set_field(
            curated,
            "tissue_type",
            obs["tissue_type"],
            "known",
            "accepted source-derived tissue context",
        )
    elif spec["accession"] == "GSE306429":
        set_field(
            curated,
            "tissue_type",
            "cell culture",
            "known",
            "GEO in-vitro compound-screen design",
        )

    if spec["accession"] in {"GSE305370", "GSE305979"}:
        set_field(
            curated,
            "media",
            "StemSpan SFEM + CC100 + thrombopoietin",
            "known",
            f"GEO {spec['accession']} overall design",
        )
    if spec["accession"] == "GSE305979":
        set_field(
            curated,
            "sequencer",
            "Illumina NovaSeq 6000",
            "known",
            "GEO GSE305979 series summary",
        )

    kind = spec["kind"]
    if kind.startswith("gse305370"):
        for field in (
            "perturbation",
            "perturbation_type",
            "perturbation_technology",
            "is_control",
            "dose",
            "dose_unit",
            "control_availability",
        ):
            dtype = (
                "Float64"
                if field == "dose"
                else ("boolean" if field == "is_control" else "string")
            )
            set_field(
                curated,
                field,
                nulls(index, dtype),
                "not_applicable",
                "observational differentiation time course",
            )
        set_field(
            curated,
            "trajectory_id",
            "CD34+ HSPC ex-vivo differentiation",
            "known",
            "GEO GSE305370 design",
        )
        for field in ("response_metric", "response_value", "response_source"):
            dtype = "Float64" if field == "response_value" else "string"
            set_field(
                curated,
                field,
                nulls(index, dtype),
                "not_applicable",
                "observational differentiation expression payload",
            )
        if kind == "gse305370_citeseq":
            timepoint = minutes_from_days(source_series(source, "time", index))
        elif kind == "gse305370_rna":
            timepoint = minutes_from_days(source_series(source, "day", index))
        else:
            timepoint = nulls(index, "Float64")
        if timepoint.notna().any():
            set_field(
                curated,
                "timepoint",
                timepoint,
                "known",
                "source H5AD day converted to minutes",
            )
            set_field(
                curated, "timepoint_unit", "minute", "known", "canonical time unit"
            )
        set_field(
            curated,
            "is_baseline",
            nulls(index, "boolean"),
            "not_applicable",
            "no untreated perturbation baseline axis",
        )
    elif spec["accession"] == "GSE305979":
        set_field(
            curated,
            "trajectory_id",
            "CD34+ HSPC megakaryocyte differentiation",
            "known",
            "GEO GSE305979 design",
        )
        for field in ("response_metric", "response_value", "response_source"):
            dtype = "Float64" if field == "response_value" else "string"
            set_field(
                curated,
                field,
                nulls(index, dtype),
                "not_applicable",
                "expression payload has no scalar OBS response",
            )
        if "TIMEPOINT_HOURS" in source:
            timepoint = minutes_from_hours(
                source_series(source, "TIMEPOINT_HOURS", index, "Float64")
            )
        elif "sample_name" in source:
            timepoint = minutes_from_days(source_series(source, "sample_name", index))
        else:
            timepoint = nulls(index, "Float64")
        time_state = np.where(timepoint.notna(), "known", "unknown")
        set_field(
            curated,
            "timepoint",
            timepoint,
            time_state,
            "source H5AD timepoint converted to minutes",
        )
        set_field(
            curated,
            "timepoint_unit",
            np.where(timepoint.notna(), "minute", pd.NA),
            time_state,
            "canonical time unit",
        )
        baseline = timepoint.eq(0).astype("boolean")
        set_field(
            curated,
            "is_baseline",
            baseline,
            np.where(timepoint.notna(), "known", "unknown"),
            "source H5AD timepoint",
        )
        if kind == "gse305979_raw":
            perturbation = nulls(index)
            set_field(
                curated,
                "perturbation",
                perturbation,
                "unknown",
                "pre-demultiplexing source has no row-level treatment assignment",
            )
            set_field(
                curated,
                "perturbation_type",
                nulls(index),
                "unknown",
                "pre-demultiplexing source has no row-level treatment assignment",
            )
            set_field(
                curated,
                "perturbation_technology",
                nulls(index),
                "unknown",
                "pre-demultiplexing source has no row-level treatment assignment",
            )
            set_field(
                curated,
                "is_control",
                nulls(index, "boolean"),
                "unknown",
                "pre-demultiplexing source has no row-level treatment assignment",
            )
            set_field(
                curated,
                "dose",
                nulls(index, "Float64"),
                "unknown",
                "pre-demultiplexing source has no row-level treatment assignment",
            )
            set_field(
                curated,
                "dose_unit",
                nulls(index),
                "unknown",
                "pre-demultiplexing source has no row-level treatment assignment",
            )
        else:
            perturbation = (
                source_series(source, "compound_name", index)
                if "compound_name" in source
                else pd.Series("No treatment", index=index, dtype="string")
            )
            control = perturbation.isin(["No treatment", "DMSO"]).astype("boolean")
            set_field(
                curated,
                "perturbation",
                perturbation,
                "known",
                "source H5AD compound assignment",
            )
            set_field(
                curated,
                "perturbation_type",
                np.where(control, "none", "drug"),
                "known",
                "source H5AD compound assignment",
            )
            set_field(
                curated,
                "perturbation_technology",
                "small molecule",
                "known",
                "GEO GSE305979 design",
            )
            set_field(
                curated,
                "is_control",
                control,
                "known",
                "source H5AD no-treatment/DMSO assignment",
            )
            dose = source_series(source, "CONCENTRATION_UM", index, "Float64")
            set_field(
                curated,
                "dose",
                dose,
                np.where(dose.notna(), "known", "unknown"),
                "source H5AD CONCENTRATION_UM",
            )
            set_field(
                curated,
                "dose_unit",
                np.where(dose.notna(), "uM", pd.NA),
                np.where(dose.notna(), "known", "unknown"),
                "source H5AD unit",
            )
        set_field(
            curated,
            "control_availability",
            "strict_control_available",
            "known",
            "GEO no-treatment/DMSO controls",
        )
    else:
        perturbation = source_series(source, "compound_name", index)
        control = perturbation.str.upper().eq("DMSO").astype("boolean")
        set_field(
            curated, "perturbation", perturbation, "known", "source H5AD compound_name"
        )
        set_field(
            curated,
            "perturbation_type",
            np.where(control, "none", "drug"),
            "known",
            "source H5AD compound assignment",
        )
        set_field(
            curated,
            "perturbation_technology",
            "small molecule",
            "known",
            "GEO GSE306429 design",
        )
        set_field(
            curated, "is_control", control, "known", "source H5AD DMSO assignment"
        )
        dose = source_series(source, "dose_uM", index, "Float64")
        dose_state = np.where(
            control, "not_applicable", np.where(dose.notna(), "known", "unknown")
        )
        dose_value = dose.mask(control)
        set_field(
            curated,
            "dose",
            dose_value,
            dose_state,
            "source H5AD dose_uM; vehicle controls have no compound dose",
        )
        set_field(
            curated,
            "dose_unit",
            np.where(control, pd.NA, np.where(dose.notna(), "uM", pd.NA)),
            dose_state,
            "source H5AD unit",
        )
        if "timepoint_hr" in source:
            timepoint = minutes_from_hours(
                source_series(source, "timepoint_hr", index, "Float64")
            )
            time_source = "source H5AD timepoint_hr converted to minutes"
        else:
            timepoint = pd.Series(1440.0, index=index, dtype="Float64")
            time_source = "GEO GSE306429 homogeneous 24-hour v-score design"
        set_field(curated, "timepoint", timepoint, "known", time_source)
        set_field(curated, "timepoint_unit", "minute", "known", "canonical time unit")
        set_field(
            curated,
            "is_baseline",
            nulls(index, "boolean"),
            "not_applicable",
            "library-matched controls are not baseline-expression rows",
        )
        set_field(
            curated,
            "control_availability",
            "strict_control_available",
            "known",
            "GEO library-matched DMSO controls",
        )
        if kind == "gse306429_vscores":
            set_field(
                curated,
                "response_metric",
                "v-score",
                "known",
                "target source-object identity",
            )
            set_field(
                curated,
                "response_value",
                nulls(index, "Float64"),
                "not_applicable",
                "gene-level response values are stored in X, not scalar OBS",
            )
            set_field(
                curated,
                "response_source",
                "X matrix",
                "known",
                "target source-object identity",
            )
        else:
            for field in ("response_metric", "response_value", "response_source"):
                dtype = "Float64" if field == "response_value" else "string"
                set_field(
                    curated,
                    field,
                    nulls(index, dtype),
                    "not_applicable",
                    "expression payload has no scalar OBS response",
                )

    if "n_counts" in source:
        counts = source_series(source, "n_counts", index, "Float64")
        set_field(
            curated,
            "n_counts",
            counts,
            np.where(counts.notna(), "known", "unknown"),
            "source H5AD n_counts",
        )
    if "n_genes" in source:
        genes = source_series(source, "n_genes", index, "Float64")
        set_field(
            curated,
            "n_genes",
            genes,
            np.where(genes.notna(), "known", "unknown"),
            "source H5AD n_genes",
        )
    mito_column = next(
        (
            name
            for name in ("pct_counts_mt", "pct_counts_mt_rna", "percent_mito")
            if name in source
        ),
        None,
    )
    if mito_column:
        mito = source_series(source, mito_column, index, "Float64")
        set_field(
            curated,
            "pct_mito",
            mito,
            np.where(mito.notna(), "known", "unknown"),
            f"source H5AD {mito_column}",
        )

    if not curated.index.equals(obs.index) or len(curated) != spec["n_obs"]:
        raise AssertionError("OBS row order/count drift")
    if "obs_uuid" in obs and (
        not obs["obs_uuid"].is_unique or not curated["obs_uuid"].equals(obs["obs_uuid"])
    ):
        raise AssertionError("OBS UUID identity drift")
    if set(CANONICAL_OBS_FIELDS) - set(curated):
        raise AssertionError("canonical OBS field coverage drift")
    return curated


def field_dispositions(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in CANONICAL_OBS_FIELDS:
        counts = {
            str(key): int(value)
            for key, value in frame[f"{field}_state"]
            .value_counts(dropna=False)
            .to_dict()
            .items()
        }
        states = set(counts)
        if states == {"known"}:
            disposition = "materialized_complete"
        elif states == {"unknown"}:
            disposition = "unknown"
        elif states == {"not_applicable"}:
            disposition = "not_applicable"
        else:
            disposition = "materialized_partial"
        result[field] = {
            "disposition": disposition,
            "state_counts": counts,
            "known_rows": int(frame[field].notna().sum()),
            "unknown_rows": int(frame[field].isna().sum()),
        }
    return result


def verify_obs(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    if not actual.index.equals(expected.index) or list(actual.columns) != list(
        expected.columns
    ):
        raise AssertionError("OBS schema/order mismatch")
    assert_frame_equal(actual, expected, check_categorical=True)


def current_member(
    ln: Any, spec: dict[str, Any], source: pd.DataFrame
) -> dict[str, Any]:
    obs_key = f"{spec['prefix']}/obs.parquet"
    obs_artifact, history = latest_artifact(ln, obs_key)
    if not (
        str(obs_artifact.uid) == spec["before_obs_uid"]
        or str(obs_artifact.description).startswith(
            f"{TASK_ID}: source-exhaustive Cellarity OBS"
        )
    ):
        raise AssertionError(
            f"unexpected latest OBS identity: {obs_key} {obs_artifact.uid}"
        )
    obs = obs_artifact.load()
    join = verify_source_join(obs, source, spec)
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    if str(x_artifact.uid) != spec["x_uid"] or str(x_artifact.hash) != spec["x_hash"]:
        raise AssertionError(f"OBS->X identity drift: {spec['prefix']}")
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    if str(var_artifact.uid) != spec["var_uid"]:
        raise AssertionError(f"X->VAR identity drift: {spec['prefix']}")
    curated = curate_obs(obs, source, spec)
    already = str(obs_artifact.description).startswith(
        f"{TASK_ID}: source-exhaustive Cellarity OBS"
    )
    if already:
        verify_obs(obs, curated)
    return {
        "obs_artifact": obs_artifact,
        "x_artifact": x_artifact,
        "var_artifact": var_artifact,
        "obs": obs,
        "curated": curated,
        "already_curated": already,
        "history_count": len(history),
        "source_join": join,
        "field_dispositions": field_dispositions(curated),
    }


def strip_runtime(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "obs": artifact_identity(result["obs_artifact"]),
        "x": artifact_identity(result["x_artifact"]),
        "var": artifact_identity(result["var_artifact"]),
        "history_count": result["history_count"],
        "already_curated": result["already_curated"],
        "source_join": result["source_join"],
        "field_dispositions": result["field_dispositions"],
    }


def collection_membership(ln: Any) -> dict[str, Any]:
    prefixes = [item["prefix"] for item in MEMBERS]
    snapshots: dict[str, Any] = {}
    for key in ("pert-gym/base-public/20260621", "pert-gym/canonical/20260621"):
        records = list(ln.Collection.filter(key=key).all())
        if len(records) != 1:
            raise AssertionError(f"Collection identity drift: {key}")
        collection = records[0]
        members = list(collection.artifacts.only("uid", "key").all())
        matches: dict[str, list[str]] = {}
        for prefix in prefixes:
            values = [
                str(item.uid)
                for item in members
                if str(item.key) == f"{prefix}/obs.parquet"
            ]
            if len(values) != 1:
                raise AssertionError(
                    f"Collection target member drift: {key} {prefix}: {values}"
                )
            matches[prefix] = values
        snapshots[key] = {
            "uid": str(collection.uid),
            "hash": str(collection.hash),
            "member_count": len(members),
            "target_obs_members": matches,
        }
    return snapshots


def publish(ln: Any, result: dict[str, Any], spec: dict[str, Any]) -> Any:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-cellarity-publish-"))
    path = root / "obs.parquet"
    result["curated"].to_parquet(path)
    artifact = ln.Artifact.from_dataframe(
        path,
        key=f"{spec['prefix']}/obs.parquet",
        revises=result["obs_artifact"],
        description=f"{TASK_ID}: source-exhaustive Cellarity OBS; exact GEO H5AD row join; accession={spec['accession']}; family={spec['filename']}",
    ).save()
    artifact.features.set_values({"X": result["x_artifact"]})
    return artifact


def emit_product(phase: str, current: int) -> None:
    print(
        "PRODUCT_EXECUTION="
        + canonical(
            {
                "product_execution": {
                    "task_id": TASK_ID,
                    "host": os.uname().nodename,
                    "pid": os.getpid(),
                    "phase": phase,
                    "payload_heartbeat_at": int(time.time()),
                    "metric": "logical_families",
                    "current": current,
                    "denominator": len(MEMBERS),
                    "unit": "logical_family",
                }
            }
        ),
        flush=True,
    )


def upload_receipt(receipt: dict[str, Any], mode: str) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    object_name = f"{STAGING_PREFIX}/{mode}-receipt-{timestamp}.json"
    payload = canonical(receipt).encode() + b"\n"
    client = storage.Client(project=BILLING_PROJECT)
    blob = client.bucket("scperturb", user_project=BILLING_PROJECT).blob(object_name)
    blob.upload_from_string(
        payload, content_type="application/json", if_generation_match=0
    )
    blob.reload()
    return {
        "uri": f"gs://scperturb/{object_name}",
        "generation": int(blob.generation),
        "size": len(payload),
        "sha256": sha256_bytes(payload),
    }


def inspect_all(
    ln: Any, manifest: dict[str, Any], source_root: Path, require_curated: bool
) -> list[dict[str, Any]]:
    receipts = []
    for index, spec in enumerate(MEMBERS, start=1):
        emit_product("verify", index - 1)
        path, source_identity = source_path(spec, manifest, source_root)
        source = load_source_inputs(path, spec["source_columns"])
        result = current_member(ln, spec, source)
        if require_curated and not result["already_curated"]:
            raise AssertionError(f"curated revision absent: {spec['prefix']}")
        receipts.append(
            {
                "identity": {
                    key: spec[key]
                    for key in ("accession", "prefix", "filename", "n_obs")
                },
                "source": source_identity,
                **strip_runtime(result),
            }
        )
        del source, result
    return receipts


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = preflight()
    frozen = load_frozen_inputs()
    manifest = load_source_manifest()
    helper_sha256 = sha256_file(Path(__file__))
    source_root = Path(tempfile.gettempdir()) / f"{TASK_ID}-cellarity-sources"
    source_root.mkdir(parents=True, exist_ok=True)
    ln = connect_pertdata()
    if (
        ln.setup.settings.instance.slug != "laminlabs/pertdata"
        or ln.setup.settings.branch.name != "jkobject"
    ):
        raise AssertionError("wrong Lamin target")
    collections_before = collection_membership(ln)
    counts_before = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    writes: list[dict[str, Any]] = []

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
            ln.track(
                key=f"pert-gym/real-dataset-curation/{REAL_DATASET_ID}/{TASK_ID}",
                kind="script",
                params={
                    "task_id": TASK_ID,
                    "helper_sha256": helper_sha256,
                    "source_manifest_sha256": sha256_file(SOURCE_MANIFEST_PATH),
                },
                new_run=True,
                pypackages=False,
                stream_tracking=False,
            )
            for index, spec in enumerate(MEMBERS, start=1):
                emit_product("mutate", index - 1)
                path, _ = source_path(spec, manifest, source_root)
                source = load_source_inputs(path, spec["source_columns"])
                result = current_member(ln, spec, source)
                if not result["already_curated"]:
                    artifact = publish(ln, result, spec)
                    writes.append(artifact_identity(artifact))
                del source, result
            try:
                ln.finish()
            except AttributeError:
                ln.context.finish()
    before_or_after = inspect_all(
        ln, manifest, source_root, require_curated=mode in {"mutate", "verify"}
    )
    collections_after = collection_membership(ln)
    if collections_after != collections_before:
        raise AssertionError("Collection drift")
    counts_after = {
        "artifacts": ln.Artifact.filter().count(),
        "collections": ln.Collection.filter().count(),
    }
    if mode == "mutate" and counts_after["artifacts"] - counts_before[
        "artifacts"
    ] != len(writes):
        raise AssertionError("artifact registry count drift")
    if mode == "verify" and counts_after != counts_before:
        raise AssertionError("verify replay changed registry counts")
    receipt = {
        "format": "pert-gym.real-dataset-obs-curation/v2",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "source_manifest_sha256": sha256_file(SOURCE_MANIFEST_PATH),
        "frozen_inputs": frozen["inputs"],
        "source_denominator": {
            "biological_datasets": 1,
            "logical_families": 10,
            "physical_members": 10,
            "observations": 2_212_441,
        },
        "members": before_or_after,
        "collections": collections_after,
        "writes": {
            "obs_revisions": len(writes),
            "var_revisions": 0,
            "x_revisions": 0,
            "collection_writes": 0,
            "deletions": 0,
            "artifacts": writes,
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "rollback_identity": [
            {
                "before_obs_uid": spec["before_obs_uid"],
                "current_obs_uid": member["obs"]["uid"],
                "key": member["obs"]["key"],
            }
            for spec, member in zip(MEMBERS, before_or_after, strict=True)
        ],
        "replay_noop": mode == "verify" and counts_before == counts_after,
        "host": {
            "hostname": capacity.hostname,
            "available_memory_bytes": capacity.available_memory_bytes,
            "free_disk_bytes": capacity.free_disk_bytes,
        },
        "completed_at": int(time.time()),
    }
    receipt["canonical_sha256"] = sha256_bytes(canonical(receipt).encode())
    pointer = upload_receipt(receipt, mode)
    emit_product("checkpointing", len(MEMBERS))
    print("CELLARITY_CURATION_RECEIPT=" + canonical(pointer), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
