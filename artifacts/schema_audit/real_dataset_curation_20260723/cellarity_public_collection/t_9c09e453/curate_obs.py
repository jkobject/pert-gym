#!/usr/bin/env python3
"""Append-only source-exhaustive OBS curation for Cellarity public data."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
import re
import shutil
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
    MIN_AVAILABLE_MEMORY_GB,
    Preflight,
    _available_memory_bytes,
    distributed_lamin_writer_lease,
    lamin_writer_lease,
    require_heavy_vm,
)

TASK_ID = "t_9c09e453"
REAL_DATASET_ID = "cellarity/public-collection"
SOURCE_MANIFEST_PATH = Path(__file__).with_name("source_manifest.json")
FROZEN_BINDINGS_PATH = Path(__file__).with_name("frozen_inputs") / "bindings.json"
PREDECESSOR_RECEIPT_PATH = Path(__file__).with_name("mutation_receipt.json")
STAGING_PREFIX = "pert-gym/staging/pert-gym/curation/cellarity/t_9c09e453"
RECEIPT_PREFIX = "data/cleaned/cellarity_public_collection/_receipts"
REPO_ROOT = Path(__file__).parents[5]
OBS_CONTRACT_PATH = REPO_ROOT / "config/obs_completed_contract_v1.json"
OBS_CONTRACT = json.loads(OBS_CONTRACT_PATH.read_text())
CANONICAL_OBS_FIELDS = tuple(OBS_CONTRACT["canonical_obs_columns"])
if len(CANONICAL_OBS_FIELDS) != OBS_CONTRACT["canonical_obs_column_count"]:
    raise AssertionError("binding OBS contract count drift")
OBS_CONTRACT_SHA256 = hashlib.sha256(OBS_CONTRACT_PATH.read_bytes()).hexdigest()
METADATA_MIN_FREE_DISK_GB = 10

# These source-backed columns remain useful but are not part of the binding
# OBS_COMPLETED/v1 denominator. Keeping the two sets separate prevents receipts
# from silently claiming a hand-maintained superset as the canonical contract.
SUPPLEMENTAL_OBS_FIELDS = (
    "guide_id",
    "perturbation_target",
    "perturbation_target_id",
    "timepoint_unit",
    "source",
    "source_accession",
    "control_availability",
    "x_semantics",
)


def metadata_preflight() -> Preflight:
    """Gate the metadata-only replay without pretending it needs 50 GiB scratch."""
    hostname, project, zone, instance = require_heavy_vm()
    free_disk = shutil.disk_usage(REPO_ROOT).free
    available_memory = _available_memory_bytes()
    if free_disk < METADATA_MIN_FREE_DISK_GB * 1024**3:
        raise RuntimeError(
            f"insufficient metadata-only disk: {free_disk / 1024**3:.1f} GiB free; "
            f"need {METADATA_MIN_FREE_DISK_GB:.1f} GiB"
        )
    if available_memory < MIN_AVAILABLE_MEMORY_GB * 1024**3:
        raise RuntimeError(
            f"insufficient memory: {available_memory / 1024**3:.1f} GiB available; "
            f"need {MIN_AVAILABLE_MEMORY_GB:.1f} GiB"
        )
    return Preflight(
        hostname=hostname,
        project=project,
        zone=zone,
        instance=instance,
        free_disk_bytes=free_disk,
        available_memory_bytes=available_memory,
        billing_project=BILLING_PROJECT,
    )


def canonical_prefix(spec: dict[str, Any]) -> str:
    """Return the flat canonical triplet prefix for one source H5AD family."""
    filename = str(spec["filename"])
    if not filename.endswith(".h5ad"):
        raise AssertionError(f"unexpected Cellarity payload filename: {filename}")
    stem = filename.removesuffix(".h5ad")
    if "/" in stem or not stem.startswith(str(spec["accession"])):
        raise AssertionError(f"unsafe canonical payload stem: {stem}")
    return f"data/cleaned/{stem}"


def replace_collection_members(
    members: list[Any], replacements: dict[str, Any]
) -> list[Any]:
    """Replace every exact predecessor UID once while preserving order and peers."""
    counts = {uid: 0 for uid in replacements}
    after: list[Any] = []
    for artifact in members:
        uid = str(artifact.uid)
        if uid in replacements:
            counts[uid] += 1
            after.append(replacements[uid])
        else:
            after.append(artifact)
    if any(count != 1 for count in counts.values()):
        raise AssertionError(f"predecessor membership drift: {counts}")
    if len({str(item.uid) for item in after}) != len(after):
        raise AssertionError("replacement introduced duplicate Collection members")
    return after


def staging_decommission_gate(objects: list[dict[str, Any]]) -> dict[str, Any]:
    """Fail closed unless the task-owned staging prefix is proven empty."""
    if objects:
        raise AssertionError(f"staging objects remain: {objects}")
    return {
        "format": "pert-gym.GCS_DECOMMISSION_READY/v1",
        "task_id": TASK_ID,
        "prefix": f"gs://scperturb/{STAGING_PREFIX}/",
        "objects_remaining": 0,
        "GCS_DECOMMISSION_READY": True,
    }


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


def ordered_sha256(values: pd.Index) -> str:
    return sha256_bytes("\n".join(values.astype(str)).encode())


def artifact_identity(artifact: Any) -> dict[str, Any]:
    return {
        "uid": str(artifact.uid),
        "key": str(artifact.key),
        "hash": str(artifact.hash),
        "version": str(artifact.version),
        "size": int(artifact.size),
        "path": str(artifact.path),
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


def load_predecessor_source_evidence() -> dict[str, Any]:
    """Reuse frozen source-join proof while explicitly withholding mutation credit."""
    receipt = json.loads(PREDECESSOR_RECEIPT_PATH.read_text())
    claimed = receipt.pop("canonical_sha256", None)
    actual = sha256_bytes(canonical(receipt).encode())
    if claimed != actual:
        raise AssertionError("predecessor receipt canonical digest drift")
    if (
        receipt.get("status") != "PASS"
        or receipt.get("mode") != "mutate"
        or receipt.get("writes", {}).get("obs_revisions") != 0
        or receipt.get("registry_counts", {}).get("before")
        != receipt.get("registry_counts", {}).get("after")
    ):
        raise AssertionError("predecessor source evidence classification drift")
    members = {
        item["identity"]["prefix"]: {
            "source": item["source"],
            "source_join": item["source_join"],
        }
        for item in receipt["members"]
    }
    if set(members) != {spec["prefix"] for spec in MEMBERS}:
        raise AssertionError("predecessor source evidence denominator drift")
    for prefix, member in members.items():
        if not member["source_join"].get("exact_index_order_match"):
            raise AssertionError(f"predecessor source join failed: {prefix}")
    return {
        "receipt_canonical_sha256": claimed,
        "members": members,
        "adjudication": {
            "mutation_credit": False,
            "reason": "receipt records zero writes and zero registry delta",
            "source_join_evidence_reusable": True,
        },
    }


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


def verify_var(var: pd.DataFrame, source_inspection: dict[str, Any]) -> dict[str, Any]:
    if "feature_class" in var:
        biological = var["feature_class"].astype("string").eq("gene")
    elif "feature_types" in var:
        biological = var["feature_types"].astype("string").eq("Gene Expression")
    else:
        biological = pd.Series(True, index=var.index, dtype="boolean")
    stable = var.get("stable_feature_id", pd.Series(pd.NA, index=var.index)).astype(
        "string"
    )
    statuses = var.get(
        "stable_feature_id_mapping_status", pd.Series(pd.NA, index=var.index)
    ).astype("string")
    exact_ensembl = stable.str.fullmatch(r"ENSG[0-9]+", na=False)
    exact_status = statuses.eq("exact_stable_id")
    non_biological = ~biological
    non_biological_na = statuses.str.startswith("not_applicable_non_gene_", na=False)
    axis_candidates = {"var.index": pd.Index(var.index.astype(str))}
    for column in (
        "pert_gym_original_var_index",
        "gene_ids",
        "ensembl_gene_id",
        "symbol",
    ):
        if column in var:
            axis_candidates[column] = pd.Index(var[column].astype(str))
    candidate_hashes = {
        source: ordered_sha256(values) for source, values in axis_candidates.items()
    }
    matching_sources = [
        source
        for source, digest in candidate_hashes.items()
        if digest == source_inspection["source_var_index_sha256"]
    ]
    axis_identity_source = matching_sources[0] if matching_sources else "unresolved"
    axis_sha256 = candidate_hashes.get(axis_identity_source)
    checks = {
        "rows_match_source": len(var) == source_inspection["source_var_rows"],
        "ordered_axis_matches_source": bool(matching_sources),
        "every_biological_feature_has_exact_human_ensembl_id": bool(
            (exact_ensembl[biological] & exact_status[biological]).all()
        ),
        "every_non_biological_feature_is_explicitly_not_applicable": bool(
            non_biological_na[non_biological].all()
        ),
    }
    if not all(checks.values()):
        raise AssertionError(f"VAR Ensembl/species gate failed: {checks}")
    return {
        "status": "PASS",
        "VAR_ENSEMBL_SPECIES_COMPLETED": True,
        "organism": "Homo sapiens",
        "species_evidence": "exact ENSG namespace on every biological gene feature",
        "rows": len(var),
        "biological_features_total": int(biological.sum()),
        "stable_ensembl_id_features": int(exact_ensembl[biological].sum()),
        "correct_species_features": int(exact_ensembl[biological].sum()),
        "non_biological_features_not_applicable": int(non_biological.sum()),
        "ordered_var_axis_sha256": axis_sha256,
        "axis_identity_source": axis_identity_source,
        "matching_axis_identity_sources": matching_sources,
        "axis_candidate_sha256": candidate_hashes,
        "checks": checks,
    }


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
        target = (
            column
            if column == "cell_id" and column in obs
            else alias
            if alias in obs
            else column
        )
        if target not in obs:
            continue
        comparisons[f"{column}->{target}"] = series_equal(source[column], obs[target])
    identity_columns: list[str] = []
    if not source.index.is_unique:
        original_index = (
            pd.Series(obs["original_obs_index"], dtype="string").reset_index(drop=True)
            if "original_obs_index" in obs
            else pd.Series(dtype="string")
        )
        identity_columns = [
            key.split("->", maxsplit=1)[0]
            for key, equal in comparisons.items()
            if equal
        ]
        identity_unique = bool(identity_columns) and not bool(
            source[identity_columns].astype("string").fillna("<NA>").duplicated().any()
        )
        obs_uuid_unique = "obs_uuid" in obs and bool(obs["obs_uuid"].is_unique)
        if (
            not source_index.equals(original_index)
            or not identity_unique
            or not obs_uuid_unique
        ):
            raise AssertionError(
                f"non-unique source index lacks exact row identity proof: {spec['prefix']}"
            )
    return {
        "rows": len(source),
        "exact_index_order_match": True,
        "index_unique": bool(source.index.is_unique),
        "row_identity_columns": identity_columns,
        "column_equalities": comparisons,
        "join_semantics": (
            "an exact unique source H5AD obs index is a one-to-one row key; duplicate "
            "indices additionally require exact original_obs_index order, unique "
            "obs_uuid, and a unique composite of value-equal preserved source columns"
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
    for field in (*CANONICAL_OBS_FIELDS, *SUPPLEMENTAL_OBS_FIELDS):
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
        baseline = timepoint.eq(0).astype("boolean")
        set_field(
            curated,
            "is_baseline",
            baseline,
            np.where(timepoint.notna(), "known", "unknown"),
            "source H5AD day; baseline is the earliest day-zero state",
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


def revise_task_owned_obs(obs: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    """Repair the binding contract without replaying the 74 GiB source inspection."""
    revised = obs.copy(deep=True)
    index = revised.index
    if "molecule_sequence" not in revised:
        set_field(
            revised,
            "molecule_sequence",
            nulls(index),
            "unknown",
            "frozen source-join evidence contains no defensible molecule sequence",
        )
    if spec["accession"] == "GSE305370":
        if "timepoint" not in revised:
            raise AssertionError("task-owned GSE305370 OBS lacks canonical timepoint")
        timepoint = pd.to_numeric(revised["timepoint"], errors="coerce").astype(
            "Float64"
        )
        baseline = timepoint.eq(0).astype("boolean")
        set_field(
            revised,
            "is_baseline",
            baseline,
            np.where(timepoint.notna(), "known", "unknown"),
            "source-backed canonical timepoint; baseline is the earliest day-zero state",
        )
    for field in CANONICAL_OBS_FIELDS:
        required = {field, f"{field}_state", f"{field}_source"}
        if missing := required - set(revised.columns):
            raise AssertionError(f"task-owned canonical OBS coverage drift: {missing}")
    if len(revised) != len(obs) or not revised.index.equals(obs.index):
        raise AssertionError("task-owned OBS row/order drift")
    return revised


def verify_obs(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    if not actual.index.equals(expected.index) or list(actual.columns) != list(
        expected.columns
    ):
        raise AssertionError("OBS schema/order mismatch")
    assert_frame_equal(actual, expected, check_categorical=True)


def current_member(
    ln: Any, spec: dict[str, Any], source: pd.DataFrame | None
) -> dict[str, Any]:
    legacy_obs_key = f"{spec['prefix']}/obs.parquet"
    obs_key = f"{canonical_prefix(spec)}/obs.parquet"
    canonical_history = list(ln.Artifact.filter(key=obs_key).all())
    if canonical_history:
        canonical_history.sort(key=lambda item: (str(item.created_at), str(item.uid)))
        obs_artifact = canonical_history[-1]
        if not bool(obs_artifact.is_latest):
            raise AssertionError(f"newest canonical OBS is not latest: {obs_key}")
        history = canonical_history
    else:
        obs_artifact, history = latest_artifact(ln, legacy_obs_key)
    if not (
        str(obs_artifact.uid) == spec["before_obs_uid"]
        or str(obs_artifact.description).startswith(
            f"{TASK_ID}: source-exhaustive Cellarity OBS"
        )
    ):
        raise AssertionError(
            f"unexpected latest OBS identity: {obs_artifact.key} {obs_artifact.uid}"
        )
    obs = obs_artifact.load()
    task_owned = str(obs_artifact.description).startswith(
        f"{TASK_ID}: source-exhaustive Cellarity OBS"
    )
    if source is None:
        if not task_owned:
            raise AssertionError(
                f"frozen source evidence may revise only task-owned OBS: {spec['prefix']}"
            )
        frozen = load_predecessor_source_evidence()["members"][spec["prefix"]]
        join = frozen["source_join"]
    else:
        join = verify_source_join(obs, source, spec)
    x_artifact = resolve_artifact(ln, obs_artifact.features.get_values()["X"])
    if str(x_artifact.uid) != spec["x_uid"] or str(x_artifact.hash) != spec["x_hash"]:
        raise AssertionError(f"OBS->X identity drift: {spec['prefix']}")
    var_artifact = resolve_artifact(ln, x_artifact.features.get_values()["var"])
    if str(var_artifact.uid) != spec["var_uid"]:
        raise AssertionError(f"X->VAR identity drift: {spec['prefix']}")
    already = False
    if task_owned:
        if source is None:
            curated = revise_task_owned_obs(obs, spec)
        else:
            baseline_artifact = resolve_artifact(ln, spec["before_obs_uid"])
            baseline_obs = baseline_artifact.load()
            verify_source_join(baseline_obs, source, spec)
            curated = curate_obs(baseline_obs, source, spec)
        try:
            verify_obs(obs, curated)
        except AssertionError:
            # A corrected curation contract must revise the task-owned OBS rather
            # than accepting an obsolete revision as an idempotent replay.
            already = False
        else:
            already = str(obs_artifact.key) == obs_key
    else:
        curated = curate_obs(obs, source, spec)
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
        "var_verification": result["var_verification"],
        "history_count": result["history_count"],
        "already_curated": result["already_curated"],
        "source_join": result["source_join"],
        "field_dispositions": result["field_dispositions"],
    }


def validate_mutation_readback(
    writes: list[dict[str, Any]], members: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if not writes:
        raise AssertionError("mutate mode produced zero writes; use verify for replay")
    readback_by_uid = {
        str(member["obs"]["uid"]): member
        for member in members
        if member.get("already_curated") is True
    }
    readback: list[dict[str, Any]] = []
    for written in writes:
        member = readback_by_uid.get(str(written["uid"]))
        if member is None or member["obs"] != written:
            raise AssertionError(
                f"post-write readback does not match written OBS {written['uid']}"
            )
        readback.append(member["obs"])
    return readback


def recover_mutation_writes(
    writes: list[dict[str, Any]],
    members: list[dict[str, Any]],
    *,
    expected: int,
) -> list[dict[str, Any]]:
    """Recover unreceipted canonical writes after a bounded writer interruption."""
    fresh_uids = {str(item["uid"]) for item in writes}
    recovered = [
        member["obs"]
        for member in members
        if str(member["obs"]["uid"]) not in fresh_uids
        and str(member["obs"]["key"]).startswith("data/cleaned/")
        and member.get("already_curated") is True
    ]
    effective = [*writes, *recovered]
    if (
        len(effective) != expected
        or len({str(item["uid"]) for item in effective}) != expected
    ):
        raise AssertionError(
            f"mutation recovery denominator drift: expected={expected} "
            f"fresh={len(writes)} recovered={len(recovered)}"
        )
    return recovered


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


def ensure_canonical_artifact_key(
    ln: Any, artifact: Any, target_key: str
) -> dict[str, Any] | None:
    """Idempotently remap one immutable payload record to its canonical key."""
    existing = list(ln.Artifact.filter(key=target_key).all())
    if existing:
        if len(existing) != 1 or str(existing[0].uid) != str(artifact.uid):
            raise AssertionError(f"canonical Artifact key collision: {target_key}")
        return None
    before = artifact_identity(artifact)
    artifact.key = target_key
    artifact.save()
    after = artifact_identity(artifact)
    if after["uid"] != before["uid"] or after["hash"] != before["hash"]:
        raise AssertionError(
            f"canonical key remap changed payload identity: {target_key}"
        )
    if after["key"] != target_key:
        raise AssertionError(f"canonical key remap readback mismatch: {target_key}")
    return {"before": before, "after": after}


def collection_identity(collection: Any) -> dict[str, Any]:
    members = sorted(
        (
            {"uid": str(item.uid), "key": str(item.key)}
            for item in collection.artifacts.only("uid", "key").all()
        ),
        key=lambda item: (item["key"], item["uid"]),
    )
    return {
        "uid": str(collection.uid),
        "key": str(collection.key),
        "hash": str(collection.hash),
        "member_count": len(members),
        "members": members,
    }


def ensure_successor_collections(
    ln: Any, published: dict[str, Any], *, allow_create: bool
) -> tuple[dict[str, Any], int]:
    """Publish/read back broad successors plus one exact logical Collection."""
    successor_specs = (
        (
            "pert-gym/base-public/20260621",
            f"pert-gym/successors/base-public/20260621/cellarity/{TASK_ID}",
        ),
        (
            "pert-gym/canonical/20260621",
            f"pert-gym/successors/canonical/20260621/cellarity/{TASK_ID}",
        ),
    )
    output: dict[str, Any] = {}
    created = 0
    for predecessor_key, successor_key in successor_specs:
        predecessors = list(ln.Collection.filter(key=predecessor_key).all())
        if len(predecessors) != 1:
            raise AssertionError(f"Collection identity drift: {predecessor_key}")
        predecessor = predecessors[0]
        before = list(predecessor.artifacts.all())
        replacement_by_uid: dict[str, Any] = {}
        for spec in MEMBERS:
            matches = [
                item
                for item in before
                if str(item.key) == f"{spec['prefix']}/obs.parquet"
            ]
            if len(matches) != 1:
                raise AssertionError(
                    f"predecessor target drift: {predecessor_key} {spec['prefix']}"
                )
            replacement_by_uid[str(matches[0].uid)] = published[spec["prefix"]]
        after = replace_collection_members(before, replacement_by_uid)
        description = canonical(
            {
                "task_id": TASK_ID,
                "predecessor_uid": str(predecessor.uid),
                "purpose": "replace 10 Cellarity OBS members with canonical source-exhaustive revisions while preserving every unrelated member",
                "replacements": {
                    uid: str(item.uid)
                    for uid, item in sorted(replacement_by_uid.items())
                },
            }
        )
        records = list(ln.Collection.filter(key=successor_key).all())
        if records:
            if len(records) != 1:
                raise AssertionError(f"successor Collection collision: {successor_key}")
            successor = records[0]
        else:
            if not allow_create:
                raise AssertionError(
                    f"required successor Collection absent: {successor_key}"
                )
            successor = ln.Collection(
                after,
                key=successor_key,
                description=description,
                skip_hash_lookup=True,
            ).save()
            created += 1
        actual = collection_identity(successor)
        expected = sorted(
            ({"uid": str(item.uid), "key": str(item.key)} for item in after),
            key=lambda item: (item["key"], item["uid"]),
        )
        if str(successor.description) != description or actual["members"] != expected:
            raise AssertionError(
                f"successor Collection readback drift: {successor_key}"
            )
        output[successor_key] = {
            "predecessor": collection_identity(predecessor),
            "successor": actual,
        }

    logical_key = "pert-gym/datasets/cellarity-public-collection"
    logical_members = [published[spec["prefix"]] for spec in MEMBERS]
    logical_description = canonical(
        {
            "task_id": TASK_ID,
            "real_dataset_id": REAL_DATASET_ID,
            "logical_families": len(MEMBERS),
            "purpose": "exact Cellarity public dataset Collection over canonical OBS members",
        }
    )
    logical_records = list(ln.Collection.filter(key=logical_key).all())
    if logical_records:
        if len(logical_records) != 1:
            raise AssertionError(f"logical Collection collision: {logical_key}")
        logical = logical_records[0]
    else:
        if not allow_create:
            raise AssertionError(f"required logical Collection absent: {logical_key}")
        logical = ln.Collection(
            logical_members,
            key=logical_key,
            description=logical_description,
            skip_hash_lookup=True,
        ).save()
        created += 1
    logical_identity = collection_identity(logical)
    expected_logical = sorted(
        ({"uid": str(item.uid), "key": str(item.key)} for item in logical_members),
        key=lambda item: (item["key"], item["uid"]),
    )
    if (
        str(logical.description) != logical_description
        or logical_identity["members"] != expected_logical
        or logical_identity["member_count"] != len(MEMBERS)
    ):
        raise AssertionError("logical Collection readback drift")
    output[logical_key] = {"successor": logical_identity}
    return output, created


def publish(ln: Any, result: dict[str, Any], spec: dict[str, Any]) -> Any:
    root = Path(tempfile.mkdtemp(prefix=f"{TASK_ID}-cellarity-publish-"))
    path = root / "obs.parquet"
    result["curated"].to_parquet(path)
    artifact = ln.Artifact.from_dataframe(
        path,
        key=f"{canonical_prefix(spec)}/obs.parquet",
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
    object_name = f"{RECEIPT_PREFIX}/{mode}-receipt-{timestamp}.json"
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
        "crc32c": str(blob.crc32c),
        "etag": str(blob.etag),
    }


def list_task_staging_objects() -> list[dict[str, Any]]:
    client = storage.Client(project=BILLING_PROJECT)
    bucket = client.bucket("scperturb", user_project=BILLING_PROJECT)
    return [
        {
            "name": str(blob.name),
            "generation": int(blob.generation),
            "size": int(blob.size),
            "crc32c": str(blob.crc32c),
        }
        for blob in client.list_blobs(bucket, prefix=f"{STAGING_PREFIX}/")
    ]


def list_durable_receipts(mode: str) -> list[dict[str, Any]]:
    client = storage.Client(project=BILLING_PROJECT)
    bucket = client.bucket("scperturb", user_project=BILLING_PROJECT)
    return [
        {
            "name": str(blob.name),
            "generation": int(blob.generation),
            "size": int(blob.size),
            "crc32c": str(blob.crc32c),
        }
        for blob in client.list_blobs(
            bucket, prefix=f"{RECEIPT_PREFIX}/{mode}-receipt-"
        )
    ]


def remote_attestation(
    receipt: dict[str, Any], remote_identity: dict[str, Any]
) -> dict[str, Any]:
    required = {"uri", "generation", "size", "sha256", "crc32c", "etag"}
    if set(remote_identity) != required:
        raise AssertionError("remote receipt identity is incomplete")
    if (
        not isinstance(remote_identity["generation"], int)
        or remote_identity["generation"] <= 0
    ):
        raise AssertionError("remote receipt generation is invalid")
    attestation = {
        "format": "pert-gym.remote-receipt-attestation/v1",
        "receipt_canonical_sha256": receipt["canonical_sha256"],
        "remote_identity": remote_identity,
    }
    attestation["canonical_sha256"] = sha256_bytes(canonical(attestation).encode())
    return attestation


def inspect_all(
    ln: Any, manifest: dict[str, Any], require_curated: bool
) -> list[dict[str, Any]]:
    receipts = []
    predecessor = load_predecessor_source_evidence()
    for index, spec in enumerate(MEMBERS, start=1):
        emit_product("verify", index - 1)
        source_identity = predecessor["members"][spec["prefix"]]["source"]
        source_record = next(
            item
            for item in manifest["target_source_objects"]
            if item["filename"] == spec["filename"]
        )
        if any(
            source_identity.get(key) != source_record.get(key)
            for key in ("url", "size", "sha256", "n_obs")
        ):
            raise AssertionError(f"frozen source identity drift: {spec['prefix']}")
        result = current_member(ln, spec, None)
        result["var_verification"] = verify_var(
            result["var_artifact"].load(), source_record["inspection"]
        )
        if require_curated and not result["already_curated"]:
            raise AssertionError(f"curated revision absent: {spec['prefix']}")
        if require_curated:
            expected_keys = {
                "obs_artifact": f"{canonical_prefix(spec)}/obs.parquet",
                "x_artifact": f"{canonical_prefix(spec)}/X.h5ad",
                "var_artifact": f"{canonical_prefix(spec)}/var.parquet",
            }
            for name, expected_key in expected_keys.items():
                if str(result[name].key) != expected_key:
                    raise AssertionError(
                        f"canonical payload key absent: {spec['prefix']} {name}"
                    )
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
        del result
    return receipts


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"plan", "mutate", "verify"}:
        raise SystemExit(f"usage: {sys.argv[0]} plan|mutate|verify")
    mode = sys.argv[1]
    if platform.system() == "Darwin":
        raise RuntimeError("refusing Mac execution")
    capacity = metadata_preflight()
    prior_mutation_receipts = (
        list_durable_receipts("mutate") if mode == "mutate" else []
    )
    if prior_mutation_receipts:
        raise RuntimeError(
            "accepted durable mutation receipt already exists; use verify rather than "
            "replaying product writes"
        )
    frozen = load_frozen_inputs()
    manifest = load_source_manifest()
    predecessor_evidence = load_predecessor_source_evidence()
    helper_sha256 = sha256_file(Path(__file__))
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
    key_remaps: list[dict[str, Any]] = []
    collection_publication: dict[str, Any] = {}
    collection_writes = 0

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
            published: dict[str, Any] = {}
            for index, spec in enumerate(MEMBERS, start=1):
                emit_product("mutate", index - 1)
                result = current_member(ln, spec, None)
                for role, artifact, filename in (
                    ("X", result["x_artifact"], "X.h5ad"),
                    ("var", result["var_artifact"], "var.parquet"),
                ):
                    remap = ensure_canonical_artifact_key(
                        ln, artifact, f"{canonical_prefix(spec)}/{filename}"
                    )
                    if remap is not None:
                        key_remaps.append({"role": role, **remap})
                if not result["already_curated"]:
                    artifact = publish(ln, result, spec)
                    writes.append(artifact_identity(artifact))
                else:
                    artifact = result["obs_artifact"]
                published[spec["prefix"]] = artifact
                del result
            collection_publication, collection_writes = ensure_successor_collections(
                ln, published, allow_create=True
            )
            try:
                ln.finish()
            except AttributeError:
                ln.context.finish()
    before_or_after = inspect_all(
        ln, manifest, require_curated=mode in {"mutate", "verify"}
    )
    if mode == "verify":
        published = {
            spec["prefix"]: resolve_artifact(ln, member["obs"]["uid"])
            for spec, member in zip(MEMBERS, before_or_after, strict=True)
        }
        collection_publication, collection_writes = ensure_successor_collections(
            ln, published, allow_create=False
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
    if (
        mode == "mutate"
        and counts_after["collections"] - counts_before["collections"]
        != collection_writes
    ):
        raise AssertionError("Collection registry count drift")
    if mode == "verify" and counts_after != counts_before:
        raise AssertionError("verify replay changed registry counts")
    recovered_writes = (
        recover_mutation_writes(writes, before_or_after, expected=len(MEMBERS))
        if mode == "mutate"
        else []
    )
    effective_writes = [*writes, *recovered_writes]
    post_write_readback = (
        validate_mutation_readback(effective_writes, before_or_after)
        if mode == "mutate"
        else []
    )
    decommission = staging_decommission_gate(list_task_staging_objects())
    receipt = {
        "format": "pert-gym.real-dataset-obs-curation/v2",
        "task_id": TASK_ID,
        "real_dataset_id": REAL_DATASET_ID,
        "status": "PASS",
        "mode": mode,
        "helper_sha256": helper_sha256,
        "source_manifest_sha256": sha256_file(SOURCE_MANIFEST_PATH),
        "obs_contract": {
            "contract_id": OBS_CONTRACT["contract_id"],
            "sha256": OBS_CONTRACT_SHA256,
            "canonical_field_count": len(CANONICAL_OBS_FIELDS),
            "canonical_fields": list(CANONICAL_OBS_FIELDS),
        },
        "predecessor_receipt_adjudication": {
            "receipt_canonical_sha256": predecessor_evidence[
                "receipt_canonical_sha256"
            ],
            **predecessor_evidence["adjudication"],
        },
        "frozen_inputs": frozen["inputs"],
        "source_denominator": {
            "biological_datasets": 1,
            "logical_families": 10,
            "physical_members": 10,
            "observations": 2_212_441,
        },
        "members": before_or_after,
        "predecessor_collections": collections_after,
        "published_collections": collection_publication,
        "writes": {
            "obs_revisions": len(writes),
            "var_revisions": 0,
            "x_revisions": 0,
            "collection_writes": collection_writes,
            "deletions": 0,
            "artifacts": writes,
            "recovered_unreceipted_artifacts": recovered_writes,
            "canonical_key_remaps": key_remaps,
        },
        "registry_counts": {"before": counts_before, "after": counts_after},
        "post_write_readback": post_write_readback,
        "gcs_decommission": decommission,
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
    attestation = remote_attestation(receipt, pointer)
    emit_product("checkpointing", len(MEMBERS))
    print("CELLARITY_CURATION_RECEIPT=" + canonical(pointer), flush=True)
    print("CELLARITY_CURATION_ATTESTATION=" + canonical(attestation), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
